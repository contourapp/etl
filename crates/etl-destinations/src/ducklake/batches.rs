//! Table batches are the atomic per-table write units used by DuckLake writes.
//! Copy, mutation, and truncate inputs are normalized into deterministic
//! batches so each attempt can replay the same SQL and replay bookkeeping.
//! Copy batches persist ids in the applied-marker table, while streaming
//! mutation and truncate batches advance a per-table progress watermark.
//! Bounded batch sizes preserve table-local ordering without letting one
//! transaction grow unbounded.

#[cfg(feature = "test-utils")]
use std::collections::HashMap;
#[cfg(feature = "test-utils")]
use std::sync::LazyLock;
use std::{
    error, fmt,
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
    types::{
        Cell, ColumnSchema, EventSequenceKey, OldTableRow, PartialTableRow, ReplicatedTableSchema,
        TableRow, UpdatedTableRow,
    },
};
use metrics::{counter, histogram};
#[cfg(feature = "test-utils")]
use parking_lot::Mutex;
use pg_escape::quote_literal;
use rand::Rng;
use tokio::{sync::Semaphore, time::Instant};
use tokio_postgres::types::PgLsn;
use tracing::{debug, trace, warn};

use crate::{
    ducklake::{
        DuckLakeTableName, LAKE_CATALOG,
        arrow_staging::{
            CastKind, PreparedRows, StagingColumnSpec, build_staging_specs,
            build_staging_specs_with_cdc, prepare_rows, prepare_rows_with_cdc,
        },
        client::{
            DuckLakeBlockingOperationContext, DuckLakeConnectionManager, format_query_error_detail,
            is_ducklake_shutdown_requested_error, run_duckdb_blocking,
            run_duckdb_blocking_with_context,
        },
        core::is_create_table_conflict,
        encoding::{cell_to_sql_literal_ref, table_row_to_sql_literal_ref},
        metrics::{
            BATCH_KIND_LABEL, DELETE_ORIGIN_LABEL, ETL_DUCKLAKE_BATCH_COMMIT_DURATION_SECONDS,
            ETL_DUCKLAKE_BATCH_PREPARED_MUTATIONS, ETL_DUCKLAKE_DELETE_PREDICATES,
            ETL_DUCKLAKE_FAILED_BATCHES_TOTAL, ETL_DUCKLAKE_REPLAYED_BATCHES_TOTAL,
            ETL_DUCKLAKE_RETRIES_TOTAL, ETL_DUCKLAKE_UPSERT_ROWS, PREPARED_ROWS_KIND_LABEL,
            RETRY_SCOPE_LABEL, SUB_BATCH_KIND_LABEL,
        },
        merge_on_read::{
            EFFECTIVE_AT_LOCAL_COLUMN, MergeOnReadScope, build_tombstone_image, expand_key_row,
            is_partition_move, version_u128,
        },
        sql::{qualified_lake_table_name, quote_identifier},
    },
    retry::{RetryAttempt, RetryDecision, RetryPolicy, retry_with_backoff},
};

/// Maximum number of ordered CDC mutations grouped into one atomic DuckLake
/// transaction.
///
/// Each batch pays at least one target-table scan for its MERGE and delete
/// statements (random-UUID keys defeat all file pruning), so the cap is set
/// high enough that a whole pipeline event delivery applies as one scan.
/// Transaction-lifetime conflicts are no longer a concern: maintenance holds
/// the write pause and watermark writes are append-only.
const CDC_MUTATION_BATCH_SIZE: usize = 65_536;
/// Value recorded under the prepared-rows-kind metric label; every staging
/// payload is now an Arrow record batch.
const PREPARED_ROWS_KIND: &str = "arrow";
/// ETL-managed marker table storing per-table applied copy batches.
const APPLIED_BATCHES_TABLE: &str = "__etl_applied_table_batches";
/// Inline small marker-table writes in the DuckLake metadata catalog instead of
/// creating Parquet files for this metadata-like table.
const APPLIED_BATCHES_TABLE_DATA_INLINING_ROW_LIMIT: usize = 256;

/// Formats an optional LSN without using debug output.
fn format_optional_lsn(lsn: Option<PgLsn>) -> String {
    lsn.map_or_else(|| "none".to_owned(), |lsn| lsn.to_string())
}

/// Formats an optional sequence key without using debug output.
fn format_optional_sequence_key(sequence_key: Option<EventSequenceKey>) -> String {
    sequence_key.map_or_else(|| "none".to_owned(), |sequence_key| sequence_key.to_string())
}

/// Returns whether one DuckDB error is the standard interrupted query error.
fn is_duckdb_interrupt_error(error: &duckdb::Error) -> bool {
    error.to_string().contains("INTERRUPT Error: Interrupted")
}

/// Sanitized DuckDB query failure for statements that may contain row values.
#[derive(Debug)]
struct DuckDbSensitiveQueryError;

impl fmt::Display for DuckDbSensitiveQueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DuckDB query failed; error message omitted because it may contain row values")
    }
}

impl error::Error for DuckDbSensitiveQueryError {}

/// Formats query context for a delete mutation without row values.
fn format_delete_mutation_error_detail(
    target_table: &str,
    key_row_count: usize,
    stage: &'static str,
) -> String {
    format!(
        "sql: DELETE FROM {target_table} USING [redacted staged keys]; key_row_count: \
         {key_row_count}; stage: {stage}"
    )
}

/// Formats query context for an update mutation without row values.
fn format_update_mutation_error_detail(
    target_table: &str,
    assignment_count: usize,
    has_predicate: bool,
) -> String {
    format!(
        "sql: UPDATE {target_table} SET [redacted assignments] WHERE [redacted predicate]; \
         assignment_count: {assignment_count}; has_predicate: {has_predicate}"
    )
}

/// ETL-managed per-table streaming replay progress for steady-state CDC
/// retries.
const STREAMING_PROGRESS_TABLE: &str = "__etl_streaming_progress";
/// Inline small progress-table writes in the DuckLake metadata catalog instead
/// of materializing files for this metadata-like table.
const STREAMING_PROGRESS_TABLE_DATA_INLINING_ROW_LIMIT: usize = 256;
/// Maximum number of times a failed write attempt is retried before giving up.
const MAX_COMMIT_RETRIES: u32 = 15;
/// Initial backoff duration before the first retry.
const INITIAL_RETRY_DELAY_MS: u64 = 50;
/// Upper bound on backoff duration.
const MAX_RETRY_DELAY_MS: u64 = 10_000;
/// Minimum retry delay for transient delete-file visibility failures.
const TRANSIENT_DELETE_FILE_RETRY_DELAY_MS: u64 = 5_000;

/// Decides whether DuckLake-owned retry loops should retry one failure.
fn ducklake_retry_decision(error: &etl::error::EtlError) -> RetryDecision {
    if is_ducklake_shutdown_requested_error(error) {
        RetryDecision::Stop
    } else {
        RetryDecision::Retry
    }
}

/// Event-level table mutations that must be applied in order.
pub(super) enum TableMutation {
    Insert(TableRow),
    Delete(OldTableRow),
    Update { delete_row: OldTableRow, new_row: UpdatedTableRow },
    Replace(TableRow),
}

/// Prepared table mutations ready for execution and retries.
enum PreparedTableMutation {
    Upsert(PreparedRows),
    /// Staged delete: key rows load into a key-only temp table, then one
    /// join-driven DELETE removes every staged key in a single target scan.
    Delete {
        /// Staging contract for the replica-identity columns, in replicated
        /// table-column order.
        key_specs: Vec<StagingColumnSpec>,
        /// Prepared identity-key rows for every deleted row.
        key_rows: PreparedRows,
        origin: &'static str,
    },
    Update {
        assignments: Vec<String>,
        predicate: String,
    },
    /// Deduped INSERT: staging ROW_NUMBER() dedup then plain INSERT (no target hash-join).
    DedupedUpsert {
        rows: PreparedRows,
        identity_columns: Vec<String>,
    },
    /// Merge-on-read append: plain INSERT of CDC-annotated rows (carrying
    /// `_etl_version` / `_etl_deleted`) with no dedup and no target scan.
    ///
    /// Unlike [`PreparedTableMutation::DedupedUpsert`], appends must *not*
    /// collapse by identity: a partition move appends both a tombstone (old
    /// partition) and a live image (new partition) for the same `id` at the
    /// same version, and both rows must survive.
    Append {
        rows: PreparedRows,
    },
    /// MERGE INTO: hash-join against target for Replace/Update mutations.
    Merge {
        rows: PreparedRows,
        identity_columns: Vec<String>,
        all_columns: Vec<String>,
    },
}

/// Borrowed row shape used to build delete predicates.
enum DeletePredicateRowRef<'a> {
    Full(&'a TableRow),
    Key(&'a TableRow),
}

impl<'a> From<&'a TableRow> for DeletePredicateRowRef<'a> {
    fn from(value: &'a TableRow) -> Self {
        Self::Full(value)
    }
}

impl<'a> From<&'a OldTableRow> for DeletePredicateRowRef<'a> {
    fn from(value: &'a OldTableRow) -> Self {
        match value {
            OldTableRow::Full(row) => Self::Full(row),
            OldTableRow::Key(row) => Self::Key(row),
        }
    }
}

/// Event-level table mutation annotated with source LSNs for idempotent replay.
pub(super) struct TrackedTableMutation {
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    tx_ordinal: u64,
    mutation: TableMutation,
}

impl TrackedTableMutation {
    /// Creates one tracked mutation preserved for retry-safe replay.
    pub(super) fn new(
        start_lsn: PgLsn,
        commit_lsn: PgLsn,
        tx_ordinal: u64,
        mutation: TableMutation,
    ) -> Self {
        Self { start_lsn, commit_lsn, tx_ordinal, mutation }
    }

    /// Returns the stable event sequence key for this mutation.
    fn sequence_key(&self) -> EventSequenceKey {
        EventSequenceKey::new(self.commit_lsn, self.tx_ordinal)
    }
}

/// Truncate event metadata preserved for idempotent replay.
#[derive(Clone, Copy)]
pub(super) struct TrackedTruncateEvent {
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    tx_ordinal: u64,
    options: i8,
}

impl TrackedTruncateEvent {
    /// Creates one tracked truncate event preserved for retry-safe replay.
    pub(super) fn new(start_lsn: PgLsn, commit_lsn: PgLsn, tx_ordinal: u64, options: i8) -> Self {
        Self { start_lsn, commit_lsn, tx_ordinal, options }
    }

    /// Returns the stable event sequence key for this truncate.
    fn sequence_key(&self) -> EventSequenceKey {
        EventSequenceKey::new(self.commit_lsn, self.tx_ordinal)
    }
}

/// Stable hash used to derive per-table batch identifiers.
struct BatchIdHasher(u64);

impl BatchIdHasher {
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;

    fn new() -> Self {
        Self(Self::OFFSET_BASIS)
    }
}

impl Default for BatchIdHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Hasher for BatchIdHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(Self::PRIME);
        }
    }
}

/// Atomic DuckLake batch kinds used by replay bookkeeping.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DuckLakeTableBatchKind {
    Copy,
    Mutation,
    Truncate,
}

impl DuckLakeTableBatchKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Copy => "copy",
            Self::Mutation => "mutation",
            Self::Truncate => "truncate",
        }
    }
}

/// Deterministic identity for one table batch.
struct DuckLakeBatchIdentity {
    batch_id: String,
    first_start_lsn: Option<PgLsn>,
    last_commit_lsn: Option<PgLsn>,
}

/// Prepared per-table work executed atomically in one DuckLake transaction.
enum PreparedDuckLakeTableBatchAction {
    Mutation(Vec<PreparedTableMutation>),
    Truncate,
}

/// Prepared atomic DuckLake table batch with replay metadata.
pub(super) struct PreparedDuckLakeTableBatch {
    table_name: DuckLakeTableName,
    batch_id: String,
    batch_kind: DuckLakeTableBatchKind,
    first_start_lsn: Option<PgLsn>,
    last_commit_lsn: Option<PgLsn>,
    first_sequence_key: Option<EventSequenceKey>,
    last_sequence_key: Option<EventSequenceKey>,
    staging_specs: Vec<StagingColumnSpec>,
    action: PreparedDuckLakeTableBatchAction,
}

impl PreparedDuckLakeTableBatch {
    /// Returns the destination table this batch targets.
    pub(super) fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Returns whether this batch uses the streaming progress replay path.
    fn uses_streaming_progress(&self) -> bool {
        matches!(
            self.batch_kind,
            DuckLakeTableBatchKind::Mutation | DuckLakeTableBatchKind::Truncate
        )
    }
}

/// One table-local streaming replay watermark.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TableStreamingProgress {
    last_sequence_key: EventSequenceKey,
}

/// Ensures the ETL-managed replay marker table exists.
pub(super) async fn ensure_applied_batches_table_exists(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    table_creation_slots: Arc<Semaphore>,
    applied_batches_table_created: Arc<AtomicBool>,
) -> EtlResult<()> {
    if applied_batches_table_created.load(Ordering::Relaxed) {
        return Ok(());
    }

    let _table_creation_permit = table_creation_slots.acquire_owned().await.map_err(|_| {
        etl_error!(ErrorKind::InvalidState, "DuckLake table creation semaphore closed")
    })?;

    if applied_batches_table_created.load(Ordering::Relaxed) {
        return Ok(());
    }

    let ddl = format!(
        r#"CREATE TABLE IF NOT EXISTS {LAKE_CATALOG}."{APPLIED_BATCHES_TABLE}" (
             table_name VARCHAR NOT NULL,
             batch_id VARCHAR NOT NULL,
             batch_kind VARCHAR NOT NULL,
             first_start_lsn UBIGINT,
             last_commit_lsn UBIGINT,
             applied_at TIMESTAMPTZ NOT NULL
             );"#
    );
    let created = Arc::clone(&applied_batches_table_created);
    let table_name = APPLIED_BATCHES_TABLE.to_owned();

    run_duckdb_blocking(pool, blocking_slots, move |conn| -> EtlResult<()> {
        match conn.execute_batch(&ddl) {
            Ok(()) => {}
            Err(error) if is_create_table_conflict(&error, &table_name) => {}
            Err(error) => {
                return Err(etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake CREATE TABLE failed",
                    format_query_error_detail(&ddl),
                    source: error
                ));
            }
        }

        let set_option_sql = format!(
            "CALL {LAKE_CATALOG}.set_option('data_inlining_row_limit', {}, table_name => {});",
            APPLIED_BATCHES_TABLE_DATA_INLINING_ROW_LIMIT,
            quote_literal(APPLIED_BATCHES_TABLE),
        );
        conn.execute_batch(&set_option_sql).map_err(|err| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake set_option failed",
                format_query_error_detail(&set_option_sql),
                source: err
            )
        })?;

        created.store(true, Ordering::Relaxed);
        Ok(())
    })
    .await
}

/// Ensures the ETL-managed streaming progress table exists.
pub(super) async fn ensure_streaming_progress_table_exists(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    table_creation_slots: Arc<Semaphore>,
    streaming_progress_table_created: Arc<AtomicBool>,
) -> EtlResult<()> {
    if streaming_progress_table_created.load(Ordering::Relaxed) {
        return Ok(());
    }

    let _table_creation_permit = table_creation_slots.acquire_owned().await.map_err(|_| {
        etl_error!(ErrorKind::InvalidState, "DuckLake table creation semaphore closed")
    })?;

    if streaming_progress_table_created.load(Ordering::Relaxed) {
        return Ok(());
    }

    let ddl = format!(
        r#"CREATE TABLE IF NOT EXISTS {LAKE_CATALOG}."{STREAMING_PROGRESS_TABLE}" (
             table_name VARCHAR NOT NULL,
             last_commit_lsn UBIGINT NOT NULL,
             last_tx_ordinal UBIGINT NOT NULL,
             updated_at TIMESTAMPTZ NOT NULL
             );"#
    );
    let created = Arc::clone(&streaming_progress_table_created);
    let table_name = STREAMING_PROGRESS_TABLE.to_owned();

    run_duckdb_blocking(pool, blocking_slots, move |conn| -> EtlResult<()> {
        match conn.execute_batch(&ddl) {
            Ok(()) => {}
            Err(err) if is_create_table_conflict(&err, &table_name) => {}
            Err(err) => {
                return Err(etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake CREATE TABLE failed",
                    format_query_error_detail(&ddl),
                    source: err
                ));
            }
        }

        let set_option_sql = format!(
            "CALL {LAKE_CATALOG}.set_option('data_inlining_row_limit', {}, table_name => {});",
            STREAMING_PROGRESS_TABLE_DATA_INLINING_ROW_LIMIT,
            quote_literal(STREAMING_PROGRESS_TABLE),
        );
        conn.execute_batch(&set_option_sql).map_err(|error| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake set_option failed",
                format_query_error_detail(&set_option_sql),
                source: error
            )
        })?;

        created.store(true, Ordering::Relaxed);
        Ok(())
    })
    .await
}

/// Applies all prepared atomic batches for one table, reusing one DuckDB
/// connection per attempt and skipping already committed segments.
pub(super) async fn apply_table_batches_with_retry(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    batches: Vec<PreparedDuckLakeTableBatch>,
) -> EtlResult<()> {
    if batches.is_empty() {
        return Ok(());
    }

    let batch_count = batches.len();
    let batches = Arc::new(batches);
    let table_name = batches[0].table_name.clone();

    retry_with_backoff(
        RetryPolicy {
            max_retries: MAX_COMMIT_RETRIES,
            initial_delay: Duration::from_millis(INITIAL_RETRY_DELAY_MS),
            max_delay: Duration::from_millis(MAX_RETRY_DELAY_MS),
        },
        ducklake_retry_decision,
        jitter_ducklake_retry_delay,
        |attempt: RetryAttempt<'_, etl::error::EtlError>| {
            counter!(
                ETL_DUCKLAKE_RETRIES_TOTAL,
                BATCH_KIND_LABEL => DuckLakeTableBatchKind::Mutation.as_str(),
                RETRY_SCOPE_LABEL => "table_sequence",
            )
            .increment(1);
            warn!(
                attempt = attempt.retry_index,
                max = attempt.max_retries,
                table = %table_name,
                batch_count,
                error = %attempt.error,
                "ducklake table batch sequence failed, retrying"
            );
        },
        move || {
            let attempt_batches = Arc::clone(&batches);
            let pool = Arc::clone(&pool);
            let blocking_slots = Arc::clone(&blocking_slots);
            async move {
                run_duckdb_blocking_with_context(pool, blocking_slots, move |conn, context| {
                    apply_table_batches(conn, attempt_batches.as_ref(), context)?;
                    Ok(())
                })
                .await
            }
        },
    )
    .await
    .map_err(|failure| {
        if is_ducklake_shutdown_requested_error(&failure.last_error) {
            return failure.last_error;
        }

        counter!(
            ETL_DUCKLAKE_FAILED_BATCHES_TOTAL,
            BATCH_KIND_LABEL => DuckLakeTableBatchKind::Mutation.as_str(),
            RETRY_SCOPE_LABEL => "table_sequence",
        )
        .increment(1);
        etl_error!(
            ErrorKind::DestinationAtomicBatchRetryable,
            "DuckLake atomic table batch sequence failed after retries",
            format!("table={table_name}, batch_count={batch_count}"),
            source: failure.last_error
        )
    })
}

/// Applies one atomic per-table batch and retries on failure.
pub(super) async fn apply_table_batch_with_retry(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    batch: PreparedDuckLakeTableBatch,
) -> EtlResult<()> {
    let table_name = batch.table_name.clone();
    let batch_id = batch.batch_id.clone();
    let batch_kind = batch.batch_kind;
    let batch = Arc::new(batch);

    retry_with_backoff(
        RetryPolicy {
            max_retries: MAX_COMMIT_RETRIES,
            initial_delay: Duration::from_millis(INITIAL_RETRY_DELAY_MS),
            max_delay: Duration::from_millis(
                MAX_RETRY_DELAY_MS.max(TRANSIENT_DELETE_FILE_RETRY_DELAY_MS),
            ),
        },
        ducklake_retry_decision,
        jitter_ducklake_retry_delay,
        |attempt: RetryAttempt<'_, etl::error::EtlError>| {
            counter!(
                ETL_DUCKLAKE_RETRIES_TOTAL,
                BATCH_KIND_LABEL => batch_kind.as_str(),
                RETRY_SCOPE_LABEL => "single_batch",
            )
            .increment(1);
            warn!(
                attempt = attempt.retry_index,
                max = attempt.max_retries,
                table = %table_name,
                batch_id = %batch_id,
                error = %attempt.error,
                "ducklake table mutation attempt failed, retrying"
            );
        },
        move || {
            let attempt_batch = Arc::clone(&batch);
            let pool = Arc::clone(&pool);
            let blocking_slots = Arc::clone(&blocking_slots);
            async move {
                run_duckdb_blocking_with_context(pool, blocking_slots, move |conn, context| {
                    if batch_kind == DuckLakeTableBatchKind::Copy {
                        if applied_batch_marker_exists(conn, attempt_batch.as_ref())? {
                            record_replayed_batch_skip(attempt_batch.as_ref());
                            return Ok(());
                        }

                        apply_table_batch(conn, attempt_batch.as_ref(), context)?;
                        return Ok(());
                    }

                    apply_table_batches(
                        conn,
                        std::slice::from_ref(attempt_batch.as_ref()),
                        context,
                    )?;
                    Ok(())
                })
                .await
            }
        },
    )
    .await
    .map_err(|failure| {
        if is_ducklake_shutdown_requested_error(&failure.last_error) {
            return failure.last_error;
        }

        counter!(
            ETL_DUCKLAKE_FAILED_BATCHES_TOTAL,
            BATCH_KIND_LABEL => batch_kind.as_str(),
            RETRY_SCOPE_LABEL => "single_batch",
        )
        .increment(1);
        etl_error!(
            ErrorKind::DestinationAtomicBatchRetryable,
            "DuckLake atomic table batch failed after retries",
            format!(
                "table={table_name}, batch_id={batch_id}, batch_kind={}",
                batch_kind.as_str()
            ),
            source: failure.last_error
        )
    })
}

/// Prepares ordered atomic batches for one table's CDC mutations.
///
/// Mutations stay in source order and are split only at the batch-size cap so
/// mixed CDC streams can commit larger insert groups without breaking atomic
/// ordering.
pub(super) fn prepare_mutation_table_batches(
    replicated_table_schema: &ReplicatedTableSchema,
    table_name: DuckLakeTableName,
    tracked_mutations: Vec<TrackedTableMutation>,
    scope: &MergeOnReadScope,
) -> EtlResult<Vec<PreparedDuckLakeTableBatch>> {
    let mut prepared_batches = Vec::new();
    let mut pending_mutations = Vec::new();

    for tracked_mutation in tracked_mutations {
        pending_mutations.push(tracked_mutation);
        if pending_mutations.len() >= CDC_MUTATION_BATCH_SIZE {
            push_prepared_mutation_batch(
                &mut prepared_batches,
                replicated_table_schema,
                &table_name,
                std::mem::take(&mut pending_mutations),
                scope,
            )?;
        }
    }

    push_prepared_mutation_batch(
        &mut prepared_batches,
        replicated_table_schema,
        &table_name,
        pending_mutations,
        scope,
    )?;

    Ok(prepared_batches)
}

/// Prepares one retry-safe atomic batch for a table-copy row chunk.
pub(super) fn prepare_copy_table_batch(
    replicated_table_schema: &ReplicatedTableSchema,
    table_name: DuckLakeTableName,
    table_rows: Vec<TableRow>,
) -> EtlResult<PreparedDuckLakeTableBatch> {
    let identity = build_copy_batch_identity(&table_name, replicated_table_schema, &table_rows)?;
    let column_schemas: Vec<_> = replicated_table_schema.column_schemas().cloned().collect();
    let identity_columns: Vec<String> = replicated_table_schema
        .identity_column_schemas()
        .map(|c| c.name.clone())
        .collect();

    let staging_specs = build_staging_specs(&column_schemas)?;
    let mutation = if identity_columns.is_empty() {
        PreparedTableMutation::Upsert(prepare_rows(table_rows, &column_schemas)?)
    } else {
        // COPY is insert-only; deduped INSERT avoids target hash-join.
        PreparedTableMutation::DedupedUpsert {
            rows: prepare_rows(table_rows, &column_schemas)?,
            identity_columns,
        }
    };

    Ok(PreparedDuckLakeTableBatch {
        table_name,
        batch_id: identity.batch_id,
        batch_kind: DuckLakeTableBatchKind::Copy,
        first_start_lsn: identity.first_start_lsn,
        last_commit_lsn: identity.last_commit_lsn,
        first_sequence_key: None,
        last_sequence_key: None,
        staging_specs,
        action: PreparedDuckLakeTableBatchAction::Mutation(vec![mutation]),
    })
}

/// Prepares the ordered atomic batch for one table's truncate events.
pub(super) fn prepare_truncate_table_batch(
    table_name: DuckLakeTableName,
    tracked_truncates: Vec<TrackedTruncateEvent>,
) -> PreparedDuckLakeTableBatch {
    let identity = build_truncate_batch_identity(&table_name, &tracked_truncates);
    PreparedDuckLakeTableBatch {
        table_name,
        batch_id: identity.batch_id,
        batch_kind: DuckLakeTableBatchKind::Truncate,
        first_start_lsn: identity.first_start_lsn,
        last_commit_lsn: identity.last_commit_lsn,
        first_sequence_key: tracked_truncates.first().map(TrackedTruncateEvent::sequence_key),
        last_sequence_key: tracked_truncates.last().map(TrackedTruncateEvent::sequence_key),
        staging_specs: Vec::new(),
        action: PreparedDuckLakeTableBatchAction::Truncate,
    }
}

/// Deletes persisted markers for one table and batch kind.
pub(super) fn clear_applied_batch_markers_for_kind(
    conn: &duckdb::Connection,
    table_name: &str,
    batch_kind: DuckLakeTableBatchKind,
) -> EtlResult<()> {
    let sql = format!(
        r#"DELETE FROM {LAKE_CATALOG}."{APPLIED_BATCHES_TABLE}"
         WHERE table_name = {} AND batch_kind = {};"#,
        quote_literal(table_name),
        quote_literal(batch_kind.as_str())
    );
    conn.execute_batch(&sql).map_err(|err| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake batch marker delete failed",
            format_query_error_detail(&sql),
            source: err
        )
    })?;
    Ok(())
}

/// Deletes the persisted streaming replay watermark for one table.
pub(super) fn clear_table_streaming_progress(
    conn: &duckdb::Connection,
    table_name: &str,
) -> EtlResult<()> {
    let sql = format!(
        r#"DELETE FROM {LAKE_CATALOG}."{STREAMING_PROGRESS_TABLE}"
         WHERE table_name = {};"#,
        quote_literal(table_name),
    );
    conn.execute_batch(&sql).map_err(|err| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress delete failed",
            format_query_error_detail(&sql),
            source: err
        )
    })?;
    Ok(())
}

/// Deletes superseded streaming-progress watermark rows, keeping the latest
/// row per table.
///
/// Progress writes are append-only to keep concurrent batch commits
/// conflict-free, so superseded rows accumulate until this prune runs. Only
/// call while pipeline writes are paused: the delete removes inlined data
/// that concurrently committing batches would otherwise conflict with.
pub(super) fn prune_streaming_progress_rows(conn: &duckdb::Connection) -> EtlResult<usize> {
    let sql = format!(
        r#"DELETE FROM {LAKE_CATALOG}."{STREAMING_PROGRESS_TABLE}" AS p
         WHERE EXISTS (
             SELECT 1 FROM {LAKE_CATALOG}."{STREAMING_PROGRESS_TABLE}" newer
             WHERE newer.table_name = p.table_name
               AND (newer.last_commit_lsn > p.last_commit_lsn
                    OR (newer.last_commit_lsn = p.last_commit_lsn
                        AND newer.last_tx_ordinal > p.last_tx_ordinal))
         );"#
    );
    conn.execute(&sql, []).map_err(|err| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress prune failed",
            format_query_error_detail(&sql),
            source: err
        )
    })
}

/// Applies jitter to one DuckLake retry delay.
fn jitter_ducklake_retry_delay(base_delay: Duration) -> Duration {
    let jitter_ratio = rand::rng().random_range(0.5..=1.5_f64);
    base_delay.mul_f64(jitter_ratio)
}

/// Replay decision for one streaming batch after reading the table watermark.
enum StreamingReplayDecision {
    Skip,
    Apply,
}

/// Records that one replay-safe batch was skipped because it was already
/// committed.
fn record_replayed_batch_skip(batch: &PreparedDuckLakeTableBatch) {
    counter!(
        ETL_DUCKLAKE_REPLAYED_BATCHES_TOTAL,
        BATCH_KIND_LABEL => batch.batch_kind.as_str(),
    )
    .increment(1);
    debug!(
        table = %batch.table_name,
        batch_id = %batch.batch_id,
        batch_kind = batch.batch_kind.as_str(),
        "ducklake table batch already committed, skipping replay"
    );
}

/// Reads the steady-state streaming replay watermark for one table.
fn read_table_streaming_progress(
    conn: &duckdb::Connection,
    table_name: &str,
) -> EtlResult<Option<TableStreamingProgress>> {
    let sql = format!(
        r#"SELECT last_commit_lsn, last_tx_ordinal
         FROM {LAKE_CATALOG}."{STREAMING_PROGRESS_TABLE}"
         WHERE table_name = {}
         ORDER BY last_commit_lsn DESC, last_tx_ordinal DESC LIMIT 1;"#,
        quote_literal(table_name),
    );
    let mut statement = conn.prepare(&sql).map_err(|err| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress query prepare failed",
            format_query_error_detail(&sql),
            source: err
        )
    })?;
    let mut rows = statement.query([]).map_err(|err| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress query failed",
            format_query_error_detail(&sql),
            source: err
        )
    })?;

    let Some(row) = rows.next().map_err(|err| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress row fetch failed",
            format_query_error_detail(&sql),
            source: err
        )
    })?
    else {
        return Ok(None);
    };

    let last_commit_lsn: u64 = row.get(0).map_err(|err| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress commit lsn read failed",
            format_query_error_detail(&sql),
            source: err
        )
    })?;
    let last_tx_ordinal: u64 = row.get(1).map_err(|err| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress tx ordinal read failed",
            format_query_error_detail(&sql),
            source: err
        )
    })?;

    Ok(Some(TableStreamingProgress {
        last_sequence_key: EventSequenceKey::new(PgLsn::from(last_commit_lsn), last_tx_ordinal),
    }))
}

/// Reads the last applied streaming sequence key for one table.
pub(super) fn read_table_streaming_progress_sequence_key(
    conn: &duckdb::Connection,
    table_name: &str,
) -> EtlResult<Option<EventSequenceKey>> {
    Ok(read_table_streaming_progress(conn, table_name)?.map(|progress| progress.last_sequence_key))
}

/// Drops already-applied tracked mutations using the persisted sequence key.
pub(super) fn retain_mutations_after_sequence_key(
    tracked_mutations: Vec<TrackedTableMutation>,
    last_sequence_key: Option<EventSequenceKey>,
) -> Vec<TrackedTableMutation> {
    match last_sequence_key {
        Some(last_sequence_key) => tracked_mutations
            .into_iter()
            .filter(|tracked_mutation| {
                compare_sequence_keys(tracked_mutation.sequence_key(), last_sequence_key)
                    == std::cmp::Ordering::Greater
            })
            .collect(),
        None => tracked_mutations,
    }
}

/// Drops already-applied tracked truncates using the persisted sequence key.
pub(super) fn retain_truncates_after_sequence_key(
    tracked_truncates: Vec<TrackedTruncateEvent>,
    last_sequence_key: Option<EventSequenceKey>,
) -> Vec<TrackedTruncateEvent> {
    match last_sequence_key {
        Some(last_sequence_key) => tracked_truncates
            .into_iter()
            .filter(|tracked_truncate| {
                compare_sequence_keys(tracked_truncate.sequence_key(), last_sequence_key)
                    == std::cmp::Ordering::Greater
            })
            .collect(),
        None => tracked_truncates,
    }
}

/// Decides whether a streaming batch must be replayed or skipped.
fn streaming_replay_decision(
    progress: TableStreamingProgress,
    batch: &PreparedDuckLakeTableBatch,
) -> EtlResult<StreamingReplayDecision> {
    let first_sequence_key = batch.first_sequence_key.ok_or_else(|| {
        etl_error!(
            ErrorKind::InvalidState,
            "DuckLake streaming batch is missing its first sequence key",
            format!("table={}, batch_kind={}", batch.table_name, batch.batch_kind.as_str())
        )
    })?;
    let last_sequence_key = batch.last_sequence_key.ok_or_else(|| {
        etl_error!(
            ErrorKind::InvalidState,
            "DuckLake streaming batch is missing its last sequence key",
            format!("table={}, batch_kind={}", batch.table_name, batch.batch_kind.as_str())
        )
    })?;

    if compare_sequence_keys(progress.last_sequence_key, first_sequence_key)
        != std::cmp::Ordering::Less
    {
        if compare_sequence_keys(progress.last_sequence_key, last_sequence_key)
            == std::cmp::Ordering::Less
        {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "DuckLake streaming progress landed inside an atomic batch",
                format!(
                    "table={}, progress={}, first={}, last={}",
                    batch.table_name,
                    progress.last_sequence_key,
                    first_sequence_key,
                    last_sequence_key
                )
            ));
        }

        return Ok(StreamingReplayDecision::Skip);
    }

    Ok(StreamingReplayDecision::Apply)
}

/// Compares two ETL event sequence keys using commit LSN then transaction
/// ordinal.
fn compare_sequence_keys(left: EventSequenceKey, right: EventSequenceKey) -> std::cmp::Ordering {
    (u64::from(left.commit_lsn), left.tx_ordinal)
        .cmp(&(u64::from(right.commit_lsn), right.tx_ordinal))
}

/// Applies all prepared atomic batches for one table on the same connection.
fn apply_table_batches(
    conn: &duckdb::Connection,
    batches: &[PreparedDuckLakeTableBatch],
    operation_context: &DuckLakeBlockingOperationContext,
) -> EtlResult<()> {
    if batches.is_empty() {
        return Ok(());
    }

    let mut streaming_progress = if batches[0].uses_streaming_progress() {
        read_table_streaming_progress(conn, batches[0].table_name())?
    } else {
        None
    };

    for batch in batches {
        if !batch.uses_streaming_progress() {
            // Copy batches keep the marker path because initial-copy retries
            // still depend on per-batch idempotency.
            if applied_batch_marker_exists(conn, batch)? {
                record_replayed_batch_skip(batch);
                continue;
            }

            apply_table_batch(conn, batch, operation_context).map_err(|error| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake atomic table batch failed",
                    format!(
                        "table={}, batch_id={}, batch_kind={}",
                        batch.table_name,
                        batch.batch_id,
                        batch.batch_kind.as_str()
                    ),
                    source: error
                )
            })?;
            continue;
        }

        if let Some(progress) = streaming_progress {
            match streaming_replay_decision(progress, batch)? {
                StreamingReplayDecision::Skip => {
                    record_replayed_batch_skip(batch);
                    continue;
                }
                StreamingReplayDecision::Apply => {}
            }
        }

        apply_table_batch(conn, batch, operation_context).map_err(|error| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake atomic table batch failed",
                format!(
                    "table={}, batch_id={}, batch_kind={}",
                    batch.table_name,
                    batch.batch_id,
                    batch.batch_kind.as_str()
                ),
                source: error
            )
        })?;

        streaming_progress = batch
            .last_sequence_key
            .map(|last_sequence_key| TableStreamingProgress { last_sequence_key });
    }

    Ok(())
}

/// Builds one prepared atomic batch from an ordered slice of tracked mutations.
fn push_prepared_mutation_batch(
    prepared_batches: &mut Vec<PreparedDuckLakeTableBatch>,
    replicated_table_schema: &ReplicatedTableSchema,
    table_name: &str,
    tracked_mutations: Vec<TrackedTableMutation>,
    scope: &MergeOnReadScope,
) -> EtlResult<()> {
    if tracked_mutations.is_empty() {
        return Ok(());
    }

    let identity =
        build_mutation_batch_identity(table_name, replicated_table_schema, &tracked_mutations)?;
    let first_sequence_key = tracked_mutations.first().map(TrackedTableMutation::sequence_key);
    let last_sequence_key = tracked_mutations.last().map(TrackedTableMutation::sequence_key);
    let column_schemas: Vec<_> = replicated_table_schema.column_schemas().cloned().collect();

    let (staging_specs, action) = if scope.contains(table_name) {
        // Merge-on-read: every mutation becomes a CDC-annotated append. The
        // staging table (and therefore the batch's RecordBatches) carry the two
        // trailing `_etl_version` / `_etl_deleted` columns.
        let staging_specs = build_staging_specs_with_cdc(&column_schemas)?;
        let action = PreparedDuckLakeTableBatchAction::Mutation(prepare_append_mutations(
            replicated_table_schema,
            tracked_mutations,
            scope.is_partitioned(table_name),
            &staging_specs,
        )?);
        (staging_specs, action)
    } else {
        let mutations = tracked_mutations.into_iter().map(|tracked| tracked.mutation).collect();
        let action = PreparedDuckLakeTableBatchAction::Mutation(prepare_table_mutations(
            replicated_table_schema,
            mutations,
        )?);
        (build_staging_specs(&column_schemas)?, action)
    };

    prepared_batches.push(PreparedDuckLakeTableBatch {
        table_name: table_name.to_owned(),
        batch_id: identity.batch_id,
        batch_kind: DuckLakeTableBatchKind::Mutation,
        first_start_lsn: identity.first_start_lsn,
        last_commit_lsn: identity.last_commit_lsn,
        first_sequence_key,
        last_sequence_key,
        staging_specs,
        action,
    });

    Ok(())
}

/// Groups ordered row mutations into retryable DuckDB operations.
fn prepare_table_mutations(
    replicated_table_schema: &ReplicatedTableSchema,
    mutations: Vec<TableMutation>,
) -> EtlResult<Vec<PreparedTableMutation>> {
    let column_schemas: Vec<_> = replicated_table_schema.column_schemas().cloned().collect();

    let identity_column_schemas: Vec<_> =
        replicated_table_schema.identity_column_schemas().cloned().collect();

    let identity_columns: Vec<String> =
        identity_column_schemas.iter().map(|c| c.name.clone()).collect();

    let all_columns: Vec<String> = replicated_table_schema
        .column_schemas()
        .map(|c| c.name.clone())
        .collect();

    let has_identity = !identity_columns.is_empty();

    let mut prepared_mutations = Vec::new();
    let mut upsert_rows = Vec::new();
    let mut deduped_insert_rows = Vec::new();
    let mut merge_rows = Vec::new();
    let mut delete_key_rows: Vec<TableRow> = Vec::new();

    /// Flushes accumulated rows into their respective prepared mutations.
    macro_rules! flush_upserts {
        ($prepared:expr, $upsert:expr, $schemas:expr) => {
            if !$upsert.is_empty() {
                $prepared.push(PreparedTableMutation::Upsert(prepare_rows(
                    std::mem::take(&mut $upsert),
                    $schemas,
                )?));
            }
        };
    }
    macro_rules! flush_deduped_inserts {
        ($prepared:expr, $rows:expr, $schemas:expr, $id_cols:expr) => {
            if !$rows.is_empty() {
                $prepared.push(PreparedTableMutation::DedupedUpsert {
                    rows: prepare_rows(std::mem::take(&mut $rows), $schemas)?,
                    identity_columns: $id_cols.clone(),
                });
            }
        };
    }
    macro_rules! flush_merge_rows {
        ($prepared:expr, $rows:expr, $schemas:expr, $id_cols:expr, $all_cols:expr) => {
            if !$rows.is_empty() {
                $prepared.push(PreparedTableMutation::Merge {
                    rows: prepare_rows(std::mem::take(&mut $rows), $schemas)?,
                    identity_columns: $id_cols.clone(),
                    all_columns: $all_cols.clone(),
                });
            }
        };
    }
    macro_rules! flush_deletes {
        ($prepared:expr, $key_rows:expr, $key_schemas:expr) => {
            if !$key_rows.is_empty() {
                $prepared.push(PreparedTableMutation::Delete {
                    key_specs: build_staging_specs($key_schemas)?,
                    key_rows: prepare_rows(std::mem::take(&mut $key_rows), $key_schemas)?,
                    origin: "delete",
                });
            }
        };
    }

    for mutation in mutations {
        match mutation {
            TableMutation::Insert(row) => {
                flush_deletes!(prepared_mutations, delete_key_rows, &identity_column_schemas);
                flush_merge_rows!(
                    prepared_mutations,
                    merge_rows,
                    &column_schemas,
                    identity_columns,
                    all_columns
                );
                if has_identity {
                    deduped_insert_rows.push(row);
                } else {
                    upsert_rows.push(row);
                }
            }
            TableMutation::Delete(row) => {
                flush_upserts!(prepared_mutations, upsert_rows, &column_schemas);
                flush_deduped_inserts!(
                    prepared_mutations,
                    deduped_insert_rows,
                    &column_schemas,
                    identity_columns
                );
                flush_merge_rows!(
                    prepared_mutations,
                    merge_rows,
                    &column_schemas,
                    identity_columns,
                    all_columns
                );
                delete_key_rows.push(TableRow::new(replica_identity_key_cells(
                    replicated_table_schema,
                    &row,
                )?));
            }
            TableMutation::Update { delete_row, new_row } => {
                flush_upserts!(prepared_mutations, upsert_rows, &column_schemas);
                flush_deduped_inserts!(
                    prepared_mutations,
                    deduped_insert_rows,
                    &column_schemas,
                    identity_columns
                );
                match new_row {
                    // Key-preserving full-row updates batch into the same
                    // MERGE as Replace mutations, so a run of updates costs
                    // one target scan instead of one per row.
                    UpdatedTableRow::Full(upsert_row)
                        if has_identity
                            && update_preserves_identity(
                                replicated_table_schema,
                                &delete_row,
                                &upsert_row,
                            ) =>
                    {
                        flush_deletes!(
                            prepared_mutations,
                            delete_key_rows,
                            &identity_column_schemas
                        );
                        merge_rows.push(upsert_row);
                    }
                    UpdatedTableRow::Full(upsert_row) => {
                        flush_merge_rows!(
                            prepared_mutations,
                            merge_rows,
                            &column_schemas,
                            identity_columns,
                            all_columns
                        );
                        flush_deletes!(
                            prepared_mutations,
                            delete_key_rows,
                            &identity_column_schemas
                        );
                        prepared_mutations.push(PreparedTableMutation::Delete {
                            key_specs: build_staging_specs(&identity_column_schemas)?,
                            key_rows: prepare_rows(
                                vec![TableRow::new(replica_identity_key_cells(
                                    replicated_table_schema,
                                    &delete_row,
                                )?)],
                                &identity_column_schemas,
                            )?,
                            origin: "update",
                        });
                        prepared_mutations.push(PreparedTableMutation::Upsert(prepare_rows(
                            vec![upsert_row],
                            &column_schemas,
                        )?));
                    }
                    UpdatedTableRow::Partial(partial_row) => {
                        flush_merge_rows!(
                            prepared_mutations,
                            merge_rows,
                            &column_schemas,
                            identity_columns,
                            all_columns
                        );
                        flush_deletes!(
                            prepared_mutations,
                            delete_key_rows,
                            &identity_column_schemas
                        );
                        prepared_mutations.push(PreparedTableMutation::Update {
                            assignments: update_assignments_from_partial_row(
                                replicated_table_schema,
                                &partial_row,
                            )?,
                            predicate: delete_predicate_from_row(
                                replicated_table_schema,
                                &delete_row,
                            )?,
                        });
                    }
                }
            }
            TableMutation::Replace(row) => {
                flush_upserts!(prepared_mutations, upsert_rows, &column_schemas);
                flush_deduped_inserts!(
                    prepared_mutations,
                    deduped_insert_rows,
                    &column_schemas,
                    identity_columns
                );
                flush_deletes!(prepared_mutations, delete_key_rows, &identity_column_schemas);
                merge_rows.push(row);
            }
        }
    }

    flush_upserts!(prepared_mutations, upsert_rows, &column_schemas);
    flush_deduped_inserts!(
        prepared_mutations,
        deduped_insert_rows,
        &column_schemas,
        identity_columns
    );
    flush_merge_rows!(
        prepared_mutations,
        merge_rows,
        &column_schemas,
        identity_columns,
        all_columns
    );
    flush_deletes!(prepared_mutations, delete_key_rows, &identity_column_schemas);

    Ok(prepared_mutations)
}

/// Groups ordered row mutations into append-only DuckDB operations for
/// merge-on-read tables.
///
/// Every source mutation becomes one or more plain INSERTs of CDC-annotated
/// rows (carrying `_etl_version` / `_etl_deleted`); no statement references the
/// target for matching, so the per-mutation full-table scan disappears.
///
/// - Insert / Replace -> one append of the new image (`deleted=false`).
/// - Update (same partition) -> one append of the new image (`deleted=false`).
/// - Update (partition move) -> a tombstone append in the old partition
///   (`deleted=true`) plus the new image in the new partition (`deleted=false`),
///   both at the same version.
/// - Delete -> one tombstone append (`deleted=true`).
///
/// `staging_specs` must be the CDC-augmented specs whose trailing two columns
/// are `_etl_version` / `_etl_deleted`; the produced batches match them so the
/// shared staging table's DDL and INSERT column list line up.
fn prepare_append_mutations(
    replicated_table_schema: &ReplicatedTableSchema,
    tracked_mutations: Vec<TrackedTableMutation>,
    partitioned: bool,
    staging_specs: &[StagingColumnSpec],
) -> EtlResult<Vec<PreparedTableMutation>> {
    let column_schemas: Vec<_> = replicated_table_schema.column_schemas().cloned().collect();
    let eff_idx = column_schemas
        .iter()
        .position(|c| c.name == EFFECTIVE_AT_LOCAL_COLUMN);

    let mut prepared_mutations = Vec::new();

    // Emits one append (plain INSERT, no dedup) for one CDC image.
    macro_rules! push_append {
        ($rows:expr, $version:expr, $deleted:expr) => {
            prepared_mutations.push(PreparedTableMutation::Append {
                rows: prepare_rows_with_cdc($rows, staging_specs, $version, $deleted)?,
            });
        };
    }

    for tracked in tracked_mutations {
        let version = version_u128(tracked.commit_lsn, tracked.tx_ordinal);
        match tracked.mutation {
            TableMutation::Insert(row) | TableMutation::Replace(row) => {
                push_append!(vec![row], version, false);
            }
            TableMutation::Delete(delete_row) => {
                if partitioned {
                    let old_full = delete_row.into_full().ok_or_else(|| {
                        append_requires_full_old_row(replicated_table_schema, "delete")
                    })?;
                    push_append!(vec![build_tombstone_image(old_full)], version, true);
                } else {
                    let key_row = delete_row.into_key().ok_or_else(|| {
                        // A partitioned-table delete would carry a Full row; an
                        // unpartitioned-table delete carries the key row. A Full
                        // image here is unexpected for an unpartitioned table.
                        etl_error!(
                            ErrorKind::InvalidState,
                            "DuckLake merge-on-read delete on unpartitioned table missing key row",
                            format!("table '{}'", replicated_table_schema.name())
                        )
                    })?;
                    let tombstone = expand_key_row(key_row, replicated_table_schema);
                    push_append!(vec![tombstone], version, true);
                }
            }
            TableMutation::Update { delete_row, new_row } => {
                // Reconstruct the complete new image. Under REPLICA IDENTITY
                // FULL a Partial new row (unchanged-TOAST) is recoverable by
                // overlaying its changed columns onto the full old row.
                let new_full = reconstruct_full_new_row(
                    replicated_table_schema,
                    &delete_row,
                    new_row,
                    &column_schemas,
                )?;

                if partitioned {
                    let eff_idx = eff_idx.ok_or_else(|| {
                        etl_error!(
                            ErrorKind::InvalidState,
                            "DuckLake merge-on-read partitioned table missing effective_at_local",
                            format!("table '{}'", replicated_table_schema.name())
                        )
                    })?;
                    let old_full = delete_row.into_full().ok_or_else(|| {
                        // Task 19 adds the key-only backlog fallback.
                        append_requires_full_old_row(replicated_table_schema, "update")
                    })?;
                    // Borrow-based move check BEFORE moving old_full into the
                    // tombstone image.
                    let moved = is_partition_move(&old_full, &new_full, eff_idx);
                    if moved {
                        push_append!(vec![build_tombstone_image(old_full)], version, true);
                    }
                    push_append!(vec![new_full], version, false);
                } else {
                    push_append!(vec![new_full], version, false);
                }
            }
        }
    }

    Ok(prepared_mutations)
}

/// Reconstructs the complete new row image for a merge-on-read update.
///
/// A `Full` new row is used directly. A `Partial` new row (unchanged-TOAST under
/// REPLICA IDENTITY FULL) is rebuilt by overlaying its present columns onto the
/// full old row: missing replicated-column positions take the old row's value,
/// the rest take the partial's present values in order. If the old row is not a
/// full image, reconstruction is impossible and an error is returned (Task 19
/// adds the key-only / read-before-write fallback).
fn reconstruct_full_new_row(
    replicated_table_schema: &ReplicatedTableSchema,
    delete_row: &OldTableRow,
    new_row: UpdatedTableRow,
    column_schemas: &[ColumnSchema],
) -> EtlResult<TableRow> {
    match new_row {
        UpdatedTableRow::Full(row) => Ok(row),
        UpdatedTableRow::Partial(partial) => {
            let old_full = delete_row.as_full().ok_or_else(|| {
                append_requires_full_old_row(replicated_table_schema, "update")
            })?;
            overlay_partial_on_full(partial, old_full, column_schemas.len(), replicated_table_schema)
        }
    }
}

/// Overlays a partial new row's present columns onto a full old row, yielding a
/// complete new image in replicated table-column order.
fn overlay_partial_on_full(
    partial: PartialTableRow,
    old_full: &TableRow,
    total_columns: usize,
    replicated_table_schema: &ReplicatedTableSchema,
) -> EtlResult<TableRow> {
    if old_full.values().len() != total_columns {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "DuckLake merge-on-read old row shape does not match schema",
            format!(
                "expected {} values for table '{}', got {}",
                total_columns,
                replicated_table_schema.name(),
                old_full.values().len()
            )
        ));
    }
    let (present, missing_indexes) = partial.into_parts();
    let missing: std::collections::HashSet<usize> = missing_indexes.into_iter().collect();
    let mut present_iter = present.into_values().into_iter();
    let mut cells = Vec::with_capacity(total_columns);
    for index in 0..total_columns {
        if missing.contains(&index) {
            cells.push(old_full.values()[index].clone());
        } else {
            let cell = present_iter.next().ok_or_else(|| {
                etl_error!(
                    ErrorKind::InvalidState,
                    "DuckLake merge-on-read partial row missing a present value",
                    format!(
                        "table '{}' partial row ran out of present values at column {index}",
                        replicated_table_schema.name()
                    )
                )
            })?;
            cells.push(cell);
        }
    }
    Ok(TableRow::new(cells))
}

/// Builds the error returned when an append-only mutation needs the full old
/// row image but only a key-only image is available (pre-cutover backlog).
fn append_requires_full_old_row(
    replicated_table_schema: &ReplicatedTableSchema,
    origin: &str,
) -> etl::error::EtlError {
    etl_error!(
        ErrorKind::SourceReplicaIdentityError,
        "DuckLake merge-on-read append requires a full old row image",
        format!(
            "table '{}' {origin} carried a key-only old row; REPLICA IDENTITY FULL is required \
             (key-only backlog fallback is deferred to a later task)",
            replicated_table_schema.name()
        )
    )
}

/// Returns whether an update's old replica-identity values equal the new
/// row's, meaning the update can merge by identity instead of running a
/// per-row delete + insert.
///
/// Conservatively returns `false` on any shape mismatch or NULL identity
/// value so callers fall back to the predicate-based path.
fn update_preserves_identity(
    replicated_table_schema: &ReplicatedTableSchema,
    delete_row: &OldTableRow,
    new_row: &TableRow,
) -> bool {
    let replicated_column_schemas: Vec<_> = replicated_table_schema.column_schemas().collect();
    let identity_column_schemas: Vec<_> =
        replicated_table_schema.identity_column_schemas().collect();
    if identity_column_schemas.is_empty()
        || new_row.values().len() != replicated_column_schemas.len()
    {
        return false;
    }

    let identity_indices: Option<Vec<usize>> = identity_column_schemas
        .iter()
        .map(|identity_column| {
            replicated_column_schemas
                .iter()
                .position(|column| column.ordinal_position == identity_column.ordinal_position)
        })
        .collect();
    let Some(identity_indices) = identity_indices else {
        return false;
    };

    let new_values: Vec<&Cell> =
        identity_indices.iter().map(|&index| &new_row.values()[index]).collect();
    let old_values: Vec<&Cell> = match delete_row {
        OldTableRow::Full(row) => {
            if row.values().len() != replicated_column_schemas.len() {
                return false;
            }
            identity_indices.iter().map(|&index| &row.values()[index]).collect()
        }
        OldTableRow::Key(row) => {
            if row.values().len() != identity_indices.len() {
                return false;
            }
            row.values().iter().collect()
        }
    };

    if new_values.iter().any(|value| matches!(value, Cell::Null))
        || old_values.iter().any(|value| matches!(value, Cell::Null))
    {
        return false;
    }
    old_values == new_values
}

/// Builds a `WHERE` clause from the replica-identity values stored in `row`.
fn delete_predicate_from_row<'a>(
    replicated_table_schema: &'a ReplicatedTableSchema,
    row: impl Into<DeletePredicateRowRef<'a>>,
) -> EtlResult<String> {
    let pairs = replica_identity_key_literals(replicated_table_schema, row)?;
    Ok(pairs
        .into_iter()
        .map(|(quoted_column, literal)| {
            if literal == "NULL" {
                format!("{quoted_column} IS NULL")
            } else {
                format!("{quoted_column} = {literal}")
            }
        })
        .collect::<Vec<_>>()
        .join(" AND "))
}

/// Extracts the replica-identity cell values for one deleted row, in
/// replicated table-column order.
fn replica_identity_key_cells<'a>(
    replicated_table_schema: &'a ReplicatedTableSchema,
    row: impl Into<DeletePredicateRowRef<'a>>,
) -> EtlResult<Vec<Cell>> {
    let pairs = replica_identity_key_values(replicated_table_schema, row.into())?;
    Ok(pairs.into_iter().map(|(_, value)| value.clone()).collect())
}

/// Extracts `(quoted column, SQL literal)` pairs for the replica-identity
/// values stored in `row`.
fn replica_identity_key_literals<'a>(
    replicated_table_schema: &'a ReplicatedTableSchema,
    row: impl Into<DeletePredicateRowRef<'a>>,
) -> EtlResult<Vec<(String, String)>> {
    let pairs = replica_identity_key_values(replicated_table_schema, row.into())?;
    Ok(pairs
        .into_iter()
        .map(|(column_schema, value)| {
            (quote_identifier(&column_schema.name).to_string(), cell_to_sql_literal_ref(value))
        })
        .collect())
}

/// Walks the replicated columns and extracts `(identity column schema, cell)`
/// pairs for the replica-identity values stored in `row`.
fn replica_identity_key_values<'a>(
    replicated_table_schema: &'a ReplicatedTableSchema,
    row: DeletePredicateRowRef<'a>,
) -> EtlResult<Vec<(&'a ColumnSchema, &'a Cell)>> {
    let replicated_column_schemas: Vec<_> = replicated_table_schema.column_schemas().collect();
    let identity_column_schemas: Vec<_> =
        replicated_table_schema.identity_column_schemas().collect();
    if identity_column_schemas.is_empty() {
        return Err(etl_error!(
            ErrorKind::SourceReplicaIdentityError,
            "DuckLake delete requires a replica identity",
            format!(
                "Table '{}' has no replicated replica-identity columns",
                replicated_table_schema.name()
            )
        ));
    }

    let key_values: Vec<_> = match row {
        DeletePredicateRowRef::Full(row) => {
            if row.values().len() != replicated_column_schemas.len() {
                return Err(etl_error!(
                    ErrorKind::InvalidState,
                    "DuckLake row shape does not match schema",
                    format!(
                        "Expected {} values for table '{}', got {}",
                        replicated_column_schemas.len(),
                        replicated_table_schema.name(),
                        row.values().len()
                    )
                ));
            }

            let mut identity_columns = identity_column_schemas.iter().copied().peekable();
            let mut key_values = Vec::with_capacity(identity_column_schemas.len());

            for (column_schema, value) in replicated_column_schemas.iter().zip(row.values()) {
                if identity_columns.peek().is_some_and(|identity_column| {
                    identity_column.ordinal_position == column_schema.ordinal_position
                }) {
                    let Some(identity_column) = identity_columns.next() else {
                        return Err(etl_error!(
                            ErrorKind::InvalidState,
                            "DuckLake replica identity schema is inconsistent",
                            format!(
                                "Table '{}' identity columns ended unexpectedly",
                                replicated_table_schema.name()
                            )
                        ));
                    };

                    key_values.push((identity_column, value));
                }
            }

            key_values
        }
        DeletePredicateRowRef::Key(row) => {
            if row.values().len() != identity_column_schemas.len() {
                return Err(etl_error!(
                    ErrorKind::InvalidState,
                    "DuckLake key image does not match replica identity",
                    format!(
                        "Expected {} key values for table '{}', got {}",
                        identity_column_schemas.len(),
                        replicated_table_schema.name(),
                        row.values().len()
                    )
                ));
            }

            identity_column_schemas.iter().copied().zip(row.values()).collect()
        }
    };

    Ok(key_values)
}

/// Builds SQL `SET` assignments from a partial update row.
fn update_assignments_from_partial_row(
    replicated_table_schema: &ReplicatedTableSchema,
    partial_row: &PartialTableRow,
) -> EtlResult<Vec<String>> {
    let replicated_column_schemas: Vec<_> = replicated_table_schema.column_schemas().collect();
    if partial_row.total_columns() != replicated_column_schemas.len() {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "DuckLake partial update row does not match schema",
            format!(
                "Expected {} replicated columns for table '{}', got {}",
                replicated_column_schemas.len(),
                replicated_table_schema.name(),
                partial_row.total_columns()
            )
        ));
    }

    if partial_row.values().is_empty() {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "DuckLake partial update row has no assignments",
            format!(
                "Table '{}' emitted an empty partial update row",
                replicated_table_schema.name()
            )
        ));
    }

    if partial_row.values().len() + partial_row.missing_column_indexes().len()
        != partial_row.total_columns()
    {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "DuckLake partial update row shape is inconsistent",
            format!(
                "Table '{}' partial row reports {} total columns but has {} present and {} missing",
                replicated_table_schema.name(),
                partial_row.total_columns(),
                partial_row.values().len(),
                partial_row.missing_column_indexes().len()
            )
        ));
    }

    let mut assignments = Vec::with_capacity(partial_row.values().len());
    let mut missing_indexes = partial_row.missing_column_indexes().iter().copied().peekable();
    let mut present_values = partial_row.values().iter();
    for (column_index, column_schema) in replicated_column_schemas.iter().enumerate() {
        if missing_indexes.peek().copied() == Some(column_index) {
            missing_indexes.next();
            continue;
        }

        let Some(value) = present_values.next() else {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "DuckLake partial update row ended early",
                format!(
                    "Table '{}' did not provide enough values for its partial update row",
                    replicated_table_schema.name()
                )
            ));
        };
        let quoted_column = quote_identifier(&column_schema.name);
        assignments.push(format!("{quoted_column} = {}", cell_to_sql_literal_ref(value)));
    }

    if missing_indexes.next().is_some() || present_values.next().is_some() {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "DuckLake partial update row shape is inconsistent",
            format!(
                "Table '{}' partial row has leftover values or missing indexes after decoding",
                replicated_table_schema.name()
            )
        ));
    }

    Ok(assignments)
}

/// Builds a deterministic identity for one ordered mutation batch.
fn build_mutation_batch_identity(
    table_name: &str,
    replicated_table_schema: &ReplicatedTableSchema,
    tracked_mutations: &[TrackedTableMutation],
) -> EtlResult<DuckLakeBatchIdentity> {
    let mut hasher = BatchIdHasher::new();
    "mutation".hash(&mut hasher);
    table_name.hash(&mut hasher);

    for tracked_mutation in tracked_mutations {
        u64::from(tracked_mutation.start_lsn).hash(&mut hasher);
        u64::from(tracked_mutation.commit_lsn).hash(&mut hasher);

        match &tracked_mutation.mutation {
            TableMutation::Insert(row) => {
                "insert".hash(&mut hasher);
                hash_table_row_ref(&mut hasher, row);
            }
            TableMutation::Delete(row) => {
                "delete".hash(&mut hasher);
                delete_predicate_from_row(replicated_table_schema, row)?.hash(&mut hasher);
            }
            TableMutation::Update { delete_row, new_row } => {
                "update".hash(&mut hasher);
                delete_predicate_from_row(replicated_table_schema, delete_row)?.hash(&mut hasher);
                match new_row {
                    UpdatedTableRow::Full(row) => hash_table_row_ref(&mut hasher, row),
                    UpdatedTableRow::Partial(row) => hash_partial_table_row_ref(&mut hasher, row)?,
                }
            }
            TableMutation::Replace(row) => {
                "replace".hash(&mut hasher);
                delete_predicate_from_row(replicated_table_schema, row)?.hash(&mut hasher);
                hash_table_row_ref(&mut hasher, row);
            }
        }
    }

    Ok(build_batch_identity(
        DuckLakeTableBatchKind::Mutation,
        tracked_mutations.first().map(|tracked_mutation| tracked_mutation.start_lsn),
        tracked_mutations.last().map(|tracked_mutation| tracked_mutation.commit_lsn),
        hasher.finish(),
    ))
}

/// Builds a deterministic identity for one ordered table-copy batch.
fn build_copy_batch_identity(
    table_name: &str,
    replicated_table_schema: &ReplicatedTableSchema,
    table_rows: &[TableRow],
) -> EtlResult<DuckLakeBatchIdentity> {
    let mut hasher = BatchIdHasher::new();
    "copy".hash(&mut hasher);
    table_name.hash(&mut hasher);

    for row in table_rows {
        delete_predicate_from_copy_row(replicated_table_schema, row)?.hash(&mut hasher);
        hash_table_row_ref(&mut hasher, row);
    }

    Ok(build_batch_identity(DuckLakeTableBatchKind::Copy, None, None, hasher.finish()))
}

/// Builds a delete predicate for a full copied row using the source primary
/// key.
fn delete_predicate_from_copy_row(
    replicated_table_schema: &ReplicatedTableSchema,
    row: &TableRow,
) -> EtlResult<String> {
    let replicated_column_schemas: Vec<_> = replicated_table_schema.column_schemas().collect();
    if row.values().len() != replicated_column_schemas.len() {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "DuckLake copy row shape does not match schema",
            format!(
                "Expected {} values for table '{}', got {}",
                replicated_column_schemas.len(),
                replicated_table_schema.name(),
                row.values().len()
            )
        ));
    }

    let mut predicates = Vec::new();
    for (column_schema, value) in replicated_column_schemas.iter().zip(row.values()) {
        if !column_schema.primary_key() {
            continue;
        }

        let quoted_column = quote_identifier(&column_schema.name);
        let predicate = match value {
            Cell::Null => format!("{quoted_column} IS NULL"),
            _ => format!("{quoted_column} = {}", cell_to_sql_literal_ref(value)),
        };
        predicates.push(predicate);
    }

    Ok(predicates.join(" AND "))
}

/// Builds a deterministic identity for one ordered truncate batch.
fn build_truncate_batch_identity(
    table_name: &str,
    tracked_truncates: &[TrackedTruncateEvent],
) -> DuckLakeBatchIdentity {
    let mut hasher = BatchIdHasher::new();
    "truncate".hash(&mut hasher);
    table_name.hash(&mut hasher);

    for tracked_truncate in tracked_truncates {
        u64::from(tracked_truncate.start_lsn).hash(&mut hasher);
        u64::from(tracked_truncate.commit_lsn).hash(&mut hasher);
        tracked_truncate.options.hash(&mut hasher);
    }

    build_batch_identity(
        DuckLakeTableBatchKind::Truncate,
        tracked_truncates.first().map(|tracked_truncate| tracked_truncate.start_lsn),
        tracked_truncates.last().map(|tracked_truncate| tracked_truncate.commit_lsn),
        hasher.finish(),
    )
}

/// Builds the final persisted batch identity string.
fn build_batch_identity(
    batch_kind: DuckLakeTableBatchKind,
    first_start_lsn: Option<PgLsn>,
    last_commit_lsn: Option<PgLsn>,
    fingerprint: u64,
) -> DuckLakeBatchIdentity {
    let first_start_lsn_u64 = first_start_lsn.map(u64::from).unwrap_or_default();
    let last_commit_lsn_u64 = last_commit_lsn.map(u64::from).unwrap_or_default();

    DuckLakeBatchIdentity {
        batch_id: format!(
            "{}:{first_start_lsn_u64:016x}:{last_commit_lsn_u64:016x}:{fingerprint:016x}",
            batch_kind.as_str()
        ),
        first_start_lsn,
        last_commit_lsn,
    }
}

/// Hashes a row using its SQL literal form so retries are independent of
/// appender encoding.
fn hash_table_row_ref(hasher: &mut BatchIdHasher, row: &TableRow) {
    table_row_to_sql_literal_ref(row).hash(hasher);
}

/// Hashes a partial row using column indexes and SQL literal forms.
fn hash_partial_table_row_ref(hasher: &mut BatchIdHasher, row: &PartialTableRow) -> EtlResult<()> {
    row.total_columns().hash(hasher);
    let mut missing_indexes = row.missing_column_indexes().iter().copied().peekable();
    let mut present_values = row.values().iter();

    for column_index in 0..row.total_columns() {
        if missing_indexes.peek().copied() == Some(column_index) {
            missing_indexes.next();
            continue;
        }

        let Some(value) = present_values.next() else {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "DuckLake partial row shape is inconsistent",
                format!("Partial row ended before replicated column index {}", column_index)
            ));
        };

        column_index.hash(hasher);
        cell_to_sql_literal_ref(value).hash(hasher);
    }

    if present_values.next().is_some() {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "DuckLake partial row shape is inconsistent",
            "Partial row contained more present values than its missing indexes allow"
        ));
    }

    Ok(())
}

/// Returns whether the atomic batch marker already exists.
fn applied_batch_marker_exists(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
) -> EtlResult<bool> {
    let sql = format!(
        r#"SELECT 1 FROM {LAKE_CATALOG}."{APPLIED_BATCHES_TABLE}"
         WHERE table_name = {} AND batch_id = {} LIMIT 1;"#,
        quote_literal(&batch.table_name),
        quote_literal(&batch.batch_id)
    );
    let mut statement = conn.prepare(&sql).map_err(|err| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake marker query prepare failed",
            format_query_error_detail(&sql),
            source: err
        )
    })?;
    let mut rows = statement.query([]).map_err(|err| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake marker query failed",
            format_query_error_detail(&sql),
            source: err
        )
    })?;

    rows.next().map(|row| row.is_some()).map_err(|err| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake marker query row fetch failed",
            format_query_error_detail(&sql),
            source: err
        )
    })
}

/// Inserts the atomic batch marker inside the open DuckLake transaction.
fn insert_applied_batch_marker(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
) -> EtlResult<()> {
    let sql = format!(
        r#"INSERT INTO {LAKE_CATALOG}."{APPLIED_BATCHES_TABLE}"
         (table_name, batch_id, batch_kind, first_start_lsn, last_commit_lsn, applied_at) VALUES ({}, {}, {}, {}, {}, current_timestamp);"#,
        quote_literal(&batch.table_name),
        quote_literal(&batch.batch_id),
        quote_literal(batch.batch_kind.as_str()),
        optional_lsn_to_sql_literal(batch.first_start_lsn),
        optional_lsn_to_sql_literal(batch.last_commit_lsn),
    );
    conn.execute_batch(&sql).map_err(|err| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake batch marker insert failed",
            format_query_error_detail(&sql),
            source: err
        )
    })?;
    Ok(())
}

/// Updates the steady-state streaming replay watermark inside the open
/// transaction.
fn update_table_streaming_progress(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
) -> EtlResult<()> {
    let last_sequence_key = batch.last_sequence_key.ok_or_else(|| {
        etl_error!(
            ErrorKind::InvalidState,
            "DuckLake streaming batch is missing its last sequence key",
            format!("table={}, batch_kind={}", batch.table_name, batch.batch_kind.as_str())
        )
    })?;
    // Append-only on purpose: the progress table is shared by every table's
    // batch transaction, and a DELETE here removes inlined data that any
    // concurrently committing batch depends on, failing its commit with a
    // DuckLake transaction conflict. Readers take the latest row per table;
    // superseded rows are pruned during paused maintenance.
    let sql = format!(
        r#"INSERT INTO {LAKE_CATALOG}."{STREAMING_PROGRESS_TABLE}"
         (table_name, last_commit_lsn, last_tx_ordinal, updated_at)
         VALUES ({}, {}, {}, current_timestamp);"#,
        quote_literal(&batch.table_name),
        u64::from(last_sequence_key.commit_lsn),
        last_sequence_key.tx_ordinal,
    );
    conn.execute_batch(&sql).map_err(|err| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress update failed",
            format_query_error_detail(&sql),
            source: err
        )
    })?;
    Ok(())
}

/// Reusable per-batch temp staging table for DuckLake upserts.
struct ReusableStagingTable {
    table_name: DuckLakeTableName,
    staging_name: String,
    created: bool,
    specs: Vec<StagingColumnSpec>,
}

impl ReusableStagingTable {
    /// Creates a fresh staging-table manager for one destination table.
    fn new(table_name: &str, specs: Vec<StagingColumnSpec>) -> Self {
        Self {
            table_name: table_name.to_owned(),
            staging_name: format!("__staging_{table_name}"),
            created: false,
            specs,
        }
    }

    /// Joins quoted staged column names for insert lists.
    fn insert_column_list(&self) -> String {
        self.specs
            .iter()
            .map(|spec| quote_identifier(&spec.name).to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Joins cast-aware select expressions for consuming SQL.
    fn select_expr_list(&self) -> String {
        self.specs.iter().map(StagingColumnSpec::select_expr).collect::<Vec<_>>().join(", ")
    }

    /// Loads one prepared row set into staging and applies it to the target
    /// table.
    fn stage_and_insert(
        &mut self,
        conn: &duckdb::Connection,
        prepared_rows: &PreparedRows,
    ) -> EtlResult<()> {
        self.prepare(conn)?;
        self.load_rows(conn, prepared_rows)?;

        let insert_columns = self.insert_column_list();
        let select_exprs = self.select_expr_list();
        let target_table = qualified_lake_table_name(&self.table_name);
        let staging_table = quote_identifier(&self.staging_name);
        let sql = format!(
            "insert into {target_table} ({insert_columns}) select {select_exprs} from \
             {staging_table};"
        );
        conn.execute_batch(&sql).map_err(|err| {
            tracing::error!(error = %DuckDbSensitiveQueryError, "error INSERT INTO");
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake INSERT SELECT failed",
                format_query_error_detail(&sql),
                source: err
            )
        })?;
        Ok(())
    }

    /// Drops the temp staging table after the batch finishes.
    fn cleanup(&self, conn: &duckdb::Connection) {
        if !self.created {
            return;
        }

        let staging_table = quote_identifier(&self.staging_name);
        if let Err(error) = conn.execute_batch(&format!("drop table if exists {staging_table}")) {
            tracing::error!(error = %error, "error drop table staging");
        }
    }

    /// Creates the temp table once, then clears it before each reuse.
    fn prepare(&mut self, conn: &duckdb::Connection) -> EtlResult<()> {
        let staging_table = quote_identifier(&self.staging_name);
        if self.created {
            let sql = format!("truncate table {staging_table};");
            conn.execute_batch(&sql).map_err(|error| {
                tracing::error!(error = %DuckDbSensitiveQueryError, "error clear staging");
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake staging table clear failed",
                    source: error
                )
            })?;
            return Ok(());
        }

        #[cfg(feature = "test-utils")]
        {
            let mut counts = STAGING_TABLE_CREATIONS_BY_TABLE.lock();
            *counts.entry(self.table_name.clone()).or_default() += 1;
        }

        let columns_ddl = staging_columns_ddl(&self.specs);
        conn.execute_batch(&format!(
            "create or replace temp table {staging_table} ({columns_ddl});"
        ))
        .map_err(|error| {
            tracing::error!(error = %error, "error CREATE TEMP TABLE");

            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake staging table creation failed",
                source: error
            )
        })?;
        self.created = true;
        Ok(())
    }

    /// Loads one prepared row payload into the temp staging table.
    fn load_rows(&self, conn: &duckdb::Connection, prepared_rows: &PreparedRows) -> EtlResult<()> {
        let mut appender = conn.appender(&self.staging_name).map_err(|error| {
            tracing::error!(error = %DuckDbSensitiveQueryError, "error appender");
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake staging appender creation failed",
                source: error
            )
        })?;
        appender.append_record_batch(prepared_rows.batch.clone()).map_err(|err| {
            tracing::error!(error = %err, "error append record batch");
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake staging append_record_batch failed",
                source: err
            )
        })?;
        appender.flush().map_err(|err| {
            tracing::error!(error = %err, "error flush");
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake staging appender flush failed",
                source: err
            )
        })?;
        Ok(())
    }
}

/// Renders the explicit column DDL for a staging table built from specs.
fn staging_columns_ddl(specs: &[StagingColumnSpec]) -> String {
    specs
        .iter()
        .map(|spec| format!("{} {}", quote_identifier(&spec.name), spec.staging_sql_type))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Applies one atomic per-table batch in a single DuckLake transaction.
fn apply_table_batch(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
    operation_context: &DuckLakeBlockingOperationContext,
) -> EtlResult<()> {
    let batch_started = Instant::now();

    conn.execute_batch("BEGIN TRANSACTION").map_err(|error| {
        tracing::error!(error = %error, "error transaction");
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake BEGIN TRANSACTION failed",
            source: error
        )
    })?;

    let mut reusable_staging_table =
        ReusableStagingTable::new(&batch.table_name, batch.staging_specs.clone());
    let result = (|| -> EtlResult<()> {
        match &batch.action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared_mutations) => {
                for prepared_mutation in prepared_mutations {
                    apply_table_mutation(
                        conn,
                        batch,
                        prepared_mutation,
                        &mut reusable_staging_table,
                        operation_context,
                    )?;
                }
            }
            PreparedDuckLakeTableBatchAction::Truncate => {
                apply_truncate_batch_action(conn, &batch.table_name)?;
            }
        }

        if batch.uses_streaming_progress() {
            update_table_streaming_progress(conn, batch)?;
        } else {
            insert_applied_batch_marker(conn, batch)?;
        }
        Ok(())
    })();

    match result {
        Ok(()) => {
            conn.execute_batch("COMMIT").map_err(|error| {
                tracing::error!(error = %error, "error commit");
                reusable_staging_table.cleanup(conn);
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake COMMIT failed",
                    source: error
                )
            })?;
            reusable_staging_table.cleanup(conn);
            histogram!(
                ETL_DUCKLAKE_BATCH_COMMIT_DURATION_SECONDS,
                BATCH_KIND_LABEL => batch.batch_kind.as_str(),
                SUB_BATCH_KIND_LABEL => batch_log_kind(batch),
            )
            .record(batch_started.elapsed().as_secs_f64());
            histogram!(
                ETL_DUCKLAKE_BATCH_PREPARED_MUTATIONS,
                BATCH_KIND_LABEL => batch.batch_kind.as_str(),
                SUB_BATCH_KIND_LABEL => batch_log_kind(batch),
            )
            .record(prepared_mutation_count(batch) as f64);
            trace!(
                table = %batch.table_name,
                batch_id = %batch.batch_id,
                batch_kind = batch.batch_kind.as_str(),
                first_start_lsn = %format_optional_lsn(batch.first_start_lsn),
                last_commit_lsn = %format_optional_lsn(batch.last_commit_lsn),
                sub_batch_kind = batch_log_kind(batch),
                insert_sub_batch_rows = apply_sub_batch_rows(batch),
                "ducklake batch committed"
            );

            #[cfg(feature = "test-utils")]
            maybe_fail_after_committed_batch_for_tests(batch.batch_kind, &batch.table_name)?;

            Ok(())
        }
        Err(err) => {
            let rollback = conn.execute_batch("ROLLBACK");
            reusable_staging_table.cleanup(conn);
            if let Err(err) = rollback {
                tracing::error!(error = %err, "error rollback");
            }

            Err(err)
        }
    }
}

/// Applies the truncate action inside an open transaction.
fn apply_truncate_batch_action(conn: &duckdb::Connection, table_name: &str) -> EtlResult<()> {
    let target_table = qualified_lake_table_name(table_name);
    let sql = format!("TRUNCATE TABLE {target_table};");
    conn.execute_batch(&sql).map_err(|error| {
        tracing::error!(error = %error, "error TRUNCATE TABLE");
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake TRUNCATE TABLE failed",
            format_query_error_detail(&sql),
            source: error
        )
    })?;
    Ok(())
}

/// Formats an optional LSN for marker-table inserts.
fn optional_lsn_to_sql_literal(lsn: Option<PgLsn>) -> String {
    lsn.map_or_else(|| "NULL".to_owned(), |value| u64::from(value).to_string())
}

/// Applies one prepared table mutation inside an open transaction.
fn apply_table_mutation(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
    prepared_mutation: &PreparedTableMutation,
    reusable_staging_table: &mut ReusableStagingTable,
    operation_context: &DuckLakeBlockingOperationContext,
) -> EtlResult<()> {
    match prepared_mutation {
        PreparedTableMutation::Upsert(prepared_rows) => {
            histogram!(
                ETL_DUCKLAKE_UPSERT_ROWS,
                BATCH_KIND_LABEL => batch.batch_kind.as_str(),
                PREPARED_ROWS_KIND_LABEL => PREPARED_ROWS_KIND,
            )
            .record(prepared_rows.row_count() as f64);
            apply_upsert_mutation(conn, prepared_rows, reusable_staging_table)
        }
        PreparedTableMutation::Delete { key_specs, key_rows, origin } => {
            histogram!(
                ETL_DUCKLAKE_DELETE_PREDICATES,
                BATCH_KIND_LABEL => batch.batch_kind.as_str(),
                DELETE_ORIGIN_LABEL => *origin,
            )
            .record(key_rows.row_count() as f64);
            apply_delete_mutation(
                conn,
                batch,
                key_specs.as_slice(),
                key_rows,
                origin,
                operation_context,
            )
        }
        PreparedTableMutation::Update { assignments, predicate } => apply_update_mutation(
            conn,
            &reusable_staging_table.table_name,
            assignments.as_slice(),
            predicate,
        ),
        PreparedTableMutation::DedupedUpsert { rows, identity_columns } => {
            histogram!(
                ETL_DUCKLAKE_UPSERT_ROWS,
                BATCH_KIND_LABEL => batch.batch_kind.as_str(),
                PREPARED_ROWS_KIND_LABEL => PREPARED_ROWS_KIND,
            )
            .record(rows.row_count() as f64);
            apply_deduped_upsert(conn, rows, reusable_staging_table, identity_columns)
        }
        PreparedTableMutation::Append { rows } => {
            histogram!(
                ETL_DUCKLAKE_UPSERT_ROWS,
                BATCH_KIND_LABEL => batch.batch_kind.as_str(),
                PREPARED_ROWS_KIND_LABEL => PREPARED_ROWS_KIND,
            )
            .record(rows.row_count() as f64);
            apply_upsert_mutation(conn, rows, reusable_staging_table)
        }
        PreparedTableMutation::Merge { rows, identity_columns, all_columns } => {
            histogram!(
                ETL_DUCKLAKE_UPSERT_ROWS,
                BATCH_KIND_LABEL => batch.batch_kind.as_str(),
                PREPARED_ROWS_KIND_LABEL => PREPARED_ROWS_KIND,
            )
            .record(rows.row_count() as f64);
            apply_merge_mutation(conn, rows, reusable_staging_table, identity_columns, all_columns)
        }
    }
}

/// Applies one upsert batch inside an open DuckLake transaction.
fn apply_upsert_mutation(
    conn: &duckdb::Connection,
    prepared_rows: &PreparedRows,
    reusable_staging_table: &mut ReusableStagingTable,
) -> EtlResult<()> {
    if prepared_rows.row_count() == 0 {
        return Ok(());
    }

    reusable_staging_table.stage_and_insert(conn, prepared_rows)
}

/// Deduplicates staging table then plain INSERT (no hash-join against target).
fn apply_deduped_upsert(
    conn: &duckdb::Connection,
    prepared_rows: &PreparedRows,
    reusable_staging_table: &mut ReusableStagingTable,
    identity_columns: &[String],
) -> EtlResult<()> {
    if prepared_rows.row_count() == 0 {
        return Ok(());
    }

    reusable_staging_table.prepare(conn)?;
    reusable_staging_table.load_rows(conn, prepared_rows)?;

    let target = qualified_lake_table_name(&reusable_staging_table.table_name);
    let staging = quote_identifier(&reusable_staging_table.staging_name);

    let identity_join = identity_columns
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Vec<_>>()
        .join(", ");

    let insert_columns = reusable_staging_table.insert_column_list();
    let select_exprs = reusable_staging_table.select_expr_list();

    // Dedup runs over the raw staging columns; casts apply in the outer
    // select feeding the INSERT.
    let sql = format!(
        "INSERT INTO {target} ({insert_columns}) \
         SELECT {select_exprs} FROM (SELECT * FROM {staging} \
         QUALIFY ROW_NUMBER() OVER (PARTITION BY {identity_join} \
         ORDER BY rowid DESC) = 1)"
    );

    conn.execute_batch(&sql).map_err(|err| {
        tracing::error!(error = %DuckDbSensitiveQueryError, "error deduped INSERT");
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake deduped INSERT failed",
            format_query_error_detail(&sql),
            source: err
        )
    })?;
    Ok(())
}

/// Deduplicates staging table then MERGE INTO target for Replace/Update mutations.
fn apply_merge_mutation(
    conn: &duckdb::Connection,
    prepared_rows: &PreparedRows,
    reusable_staging_table: &mut ReusableStagingTable,
    identity_columns: &[String],
    all_columns: &[String],
) -> EtlResult<()> {
    if prepared_rows.row_count() == 0 {
        return Ok(());
    }

    reusable_staging_table.prepare(conn)?;
    reusable_staging_table.load_rows(conn, prepared_rows)?;

    let target = qualified_lake_table_name(&reusable_staging_table.table_name);
    let staging = quote_identifier(&reusable_staging_table.staging_name);

    let identity_join = identity_columns
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Vec<_>>()
        .join(", ");

    // Dedup runs over the raw staging columns; casts apply in the outer
    // select so `source` carries the target column types.
    let select_exprs = reusable_staging_table.select_expr_list();
    let deduped_source = format!(
        "(SELECT {select_exprs} FROM (SELECT * FROM {staging} \
         QUALIFY ROW_NUMBER() OVER (PARTITION BY {identity_join} \
         ORDER BY rowid DESC) = 1))"
    );

    let on_clause = identity_columns
        .iter()
        .map(|c| {
            let q = quote_identifier(c);
            format!("target.{q} = source.{q}")
        })
        .collect::<Vec<_>>()
        .join(" AND ");

    let update_set = all_columns
        .iter()
        .filter(|c| !identity_columns.contains(c))
        .map(|c| {
            let q = quote_identifier(c);
            format!("{q} = source.{q}")
        })
        .collect::<Vec<_>>()
        .join(", ");

    // DuckLake rewrites every matched row even when nothing changed
    // (duckdb/ducklake#462), bloating delete files and new data files, so
    // only update rows whose non-identity columns actually differ.
    let change_detection = all_columns
        .iter()
        .filter(|c| !identity_columns.contains(c))
        .map(|c| {
            let q = quote_identifier(c);
            format!("target.{q} IS DISTINCT FROM source.{q}")
        })
        .collect::<Vec<_>>()
        .join(" OR ");

    let insert_cols = all_columns
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Vec<_>>()
        .join(", ");
    let insert_vals = all_columns
        .iter()
        .map(|c| format!("source.{}", quote_identifier(c)))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = if update_set.is_empty() {
        // All columns are identity columns; no SET clause needed.
        format!(
            "MERGE INTO {target} AS target \
             USING {deduped_source} AS source \
             ON ({on_clause}) \
             WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        )
    } else {
        format!(
            "MERGE INTO {target} AS target \
             USING {deduped_source} AS source \
             ON ({on_clause}) \
             WHEN MATCHED AND ({change_detection}) THEN UPDATE SET {update_set} \
             WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        )
    };

    conn.execute_batch(&sql).map_err(|err| {
        tracing::error!(error = %DuckDbSensitiveQueryError, "error MERGE INTO");
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake MERGE INTO failed",
            format_query_error_detail(&sql),
            source: err
        )
    })?;
    Ok(())
}

/// Applies one delete batch inside an open DuckLake transaction.
///
/// Key rows load into a key-only temp table, then a single join-driven DELETE
/// removes every staged key in one target scan instead of one scan per
/// predicate chunk.
fn apply_delete_mutation(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
    key_specs: &[StagingColumnSpec],
    key_rows: &PreparedRows,
    origin: &'static str,
    operation_context: &DuckLakeBlockingOperationContext,
) -> EtlResult<()> {
    let key_row_count = key_rows.row_count();
    if key_row_count == 0 {
        return Ok(());
    }
    if key_specs.is_empty() {
        return Err(etl_error!(
            ErrorKind::SourceReplicaIdentityError,
            "DuckLake delete requires a replica identity",
            format!("Table '{}' staged a delete without identity columns", batch.table_name)
        ));
    }

    let target_table = qualified_lake_table_name(&batch.table_name);
    let staging_name = format!("__staging_delete_{}", batch.table_name);
    let staging_table = quote_identifier(&staging_name);

    let map_delete_error = |error: Option<&duckdb::Error>, stage: &'static str| {
        let duckdb_interrupted = error.is_some_and(is_duckdb_interrupt_error);
        tracing::error!(
            error = %DuckDbSensitiveQueryError,
            table = %batch.table_name,
            batch_id = %batch.batch_id,
            batch_kind = batch.batch_kind.as_str(),
            first_start_lsn = %format_optional_lsn(batch.first_start_lsn),
            last_commit_lsn = %format_optional_lsn(batch.last_commit_lsn),
            first_sequence_key = %format_optional_sequence_key(batch.first_sequence_key),
            last_sequence_key = %format_optional_sequence_key(batch.last_sequence_key),
            delete_origin = origin,
            delete_key_row_count = key_row_count,
            delete_stage = stage,
            duckdb_interrupted,
            ducklake_interrupt_reason = operation_context.interrupt_reason_label(),
            ducklake_operation_id = operation_context.operation_id(),
            ducklake_operation_kind = operation_context.operation_kind(),
            ducklake_operation_timeout_ms = operation_context.timeout_ms(),
            "error DELETE FROM"
        );
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake DELETE failed",
            format_delete_mutation_error_detail(&target_table, key_row_count, stage),
            source: DuckDbSensitiveQueryError
        )
    };

    let columns_ddl = staging_columns_ddl(key_specs);
    conn.execute_batch(&format!("create or replace temp table {staging_table} ({columns_ddl});"))
        .map_err(|error| map_delete_error(Some(&error), "create_staging"))?;

    {
        let mut appender = conn
            .appender(&staging_name)
            .map_err(|error| map_delete_error(Some(&error), "load_staging"))?;
        appender
            .append_record_batch(key_rows.batch.clone())
            .map_err(|error| map_delete_error(Some(&error), "load_staging"))?;
        appender.flush().map_err(|error| map_delete_error(Some(&error), "load_staging"))?;
    }

    let join_clause = key_specs
        .iter()
        .map(|spec| {
            let quoted = quote_identifier(&spec.name);
            let staged = match &spec.cast {
                CastKind::Identity => format!("{staging_table}.{quoted}"),
                CastKind::To(target) => format!("CAST({staging_table}.{quoted} AS {target})"),
            };
            format!("{staged} IS NOT DISTINCT FROM target.{quoted}")
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql_query = format!(
        "DELETE FROM {target_table} AS target \
         WHERE EXISTS (SELECT 1 FROM {staging_table} WHERE {join_clause});"
    );
    conn.execute_batch(&sql_query)
        .map_err(|error| map_delete_error(Some(&error), "delete_join"))?;

    if let Err(error) = conn.execute_batch(&format!("drop table if exists {staging_table};")) {
        tracing::error!(error = %error, "error drop table delete staging");
    }
    Ok(())
}

/// Applies one update statement inside an open DuckLake transaction.
fn apply_update_mutation(
    conn: &duckdb::Connection,
    table_name: &str,
    assignments: &[String],
    predicate: &str,
) -> EtlResult<()> {
    if assignments.is_empty() {
        return Ok(());
    }

    let set_clause = assignments.join(", ");
    let target_table = qualified_lake_table_name(table_name);
    let sql_query = format!("UPDATE {target_table} SET {set_clause} WHERE {predicate};");
    conn.execute_batch(&sql_query).map_err(|_err| {
        tracing::error!(error = %DuckDbSensitiveQueryError, "error UPDATE");
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake UPDATE failed",
            format_update_mutation_error_detail(&target_table, assignments.len(), !predicate.is_empty()),
            source: DuckDbSensitiveQueryError
        )
    })?;

    Ok(())
}

/// Returns the number of prepared mutation statements in one atomic batch.
fn prepared_mutation_count(batch: &PreparedDuckLakeTableBatch) -> usize {
    match &batch.action {
        PreparedDuckLakeTableBatchAction::Truncate => 1,
        PreparedDuckLakeTableBatchAction::Mutation(prepared_mutations) => prepared_mutations.len(),
    }
}

/// Returns the insert row count when the batch is a pure insert sub-batch.
fn apply_sub_batch_rows(batch: &PreparedDuckLakeTableBatch) -> Option<usize> {
    let PreparedDuckLakeTableBatchAction::Mutation(prepared_mutations) = &batch.action else {
        return None;
    };

    if prepared_mutations.len() != 1 {
        return None;
    }

    match &prepared_mutations[0] {
        PreparedTableMutation::Upsert(prepared_rows) => Some(prepared_rows.row_count()),
        PreparedTableMutation::Append { rows }
        | PreparedTableMutation::DedupedUpsert { rows, .. }
        | PreparedTableMutation::Merge { rows, .. } => Some(rows.row_count()),
        PreparedTableMutation::Delete { .. } | PreparedTableMutation::Update { .. } => None,
    }
}

/// Classifies a prepared batch for concise INFO logging.
fn batch_log_kind(batch: &PreparedDuckLakeTableBatch) -> &'static str {
    match &batch.action {
        PreparedDuckLakeTableBatchAction::Truncate => "truncate",
        PreparedDuckLakeTableBatchAction::Mutation(prepared_mutations) => {
            match prepared_mutations.as_slice() {
                [PreparedTableMutation::Upsert(_)] => "insert",
                [PreparedTableMutation::Delete { origin, .. }]
                | [
                    PreparedTableMutation::Delete { origin, .. },
                    PreparedTableMutation::Upsert(_),
                ] => origin,
                [PreparedTableMutation::Update { .. }] => "update",
                _ => "mutation",
            }
        }
    }
}

#[cfg(feature = "test-utils")]
static FAIL_AFTER_ATOMIC_BATCH_COMMIT_TABLE: LazyLock<Mutex<Option<String>>> =
    LazyLock::new(|| Mutex::new(None));
#[cfg(feature = "test-utils")]
static FAIL_AFTER_COPY_BATCH_COMMIT_TABLE: LazyLock<Mutex<Option<String>>> =
    LazyLock::new(|| Mutex::new(None));
#[cfg(feature = "test-utils")]
static STAGING_TABLE_CREATIONS_BY_TABLE: LazyLock<Mutex<HashMap<String, usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Arms a test hook that injects one post-commit failure for the next atomic
/// batch.
#[cfg(feature = "test-utils")]
pub fn arm_fail_after_atomic_batch_commit_once_for_tests(table_name: &str) {
    *FAIL_AFTER_ATOMIC_BATCH_COMMIT_TABLE.lock() = Some(table_name.to_owned());
}

/// Arms a test hook that injects one post-commit failure for the next copy
/// batch.
#[cfg(feature = "test-utils")]
pub fn arm_fail_after_copy_batch_commit_once_for_tests(table_name: &str) {
    *FAIL_AFTER_COPY_BATCH_COMMIT_TABLE.lock() = Some(table_name.to_owned());
}

/// Clears DuckLake destination test hooks.
#[cfg(feature = "test-utils")]
pub fn reset_ducklake_test_hooks() {
    *FAIL_AFTER_ATOMIC_BATCH_COMMIT_TABLE.lock() = None;
    *FAIL_AFTER_COPY_BATCH_COMMIT_TABLE.lock() = None;
    STAGING_TABLE_CREATIONS_BY_TABLE.lock().clear();
}

/// Returns the number of staging-table creations performed for one table since
/// the last reset.
#[cfg(feature = "test-utils")]
pub fn ducklake_staging_table_creations_for_tests(table_name: &str) -> usize {
    STAGING_TABLE_CREATIONS_BY_TABLE.lock().get(table_name).copied().unwrap_or_default()
}

/// Injects a synthetic failure after commit so retries must rely on the correct
/// marker path.
#[cfg(feature = "test-utils")]
fn maybe_fail_after_committed_batch_for_tests(
    batch_kind: DuckLakeTableBatchKind,
    table_name: &str,
) -> EtlResult<()> {
    match batch_kind {
        DuckLakeTableBatchKind::Copy => maybe_fail_after_copy_batch_commit_for_tests(table_name),
        DuckLakeTableBatchKind::Mutation | DuckLakeTableBatchKind::Truncate => {
            maybe_fail_after_atomic_batch_commit_for_tests(table_name)
        }
    }
}

/// Injects a synthetic failure after commit so retries must rely on the
/// progress row.
#[cfg(feature = "test-utils")]
fn maybe_fail_after_atomic_batch_commit_for_tests(table_name: &str) -> EtlResult<()> {
    let mut fail_table = FAIL_AFTER_ATOMIC_BATCH_COMMIT_TABLE.lock();
    if fail_table.as_deref() == Some(table_name) {
        *fail_table = None;
        return Err(etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake test hook injected post-commit failure"
        ));
    }

    Ok(())
}

/// Injects a synthetic failure after commit so copy retries must rely on the
/// marker table.
#[cfg(feature = "test-utils")]
fn maybe_fail_after_copy_batch_commit_for_tests(table_name: &str) -> EtlResult<()> {
    let mut fail_table = FAIL_AFTER_COPY_BATCH_COMMIT_TABLE.lock();
    if fail_table.as_deref() == Some(table_name) {
        *fail_table = None;
        return Err(etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake test hook injected copy post-commit failure"
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use etl::types::{
        ColumnSchema, IdentityMask, OldTableRow, PartialTableRow, ReplicatedTableSchema,
        ReplicationMask, TableId, TableName, TableSchema, Type as PgType, UpdatedTableRow,
    };

    use super::*;

    fn make_schema() -> TableSchema {
        TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, Some(1), false),
                ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 2, None, true),
            ],
        )
    }

    fn make_replicated_schema() -> ReplicatedTableSchema {
        ReplicatedTableSchema::all(Arc::new(make_schema()))
    }

    fn make_prepared_batch(table_name: &str) -> PreparedDuckLakeTableBatch {
        PreparedDuckLakeTableBatch {
            table_name: table_name.to_owned(),
            batch_id: "test-batch".to_owned(),
            batch_kind: DuckLakeTableBatchKind::Mutation,
            first_start_lsn: None,
            last_commit_lsn: None,
            first_sequence_key: None,
            last_sequence_key: None,
            staging_specs: vec![],
            action: PreparedDuckLakeTableBatchAction::Mutation(vec![]),
        }
    }

    fn assert_query_failure_omits_sensitive_value(
        error: &etl::error::EtlError,
        description: &'static str,
        sensitive_value: &str,
    ) {
        assert_eq!(error.kind(), ErrorKind::DestinationQueryFailed);
        assert_eq!(error.description(), Some(description));
        assert!(!error.to_string().contains(sensitive_value));
        assert!(!error.detail().is_some_and(|detail| detail.contains(sensitive_value)));
        let source = error.source().expect("expected sanitized source");
        assert!(!source.to_string().contains(sensitive_value));
        assert!(source.to_string().contains("omitted because it may contain row values"));
    }

    #[test]
    fn apply_delete_mutation_failure_omits_row_values_from_detail() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let batch = make_prepared_batch("users");
        let operation_context = DuckLakeBlockingOperationContext::for_tests();
        let sensitive_value = "alice@example.com";
        let key_schemas =
            vec![ColumnSchema::new("email".to_owned(), PgType::TEXT, -1, 1, Some(1), false)];
        let key_specs = build_staging_specs(&key_schemas).unwrap();
        let key_rows = prepare_rows(
            vec![TableRow::new(vec![Cell::String(sensitive_value.to_owned())])],
            &key_schemas,
        )
        .unwrap();

        let error = apply_delete_mutation(
            &conn,
            &batch,
            key_specs.as_slice(),
            &key_rows,
            "delete",
            &operation_context,
        )
        .unwrap_err();

        assert_query_failure_omits_sensitive_value(
            &error,
            "DuckLake DELETE failed",
            sensitive_value,
        );
    }

    #[test]
    fn apply_update_mutation_failure_omits_row_values_from_detail() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let sensitive_value = "secret-token";
        let assignments = vec![format!("\"token\" = '{sensitive_value}'")];
        let predicate = format!("\"token\" = '{sensitive_value}'");

        let error =
            apply_update_mutation(&conn, "users", assignments.as_slice(), &predicate).unwrap_err();

        assert_query_failure_omits_sensitive_value(
            &error,
            "DuckLake UPDATE failed",
            sensitive_value,
        );
    }

    #[test]
    fn delete_predicate_from_row_uses_only_replica_identity_columns() {
        let replicated_table_schema = ReplicatedTableSchema::all(Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("tenant_id".to_owned(), PgType::INT4, -1, 1, Some(1), false),
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 2, Some(2), false),
                ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 3, None, true),
            ],
        )));
        let row =
            TableRow::new(vec![Cell::I32(7), Cell::I32(42), Cell::String("alice".to_owned())]);

        assert_eq!(
            delete_predicate_from_row(&replicated_table_schema, &row).unwrap(),
            "\"tenant_id\" = 7 AND \"id\" = 42"
        );
    }

    #[test]
    fn delete_predicate_from_row_supports_alternative_identity_without_primary_key() {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, None, false),
                ColumnSchema::new("email".to_owned(), PgType::TEXT, -1, 2, None, false),
                ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 3, None, true),
            ],
        ));
        let replicated_table_schema = ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicationMask::all(&table_schema),
            IdentityMask::from_bytes(vec![0, 1, 0]),
        );
        let row = TableRow::new(vec![
            Cell::I32(7),
            Cell::String("alice@example.com".to_owned()),
            Cell::String("alice".to_owned()),
        ]);

        assert_eq!(
            delete_predicate_from_row(&replicated_table_schema, &row).unwrap(),
            "\"email\" = 'alice@example.com'"
        );
    }

    #[test]
    fn delete_predicate_from_row_uses_full_replica_identity_columns() {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, Some(1), false),
                ColumnSchema::new("email".to_owned(), PgType::TEXT, -1, 2, None, false),
                ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 3, None, true),
            ],
        ));
        let replicated_table_schema = ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicationMask::all(&table_schema),
            IdentityMask::from_bytes(vec![1, 1, 1]),
        );
        let row = TableRow::new(vec![
            Cell::I32(7),
            Cell::String("alice@example.com".to_owned()),
            Cell::String("alice".to_owned()),
        ]);

        assert_eq!(
            delete_predicate_from_row(&replicated_table_schema, &row).unwrap(),
            "\"id\" = 7 AND \"email\" = 'alice@example.com' AND \"name\" = 'alice'"
        );
    }

    #[test]
    fn delete_predicate_from_row_rejects_missing_replica_identity() {
        let table_schema = Arc::new(make_schema());
        let replicated_table_schema = ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicationMask::all(&table_schema),
            IdentityMask::from_bytes(vec![0, 0]),
        );
        let row = TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_owned())]);

        let error = delete_predicate_from_row(&replicated_table_schema, &row).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::SourceReplicaIdentityError);
        assert_eq!(error.description(), Some("DuckLake delete requires a replica identity"));
    }

    #[test]
    fn prepare_table_mutations_replace_emits_merge() {
        let replicated_table_schema = make_replicated_schema();
        let row = TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_owned())]);

        let prepared =
            prepare_table_mutations(&replicated_table_schema, vec![TableMutation::Replace(row)])
                .unwrap();

        assert_eq!(prepared.len(), 1);
        match &prepared[0] {
            PreparedTableMutation::Merge { rows, identity_columns, all_columns, .. } => {
                assert_eq!(identity_columns, &vec!["id".to_owned()]);
                assert_eq!(all_columns.len(), 2);
                assert_eq!(rows.row_count(), 1);
            }
            _ => panic!("expected merge"),
        }
    }

    #[test]
    fn prepare_table_mutations_update_emits_update_statement() {
        let replicated_table_schema = make_replicated_schema();
        let prepared = prepare_table_mutations(
            &replicated_table_schema,
            vec![TableMutation::Update {
                delete_row: OldTableRow::Key(TableRow::new(vec![Cell::I32(1)])),
                new_row: UpdatedTableRow::Partial(PartialTableRow::new(
                    2,
                    TableRow::new(vec![Cell::I32(1), Cell::String("after".to_owned())]),
                    vec![],
                )),
            }],
        )
        .unwrap();

        assert_eq!(prepared.len(), 1);
        match &prepared[0] {
            PreparedTableMutation::Update { assignments, predicate } => {
                assert_eq!(
                    assignments,
                    &vec!["\"id\" = 1".to_owned(), "\"name\" = 'after'".to_owned()]
                );
                assert_eq!(predicate, "\"id\" = 1");
            }
            _ => panic!("expected update"),
        }
    }

    #[test]
    fn prepare_table_mutations_update_uses_alternative_identity_key_for_changed_key_update() {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, Some(1), false),
                ColumnSchema::new("email".to_owned(), PgType::TEXT, -1, 2, None, false),
                ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 3, None, true),
                ColumnSchema::new("payload".to_owned(), PgType::TEXT, -1, 4, None, true),
            ],
        ));
        let replicated_table_schema = ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicationMask::all(&table_schema),
            IdentityMask::from_bytes(vec![0, 1, 0, 0]),
        );

        let prepared = prepare_table_mutations(
            &replicated_table_schema,
            vec![TableMutation::Update {
                delete_row: OldTableRow::Key(TableRow::new(vec![Cell::String(
                    "alice@example.com".to_owned(),
                )])),
                new_row: UpdatedTableRow::Partial(PartialTableRow::new(
                    4,
                    TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice@new.example.com".to_owned()),
                        Cell::String("ripe".to_owned()),
                    ]),
                    vec![3],
                )),
            }],
        )
        .unwrap();

        assert_eq!(prepared.len(), 1);
        match &prepared[0] {
            PreparedTableMutation::Update { assignments, predicate } => {
                assert_eq!(
                    assignments,
                    &vec![
                        "\"id\" = 1".to_owned(),
                        "\"email\" = 'alice@new.example.com'".to_owned(),
                        "\"name\" = 'ripe'".to_owned(),
                    ]
                );
                assert_eq!(predicate, "\"email\" = 'alice@example.com'");
            }
            _ => panic!("expected update"),
        }
    }

    #[test]
    fn prepare_table_mutations_update_uses_full_replica_identity_predicate() {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, Some(1), false),
                ColumnSchema::new("email".to_owned(), PgType::TEXT, -1, 2, None, false),
                ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 3, None, true),
                ColumnSchema::new("payload".to_owned(), PgType::TEXT, -1, 4, None, true),
            ],
        ));
        let replicated_table_schema = ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicationMask::all(&table_schema),
            IdentityMask::from_bytes(vec![1, 1, 1, 1]),
        );

        let prepared = prepare_table_mutations(
            &replicated_table_schema,
            vec![TableMutation::Update {
                delete_row: OldTableRow::Full(TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String("alice@example.com".to_owned()),
                    Cell::String("seed".to_owned()),
                    Cell::String("toast".to_owned()),
                ])),
                new_row: UpdatedTableRow::Partial(PartialTableRow::new(
                    4,
                    TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice@example.com".to_owned()),
                        Cell::String("grown".to_owned()),
                    ]),
                    vec![3],
                )),
            }],
        )
        .unwrap();

        assert_eq!(prepared.len(), 1);
        match &prepared[0] {
            PreparedTableMutation::Update { assignments, predicate } => {
                assert_eq!(
                    assignments,
                    &vec![
                        "\"id\" = 1".to_owned(),
                        "\"email\" = 'alice@example.com'".to_owned(),
                        "\"name\" = 'grown'".to_owned(),
                    ]
                );
                assert_eq!(
                    predicate,
                    "\"id\" = 1 AND \"email\" = 'alice@example.com' AND \"name\" = 'seed' AND \
                     \"payload\" = 'toast'"
                );
            }
            _ => panic!("expected update"),
        }
    }

    #[test]
    fn prepare_mutation_table_batches_insert_only_uses_single_upsert_operation() {
        let replicated_table_schema = make_replicated_schema();
        let batches = prepare_mutation_table_batches(
            &replicated_table_schema,
            "public_users".to_owned(),
            vec![
                TrackedTableMutation::new(
                    PgLsn::from(10),
                    PgLsn::from(20),
                    0,
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice".to_owned()),
                    ])),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(10),
                    PgLsn::from(20),
                    1,
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("bob".to_owned()),
                    ])),
                ),
            ],
            &MergeOnReadScope::default(),
        )
        .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].batch_kind, DuckLakeTableBatchKind::Mutation);
        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => {
                assert_eq!(prepared.len(), 1);
                match &prepared[0] {
                    PreparedTableMutation::DedupedUpsert { rows, .. } => {
                        assert_eq!(rows.row_count(), 2);
                    }
                    _ => panic!("expected deduped upsert"),
                }
            }
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn prepare_mutation_table_batches_split_mixed_cdc_at_delete_boundaries() {
        let replicated_table_schema = make_replicated_schema();
        let batches = prepare_mutation_table_batches(
            &replicated_table_schema,
            "public_users".to_owned(),
            vec![
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(110),
                    0,
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(0),
                        Cell::String("seed".to_owned()),
                    ])),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(110),
                    1,
                    TableMutation::Delete(OldTableRow::Full(TableRow::new(vec![
                        Cell::I32(0),
                        Cell::String("seed".to_owned()),
                    ]))),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(110),
                    2,
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(999),
                        Cell::String("tail".to_owned()),
                    ])),
                ),
            ],
            &MergeOnReadScope::default(),
        )
        .unwrap();

        assert_eq!(batches.len(), 1);

        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => {
                assert_eq!(prepared.len(), 3);
                assert!(matches!(
                    prepared[0],
                    PreparedTableMutation::DedupedUpsert { .. }
                ));
                assert!(matches!(prepared[1], PreparedTableMutation::Delete { .. }));
                assert!(matches!(
                    prepared[2],
                    PreparedTableMutation::DedupedUpsert { .. }
                ));
            }
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn prepare_mutation_table_batches_group_contiguous_deletes() {
        let replicated_table_schema = make_replicated_schema();
        let batches = prepare_mutation_table_batches(
            &replicated_table_schema,
            "public_users".to_owned(),
            vec![
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(110),
                    0,
                    TableMutation::Delete(OldTableRow::Full(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice".to_owned()),
                    ]))),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(110),
                    PgLsn::from(120),
                    0,
                    TableMutation::Delete(OldTableRow::Full(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("bob".to_owned()),
                    ]))),
                ),
            ],
            &MergeOnReadScope::default(),
        )
        .unwrap();

        assert_eq!(batches.len(), 1);
        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => {
                assert_eq!(prepared.len(), 1);
                match &prepared[0] {
                    PreparedTableMutation::Delete { key_specs, key_rows, origin } => {
                        assert_eq!(origin, &"delete");
                        assert_eq!(key_specs.len(), 1);
                        assert_eq!(key_specs[0].name, "id");
                        assert_eq!(key_rows.row_count(), 2);
                    }
                    _ => panic!("expected delete batch"),
                }
            }
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn prepare_mutation_table_batches_group_contiguous_updates() {
        let replicated_table_schema = make_replicated_schema();
        let batches = prepare_mutation_table_batches(
            &replicated_table_schema,
            "public_users".to_owned(),
            vec![
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(110),
                    0,
                    TableMutation::Update {
                        delete_row: OldTableRow::Full(TableRow::new(vec![
                            Cell::I32(1),
                            Cell::String("before-a".to_owned()),
                        ])),
                        new_row: UpdatedTableRow::Full(TableRow::new(vec![
                            Cell::I32(1),
                            Cell::String("after-a".to_owned()),
                        ])),
                    },
                ),
                TrackedTableMutation::new(
                    PgLsn::from(110),
                    PgLsn::from(120),
                    0,
                    TableMutation::Update {
                        delete_row: OldTableRow::Full(TableRow::new(vec![
                            Cell::I32(2),
                            Cell::String("before-b".to_owned()),
                        ])),
                        new_row: UpdatedTableRow::Full(TableRow::new(vec![
                            Cell::I32(2),
                            Cell::String("after-b".to_owned()),
                        ])),
                    },
                ),
            ],
            &MergeOnReadScope::default(),
        )
        .unwrap();

        assert_eq!(batches.len(), 1);
        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => {
                // Key-preserving updates batch into one MERGE so a run of
                // updates costs a single target scan.
                assert_eq!(prepared.len(), 1);
                match &prepared[0] {
                    PreparedTableMutation::Merge { rows, identity_columns, .. } => {
                        assert_eq!(identity_columns, &vec!["id".to_owned()]);
                        assert_eq!(rows.row_count(), 2);
                    }
                    _ => panic!("expected merge batch"),
                }
            }
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn prepare_mutation_table_batches_split_non_inserts_at_cap() {
        let replicated_table_schema = make_replicated_schema();
        let tracked = (0..=CDC_MUTATION_BATCH_SIZE)
            .map(|idx| {
                TrackedTableMutation::new(
                    PgLsn::from(100 + idx as u64),
                    PgLsn::from(200 + idx as u64),
                    0,
                    TableMutation::Delete(OldTableRow::Full(TableRow::new(vec![
                        Cell::I32(idx as i32),
                        Cell::String(format!("name-{idx}")),
                    ]))),
                )
            })
            .collect();
        let batches = prepare_mutation_table_batches(
            &replicated_table_schema,
            "public_users".to_owned(),
            tracked,
            &MergeOnReadScope::default(),
        )
        .unwrap();

        assert_eq!(batches.len(), 2);

        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => match &prepared[0] {
                PreparedTableMutation::Delete { key_rows, .. } => {
                    assert_eq!(key_rows.row_count(), CDC_MUTATION_BATCH_SIZE);
                }
                _ => panic!("expected delete batch"),
            },
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }

        match &batches[1].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => match &prepared[0] {
                PreparedTableMutation::Delete { key_rows, .. } => {
                    assert_eq!(key_rows.row_count(), 1);
                }
                _ => panic!("expected delete batch"),
            },
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn prepare_mutation_table_batches_merge_key_preserving_update_between_inserts() {
        let replicated_table_schema = make_replicated_schema();
        let batches = prepare_mutation_table_batches(
            &replicated_table_schema,
            "public_users".to_owned(),
            vec![
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(110),
                    0,
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(0),
                        Cell::String("seed".to_owned()),
                    ])),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(110),
                    PgLsn::from(120),
                    1,
                    TableMutation::Update {
                        delete_row: OldTableRow::Full(TableRow::new(vec![
                            Cell::I32(0),
                            Cell::String("seed".to_owned()),
                        ])),
                        new_row: UpdatedTableRow::Full(TableRow::new(vec![
                            Cell::I32(0),
                            Cell::String("grown".to_owned()),
                        ])),
                    },
                ),
                TrackedTableMutation::new(
                    PgLsn::from(120),
                    PgLsn::from(130),
                    2,
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(999),
                        Cell::String("tail".to_owned()),
                    ])),
                ),
            ],
            &MergeOnReadScope::default(),
        )
        .unwrap();

        assert_eq!(batches.len(), 1);

        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => {
                assert_eq!(prepared.len(), 3);
                assert!(matches!(
                    prepared[0],
                    PreparedTableMutation::DedupedUpsert { .. }
                ));
                assert!(matches!(prepared[1], PreparedTableMutation::Merge { .. }));
                assert!(matches!(
                    prepared[2],
                    PreparedTableMutation::DedupedUpsert { .. }
                ));
            }
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn retain_mutations_after_sequence_key_drops_applied_prefix() {
        let retained = retain_mutations_after_sequence_key(
            vec![
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(110),
                    0,
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("one".to_owned()),
                    ])),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(120),
                    0,
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("two".to_owned()),
                    ])),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(130),
                    0,
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(3),
                        Cell::String("three".to_owned()),
                    ])),
                ),
            ],
            Some(EventSequenceKey::new(PgLsn::from(120), 0)),
        );

        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].sequence_key(), EventSequenceKey::new(PgLsn::from(130), 0));
    }

    #[test]
    fn retain_truncates_after_sequence_key_drops_applied_prefix() {
        let retained = retain_truncates_after_sequence_key(
            vec![
                TrackedTruncateEvent::new(PgLsn::from(100), PgLsn::from(200), 0, 0),
                TrackedTruncateEvent::new(PgLsn::from(100), PgLsn::from(200), 1, 0),
                TrackedTruncateEvent::new(PgLsn::from(100), PgLsn::from(210), 0, 0),
            ],
            Some(EventSequenceKey::new(PgLsn::from(200), 0)),
        );

        assert_eq!(retained.len(), 2);
        assert_eq!(retained[0].sequence_key(), EventSequenceKey::new(PgLsn::from(200), 1));
        assert_eq!(retained[1].sequence_key(), EventSequenceKey::new(PgLsn::from(210), 0));
    }

    #[test]
    fn build_mutation_batch_identity_is_deterministic() {
        let replicated_table_schema = make_replicated_schema();
        let tracked = vec![
            TrackedTableMutation::new(
                PgLsn::from(100),
                PgLsn::from(200),
                0,
                TableMutation::Insert(TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String("alice".to_owned()),
                ])),
            ),
            TrackedTableMutation::new(
                PgLsn::from(100),
                PgLsn::from(200),
                1,
                TableMutation::Delete(OldTableRow::Full(TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String("alice".to_owned()),
                ]))),
            ),
        ];

        let first =
            build_mutation_batch_identity("public_users", &replicated_table_schema, &tracked)
                .unwrap();
        let second =
            build_mutation_batch_identity("public_users", &replicated_table_schema, &tracked)
                .unwrap();

        assert_eq!(first.batch_id, second.batch_id);
        assert_eq!(first.first_start_lsn, Some(PgLsn::from(100)));
        assert_eq!(first.last_commit_lsn, Some(PgLsn::from(200)));
    }

    #[test]
    fn build_mutation_batch_identity_changes_with_order_and_lsn() {
        let replicated_table_schema = make_replicated_schema();
        let original = build_mutation_batch_identity(
            "public_users",
            &replicated_table_schema,
            &[
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(200),
                    0,
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice".to_owned()),
                    ])),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(200),
                    1,
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("bob".to_owned()),
                    ])),
                ),
            ],
        )
        .unwrap();
        let reordered = build_mutation_batch_identity(
            "public_users",
            &replicated_table_schema,
            &[
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(200),
                    0,
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("bob".to_owned()),
                    ])),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(200),
                    1,
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice".to_owned()),
                    ])),
                ),
            ],
        )
        .unwrap();
        let changed_lsn = build_mutation_batch_identity(
            "public_users",
            &replicated_table_schema,
            &[TrackedTableMutation::new(
                PgLsn::from(101),
                PgLsn::from(201),
                0,
                TableMutation::Insert(TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String("alice".to_owned()),
                ])),
            )],
        )
        .unwrap();

        assert_ne!(original.batch_id, reordered.batch_id);
        assert_ne!(original.batch_id, changed_lsn.batch_id);
    }

    #[test]
    fn build_truncate_batch_identity_changes_with_lsn() {
        let first = build_truncate_batch_identity(
            "public_users",
            &[TrackedTruncateEvent::new(PgLsn::from(300), PgLsn::from(400), 0, 0)],
        );
        let second = build_truncate_batch_identity(
            "public_users",
            &[TrackedTruncateEvent::new(PgLsn::from(301), PgLsn::from(401), 0, 0)],
        );

        assert_ne!(first.batch_id, second.batch_id);
    }
}

/// Merge-on-read append routing tests.
///
/// These exercise [`prepare_append_mutations`] both at the shape level (how many
/// appends, which `_etl_version` / `_etl_deleted` each carries) and end-to-end
/// against a real in-memory DuckDB table whose DDL carries the two CDC columns.
/// The end-to-end half stages every produced `RecordBatch` and runs the same
/// plain INSERT the apply path runs, then resolves current state with the
/// production `ROW_NUMBER` dedup query — proving the routed batches line up with
/// the CDC staging contract and dedup correctly.
#[cfg(test)]
mod merge_on_read_apply_tests {
    use std::str::FromStr;
    use std::sync::Arc;

    use chrono::{DateTime, Utc};
    use duckdb::Connection;
    use duckdb::arrow::array::{Array, BooleanArray, Decimal128Array, Int32Array};
    use etl::types::{
        Cell, ColumnSchema, PartialTableRow, PgLsn, ReplicatedTableSchema, TableId, TableName,
        TableRow, TableSchema, Type as PgType, UpdatedTableRow,
    };

    use super::*;

    const ID_A: &str = "11111111-1111-1111-1111-111111111111";

    /// Partitioned in-scope schema: id UUID PK, debit NUMERIC,
    /// effective_at_local TIMESTAMPTZ (partition key), description TEXT.
    fn partitioned_schema() -> ReplicatedTableSchema {
        let table_schema = TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "lines".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), PgType::UUID, -1, 1, Some(1), false),
                ColumnSchema::new("debit".to_owned(), PgType::NUMERIC, -1, 2, None, true),
                ColumnSchema::new(
                    "effective_at_local".to_owned(),
                    PgType::TIMESTAMPTZ,
                    -1,
                    3,
                    None,
                    false,
                ),
                ColumnSchema::new("description".to_owned(), PgType::TEXT, -1, 4, None, true),
            ],
        );
        ReplicatedTableSchema::all(Arc::new(table_schema))
    }

    fn ts(s: &str) -> Cell {
        let dt: DateTime<Utc> = s.parse().expect("valid timestamp");
        Cell::TimestampTz(dt)
    }

    fn full_row(eff: &str, desc: &str) -> TableRow {
        TableRow::new(vec![
            Cell::String(ID_A.to_owned()),
            Cell::Numeric(etl::types::PgNumeric::from_str("12.50").unwrap()),
            ts(eff),
            Cell::String(desc.to_owned()),
        ])
    }

    fn tracked(seq: u64, mutation: TableMutation) -> TrackedTableMutation {
        TrackedTableMutation::new(PgLsn::from(100u64), PgLsn::from(100u64), seq, mutation)
    }

    /// Extracts `(version, deleted)` from one produced append batch.
    fn append_cdc(mutation: &PreparedTableMutation) -> (u128, bool) {
        let PreparedTableMutation::Append { rows } = mutation else {
            panic!("expected Append, got a different prepared mutation");
        };
        let batch = &rows.batch;
        let ncols = batch.num_columns();
        let version_col = batch
            .column(ncols - 2)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("version column is Decimal128");
        let deleted_col = batch
            .column(ncols - 1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("deleted column is Boolean");
        (version_col.value(0) as u128, deleted_col.value(0))
    }

    #[test]
    fn insert_then_same_partition_update_emits_two_live_appends() {
        let schema = partitioned_schema();
        let specs = build_staging_specs_with_cdc(
            &schema.column_schemas().cloned().collect::<Vec<_>>(),
        )
        .unwrap();
        let muts = vec![
            tracked(0, TableMutation::Insert(full_row("2026-05-10T00:00:00Z", "v1"))),
            tracked(
                1,
                TableMutation::Update {
                    delete_row: OldTableRow::Full(full_row("2026-05-10T00:00:00Z", "v1")),
                    new_row: UpdatedTableRow::Full(full_row("2026-05-12T00:00:00Z", "v2")),
                },
            ),
        ];
        let appends = prepare_append_mutations(&schema, muts, true, &specs).unwrap();
        assert_eq!(appends.len(), 2, "insert + same-partition update => 2 live appends");
        let (v0, d0) = append_cdc(&appends[0]);
        let (v1, d1) = append_cdc(&appends[1]);
        assert!(!d0 && !d1, "both live");
        assert!(v1 > v0, "second append has the higher version");
        assert_eq!(v0, version_u128(PgLsn::from(100u64), 0));
        assert_eq!(v1, version_u128(PgLsn::from(100u64), 1));
    }

    #[test]
    fn delete_emits_single_tombstone_append() {
        let schema = partitioned_schema();
        let specs = build_staging_specs_with_cdc(
            &schema.column_schemas().cloned().collect::<Vec<_>>(),
        )
        .unwrap();
        let muts = vec![tracked(
            3,
            TableMutation::Delete(OldTableRow::Full(full_row("2026-05-10T00:00:00Z", "v1"))),
        )];
        let appends = prepare_append_mutations(&schema, muts, true, &specs).unwrap();
        assert_eq!(appends.len(), 1);
        let (_, deleted) = append_cdc(&appends[0]);
        assert!(deleted, "delete => tombstone");
    }

    #[test]
    fn partition_move_emits_tombstone_then_live() {
        let schema = partitioned_schema();
        let specs = build_staging_specs_with_cdc(
            &schema.column_schemas().cloned().collect::<Vec<_>>(),
        )
        .unwrap();
        let muts = vec![tracked(
            5,
            TableMutation::Update {
                delete_row: OldTableRow::Full(full_row("2026-05-10T00:00:00Z", "v1")),
                new_row: UpdatedTableRow::Full(full_row("2026-07-02T00:00:00Z", "v2")),
            },
        )];
        let appends = prepare_append_mutations(&schema, muts, true, &specs).unwrap();
        assert_eq!(appends.len(), 2, "move => tombstone + live");
        let (vt, dt) = append_cdc(&appends[0]);
        let (vl, dl) = append_cdc(&appends[1]);
        assert!(dt, "first append is the old-partition tombstone");
        assert!(!dl, "second append is the new-partition live image");
        assert_eq!(vt, vl, "tombstone and live share the same version V");
    }

    #[test]
    fn partial_update_reconstructs_full_image_from_old_row() {
        // Unchanged-TOAST update: only id + debit present; description missing,
        // taken from the full old row. Same partition, so one live append.
        let schema = partitioned_schema();
        let specs = build_staging_specs_with_cdc(
            &schema.column_schemas().cloned().collect::<Vec<_>>(),
        )
        .unwrap();
        // Present columns (replicated order, missing excluded): id, debit, eff.
        let present = TableRow::new(vec![
            Cell::String(ID_A.to_owned()),
            Cell::Numeric(etl::types::PgNumeric::from_str("99.00").unwrap()),
            ts("2026-05-20T00:00:00Z"),
        ]);
        let partial = PartialTableRow::new(4, present, vec![3]); // description missing
        let muts = vec![tracked(
            7,
            TableMutation::Update {
                delete_row: OldTableRow::Full(full_row("2026-05-10T00:00:00Z", "kept-desc")),
                new_row: UpdatedTableRow::Partial(partial),
            },
        )];
        let appends = prepare_append_mutations(&schema, muts, true, &specs).unwrap();
        assert_eq!(appends.len(), 1, "same-partition partial update => 1 live append");
        let (_, deleted) = append_cdc(&appends[0]);
        assert!(!deleted);
        // The reconstructed description must come from the old row.
        let PreparedTableMutation::Append { rows } = &appends[0] else { unreachable!() };
        let desc = rows
            .batch
            .column(3)
            .as_any()
            .downcast_ref::<duckdb::arrow::array::StringArray>()
            .expect("description is Utf8");
        assert_eq!(desc.value(0), "kept-desc");
    }

    /// Unpartitioned in-scope schema: id INT4 PK not-null, label TEXT nullable,
    /// score INT4 not-null.
    fn unpartitioned_schema() -> ReplicatedTableSchema {
        let table_schema = TableSchema::new(
            TableId::new(2),
            TableName::new("public".to_owned(), "observations".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, Some(1), false),
                ColumnSchema::new("label".to_owned(), PgType::TEXT, -1, 2, None, true),
                ColumnSchema::new("score".to_owned(), PgType::INT4, -1, 3, None, false),
            ],
        );
        ReplicatedTableSchema::all(Arc::new(table_schema))
    }

    #[test]
    fn key_only_delete_on_unpartitioned_table_emits_expanded_tombstone() {
        // Unpartitioned table delete: OldTableRow::Key carries only the PK.
        // prepare_append_mutations must expand the key row to full width and
        // emit exactly one Append with _etl_deleted = true.
        let schema = unpartitioned_schema();
        let specs = build_staging_specs_with_cdc(
            &schema.column_schemas().cloned().collect::<Vec<_>>(),
        )
        .unwrap();
        let muts = vec![tracked(
            11,
            TableMutation::Delete(OldTableRow::Key(TableRow::new(vec![Cell::I32(42)]))),
        )];
        let appends = prepare_append_mutations(&schema, muts, false, &specs).unwrap();
        assert_eq!(appends.len(), 1, "one tombstone append for the delete");
        let (_, deleted) = append_cdc(&appends[0]);
        assert!(deleted, "delete => tombstone (_etl_deleted = true)");

        // Inspect the expanded row: PK kept, nullable TEXT -> NULL, non-nullable INT4 -> 0.
        let PreparedTableMutation::Append { rows } = &appends[0] else {
            unreachable!("already asserted Append above");
        };
        let batch = &rows.batch;
        // col 0: id (INT4 PK) must be 42.
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id column is Int32");
        assert_eq!(id_col.value(0), 42, "PK value preserved by expand_key_row");
        // col 1: label (nullable TEXT) must be NULL.
        assert!(batch.column(1).is_null(0), "nullable TEXT column expanded to NULL");
        // col 2: score (non-nullable INT4) must be 0.
        let score_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("score column is Int32");
        assert_eq!(score_col.value(0), 0, "non-nullable INT4 column expanded to zero");
    }

    #[test]
    fn key_only_old_row_on_partitioned_delete_errors() {
        let schema = partitioned_schema();
        let specs = build_staging_specs_with_cdc(
            &schema.column_schemas().cloned().collect::<Vec<_>>(),
        )
        .unwrap();
        let muts = vec![tracked(
            9,
            TableMutation::Delete(OldTableRow::Key(TableRow::new(vec![Cell::String(
                ID_A.to_owned(),
            )]))),
        )];
        let err = match prepare_append_mutations(&schema, muts, true, &specs) {
            Ok(_) => panic!("expected key-only delete to error"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), ErrorKind::SourceReplicaIdentityError);
    }

    /// Builds the cdc-augmented target table DDL, stages every produced append,
    /// runs the same plain INSERT the apply path runs, then resolves current
    /// state with the production dedup query. Returns `(version, deleted, desc)`
    /// for the surviving row of `ID_A` after dedup (NOT filtering tombstones),
    /// scoped to the given month so partition semantics are observable.
    fn apply_and_read_back(
        appends: &[PreparedTableMutation],
        specs: &[StagingColumnSpec],
        month: u32,
    ) -> Option<(String, bool, Option<String>)> {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("SET TimeZone = 'UTC';").unwrap();
        let target_ddl = specs
            .iter()
            .map(|s| format!("{} {}", quote_identifier(&s.name), s.staging_sql_type))
            .collect::<Vec<_>>()
            .join(", ");
        let staging_ddl = target_ddl.clone();
        conn.execute_batch(&format!(
            "CREATE TABLE target ({target_ddl}); CREATE TEMP TABLE staging ({staging_ddl});"
        ))
        .unwrap();

        let insert_columns =
            specs.iter().map(|s| quote_identifier(&s.name)).collect::<Vec<_>>().join(", ");
        let select_exprs =
            specs.iter().map(StagingColumnSpec::select_expr).collect::<Vec<_>>().join(", ");

        for mutation in appends {
            let PreparedTableMutation::Append { rows } = mutation else {
                panic!("expected Append");
            };
            conn.execute_batch("DELETE FROM staging;").unwrap();
            let mut appender = conn.appender("staging").unwrap();
            appender.append_record_batch(rows.batch.clone()).unwrap();
            appender.flush().unwrap();
            drop(appender);
            conn.execute_batch(&format!(
                "INSERT INTO target ({insert_columns}) SELECT {select_exprs} FROM staging;"
            ))
            .unwrap();
        }

        // Production dedup, scoped to one partition month (the pruning predicate).
        let sql = format!(
            "SELECT \"_etl_version\"::VARCHAR, \"_etl_deleted\", \"description\" FROM (\
               SELECT * FROM target WHERE month(\"effective_at_local\") = {month} \
               QUALIFY ROW_NUMBER() OVER (PARTITION BY \"id\" \
                 ORDER BY \"_etl_version\" DESC, \"_etl_deleted\" ASC) = 1)"
        );
        conn.query_row(&sql, [], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, bool>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .ok()
    }

    #[test]
    fn end_to_end_same_partition_update_dedups_to_latest() {
        let schema = partitioned_schema();
        let specs = build_staging_specs_with_cdc(
            &schema.column_schemas().cloned().collect::<Vec<_>>(),
        )
        .unwrap();
        let muts = vec![
            tracked(0, TableMutation::Insert(full_row("2026-05-10T00:00:00Z", "v1"))),
            tracked(
                1,
                TableMutation::Update {
                    delete_row: OldTableRow::Full(full_row("2026-05-10T00:00:00Z", "v1")),
                    new_row: UpdatedTableRow::Full(full_row("2026-05-12T00:00:00Z", "v2")),
                },
            ),
        ];
        let appends = prepare_append_mutations(&schema, muts, true, &specs).unwrap();
        let (_, deleted, desc) = apply_and_read_back(&appends, &specs, 5).unwrap();
        assert!(!deleted, "latest is live");
        assert_eq!(desc.as_deref(), Some("v2"), "dedup picks the higher version");
    }

    #[test]
    fn end_to_end_delete_latest_is_tombstone() {
        let schema = partitioned_schema();
        let specs = build_staging_specs_with_cdc(
            &schema.column_schemas().cloned().collect::<Vec<_>>(),
        )
        .unwrap();
        let muts = vec![
            tracked(0, TableMutation::Insert(full_row("2026-05-10T00:00:00Z", "v1"))),
            tracked(
                1,
                TableMutation::Delete(OldTableRow::Full(full_row("2026-05-10T00:00:00Z", "v1"))),
            ),
        ];
        let appends = prepare_append_mutations(&schema, muts, true, &specs).unwrap();
        let (_, deleted, _) = apply_and_read_back(&appends, &specs, 5).unwrap();
        assert!(deleted, "latest version row is the tombstone");
    }

    #[test]
    fn end_to_end_move_old_partition_tombstoned_new_partition_live() {
        let schema = partitioned_schema();
        let specs = build_staging_specs_with_cdc(
            &schema.column_schemas().cloned().collect::<Vec<_>>(),
        )
        .unwrap();
        let muts = vec![
            tracked(0, TableMutation::Insert(full_row("2026-05-10T00:00:00Z", "v1"))),
            tracked(
                1,
                TableMutation::Update {
                    delete_row: OldTableRow::Full(full_row("2026-05-10T00:00:00Z", "v1")),
                    new_row: UpdatedTableRow::Full(full_row("2026-07-02T00:00:00Z", "v2")),
                },
            ),
        ];
        let appends = prepare_append_mutations(&schema, muts, true, &specs).unwrap();
        // Old partition (May): the move tombstone wins -> deleted row.
        let (_, deleted_may, _) = apply_and_read_back(&appends, &specs, 5).unwrap();
        assert!(deleted_may, "old partition row is a tombstone");
        // New partition (July): the live image is present.
        let (_, deleted_jul, desc_jul) = apply_and_read_back(&appends, &specs, 7).unwrap();
        assert!(!deleted_jul, "new partition has the live image");
        assert_eq!(desc_jul.as_deref(), Some("v2"));
    }

    /// Task 7 — regression guard: every mutation type must produce only `Append`
    /// ops (never `Merge`, `Delete`, `Update`, `DedupedUpsert`, or `Upsert`).
    ///
    /// `Append` is the only scan-free INSERT path; any other variant would
    /// reintroduce a target-table scan for in-scope tables. This test fails if
    /// a future change routes any mutation through the non-append path.
    #[test]
    fn merge_on_read_emits_only_append_ops() {
        let schema = partitioned_schema();
        let specs = build_staging_specs_with_cdc(
            &schema.column_schemas().cloned().collect::<Vec<_>>(),
        )
        .unwrap();

        // Representative mix:
        //   seq 0: Insert
        //   seq 1: same-partition Update (effective_at stays in May)
        //   seq 2: partition-move Update (effective_at moves from May to July)
        //   seq 3: Delete
        let muts = vec![
            tracked(0, TableMutation::Insert(full_row("2026-05-10T00:00:00Z", "v1"))),
            tracked(
                1,
                TableMutation::Update {
                    delete_row: OldTableRow::Full(full_row("2026-05-10T00:00:00Z", "v1")),
                    new_row: UpdatedTableRow::Full(full_row("2026-05-12T00:00:00Z", "v2")),
                },
            ),
            tracked(
                2,
                TableMutation::Update {
                    delete_row: OldTableRow::Full(full_row("2026-05-12T00:00:00Z", "v2")),
                    new_row: UpdatedTableRow::Full(full_row("2026-07-01T00:00:00Z", "v3")),
                },
            ),
            tracked(
                3,
                TableMutation::Delete(OldTableRow::Full(full_row("2026-07-01T00:00:00Z", "v3"))),
            ),
        ];

        let ops = prepare_append_mutations(&schema, muts, true, &specs).unwrap();

        // Every op must be an Append — never Merge, Delete, Update, DedupedUpsert, or Upsert.
        for (i, op) in ops.iter().enumerate() {
            assert!(
                matches!(op, PreparedTableMutation::Append { .. }),
                "op[{i}] is not an Append; in-scope tables must never produce scan-bearing ops"
            );
        }

        // Sanity-check the expected count:
        // Insert (1) + same-partition update (1) + partition-move (2: tombstone + live) + delete (1) = 5
        assert_eq!(ops.len(), 5, "expected 5 append ops for the representative mix");
    }

    /// Task 8 — unchanged-TOAST behavioral guard: a partial update (simulating
    /// an unchanged-TOAST column) must produce an appended image that carries the
    /// real old-row value for the missing column, not a placeholder or NULL.
    ///
    /// Unchanged-TOAST is resolved by the `etl` crate upstream of this layer:
    /// `convert_tuple_data_to_cell` turns `UnchangedToast` into either a
    /// recovered `Cell` (if the old row is available) or a `Missing` index
    /// (surfaced as `UpdatedTableRow::Partial`). `reconstruct_full_new_row` then
    /// overlays the partial row onto the full old image so no placeholder can
    /// reach the append. This test proves that invariant for the unpartitioned path.
    ///
    /// See also: `partial_update_reconstructs_full_image_from_old_row` (which
    /// covers the partitioned path) and the `overlay_partial_on_full` function,
    /// which is the mechanism that prevents unchanged-TOAST corruption.
    #[test]
    fn unchanged_toast_column_carries_real_old_value_in_append() {
        // Unpartitioned schema: id INT4 PK, label TEXT nullable, score INT4.
        let schema = unpartitioned_schema();
        let specs = build_staging_specs_with_cdc(
            &schema.column_schemas().cloned().collect::<Vec<_>>(),
        )
        .unwrap();

        // Simulate an unchanged-TOAST update: only `id` and `score` changed;
        // `label` is missing (index 1) — this is exactly what
        // `convert_update_tuple_to_updated_table_row` produces when PostgreSQL
        // emits `UnchangedToast` for a column it cannot recover from the old row.
        let old_full_row = TableRow::new(vec![
            Cell::I32(7),
            Cell::String("original-label".to_owned()),
            Cell::I32(10),
        ]);
        let present = TableRow::new(vec![
            Cell::I32(7),  // id unchanged
            Cell::I32(99), // score changed
        ]);
        // `label` (index 1) is missing — the unchanged-TOAST placeholder.
        let partial = PartialTableRow::new(3, present, vec![1]);

        let muts = vec![tracked(
            42,
            TableMutation::Update {
                delete_row: OldTableRow::Full(old_full_row),
                new_row: UpdatedTableRow::Partial(partial),
            },
        )];

        let ops = prepare_append_mutations(&schema, muts, false, &specs).unwrap();

        // Must produce exactly one Append (same-partition unpartitioned update).
        assert_eq!(ops.len(), 1);
        let PreparedTableMutation::Append { rows } = &ops[0] else {
            panic!("expected Append");
        };

        // The label column (index 1) must carry the real old value, not NULL or
        // a placeholder. If unchanged-TOAST leaked through, this would be NULL.
        let label_col = rows
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<duckdb::arrow::array::StringArray>()
            .expect("label column is Utf8");
        assert_eq!(
            label_col.value(0),
            "original-label",
            "unchanged-TOAST column must carry the real old-row value in the append image"
        );

        // The score column (index 2) must carry the new value.
        let score_col = rows
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<duckdb::arrow::array::Int32Array>()
            .expect("score column is Int32");
        assert_eq!(score_col.value(0), 99, "changed column carries the new value");
    }
}
