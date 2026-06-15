use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
};

use super::merge_on_read::DEDUP_ORDER_BY;

/// Shared transactional core: snapshot winners into a temp table, DELETE every
/// row from `table`, then INSERT back only the surviving live rows.
///
/// The QUALIFY window keeps the highest-version row per `id`; a tombstone
/// surviving as the winner causes its entire group to be dropped (because of
/// the `WHERE _etl_deleted IS NOT TRUE` on the INSERT). This is the full-table form —
/// callers that need incrementality must add their own scoping on top.
fn collapse_by_id(conn: &duckdb::Connection, table: &str) -> EtlResult<()> {
    let sql_begin = "BEGIN TRANSACTION";
    let sql_keep = format!(
        "CREATE OR REPLACE TEMP TABLE _keep AS \
         SELECT * FROM {table} \
         QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY {DEDUP_ORDER_BY}) = 1;"
    );
    let sql_delete = format!("DELETE FROM {table};");
    let sql_insert = format!(
        "INSERT INTO {table} SELECT * FROM _keep WHERE _etl_deleted IS NOT TRUE;"
    );
    let sql_commit = "COMMIT";

    conn.execute_batch(sql_begin).map_err(|err| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake compaction BEGIN TRANSACTION failed",
            format!("table={table}"),
            source: err
        )
    })?;

    let result = (|| -> EtlResult<()> {
        conn.execute_batch(&sql_keep).map_err(|err| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake compaction snapshot failed",
                format!("table={table}"),
                source: err
            )
        })?;
        conn.execute_batch(&sql_delete).map_err(|err| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake compaction delete failed",
                format!("table={table}"),
                source: err
            )
        })?;
        conn.execute_batch(&sql_insert).map_err(|err| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake compaction insert failed",
                format!("table={table}"),
                source: err
            )
        })?;
        Ok(())
    })();

    match result {
        Ok(()) => {
            conn.execute_batch(sql_commit).map_err(|err| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake compaction COMMIT failed",
                    format!("table={table}"),
                    source: err
                )
            })?;
            Ok(())
        }
        Err(err) => {
            if let Err(rollback_err) = conn.execute_batch("ROLLBACK") {
                tracing::error!(error = %rollback_err, "DuckLake compaction ROLLBACK failed");
            }
            Err(err)
        }
    }
}

/// Compacts the merge-on-read append log for `table`, collapsing multiple
/// versioned rows per `id` down to at most one surviving live row.
///
/// **Current behavior (full-table):** scans every row in `table` with a single
/// `PARTITION BY id ORDER BY _etl_version DESC, _etl_deleted ASC` window,
/// keeping only the highest-version row per `id` across all physical DuckLake
/// partitions. Rows whose surviving image is a tombstone (`_etl_deleted = true`)
/// are dropped entirely — the `id` disappears from the table. Because the
/// deduplication crosses all partitions in one pass, a partition-move tombstone
/// and its corresponding live image in a different partition are handled
/// correctly: the higher-version image wins and the lower-version one is removed.
///
/// **Implementation:** delegates to [`collapse_by_id`] which runs three
/// statements inside one transaction:
/// 1. `CREATE OR REPLACE TEMP TABLE _keep AS SELECT * QUALIFY …` — snapshot
///    winners without touching the target.
/// 2. `DELETE FROM <table>` — physically clear every row.
/// 3. `INSERT INTO <table> SELECT * FROM _keep WHERE _etl_deleted IS NOT TRUE` —
///    write back only live survivors.
///
/// # TODO: scope to recently-written partitions for incrementality
/// A future incremental pass would identify recently-written DuckLake partitions
/// via a watermark and restrict the DELETE + INSERT to those partition ranges,
/// making the cost O(new data) rather than O(table).
///
/// # Errors
/// Returns an `EtlError` wrapping the underlying `duckdb::Error` on any SQL
/// failure, mirroring the error handling pattern in `batches.rs`.
pub(super) fn compact_partition(conn: &duckdb::Connection, table: &str) -> EtlResult<()> {
    collapse_by_id(conn, table)
}

/// One-time cross-partition dedup pass run at cutover to remove
/// *backlog-move strandings*.
///
/// **Problem:** rows produced before `REPLICA IDENTITY FULL` was enabled may
/// have a partition-move recorded as a new-partition INSERT with no matching
/// tombstone for the old partition. After the backlog is loaded both images
/// exist as live rows with different `_etl_version` values. A per-partition
/// compaction pass cannot remove the stale image because it never sees a
/// tombstone for it — the two live rows live in different physical DuckLake
/// partitions.
///
/// **Solution:** a single full-table `PARTITION BY id` collapse (via
/// [`collapse_by_id`]) naturally dedups across physical partition boundaries
/// because DuckDB reads all rows into the same execution context. The
/// highest-version image per `id` survives; the lower-version stranded image
/// is dropped.
///
/// This function is **idempotent**: calling it on an already-clean table is a
/// no-op (the single surviving row per `id` is already the winner).
///
/// # Errors
/// Returns an `EtlError` wrapping the underlying `duckdb::Error` on any SQL
/// failure.
pub(super) fn global_dedup_by_id(conn: &duckdb::Connection, table: &str) -> EtlResult<()> {
    collapse_by_id(conn, table)
}

/// Runs merge-on-read compaction across all given tables, collapsing
/// multi-version append logs down to at most one live row per `id`.
///
/// Callers control invocation frequency — the maintenance interval in
/// contour-core determines how often this runs. Per-table version-count gating
/// (skipping tables with few versions) is a future optimization; for now every
/// listed table is compacted on each call.
///
/// Stops on the first error and returns it; tables listed after the failing one
/// are not processed.
///
/// # Errors
/// Returns an `EtlError` wrapping the underlying `duckdb::Error` on the first
/// table that fails.
pub fn run_merge_on_read_compaction(
    conn: &duckdb::Connection,
    tables: &[String],
) -> EtlResult<()> {
    for table in tables {
        tracing::info!(table = %table, "DuckLake: compacting merge-on-read table");
        compact_partition(conn, table)?;
    }
    Ok(())
}

/// Runs a one-time global dedup pass across all given tables, removing
/// backlog-move strandings that per-partition compaction cannot resolve.
///
/// This is the cutover pass — run once after the initial backlog load before
/// switching to incremental compaction via [`run_merge_on_read_compaction`].
///
/// Stops on the first error and returns it; tables listed after the failing one
/// are not processed.
///
/// # Errors
/// Returns an `EtlError` wrapping the underlying `duckdb::Error` on the first
/// table that fails.
pub fn run_merge_on_read_global_dedup(
    conn: &duckdb::Connection,
    tables: &[String],
) -> EtlResult<()> {
    for table in tables {
        tracing::info!(table = %table, "DuckLake: running global dedup on table");
        global_dedup_by_id(conn, table)?;
    }
    Ok(())
}

#[cfg(test)]
mod compaction_tests {
    use duckdb::Connection;

    use super::{compact_partition, global_dedup_by_id, run_merge_on_read_compaction};

    fn open() -> Connection {
        Connection::open_in_memory().unwrap()
    }

    fn setup(conn: &Connection) {
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER, credit INTEGER, _etl_version UHUGEINT, _etl_deleted BOOLEAN);",
        )
        .unwrap();
    }

    #[test]
    fn compaction_keeps_latest_drops_tombstone_group() {
        let conn = open();
        setup(&conn);

        conn.execute_batch(
            "INSERT INTO t VALUES
               (1, 10, 1, false),
               (1, 12, 2, false),
               (2, 5,  1, false),
               (2, 0,  2, true);",
        )
        .unwrap();

        compact_partition(&conn, "t").unwrap();

        let id1_credit: i32 = conn
            .query_row("SELECT credit FROM t WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(id1_credit, 12, "id=1: must keep the v2 live row with credit=12");

        let id2_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t WHERE id = 2", [], |r| r.get(0))
            .unwrap();
        assert_eq!(id2_count, 0, "id=2: tombstone group must be fully dropped");

        let total: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(total, 1, "exactly one live row survives");
    }

    #[test]
    fn compaction_live_beats_tombstone_at_same_version() {
        // Move case: both a tombstone and a live image arrive at the same
        // _etl_version. The _etl_deleted ASC tie-break in DEDUP_ORDER_BY means
        // the live row (deleted=false) sorts first, so it must survive.
        let conn = open();
        setup(&conn);

        conn.execute_batch(
            "INSERT INTO t VALUES
               (3, 99, 5, false),
               (3, 0,  5, true);",
        )
        .unwrap();

        compact_partition(&conn, "t").unwrap();

        let credit: i32 = conn
            .query_row("SELECT credit FROM t WHERE id = 3", [], |r| r.get(0))
            .unwrap();
        assert_eq!(credit, 99, "live image must survive when tombstone shares version");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t WHERE id = 3", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1, "exactly one row survives for id=3");
    }

    #[test]
    fn global_dedup_removes_cross_partition_stranded_image() {
        // Backlog-move shape: id=7 was moved from effective_month=3 to
        // effective_month=7. Because REPLICA IDENTITY FULL was not yet enabled,
        // the old-partition image has NO tombstone. Both live rows exist in the
        // table; the higher-version (v2, month=7) image must survive and the
        // stranded lower-version (v1, month=3) image must be dropped.
        let conn = open();
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER, effective_month INTEGER, credit INTEGER, \
             _etl_version UHUGEINT, _etl_deleted BOOLEAN);",
        )
        .unwrap();

        conn.execute_batch(
            "INSERT INTO t VALUES
               (7, 3, 10, 1, false),
               (7, 7, 10, 2, false);",
        )
        .unwrap();

        global_dedup_by_id(&conn, "t").unwrap();

        let total: i64 = conn
            .query_row("SELECT COUNT(*) FROM t WHERE id = 7", [], |r| r.get(0))
            .unwrap();
        assert_eq!(total, 1, "exactly one row survives for id=7 after dedup");

        let month: i32 = conn
            .query_row("SELECT effective_month FROM t WHERE id = 7", [], |r| r.get(0))
            .unwrap();
        assert_eq!(month, 7, "the higher-version (month=7) image must survive; stranded month=3 dropped");

        // Idempotency: calling again must leave the result unchanged.
        global_dedup_by_id(&conn, "t").unwrap();

        let total_after: i64 = conn
            .query_row("SELECT COUNT(*) FROM t WHERE id = 7", [], |r| r.get(0))
            .unwrap();
        assert_eq!(total_after, 1, "idempotent: still one row after second call");

        let month_after: i32 = conn
            .query_row("SELECT effective_month FROM t WHERE id = 7", [], |r| r.get(0))
            .unwrap();
        assert_eq!(month_after, 7, "idempotent: month=7 still survives after second call");
    }

    #[test]
    fn run_merge_on_read_compaction_compacts_multiple_tables() {
        // Two tables, each with a multi-version row and a tombstone group.
        // After run_merge_on_read_compaction both must be collapsed correctly:
        //   t1: id=1 (v1→v2 live) survives with credit=20; id=2 (tombstone) dropped.
        //   t2: id=10 (v1→v2 live) survives with score=99; id=20 (tombstone) dropped.
        let conn = open();

        conn.execute_batch(
            "CREATE TABLE t1 (id INTEGER, credit INTEGER, _etl_version UHUGEINT, _etl_deleted BOOLEAN);",
        )
        .unwrap();
        conn.execute_batch(
            "INSERT INTO t1 VALUES
               (1, 10, 1, false),
               (1, 20, 2, false),
               (2, 5,  1, false),
               (2, 0,  2, true);",
        )
        .unwrap();

        conn.execute_batch(
            "CREATE TABLE t2 (id INTEGER, score INTEGER, _etl_version UHUGEINT, _etl_deleted BOOLEAN);",
        )
        .unwrap();
        conn.execute_batch(
            "INSERT INTO t2 VALUES
               (10, 50,  1, false),
               (10, 99,  2, false),
               (20, 30,  1, false),
               (20, 0,   2, true);",
        )
        .unwrap();

        run_merge_on_read_compaction(&conn, &["t1".to_string(), "t2".to_string()]).unwrap();

        // t1 assertions
        let t1_credit: i32 = conn
            .query_row("SELECT credit FROM t1 WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(t1_credit, 20, "t1 id=1: must keep v2 live row with credit=20");

        let t1_id2_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t1 WHERE id = 2", [], |r| r.get(0))
            .unwrap();
        assert_eq!(t1_id2_count, 0, "t1 id=2: tombstone group must be fully dropped");

        let t1_total: i64 = conn
            .query_row("SELECT COUNT(*) FROM t1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(t1_total, 1, "t1: exactly one live row survives");

        // t2 assertions
        let t2_score: i32 = conn
            .query_row("SELECT score FROM t2 WHERE id = 10", [], |r| r.get(0))
            .unwrap();
        assert_eq!(t2_score, 99, "t2 id=10: must keep v2 live row with score=99");

        let t2_id20_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t2 WHERE id = 20", [], |r| r.get(0))
            .unwrap();
        assert_eq!(t2_id20_count, 0, "t2 id=20: tombstone group must be fully dropped");

        let t2_total: i64 = conn
            .query_row("SELECT COUNT(*) FROM t2", [], |r| r.get(0))
            .unwrap();
        assert_eq!(t2_total, 1, "t2: exactly one live row survives");
    }

    #[test]
    fn compaction_keeps_null_base_generation_rows() {
        // Regression guard for the in-place cutover data-loss bug.
        //
        // After ADD COLUMN IF NOT EXISTS, pre-migration "base generation" rows have
        // NULL _etl_version and NULL _etl_deleted. The old `WHERE NOT _etl_deleted`
        // filter treated NULL as falsy (NOT NULL = NULL, not TRUE) and dropped every
        // base row. The fix uses `_etl_deleted IS NOT TRUE` which preserves NULL rows.
        //
        // Two scenarios:
        //   id=1: pure base row (NULL version, NULL deleted) — must survive compaction.
        //   id=2: base row (NULL version, NULL deleted) PLUS a later live append
        //          (real version, deleted=false) — the append must win (NULLS LAST
        //          in ORDER BY causes the base row to sort after the real version),
        //          and the surviving row must carry the append's credit value.
        let conn = open();
        setup(&conn);

        conn.execute_batch(
            "INSERT INTO t (id, credit, _etl_version, _etl_deleted) VALUES
               (1, 10, NULL, NULL),
               (2, 20, NULL, NULL),
               (2, 25, 1,    false);",
        )
        .unwrap();

        compact_partition(&conn, "t").unwrap();

        // id=1: pure base row must survive (not dropped by IS NOT TRUE filter).
        let id1_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(id1_count, 1, "id=1: pure base-generation row must survive compaction");

        let id1_credit: i32 = conn
            .query_row("SELECT credit FROM t WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(id1_credit, 10, "id=1: base-generation row must retain its original value");

        // id=2: real append (v1, deleted=false) wins over base NULL row via NULLS LAST;
        // only one row survives and it carries the append's credit (25, not 20).
        let id2_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t WHERE id = 2", [], |r| r.get(0))
            .unwrap();
        assert_eq!(id2_count, 1, "id=2: exactly one row survives when base and append coexist");

        let id2_credit: i32 = conn
            .query_row("SELECT credit FROM t WHERE id = 2", [], |r| r.get(0))
            .unwrap();
        assert_eq!(id2_credit, 25, "id=2: real append must win over base-generation row (NULLS LAST)");

        let total: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(total, 2, "two rows survive: one pure base (id=1) and one real append (id=2)");
    }
}
