use std::collections::HashSet;

use chrono::Datelike;
// Consumed by later tasks (schema column construction, compaction SQL, etc.).
#[allow(dead_code)]
use etl::types::{EventSequenceKey, PgLsn};
use etl::types::{ArrayCell, Cell, ReplicatedTableSchema, TableRow, Type, is_array_type};

/// Column name for the packed CDC version written to every merge-on-read row.
// Consumed by later tasks (schema column construction, compaction SQL, etc.).
#[allow(dead_code)]
pub const ETL_VERSION_COLUMN: &str = "_etl_version";

/// Partition-key column on the partitioned in-scope fact tables
/// (`public_lines`, `public_measurements`). Its `(year, month)` determines a
/// row's partition, so a change to it across an update is a partition move.
pub const EFFECTIVE_AT_LOCAL_COLUMN: &str = "effective_at_local";

/// Column name for the boolean tombstone flag written on DELETE rows.
// Consumed by later tasks (schema column construction, compaction SQL, etc.).
#[allow(dead_code)]
pub const ETL_DELETED_COLUMN: &str = "_etl_deleted";

/// DuckDB SQL type used for the version column. UHUGEINT is a single 128-bit
/// unsigned integer that preserves the natural u128 ordering produced by
/// [`version_u128`], so `ORDER BY _etl_version DESC` correctly picks the
/// most-recent row during merge-on-read deduplication.
// Consumed by later tasks (schema column construction, compaction SQL, etc.).
#[allow(dead_code)]
pub const ETL_VERSION_SQL_TYPE: &str = "UHUGEINT";

/// ORDER BY clause fragment used in deduplication queries and compaction views.
/// Descending version picks the latest mutation; ascending deleted places
/// non-deleted rows first among ties (so a live row beats an older tombstone
/// if they somehow share a version).
// Consumed by later tasks (schema column construction, compaction SQL, etc.).
#[allow(dead_code)]
pub const DEDUP_ORDER_BY: &str = "_etl_version DESC, _etl_deleted ASC";

/// Packs `(commit_lsn, tx_ordinal)` into a monotone `u128` version key.
///
/// `commit_lsn` occupies the high 64 bits so cross-transaction ordering is
/// preserved, and `tx_ordinal` occupies the low 64 bits so multiple events
/// within the same transaction sort correctly.
///
/// The result is written to the `_etl_version` column (`UHUGEINT`) so that
/// `ORDER BY _etl_version DESC` selects the most-recent row during
/// merge-on-read deduplication.
// Consumed by later tasks (appender / row encoding, etc.).
#[allow(dead_code)]
pub fn version_u128(commit_lsn: PgLsn, tx_ordinal: u64) -> u128 {
    EventSequenceKey::new(commit_lsn, tx_ordinal).as_u128()
}

/// Returns `true` if an update moved the row to a different `(year, month)` partition
/// of the `effective_at_local` column (identified by `eff_idx`).
///
/// Conservatively returns `true` when either value is missing or NULL so a stale
/// row is never stranded in the wrong partition.
pub fn is_partition_move(old: &TableRow, new: &TableRow, eff_idx: usize) -> bool {
    match (eff_ym(old, eff_idx), eff_ym(new, eff_idx)) {
        (Some(a), Some(b)) => a != b,
        _ => true,
    }
}

/// Extracts `(year, month)` from the `effective_at_local` cell at `i`, or
/// `None` if the cell is absent or not a timestamptz.
fn eff_ym(row: &TableRow, i: usize) -> Option<(i32, u32)> {
    match row.values().get(i) {
        Some(Cell::TimestampTz(ts)) => Some((ts.year(), ts.month())),
        _ => None,
    }
}

/// Returns a tombstone image for a deleted or partition-moved row.
///
/// The tombstone is the old row image itself so it carries the old partition key.
/// `_etl_deleted` is applied later at encode time, not here.
pub fn build_tombstone_image(old: TableRow) -> TableRow {
    old
}

/// Expands a PK-only key row to full column width for unpartitioned
/// merge-on-read tables.
///
/// PK columns keep their real values. Non-PK columns get `Cell::Null` if
/// nullable, or a type-appropriate zero value if non-nullable (since the
/// appender must write a well-formed row for every column). Array types always
/// use an empty array regardless of nullability, mirroring the ClickHouse
/// `expand_key_row` logic in `crates/etl-destinations/src/clickhouse/core.rs`.
pub fn expand_key_row(key_row: TableRow, schema: &ReplicatedTableSchema) -> TableRow {
    let key_cells = key_row.into_values();
    let mut key_iter = key_cells.into_iter();
    let cells: Vec<Cell> = schema
        .column_schemas()
        .map(|col| {
            if col.primary_key_ordinal_position.is_some() {
                key_iter.next().unwrap_or(Cell::Null)
            } else if col.nullable && !is_array_type(&col.typ) {
                Cell::Null
            } else {
                default_cell(&col.typ)
            }
        })
        .collect();
    TableRow::new(cells)
}

/// Returns a zero-value `Cell` for a Postgres type, mirroring the ClickHouse
/// `default_cell` helper. Array types produce empty arrays. All other
/// non-primitive types fall back to an empty `String`.
fn default_cell(typ: &Type) -> Cell {
    match *typ {
        Type::BOOL => Cell::Bool(false),
        Type::INT2 => Cell::I16(0),
        Type::INT4 => Cell::I32(0),
        Type::INT8 => Cell::I64(0),
        Type::OID => Cell::U32(0),
        Type::FLOAT4 => Cell::F32(0.0),
        Type::FLOAT8 => Cell::F64(0.0),
        Type::DATE => Cell::Date(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()),
        Type::TIMESTAMP => Cell::Timestamp(chrono::DateTime::UNIX_EPOCH.naive_utc()),
        Type::TIMESTAMPTZ => Cell::TimestampTz(chrono::DateTime::UNIX_EPOCH),
        Type::UUID => Cell::Uuid(uuid::Uuid::nil()),
        Type::CHAR
        | Type::BPCHAR
        | Type::VARCHAR
        | Type::NAME
        | Type::TEXT
        | Type::NUMERIC
        | Type::MONEY
        | Type::TIME
        | Type::JSON
        | Type::JSONB
        | Type::BYTEA => Cell::String(String::new()),
        Type::BOOL_ARRAY => Cell::Array(ArrayCell::Bool(Vec::new())),
        Type::INT2_ARRAY => Cell::Array(ArrayCell::I16(Vec::new())),
        Type::INT4_ARRAY => Cell::Array(ArrayCell::I32(Vec::new())),
        Type::INT8_ARRAY => Cell::Array(ArrayCell::I64(Vec::new())),
        Type::OID_ARRAY => Cell::Array(ArrayCell::U32(Vec::new())),
        Type::FLOAT4_ARRAY => Cell::Array(ArrayCell::F32(Vec::new())),
        Type::FLOAT8_ARRAY => Cell::Array(ArrayCell::F64(Vec::new())),
        Type::TEXT_ARRAY
        | Type::VARCHAR_ARRAY
        | Type::CHAR_ARRAY
        | Type::BPCHAR_ARRAY
        | Type::NAME_ARRAY
        | Type::MONEY_ARRAY => Cell::Array(ArrayCell::String(Vec::new())),
        Type::NUMERIC_ARRAY => Cell::Array(ArrayCell::Numeric(Vec::new())),
        Type::DATE_ARRAY => Cell::Array(ArrayCell::Date(Vec::new())),
        Type::TIME_ARRAY => Cell::Array(ArrayCell::Time(Vec::new())),
        Type::TIMESTAMP_ARRAY => Cell::Array(ArrayCell::Timestamp(Vec::new())),
        Type::TIMESTAMPTZ_ARRAY => Cell::Array(ArrayCell::TimestampTz(Vec::new())),
        Type::UUID_ARRAY => Cell::Array(ArrayCell::Uuid(Vec::new())),
        Type::JSON_ARRAY | Type::JSONB_ARRAY => Cell::Array(ArrayCell::Json(Vec::new())),
        Type::BYTEA_ARRAY => Cell::Array(ArrayCell::Bytes(Vec::new())),
        _ if is_array_type(typ) => Cell::Array(ArrayCell::String(Vec::new())),
        _ => Cell::String(String::new()),
    }
}

/// Predicate that identifies which tables use merge-on-read CDC semantics and
/// which of those are additionally partitioned.
///
/// For tables in scope, CDC mutations become append-only rows annotated with
/// `_etl_version` and `_etl_deleted`. The view layer merges on read.
/// Tables not in this scope retain the default in-place update/delete behavior.
///
/// Partitioned tables (`public_lines`, `public_measurements`) require
/// partition-aware handling during compaction; non-partitioned tables in scope
/// (e.g. `public_observations`) do not.
#[derive(Clone, Debug, Default)]
pub struct MergeOnReadScope {
    tables: HashSet<String>,
    partitioned: HashSet<String>,
}

impl MergeOnReadScope {
    /// Builds a scope from an iterable of table names.
    ///
    /// Tables named `public_lines` or `public_measurements` are automatically
    /// marked as partitioned within the scope.
    pub fn from_tables<I, S>(t: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let tables: HashSet<String> = t.into_iter().map(Into::into).collect();
        let partitioned = tables
            .iter()
            .filter(|t| *t == "public_lines" || *t == "public_measurements")
            .cloned()
            .collect();
        Self { tables, partitioned }
    }

    /// Returns `true` if `t` is in the merge-on-read scope.
    pub fn contains(&self, t: &str) -> bool {
        self.tables.contains(t)
    }

    /// Returns `true` if `t` is in the merge-on-read scope and is partitioned.
    pub fn is_partitioned(&self, t: &str) -> bool {
        self.partitioned.contains(t)
    }

    /// Returns all table names in the merge-on-read scope.
    pub fn tables(&self) -> Vec<String> {
        self.tables.iter().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_membership() {
        let s = MergeOnReadScope::from_tables(["public_lines", "public_observations"]);
        assert!(s.contains("public_lines"));
        assert!(s.contains("public_observations"));
        assert!(!s.contains("public_dimension__values"));
        assert!(s.is_partitioned("public_lines"));
        assert!(!s.is_partitioned("public_observations"));
    }
}

#[cfg(test)]
mod partition_move_tests {
    use std::sync::Arc;

    use chrono::DateTime;
    use etl::types::{
        Cell, ColumnSchema, TableId, TableName, TableRow, TableSchema, Type,
        ReplicatedTableSchema,
    };
    use tokio_postgres::types::Oid;

    use super::{build_tombstone_image, default_cell, expand_key_row, is_partition_move};

    /// Index of the `effective_at_local` cell within `row_eff`.
    fn eff_idx() -> usize {
        0
    }

    /// Builds a `TableRow` whose only cell is a `TimestampTz` parsed from an
    /// RFC 3339 string. `eff_idx()` identifies that cell.
    fn row_eff(ts_str: &str) -> TableRow {
        let ts: DateTime<chrono::Utc> = ts_str.parse().expect("valid RFC 3339 timestamp");
        TableRow::new(vec![Cell::TimestampTz(ts)])
    }

    /// Reads the `effective_at_local` cell back from a row.
    fn cell_eff(row: &TableRow) -> &Cell {
        &row.values()[eff_idx()]
    }

    /// Builds a 3-column `ReplicatedTableSchema`:
    ///   col 0: `id` INT4 PK not-null
    ///   col 1: `label` TEXT nullable
    ///   col 2: `score` INT4 not-null (non-nullable, non-PK)
    fn three_col_schema() -> ReplicatedTableSchema {
        let table_schema = TableSchema::new(
            TableId::new(Oid::from(1u32)),
            TableName::new("public".to_owned(), "test_table".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, Some(1), false),
                ColumnSchema::new("label".to_owned(), Type::TEXT, -1, 2, None, true),
                ColumnSchema::new("score".to_owned(), Type::INT4, -1, 3, None, false),
            ],
        );
        ReplicatedTableSchema::all(Arc::new(table_schema))
    }

    #[test]
    fn move_on_month_change() {
        assert!(is_partition_move(
            &row_eff("2026-03-15T00:00:00Z"),
            &row_eff("2026-07-02T00:00:00Z"),
            eff_idx()
        ));
    }

    #[test]
    fn no_move_same_month() {
        assert!(!is_partition_move(
            &row_eff("2026-03-01T00:00:00Z"),
            &row_eff("2026-03-28T00:00:00Z"),
            eff_idx()
        ));
    }

    #[test]
    fn tombstone_keeps_old_partition_key() {
        let old = row_eff("2026-03-15T00:00:00Z");
        let old_copy = old.clone();
        let tombstone = build_tombstone_image(old);
        assert_eq!(cell_eff(&tombstone), cell_eff(&old_copy));
    }

    #[test]
    fn missing_cell_is_conservative_move() {
        // If eff_idx is out of bounds, treat as a partition move.
        let row = TableRow::new(vec![Cell::Null]);
        assert!(is_partition_move(&row, &row_eff("2026-03-15T00:00:00Z"), 99));
    }

    #[test]
    fn null_cell_is_conservative_move() {
        let row = TableRow::new(vec![Cell::Null]);
        assert!(is_partition_move(&row, &row_eff("2026-03-15T00:00:00Z"), eff_idx()));
    }

    #[test]
    fn expand_key_row_fills_non_pk_columns() {
        let schema = three_col_schema();
        // Key row has only the PK value (id = 42).
        let key_row = TableRow::new(vec![Cell::I32(42)]);
        let expanded = expand_key_row(key_row, &schema);
        let vals = expanded.values();
        // col 0: PK kept
        assert_eq!(vals[0], Cell::I32(42));
        // col 1: nullable TEXT -> Null
        assert_eq!(vals[1], Cell::Null);
        // col 2: non-nullable INT4 -> zero
        assert_eq!(vals[2], default_cell(&Type::INT4));
    }
}

/// Probes whether DuckDB supports UHUGEINT with correct ordering semantics.
///
/// UHUGEINT is a 128-bit unsigned integer type added in DuckDB 0.10. The probe
/// inserts three rows (including a value exceeding u64::MAX to exercise the
/// high bits) and verifies that QUALIFY + ORDER BY selects the largest value
/// correctly. A PASS here confirms we can store `_etl_version` as UHUGEINT and
/// rely on `ORDER BY _etl_version DESC` for dedup ordering.
#[cfg(test)]
mod version_probe {
    use duckdb::Connection;

    #[test]
    fn uhugeint_roundtrips_and_orders() {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch(
            "CREATE TABLE t(id BIGINT, v UHUGEINT);
             INSERT INTO t VALUES (1,0),(1,18446744073709551616),(1,1);",
        )
        .unwrap();
        let m: String = c
            .query_row(
                "SELECT v::VARCHAR FROM t \
                 QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY v DESC) = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(m, "18446744073709551616");
    }
}

#[cfg(test)]
mod version_ordering_tests {
    use super::*;
    use etl::types::PgLsn;

    #[test]
    fn version_ordering() {
        assert!(version_u128(PgLsn::from(5u64), 1) > version_u128(PgLsn::from(5u64), 0));
        assert!(version_u128(PgLsn::from(6u64), 0) > version_u128(PgLsn::from(5u64), 1));
    }
}
