use std::collections::HashSet;

// Consumed by later tasks (schema column construction, compaction SQL, etc.).
#[allow(dead_code)]
use etl::types::{EventSequenceKey, PgLsn};

/// Column name for the packed CDC version written to every merge-on-read row.
// Consumed by later tasks (schema column construction, compaction SQL, etc.).
#[allow(dead_code)]
pub const ETL_VERSION_COLUMN: &str = "_etl_version";

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
