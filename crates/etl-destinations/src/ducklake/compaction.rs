use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
};

use super::merge_on_read::{DEDUP_ORDER_BY, EFFECTIVE_AT_LOCAL_COLUMN};
use super::sql::qualified_lake_table_name;

/// Collapses one scope — the whole table (`where_pred = None`) or a single
/// partition / id-set — to one live row per `id` via a snapshot-temp / `DELETE` /
/// re-`INSERT` rewrite in one transaction (`DEDUP_ORDER_BY` picks the winner;
/// tombstone winners are dropped).
///
/// Two guards: empty or already-collapsed scopes skip before any transaction
/// (this is what makes the per-partition driver incremental); a collapse to zero
/// live rows skips the `DELETE`, since DuckLake 1.5.3 FATALs (uncatchable process
/// abort) on a collapse-to-empty — the tombstones are left in place and reads
/// ignore them anyway.
fn collapse_scope(
    conn: &duckdb::Connection,
    table: &str,
    where_pred: Option<&str>,
) -> EtlResult<()> {
    let qualified = qualified_lake_table_name(table);
    let where_sql = match where_pred {
        Some(pred) => format!(" WHERE {pred}"),
        None => String::new(),
    };

    let probe = |label: &'static str, sql: String| -> EtlResult<i64> {
        conn.query_row(&sql, [], |r| r.get(0)).map_err(|err| {
            etl_error!(ErrorKind::DestinationQueryFailed, label, format!("table={table}"), source: err)
        })
    };
    let (rows, distinct_ids, tombstones): (i64, i64, i64) = conn
        .query_row(
            &format!(
                "SELECT count(*), count(DISTINCT id), \
                 count(*) FILTER (WHERE _etl_deleted IS TRUE) FROM {qualified}{where_sql}"
            ),
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .map_err(|err| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake compaction scope probe failed",
                format!("table={table}"),
                source: err
            )
        })?;
    // Nothing to collapse: empty scope, or already one row per id with no
    // tombstones to drop.
    if rows == 0 || (rows == distinct_ids && tombstones == 0) {
        return Ok(());
    }

    conn.execute_batch("BEGIN TRANSACTION").map_err(|err| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake compaction BEGIN TRANSACTION failed",
            format!("table={table}"),
            source: err
        )
    })?;

    let result = (|| -> EtlResult<()> {
        conn.execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE _keep AS \
             SELECT * FROM {qualified}{where_sql} \
             QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY {DEDUP_ORDER_BY}) = 1;"
        ))
        .map_err(|err| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake compaction snapshot failed",
                format!("table={table}"),
                source: err
            )
        })?;

        let live = probe(
            "DuckLake compaction survivor count failed",
            "SELECT count(*) FROM _keep WHERE _etl_deleted IS NOT TRUE".to_owned(),
        )?;
        // Collapse-to-empty: skip the destructive rewrite (see the fn doc).
        if live == 0 {
            return Ok(());
        }

        conn.execute_batch(&format!("DELETE FROM {qualified}{where_sql};"))
            .map_err(|err| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake compaction delete failed",
                    format!("table={table}"),
                    source: err
                )
            })?;
        conn.execute_batch(&format!(
            "INSERT INTO {qualified} SELECT * FROM _keep WHERE _etl_deleted IS NOT TRUE;"
        ))
        .map_err(|err| {
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
            conn.execute_batch("COMMIT").map_err(|err| {
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

/// Lists the distinct `(year, month)` partitions of `effective_at_local` present
/// in a partitioned table.
fn distinct_month_partitions(
    conn: &duckdb::Connection,
    table: &str,
) -> EtlResult<Vec<(i64, i64)>> {
    let qualified = qualified_lake_table_name(table);
    let mut stmt = conn
        .prepare(&format!(
            "SELECT DISTINCT year({EFFECTIVE_AT_LOCAL_COLUMN}) AS y, \
             month({EFFECTIVE_AT_LOCAL_COLUMN}) AS m FROM {qualified} \
             WHERE {EFFECTIVE_AT_LOCAL_COLUMN} IS NOT NULL ORDER BY y, m"
        ))
        .map_err(|err| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake compaction partition listing failed",
                format!("table={table}"),
                source: err
            )
        })?;
    let parts = stmt
        .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))
        .and_then(|rows| rows.collect::<Result<Vec<_>, _>>())
        .map_err(|err| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake compaction partition listing failed",
                format!("table={table}"),
                source: err
            )
        })?;
    Ok(parts)
}

/// Compacts one merge-on-read table to one live row per `id`. Partitioned tables
/// collapse one `(year, month)` of `effective_at_local` at a time so memory/spill
/// stay bounded (forecasts are just more month partitions) and per-partition
/// guards make it incremental; unpartitioned tables take a single guarded pass.
pub(super) fn compact_table(
    conn: &duckdb::Connection,
    table: &str,
    partitioned: bool,
) -> EtlResult<()> {
    if !partitioned {
        return collapse_scope(conn, table, None);
    }
    for (year, month) in distinct_month_partitions(conn, table)? {
        let pred = format!(
            "year({EFFECTIVE_AT_LOCAL_COLUMN}) = {year} AND \
             month({EFFECTIVE_AT_LOCAL_COLUMN}) = {month}"
        );
        collapse_scope(conn, table, Some(&pred))?;
    }
    // Rows without an effective_at_local fall outside every month partition;
    // collapse them separately (guarded, so a no-op when none exist).
    collapse_scope(conn, table, Some(&format!("{EFFECTIVE_AT_LOCAL_COLUMN} IS NULL")))
}

/// Runs incremental merge-on-read compaction across the given tables. Each entry
/// is `(table_name, is_partitioned)`. Stops on the first error.
pub fn run_merge_on_read_compaction(
    conn: &duckdb::Connection,
    tables: &[(String, bool)],
) -> EtlResult<()> {
    for (table, partitioned) in tables {
        tracing::info!(table = %table, "DuckLake: compacting merge-on-read table");
        compact_table(conn, table, *partitioned)?;
    }
    Ok(())
}

#[cfg(test)]
mod compaction_tests {
    use duckdb::Connection;

    use super::{compact_table, run_merge_on_read_compaction};

    fn open() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        // Production merge-on-read tables live in the DuckLake `lake` catalog, and
        // collapse_by_id references them as "lake"."<table>". Attach an in-memory
        // catalog named `lake` and make it the default so the bare-name table SQL
        // in these tests resolves to the same tables collapse_by_id targets.
        conn.execute_batch("ATTACH ':memory:' AS lake; USE lake;").unwrap();
        conn
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

        compact_table(&conn, "t", false).unwrap();

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

        compact_table(&conn, "t", false).unwrap();

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

        run_merge_on_read_compaction(&conn, &[("t1".to_string(), false), ("t2".to_string(), false)]).unwrap();

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

        compact_table(&conn, "t", false).unwrap();

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

    /// Regression guard for the production crash: an all-tombstone scope collapses
    /// to zero live rows, so the destructive DELETE is skipped (DuckLake 1.5.3
    /// FATALs on a collapse-to-empty) and the rows are left untouched.
    #[test]
    fn collapse_to_empty_scope_is_skipped() {
        let conn = open();
        setup(&conn);
        conn.execute_batch(
            "INSERT INTO t VALUES
               (1, 0, 1, true),
               (1, 0, 2, true),
               (2, 0, 1, true);",
        )
        .unwrap();

        compact_table(&conn, "t", false).unwrap();

        let total: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(total, 3, "all-tombstone scope: DELETE skipped, rows left in place");
    }

    /// Already-collapsed scope (one live row per id) is a cheap no-op — the
    /// steady-state path that makes per-partition compaction incremental.
    #[test]
    fn already_collapsed_scope_is_noop() {
        let conn = open();
        setup(&conn);
        conn.execute_batch("INSERT INTO t VALUES (1, 10, 1, false), (2, 20, 1, false);")
            .unwrap();

        compact_table(&conn, "t", false).unwrap();

        let total: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(total, 2, "already collapsed: unchanged");
    }

    /// Per-partition collapse keeps the latest version per id within each month
    /// and drops tombstoned ids.
    #[test]
    fn partitioned_compaction_collapses_each_month() {
        let conn = open();
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER, credit INTEGER, effective_at_local TIMESTAMP, \
             _etl_version UHUGEINT, _etl_deleted BOOLEAN);",
        )
        .unwrap();
        conn.execute_batch(
            "INSERT INTO t VALUES
               (1, 10, TIMESTAMP '2026-05-10', 1, false),
               (1, 12, TIMESTAMP '2026-05-10', 2, false),
               (2, 5,  TIMESTAMP '2026-05-11', 1, false),
               (2, 0,  TIMESTAMP '2026-05-11', 2, true),
               (3, 7,  TIMESTAMP '2026-07-01', 1, false),
               (3, 9,  TIMESTAMP '2026-07-01', 2, false);",
        )
        .unwrap();

        compact_table(&conn, "t", true).unwrap();

        let c1: i32 =
            conn.query_row("SELECT credit FROM t WHERE id = 1", [], |r| r.get(0)).unwrap();
        assert_eq!(c1, 12, "id=1: May v2 survives");
        let c2: i64 =
            conn.query_row("SELECT COUNT(*) FROM t WHERE id = 2", [], |r| r.get(0)).unwrap();
        assert_eq!(c2, 0, "id=2: tombstoned, dropped");
        let c3: i32 =
            conn.query_row("SELECT credit FROM t WHERE id = 3", [], |r| r.get(0)).unwrap();
        assert_eq!(c3, 9, "id=3: July v2 survives");
        let total: i64 = conn.query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0)).unwrap();
        assert_eq!(total, 2, "one live row each for id=1 (May) and id=3 (July)");
    }

    /// Per-month independence: id=1 is live in both May and July; `compact_table`
    /// collapses each month separately so both survive (a full-table collapse
    /// would leave one).
    #[test]
    fn compact_table_collapses_months_independently() {
        let conn = open();
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER, credit INTEGER, effective_at_local TIMESTAMP, \
             _etl_version UHUGEINT, _etl_deleted BOOLEAN);",
        )
        .unwrap();
        conn.execute_batch(
            "INSERT INTO t VALUES
               (1, 10, TIMESTAMP '2026-05-10', 1, false),
               (1, 12, TIMESTAMP '2026-05-10', 2, false),
               (1, 50, TIMESTAMP '2026-07-01', 3, false),
               (1, 99, TIMESTAMP '2026-07-01', 4, false);",
        )
        .unwrap();

        compact_table(&conn, "t", true).unwrap();

        let total: i64 = conn.query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0)).unwrap();
        assert_eq!(total, 2, "per-month independence: id=1 keeps one row in each of May and July");
        let may: i32 = conn
            .query_row("SELECT credit FROM t WHERE month(effective_at_local) = 5", [], |r| r.get(0))
            .unwrap();
        assert_eq!(may, 12, "May: v2 wins");
        let jul: i32 = conn
            .query_row("SELECT credit FROM t WHERE month(effective_at_local) = 7", [], |r| r.get(0))
            .unwrap();
        assert_eq!(jul, 99, "July: v4 wins");
    }
}
