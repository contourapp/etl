use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
};

use super::merge_on_read::DEDUP_ORDER_BY;

/// Compacts the merge-on-read append log for `table`, collapsing multiple
/// versioned rows per `id` down to at most one surviving live row.
///
/// **Correctness model:** for each `id`, keep only the max-version row
/// (tie-breaking `_etl_deleted ASC` so a live image beats a tombstone at equal
/// version). If the surviving row is a tombstone, the entire `id` group is
/// dropped — a deleted row vanishes; a moved-away partition row disappears while
/// its live image survives in the new partition's own compaction pass.
///
/// **Implementation:** runs three statements inside one transaction:
/// 1. `CREATE OR REPLACE TEMP TABLE _keep AS SELECT * QUALIFY …` — snapshot
///    winners without touching the target.
/// 2. `DELETE FROM <table>` — physically clear every row.
/// 3. `INSERT INTO <table> SELECT * FROM _keep WHERE NOT _etl_deleted` —
///    write back only live survivors.
///
/// # TODO: scope to recently-written partitions for incrementality
/// The current implementation is a full-table collapse, which is correct but
/// O(table). A future incremental pass would identify recently-written
/// DuckLake partitions via a watermark and restrict the DELETE + INSERT to
/// those partition ranges, making the cost O(new data) rather than O(table).
///
/// # Errors
/// Returns an `EtlError` wrapping the underlying `duckdb::Error` on any SQL
/// failure, mirroring the error handling pattern in `batches.rs`.
// Consumed by Task 11 (maintenance task wiring).
#[allow(dead_code)]
pub(super) fn compact_partition(conn: &duckdb::Connection, table: &str) -> EtlResult<()> {
    let sql_begin = "BEGIN TRANSACTION";
    let sql_keep = format!(
        "CREATE OR REPLACE TEMP TABLE _keep AS \
         SELECT * FROM {table} \
         QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY {DEDUP_ORDER_BY}) = 1;"
    );
    let sql_delete = format!("DELETE FROM {table};");
    let sql_insert = format!(
        "INSERT INTO {table} SELECT * FROM _keep WHERE NOT _etl_deleted;"
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

#[cfg(test)]
mod compaction_tests {
    use duckdb::Connection;

    use super::compact_partition;

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
}
