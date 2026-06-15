use etl::types::{ColumnSchema, Type, is_array_type};

use crate::ducklake::{
    merge_on_read::{ETL_DELETED_COLUMN, ETL_VERSION_COLUMN, ETL_VERSION_SQL_TYPE},
    sql::{qualified_lake_table_name, quote_identifier},
};

/// Returns the DuckLake SQL type string for a given Postgres scalar type.
fn postgres_scalar_type_to_ducklake_sql(typ: &Type) -> &'static str {
    match typ {
        &Type::BOOL => "boolean",
        &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT => "varchar",
        &Type::INT2 => "smallint",
        &Type::INT4 => "integer",
        &Type::INT8 => "bigint",
        &Type::FLOAT4 => "float",
        &Type::FLOAT8 => "double",
        // Contour: DECIMAL preserves arithmetic; NaN/Infinity coerced to NULL at write time.
        &Type::NUMERIC => "DECIMAL(38, 10)",
        &Type::DATE => "date",
        &Type::TIME => "time",
        &Type::TIMESTAMP => "timestamp",
        &Type::TIMESTAMPTZ => "timestamptz",
        &Type::UUID => "uuid",
        &Type::JSON | &Type::JSONB => "json",
        &Type::OID => "ubigint",
        &Type::BYTEA => "blob",
        _ => "varchar",
    }
}

/// Returns the DuckDB SQL type string for a given Postgres array type.
fn postgres_array_type_to_ducklake_sql(typ: &Type) -> &'static str {
    match typ {
        &Type::BOOL_ARRAY => "boolean[]",
        &Type::CHAR_ARRAY
        | &Type::BPCHAR_ARRAY
        | &Type::VARCHAR_ARRAY
        | &Type::NAME_ARRAY
        | &Type::TEXT_ARRAY => "varchar[]",
        &Type::INT2_ARRAY => "smallint[]",
        &Type::INT4_ARRAY => "integer[]",
        &Type::INT8_ARRAY => "bigint[]",
        &Type::FLOAT4_ARRAY => "float[]",
        &Type::FLOAT8_ARRAY => "double[]",
        &Type::NUMERIC_ARRAY => "DECIMAL(38, 10)[]",
        &Type::DATE_ARRAY => "date[]",
        &Type::TIME_ARRAY => "time[]",
        &Type::TIMESTAMP_ARRAY => "timestamp[]",
        &Type::TIMESTAMPTZ_ARRAY => "timestamptz[]",
        &Type::UUID_ARRAY => "uuid[]",
        &Type::JSON_ARRAY | &Type::JSONB_ARRAY => "json[]",
        &Type::OID_ARRAY => "ubigint[]",
        &Type::BYTEA_ARRAY => "blob[]",
        _ => "varchar[]",
    }
}

/// Maps a Postgres range type to a DuckDB STRUCT DDL type.
fn postgres_range_type_to_ducklake_sql(typ: &Type) -> Option<String> {
    let inner = match *typ {
        Type::INT4_RANGE => "integer",
        Type::INT8_RANGE => "bigint",
        Type::NUM_RANGE => "DECIMAL(38, 10)",
        Type::TS_RANGE => "timestamp",
        Type::TSTZ_RANGE => "timestamptz",
        Type::DATE_RANGE => "date",
        _ => return None,
    };
    Some(format!("STRUCT(\"lower\" {inner}, \"upper\" {inner})"))
}

/// Maps a Postgres range array type to a DuckDB STRUCT[] DDL type.
fn postgres_range_array_type_to_ducklake_sql(typ: &Type) -> Option<String> {
    let inner = match *typ {
        Type::INT4_RANGE_ARRAY => "integer",
        Type::INT8_RANGE_ARRAY => "bigint",
        Type::NUM_RANGE_ARRAY => "DECIMAL(38, 10)",
        Type::TS_RANGE_ARRAY => "timestamp",
        Type::TSTZ_RANGE_ARRAY => "timestamptz",
        Type::DATE_RANGE_ARRAY => "date",
        _ => return None,
    };
    Some(format!("STRUCT(\"lower\" {inner}, \"upper\" {inner})[]"))
}

/// Returns the DuckLake SQL type string for a Postgres column type.
fn postgres_column_type_to_ducklake_sql(typ: &Type) -> String {
    if let Some(range_sql) = postgres_range_type_to_ducklake_sql(typ) {
        return range_sql;
    }
    if let Some(range_array_sql) = postgres_range_array_type_to_ducklake_sql(typ) {
        return range_array_sql;
    }
    if is_array_type(typ) {
        postgres_array_type_to_ducklake_sql(typ).to_owned()
    } else {
        postgres_scalar_type_to_ducklake_sql(typ).to_owned()
    }
}

/// Builds one DuckLake column definition.
///
/// For example, a non-null source `name text` column becomes
/// `"name" varchar not null`.
fn ducklake_column_definition(column_schema: &ColumnSchema, include_not_null: bool) -> String {
    let column_name = quote_identifier(&column_schema.name);
    let duckdb_type = postgres_column_type_to_ducklake_sql(&column_schema.typ);
    let nullability = if include_not_null && !column_schema.nullable { " not null" } else { "" };
    format!("{column_name} {duckdb_type}{nullability}")
}

/// Builds a `create table if not exists` DDL statement for the given table name
/// and schema.
///
/// The supplied columns are the destination-visible replicated columns in
/// write order.
pub(super) fn build_create_table_sql_ducklake(
    table_name: &str,
    column_schemas: &[ColumnSchema],
) -> String {
    let table_name = qualified_lake_table_name(table_name);
    let col_defs: Vec<String> = column_schemas
        .iter()
        .map(|col| format!("  {}", ducklake_column_definition(col, true)))
        .collect();

    format!("create table if not exists {table_name} ({})", col_defs.join(",\n"))
}

/// Builds a `create table if not exists` DDL statement with the two trailing
/// merge-on-read CDC columns (`_etl_version UHUGEINT`, `_etl_deleted BOOLEAN`).
///
/// Both columns are nullable so existing rows written before CDC was enabled
/// do not require a rewrite — NULL version = base generation, NULL deleted = live.
pub(super) fn build_create_table_sql_with_cdc(
    table_name: &str,
    column_schemas: &[ColumnSchema],
) -> String {
    let mut sql = build_create_table_sql_ducklake(table_name, column_schemas);
    let trailing = format!(
        ",\n  {} {},\n  {} BOOLEAN",
        quote_identifier(ETL_VERSION_COLUMN),
        ETL_VERSION_SQL_TYPE,
        quote_identifier(ETL_DELETED_COLUMN)
    );
    let close = sql.rfind(')').expect("create table has closing paren");
    sql.insert_str(close, &trailing);
    sql
}

/// Builds one or two `ALTER TABLE … ADD COLUMN IF NOT EXISTS` statements that
/// add the merge-on-read CDC columns to an already-existing table.
///
/// Both columns are added as nullable so existing rows need no backfill.
/// `IF NOT EXISTS` makes the statements safe to replay.
pub(super) fn build_add_cdc_columns_sql(qualified_table: &str) -> String {
    format!(
        "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS {v} {vt};\nALTER TABLE {t} ADD COLUMN IF NOT EXISTS {d} BOOLEAN;",
        t = qualified_table,
        v = quote_identifier(ETL_VERSION_COLUMN),
        vt = ETL_VERSION_SQL_TYPE,
        d = quote_identifier(ETL_DELETED_COLUMN)
    )
}

/// Builds a DuckLake `alter table add column` statement.
///
/// Added columns are always nullable at the destination because existing rows
/// cannot be backfilled from the source-side default expression.
pub(super) fn build_add_column_sql_ducklake(
    table_name: &str,
    column_schema: &ColumnSchema,
) -> String {
    let table_name = qualified_lake_table_name(table_name);
    let column_definition = ducklake_column_definition(column_schema, false);

    format!("alter table {table_name} add column {column_definition}")
}

/// Builds a DuckLake `alter table drop column` statement.
pub(super) fn build_drop_column_sql_ducklake(table_name: &str, column_name: &str) -> String {
    let table_name = qualified_lake_table_name(table_name);
    let column_name = quote_identifier(column_name);

    format!("alter table {table_name} drop column {column_name}")
}

/// Builds a DuckLake `alter table rename column` statement.
pub(super) fn build_rename_column_sql_ducklake(
    table_name: &str,
    old_name: &str,
    new_name: &str,
) -> String {
    let table_name = qualified_lake_table_name(table_name);
    let old_name = quote_identifier(old_name);
    let new_name = quote_identifier(new_name);

    format!("alter table {table_name} rename column {old_name} to {new_name}")
}

#[derive(Debug, Clone)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub struct SortKey {
    pub column: String,
    pub direction: SortDirection,
}

#[derive(Debug, Clone)]
pub struct TableStorageConfig {
    pub sort_keys: Vec<SortKey>,
    pub partition_by: Vec<String>,
}

/// Generates ALTER TABLE statements for sort keys and partition expressions.
pub(super) fn build_alter_table_storage_sql(
    table_name: &str,
    config: &TableStorageConfig,
) -> Vec<String> {
    let qualified = qualified_lake_table_name(table_name);
    let mut stmts = Vec::new();

    if !config.sort_keys.is_empty() {
        let keys: Vec<String> = config
            .sort_keys
            .iter()
            .map(|k| {
                let dir = match k.direction {
                    SortDirection::Asc => "ASC",
                    SortDirection::Desc => "DESC",
                };
                format!("{} {}", quote_identifier(&k.column), dir)
            })
            .collect();
        stmts.push(format!(
            "ALTER TABLE {qualified} SET SORTED BY ({});",
            keys.join(", ")
        ));
    }

    if !config.partition_by.is_empty() {
        stmts.push(format!(
            "ALTER TABLE {qualified} SET PARTITIONED BY ({});",
            config.partition_by.join(", ")
        ));
    }

    stmts
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_line_columns() -> Vec<ColumnSchema> {
        vec![
            ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, Some(1), false),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, None, true),
        ]
    }

    #[test]
    fn create_includes_cdc() {
        let sql = build_create_table_sql_with_cdc("public_lines", &sample_line_columns());
        assert!(sql.contains("\"_etl_version\" UHUGEINT"), "sql: {sql}");
        assert!(sql.contains("\"_etl_deleted\" BOOLEAN"), "sql: {sql}");
    }

    #[test]
    fn add_cdc_is_idempotent_nullable() {
        let sql = build_add_cdc_columns_sql("\"lake\".\"public_lines\"");
        assert!(
            sql.contains("ADD COLUMN IF NOT EXISTS \"_etl_version\" UHUGEINT"),
            "sql: {sql}"
        );
        assert!(
            sql.contains("ADD COLUMN IF NOT EXISTS \"_etl_deleted\" BOOLEAN"),
            "sql: {sql}"
        );
    }

    #[test]
    fn scalar_type_mapping() {
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::BOOL), "boolean");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::TEXT), "varchar");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::INT2), "smallint");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::INT4), "integer");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::INT8), "bigint");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::FLOAT4), "float");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::FLOAT8), "double");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::NUMERIC), "DECIMAL(38, 10)");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::DATE), "date");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::TIME), "time");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::TIMESTAMP), "timestamp");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::TIMESTAMPTZ), "timestamptz");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::UUID), "uuid");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::JSON), "json");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::JSONB), "json");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::OID), "ubigint");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::BYTEA), "blob");
    }

    #[test]
    fn array_type_mapping() {
        assert_eq!(postgres_array_type_to_ducklake_sql(&Type::BOOL_ARRAY), "boolean[]");
        assert_eq!(postgres_array_type_to_ducklake_sql(&Type::TEXT_ARRAY), "varchar[]");
        assert_eq!(postgres_array_type_to_ducklake_sql(&Type::INT4_ARRAY), "integer[]");
        assert_eq!(postgres_array_type_to_ducklake_sql(&Type::FLOAT8_ARRAY), "double[]");
        assert_eq!(postgres_array_type_to_ducklake_sql(&Type::UUID_ARRAY), "uuid[]");
    }

    #[test]
    fn build_create_table_sql_qualifies_lake_catalog() {
        let sql = build_create_table_sql_ducklake(
            "odd\"table",
            &[ColumnSchema::new("select".to_owned(), Type::INT4, -1, 1, Some(1), false)],
        );

        assert!(sql.starts_with("create table if not exists \"lake\".\"odd\"\"table\""));
        assert!(sql.contains("  \"select\" integer not null"));
    }

    #[test]
    fn build_add_column_sql_keeps_added_columns_nullable() {
        let sql = build_add_column_sql_ducklake(
            "test_table",
            &ColumnSchema::new("score".to_owned(), Type::INT4, -1, 4, None, false),
        );

        assert_eq!(sql, r#"alter table "lake"."test_table" add column "score" integer"#);
    }

    /// Simulates the routing in `issue_create_table_stmt`: in-scope tables use
    /// the CDC variant; out-of-scope tables use the plain variant.
    fn select_create_ddl(table_name: &str, is_in_scope: bool, cols: &[ColumnSchema]) -> String {
        if is_in_scope {
            build_create_table_sql_with_cdc(table_name, cols)
        } else {
            build_create_table_sql_ducklake(table_name, cols)
        }
    }

    #[test]
    fn in_scope_create_includes_cdc_columns() {
        let cols = sample_line_columns();
        let sql = select_create_ddl("public_lines", true, &cols);
        assert!(sql.contains("\"_etl_version\" UHUGEINT"), "sql: {sql}");
        assert!(sql.contains("\"_etl_deleted\" BOOLEAN"), "sql: {sql}");
    }

    #[test]
    fn out_of_scope_create_excludes_cdc_columns() {
        let cols = sample_line_columns();
        let sql = select_create_ddl("public_dimension__values", false, &cols);
        assert!(!sql.contains("_etl_version"), "sql: {sql}");
        assert!(!sql.contains("_etl_deleted"), "sql: {sql}");
    }

    #[test]
    fn add_cdc_sql_uses_qualified_lake_table_name() {
        let qualified = qualified_lake_table_name("public_lines");
        let sql = build_add_cdc_columns_sql(&qualified);
        assert!(
            sql.contains("\"lake\".\"public_lines\""),
            "alter should target the qualified lake table: {sql}"
        );
        assert!(sql.contains("ADD COLUMN IF NOT EXISTS"), "sql: {sql}");
    }

    #[test]
    fn build_drop_column_sql_quotes_identifiers() {
        let sql = build_drop_column_sql_ducklake("table\"name", "old\"column");

        assert_eq!(sql, r#"alter table "lake"."table""name" drop column "old""column""#);
    }

    #[test]
    fn build_rename_column_sql_quotes_identifiers() {
        let sql = build_rename_column_sql_ducklake("table\"name", "old\"column", "new\"column");

        assert_eq!(
            sql,
            r#"alter table "lake"."table""name" rename column "old""column" to "new""column""#
        );
    }
}
