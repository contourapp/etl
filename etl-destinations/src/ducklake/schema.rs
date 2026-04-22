use etl::types::{ColumnSchema, Type, is_array_type, is_range_array_type, is_range_type};
use pg_escape::quote_identifier;

/// Returns the DuckLake SQL type string for a given Postgres scalar type.
fn postgres_scalar_type_to_ducklake_sql(typ: &Type) -> &'static str {
    match typ {
        &Type::BOOL => "BOOLEAN",
        &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT => "VARCHAR",
        &Type::INT2 => "SMALLINT",
        &Type::INT4 => "INTEGER",
        &Type::INT8 => "BIGINT",
        &Type::FLOAT4 => "FLOAT",
        &Type::FLOAT8 => "DOUBLE",
        // NUMERIC maps to DECIMAL(38, 10): DuckDB's max precision with 10
        // fractional digits. Values are cast at write time via SQL literals so
        // NaN/Infinity can be coerced to NULL (DECIMAL has no representation).
        &Type::NUMERIC => "DECIMAL(38, 10)",
        &Type::DATE => "DATE",
        &Type::TIME => "TIME",
        &Type::TIMESTAMP => "TIMESTAMP",
        &Type::TIMESTAMPTZ => "TIMESTAMPTZ",
        &Type::UUID => "UUID",
        &Type::JSON | &Type::JSONB => "JSON",
        &Type::OID => "UBIGINT",
        &Type::BYTEA => "BLOB",
        _ => "VARCHAR",
    }
}

/// Returns the DuckDB SQL type string for a given Postgres array type.
fn postgres_array_type_to_ducklake_sql(typ: &Type) -> &'static str {
    match typ {
        &Type::BOOL_ARRAY => "BOOLEAN[]",
        &Type::CHAR_ARRAY
        | &Type::BPCHAR_ARRAY
        | &Type::VARCHAR_ARRAY
        | &Type::NAME_ARRAY
        | &Type::TEXT_ARRAY => "VARCHAR[]",
        &Type::INT2_ARRAY => "SMALLINT[]",
        &Type::INT4_ARRAY => "INTEGER[]",
        &Type::INT8_ARRAY => "BIGINT[]",
        &Type::FLOAT4_ARRAY => "FLOAT[]",
        &Type::FLOAT8_ARRAY => "DOUBLE[]",
        &Type::NUMERIC_ARRAY => "DECIMAL(38, 10)[]",
        &Type::DATE_ARRAY => "DATE[]",
        &Type::TIME_ARRAY => "TIME[]",
        &Type::TIMESTAMP_ARRAY => "TIMESTAMP[]",
        &Type::TIMESTAMPTZ_ARRAY => "TIMESTAMPTZ[]",
        &Type::UUID_ARRAY => "UUID[]",
        &Type::JSON_ARRAY | &Type::JSONB_ARRAY => "JSON[]",
        &Type::OID_ARRAY => "UBIGINT[]",
        &Type::BYTEA_ARRAY => "BLOB[]",
        _ => "VARCHAR[]",
    }
}

/// Returns the DuckDB SQL type string for a Postgres scalar range type.
fn postgres_range_type_to_ducklake_sql(typ: &Type) -> &'static str {
    match typ {
        &Type::TSTZ_RANGE => "STRUCT(\"lower\" TIMESTAMPTZ, \"upper\" TIMESTAMPTZ)",
        &Type::TS_RANGE => "STRUCT(\"lower\" TIMESTAMP, \"upper\" TIMESTAMP)",
        &Type::DATE_RANGE => "STRUCT(\"lower\" DATE, \"upper\" DATE)",
        &Type::INT4_RANGE => "STRUCT(\"lower\" INTEGER, \"upper\" INTEGER)",
        &Type::INT8_RANGE => "STRUCT(\"lower\" BIGINT, \"upper\" BIGINT)",
        &Type::NUM_RANGE => "STRUCT(\"lower\" DECIMAL(38, 10), \"upper\" DECIMAL(38, 10))",
        _ => "VARCHAR",
    }
}

/// Returns the DuckDB SQL type string for a Postgres range array type.
fn postgres_range_array_type_to_ducklake_sql(typ: &Type) -> &'static str {
    match typ {
        &Type::TSTZ_RANGE_ARRAY => "STRUCT(\"lower\" TIMESTAMPTZ, \"upper\" TIMESTAMPTZ)[]",
        &Type::TS_RANGE_ARRAY => "STRUCT(\"lower\" TIMESTAMP, \"upper\" TIMESTAMP)[]",
        &Type::DATE_RANGE_ARRAY => "STRUCT(\"lower\" DATE, \"upper\" DATE)[]",
        &Type::INT4_RANGE_ARRAY => "STRUCT(\"lower\" INTEGER, \"upper\" INTEGER)[]",
        &Type::INT8_RANGE_ARRAY => "STRUCT(\"lower\" BIGINT, \"upper\" BIGINT)[]",
        &Type::NUM_RANGE_ARRAY => "STRUCT(\"lower\" DECIMAL(38, 10), \"upper\" DECIMAL(38, 10))[]",
        _ => "VARCHAR[]",
    }
}

/// Returns the DuckLake SQL type string for a Postgres column type.
pub fn postgres_column_type_to_ducklake_sql(typ: &Type) -> &'static str {
    if is_range_array_type(typ) {
        postgres_range_array_type_to_ducklake_sql(typ)
    } else if is_range_type(typ) {
        postgres_range_type_to_ducklake_sql(typ)
    } else if is_array_type(typ) {
        postgres_array_type_to_ducklake_sql(typ)
    } else {
        postgres_scalar_type_to_ducklake_sql(typ)
    }
}

/// Builds a `CREATE TABLE IF NOT EXISTS` DDL statement for the given table name and schema.
///
/// CDC columns (`cdc_operation` and `cdc_lsn`) are appended at the end and must already
/// be included in `column_schemas` (added by `modify_schema_with_cdc_columns` before calling
/// this function).
pub fn build_create_table_sql_ducklake(
    table_name: &str,
    column_schemas: &[ColumnSchema],
) -> String {
    let table_name = quote_identifier(table_name);
    let col_defs: Vec<String> = column_schemas
        .iter()
        .map(|col| {
            let column_name = quote_identifier(&col.name);
            let duckdb_type = postgres_column_type_to_ducklake_sql(&col.typ);
            let nullability = if col.nullable { "" } else { " NOT NULL" };
            format!("  {column_name} {duckdb_type}{nullability}")
        })
        .collect();

    format!(
        "CREATE TABLE IF NOT EXISTS {table_name} ({})",
        col_defs.join(",\n")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scalar_type_mapping() {
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::BOOL), "BOOLEAN");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::TEXT), "VARCHAR");
        assert_eq!(
            postgres_scalar_type_to_ducklake_sql(&Type::INT2),
            "SMALLINT"
        );
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::INT4), "INTEGER");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::INT8), "BIGINT");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::FLOAT4), "FLOAT");
        assert_eq!(
            postgres_scalar_type_to_ducklake_sql(&Type::FLOAT8),
            "DOUBLE"
        );
        assert_eq!(
            postgres_scalar_type_to_ducklake_sql(&Type::NUMERIC),
            "DECIMAL(38, 10)"
        );
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::DATE), "DATE");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::TIME), "TIME");
        assert_eq!(
            postgres_scalar_type_to_ducklake_sql(&Type::TIMESTAMP),
            "TIMESTAMP"
        );
        assert_eq!(
            postgres_scalar_type_to_ducklake_sql(&Type::TIMESTAMPTZ),
            "TIMESTAMPTZ"
        );
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::UUID), "UUID");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::JSON), "JSON");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::JSONB), "JSON");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::OID), "UBIGINT");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::BYTEA), "BLOB");
    }

    #[test]
    fn test_array_type_mapping() {
        assert_eq!(
            postgres_array_type_to_ducklake_sql(&Type::BOOL_ARRAY),
            "BOOLEAN[]"
        );
        assert_eq!(
            postgres_array_type_to_ducklake_sql(&Type::TEXT_ARRAY),
            "VARCHAR[]"
        );
        assert_eq!(
            postgres_array_type_to_ducklake_sql(&Type::INT4_ARRAY),
            "INTEGER[]"
        );
        assert_eq!(
            postgres_array_type_to_ducklake_sql(&Type::FLOAT8_ARRAY),
            "DOUBLE[]"
        );
        assert_eq!(
            postgres_array_type_to_ducklake_sql(&Type::UUID_ARRAY),
            "UUID[]"
        );
    }

    #[test]
    fn test_range_type_mapping() {
        assert_eq!(
            postgres_range_type_to_ducklake_sql(&Type::TSTZ_RANGE),
            "STRUCT(\"lower\" TIMESTAMPTZ, \"upper\" TIMESTAMPTZ)"
        );
        assert_eq!(
            postgres_range_type_to_ducklake_sql(&Type::TS_RANGE),
            "STRUCT(\"lower\" TIMESTAMP, \"upper\" TIMESTAMP)"
        );
        assert_eq!(
            postgres_range_type_to_ducklake_sql(&Type::DATE_RANGE),
            "STRUCT(\"lower\" DATE, \"upper\" DATE)"
        );
        assert_eq!(
            postgres_range_type_to_ducklake_sql(&Type::INT4_RANGE),
            "STRUCT(\"lower\" INTEGER, \"upper\" INTEGER)"
        );
        assert_eq!(
            postgres_range_type_to_ducklake_sql(&Type::INT8_RANGE),
            "STRUCT(\"lower\" BIGINT, \"upper\" BIGINT)"
        );
        assert_eq!(
            postgres_range_type_to_ducklake_sql(&Type::NUM_RANGE),
            "STRUCT(\"lower\" DECIMAL(38, 10), \"upper\" DECIMAL(38, 10))"
        );
    }

    #[test]
    fn test_range_array_type_mapping() {
        assert_eq!(
            postgres_range_array_type_to_ducklake_sql(&Type::TSTZ_RANGE_ARRAY),
            "STRUCT(\"lower\" TIMESTAMPTZ, \"upper\" TIMESTAMPTZ)[]"
        );
        assert_eq!(
            postgres_range_array_type_to_ducklake_sql(&Type::TS_RANGE_ARRAY),
            "STRUCT(\"lower\" TIMESTAMP, \"upper\" TIMESTAMP)[]"
        );
        assert_eq!(
            postgres_range_array_type_to_ducklake_sql(&Type::DATE_RANGE_ARRAY),
            "STRUCT(\"lower\" DATE, \"upper\" DATE)[]"
        );
    }

    #[test]
    fn test_column_type_dispatches_range_types() {
        assert_eq!(
            postgres_column_type_to_ducklake_sql(&Type::TSTZ_RANGE),
            "STRUCT(\"lower\" TIMESTAMPTZ, \"upper\" TIMESTAMPTZ)"
        );
        assert_eq!(
            postgres_column_type_to_ducklake_sql(&Type::TSTZ_RANGE_ARRAY),
            "STRUCT(\"lower\" TIMESTAMPTZ, \"upper\" TIMESTAMPTZ)[]"
        );
        // Non-range types still work
        assert_eq!(
            postgres_column_type_to_ducklake_sql(&Type::INT4),
            "INTEGER"
        );
        assert_eq!(
            postgres_column_type_to_ducklake_sql(&Type::INT4_ARRAY),
            "INTEGER[]"
        );
    }

    #[test]
    fn test_build_create_table_sql_quotes_identifiers() {
        let sql = build_create_table_sql_ducklake(
            "odd\"table",
            &[ColumnSchema::new(
                "select".to_string(),
                Type::INT4,
                -1,
                false,
                true,
            )],
        );

        assert!(sql.starts_with("CREATE TABLE IF NOT EXISTS \"odd\"\"table\""));
        assert!(sql.contains("  \"select\" INTEGER NOT NULL"));
    }
}
