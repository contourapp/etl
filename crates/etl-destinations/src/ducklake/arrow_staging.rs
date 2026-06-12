//! Arrow RecordBatch staging for DuckLake batch writes.

use duckdb::arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
    types::{ColumnSchema, Type, is_array_type, is_range_array_type, is_range_type},
};

use crate::ducklake::sql::quote_identifier;

/// How a staged column reaches its target type in consuming SQL.
pub(super) enum CastKind {
    /// Staging column already has the target type.
    Identity,
    /// Staging column needs `CAST(col AS <target>)` when read.
    To(String),
}

/// Per-column staging contract: the Arrow type appended, the staging table
/// column type, and the cast applied by consuming SQL.
pub(super) struct StagingColumnSpec {
    pub name: String,
    pub arrow_type: DataType,
    pub staging_sql_type: String,
    pub cast: CastKind,
}

impl StagingColumnSpec {
    /// Renders this column for the SELECT list of consuming SQL.
    pub fn select_expr(&self) -> String {
        let quoted = quote_identifier(&self.name);
        match &self.cast {
            CastKind::Identity => quoted,
            CastKind::To(target) => format!("CAST({quoted} AS {target}) AS {quoted}"),
        }
    }
}

/// Arrow fields for the range struct: {lower, upper} of the bound type.
pub(super) fn range_struct_fields(bound: &DataType) -> Fields {
    Fields::from(vec![
        Field::new("lower", bound.clone(), true),
        Field::new("upper", bound.clone(), true),
    ])
}

fn timestamptz_arrow() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

/// (arrow bound type, staging SQL bound type) for a range's bounds.
fn range_bound_mapping(typ: &Type) -> Option<(DataType, &'static str)> {
    match *typ {
        Type::INT4_RANGE | Type::INT4_RANGE_ARRAY => Some((DataType::Int32, "INTEGER")),
        Type::INT8_RANGE | Type::INT8_RANGE_ARRAY => Some((DataType::Int64, "BIGINT")),
        Type::NUM_RANGE | Type::NUM_RANGE_ARRAY => {
            Some((DataType::Decimal128(38, 10), "DECIMAL(38, 10)"))
        }
        Type::TS_RANGE | Type::TS_RANGE_ARRAY => {
            Some((DataType::Timestamp(TimeUnit::Microsecond, None), "TIMESTAMP"))
        }
        Type::TSTZ_RANGE | Type::TSTZ_RANGE_ARRAY => Some((timestamptz_arrow(), "TIMESTAMPTZ")),
        Type::DATE_RANGE | Type::DATE_RANGE_ARRAY => Some((DataType::Date32, "DATE")),
        _ => None,
    }
}

/// (arrow element type, staging SQL element type, optional cast target) for
/// scalar (non-array, non-range) Postgres types.
fn scalar_mapping(typ: &Type) -> Option<(DataType, &'static str, Option<&'static str>)> {
    Some(match *typ {
        Type::BOOL => (DataType::Boolean, "BOOLEAN", None),
        Type::INT2 => (DataType::Int16, "SMALLINT", None),
        Type::INT4 => (DataType::Int32, "INTEGER", None),
        Type::OID => (DataType::UInt32, "UINTEGER", None),
        Type::INT8 => (DataType::Int64, "BIGINT", None),
        Type::FLOAT4 => (DataType::Float32, "FLOAT", None),
        Type::FLOAT8 => (DataType::Float64, "DOUBLE", None),
        Type::NUMERIC => (DataType::Decimal128(38, 10), "DECIMAL(38, 10)", None),
        Type::DATE => (DataType::Date32, "DATE", None),
        Type::TIME => (DataType::Time64(TimeUnit::Microsecond), "TIME", None),
        Type::TIMESTAMP => {
            (DataType::Timestamp(TimeUnit::Microsecond, None), "TIMESTAMP", None)
        }
        Type::TIMESTAMPTZ => (timestamptz_arrow(), "TIMESTAMPTZ", None),
        Type::UUID => (DataType::Utf8, "VARCHAR", Some("UUID")),
        Type::JSON | Type::JSONB => (DataType::Utf8, "VARCHAR", Some("JSON")),
        Type::BYTEA => (DataType::Binary, "BLOB", None),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::CHAR => {
            (DataType::Utf8, "VARCHAR", None)
        }
        _ => return None,
    })
}

/// Maps a scalar array type to its element scalar type.
fn array_element_type(typ: &Type) -> Option<Type> {
    Some(match *typ {
        Type::BOOL_ARRAY => Type::BOOL,
        Type::INT2_ARRAY => Type::INT2,
        Type::INT4_ARRAY => Type::INT4,
        Type::OID_ARRAY => Type::OID,
        Type::INT8_ARRAY => Type::INT8,
        Type::FLOAT4_ARRAY => Type::FLOAT4,
        Type::FLOAT8_ARRAY => Type::FLOAT8,
        Type::NUMERIC_ARRAY => Type::NUMERIC,
        Type::DATE_ARRAY => Type::DATE,
        Type::TIME_ARRAY => Type::TIME,
        Type::TIMESTAMP_ARRAY => Type::TIMESTAMP,
        Type::TIMESTAMPTZ_ARRAY => Type::TIMESTAMPTZ,
        Type::UUID_ARRAY => Type::UUID,
        Type::JSON_ARRAY => Type::JSON,
        Type::JSONB_ARRAY => Type::JSONB,
        Type::BYTEA_ARRAY => Type::BYTEA,
        Type::TEXT_ARRAY => Type::TEXT,
        Type::VARCHAR_ARRAY => Type::VARCHAR,
        Type::BPCHAR_ARRAY => Type::BPCHAR,
        Type::CHAR_ARRAY => Type::CHAR,
        _ => return None,
    })
}

fn list_of(element: DataType) -> DataType {
    DataType::List(std::sync::Arc::new(Field::new("item", element, true)))
}

/// Builds the per-column staging contract for one table.
pub(super) fn build_staging_specs(
    column_schemas: &[ColumnSchema],
) -> EtlResult<Vec<StagingColumnSpec>> {
    column_schemas
        .iter()
        .map(|column| {
            let typ = &column.typ;
            let (arrow_type, staging_sql_type, cast) = if is_range_type(typ) {
                let (bound_arrow, bound_sql) =
                    range_bound_mapping(typ).ok_or_else(|| unsupported(column))?;
                (
                    DataType::Struct(range_struct_fields(&bound_arrow)),
                    format!("STRUCT(\"lower\" {bound_sql}, \"upper\" {bound_sql})"),
                    CastKind::Identity,
                )
            } else if is_range_array_type(typ) {
                let (bound_arrow, bound_sql) =
                    range_bound_mapping(typ).ok_or_else(|| unsupported(column))?;
                (
                    list_of(DataType::Struct(range_struct_fields(&bound_arrow))),
                    format!("STRUCT(\"lower\" {bound_sql}, \"upper\" {bound_sql})[]"),
                    CastKind::Identity,
                )
            } else if let Some(element) = array_element_type(typ) {
                let (el_arrow, el_sql, el_cast) =
                    scalar_mapping(&element).ok_or_else(|| unsupported(column))?;
                (
                    list_of(el_arrow),
                    format!("{el_sql}[]"),
                    match el_cast {
                        Some(target) => CastKind::To(format!("{target}[]")),
                        None => CastKind::Identity,
                    },
                )
            } else if is_array_type(typ) {
                // Unknown array type (e.g. enum[], citext[], interval[]) — mirror
                // schema.rs catch-all: target DDL maps these to varchar[].
                (list_of(DataType::Utf8), "VARCHAR[]".to_owned(), CastKind::Identity)
            } else if let Some((arrow, sql, cast)) = scalar_mapping(typ) {
                (
                    arrow,
                    sql.to_owned(),
                    match cast {
                        Some(target) => CastKind::To(target.to_owned()),
                        None => CastKind::Identity,
                    },
                )
            } else {
                // Unknown scalar type (e.g. enum, citext, interval) — mirror
                // schema.rs catch-all: target DDL maps these to varchar.
                (DataType::Utf8, "VARCHAR".to_owned(), CastKind::Identity)
            };
            Ok(StagingColumnSpec {
                name: column.name.clone(),
                arrow_type,
                staging_sql_type,
                cast,
            })
        })
        .collect()
}

fn unsupported(column: &ColumnSchema) -> etl::error::EtlError {
    etl_error!(
        ErrorKind::ConversionError,
        "Unsupported column type for Arrow staging",
        format!("column `{}` has unsupported type {:?}", column.name, column.typ)
    )
}

#[cfg(test)]
mod spec_tests {
    use super::*;
    use duckdb::arrow::datatypes::DataType;
    use etl::types::{ColumnSchema, Type};

    fn col(name: &str, typ: Type) -> ColumnSchema {
        ColumnSchema::new(name.to_owned(), typ, -1, 1, None, true)
    }

    #[test]
    fn specs_for_scalars_are_identity() {
        let specs = build_staging_specs(&[col("n", Type::INT8), col("t", Type::TEXT)]).unwrap();
        assert_eq!(specs[0].arrow_type, DataType::Int64);
        assert_eq!(specs[0].staging_sql_type, "BIGINT");
        assert!(matches!(specs[0].cast, CastKind::Identity));
        assert_eq!(specs[1].staging_sql_type, "VARCHAR");
    }

    #[test]
    fn uuid_stages_as_varchar_with_cast() {
        let specs = build_staging_specs(&[col("id", Type::UUID)]).unwrap();
        assert_eq!(specs[0].arrow_type, DataType::Utf8);
        assert_eq!(specs[0].staging_sql_type, "VARCHAR");
        assert!(matches!(specs[0].cast, CastKind::To(ref t) if t == "UUID"));
    }

    #[test]
    fn numeric_stages_as_decimal_38_10() {
        let specs = build_staging_specs(&[col("amt", Type::NUMERIC)]).unwrap();
        assert_eq!(specs[0].arrow_type, DataType::Decimal128(38, 10));
        assert_eq!(specs[0].staging_sql_type, "DECIMAL(38, 10)");
    }

    #[test]
    fn uuid_array_stages_as_varchar_list_with_cast() {
        let specs = build_staging_specs(&[col("tags", Type::UUID_ARRAY)]).unwrap();
        assert!(matches!(specs[0].arrow_type, DataType::List(_)));
        assert_eq!(specs[0].staging_sql_type, "VARCHAR[]");
        assert!(matches!(specs[0].cast, CastKind::To(ref t) if t == "UUID[]"));
    }

    #[test]
    fn tstzrange_array_stages_as_struct_list() {
        let specs = build_staging_specs(&[col("er", Type::TSTZ_RANGE_ARRAY)]).unwrap();
        assert_eq!(
            specs[0].staging_sql_type,
            "STRUCT(\"lower\" TIMESTAMPTZ, \"upper\" TIMESTAMPTZ)[]"
        );
        assert!(matches!(specs[0].cast, CastKind::Identity));
    }

    #[test]
    fn unknown_scalar_type_falls_back_to_varchar() {
        // INTERVAL has no dedicated mapping; target DDL maps it to varchar.
        let specs = build_staging_specs(&[col("iv", Type::INTERVAL)]).unwrap();
        assert_eq!(specs[0].arrow_type, DataType::Utf8);
        assert_eq!(specs[0].staging_sql_type, "VARCHAR");
        assert!(matches!(specs[0].cast, CastKind::Identity));
    }

    #[test]
    fn unknown_array_type_falls_back_to_varchar_list() {
        let specs = build_staging_specs(&[col("ivs", Type::INTERVAL_ARRAY)]).unwrap();
        assert!(matches!(specs[0].arrow_type, DataType::List(_)));
        assert_eq!(specs[0].staging_sql_type, "VARCHAR[]");
    }

    #[test]
    fn select_expr_renders_cast_and_identity() {
        let specs =
            build_staging_specs(&[col("id", Type::UUID), col("n", Type::INT4)]).unwrap();
        // quote_identifier always double-quotes: "id" -> "\"id\""
        assert_eq!(specs[0].select_expr(), r#"CAST("id" AS UUID) AS "id""#);
        assert_eq!(specs[1].select_expr(), r#""n""#);
    }
}

#[cfg(test)]
mod spike_tests {
    use duckdb::arrow::array::{
        ArrayRef, Decimal128Builder, ListBuilder, StringBuilder, StructBuilder,
        TimestampMicrosecondBuilder,
    };
    use duckdb::arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
    use duckdb::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn range_struct_fields() -> Fields {
        Fields::from(vec![
            Field::new(
                "lower",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "upper",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ])
    }

    /// Spike: append_record_batch into a temp table shaped like staging.
    /// Answers whether decimal, timestamptz, and list<struct> ingest with
    /// exact types, and that uuid works as VARCHAR + CAST at read time.
    #[test]
    fn spike_append_record_batch_type_matrix() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TEMP TABLE staging_spike (
                id VARCHAR,
                amount DECIMAL(38, 10),
                \"at\" TIMESTAMPTZ,
                ranges STRUCT(\"lower\" TIMESTAMPTZ, \"upper\" TIMESTAMPTZ)[]
            );",
        )
        .unwrap();

        let mut id = StringBuilder::new();
        id.append_value("01234567-89ab-cdef-0123-456789abcdef");
        id.append_null();

        let mut amount =
            Decimal128Builder::new().with_precision_and_scale(38, 10).unwrap();
        amount.append_value(1_234_500_000_000_i128); // 123.45
        amount.append_null();

        let mut at = TimestampMicrosecondBuilder::new().with_timezone("UTC");
        at.append_value(1_750_000_000_000_000); // 2025-06-15T...Z
        at.append_null();

        let mut ranges = ListBuilder::new(StructBuilder::from_fields(
            range_struct_fields(),
            0,
        ))
        .with_field(Field::new(
            "item",
            DataType::Struct(range_struct_fields()),
            true,
        ));
        {
            let sb = ranges.values();
            sb.field_builder::<TimestampMicrosecondBuilder>(0)
                .unwrap()
                .append_value(1_750_000_000_000_000);
            sb.field_builder::<TimestampMicrosecondBuilder>(1)
                .unwrap()
                .append_null(); // unbounded upper
            sb.append(true);
        }
        ranges.append(true); // row 0: one-element list
        ranges.append(true); // row 1: empty list

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(38, 10), true),
            Field::new(
                "at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "ranges",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(range_struct_fields()),
                    true,
                ))),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id.finish()) as ArrayRef,
                Arc::new(amount.finish()),
                Arc::new(at.finish()),
                Arc::new(ranges.finish()),
            ],
        )
        .unwrap();

        let mut appender = conn.appender("staging_spike").unwrap();
        appender.append_record_batch(batch).unwrap();
        appender.flush().unwrap();

        // uuid via CAST at read time; everything else direct.
        let (cast_id, amount_text, at_text, lower_text, list_len): (
            String,
            String,
            String,
            String,
            i64,
        ) = conn
            .query_row(
                "SELECT CAST(id AS UUID)::VARCHAR, amount::VARCHAR, \"at\"::VARCHAR,
                        ranges[1].\"lower\"::VARCHAR, len(ranges)
                 FROM staging_spike WHERE id IS NOT NULL",
                [],
                |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?))
                },
            )
            .unwrap();
        assert_eq!(cast_id, "01234567-89ab-cdef-0123-456789abcdef");
        assert_eq!(amount_text, "123.4500000000");
        assert!(at_text.starts_with("2025-06-15"));
        assert!(lower_text.starts_with("2025-06-15"));
        assert_eq!(list_len, 1);

        let null_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM staging_spike
                 WHERE id IS NULL AND amount IS NULL AND \"at\" IS NULL AND len(ranges) = 0",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(null_count, 1);
    }
}
