//! Arrow RecordBatch staging for DuckLake batch writes.

use std::sync::Arc;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use duckdb::arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, ListBuilder,
    PrimitiveBuilder, StringBuilder, StructBuilder, Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder, UInt32Builder,
};
use duckdb::arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Fields, Schema, TimeUnit};
use duckdb::arrow::record_batch::RecordBatch;
use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
    types::{
        ArrayCell, Cell, ColumnSchema, PgNumeric, TableRow, Type, is_array_type,
        is_range_array_type, is_range_type,
    },
};

use crate::ducklake::encoding::{ParsedRange, parse_range_array_text, parse_range_text};
use crate::ducklake::sql::quote_identifier;

/// How a staged column reaches its target type in consuming SQL.
#[derive(Clone)]
pub(super) enum CastKind {
    /// Staging column already has the target type.
    Identity,
    /// Staging column needs `CAST(col AS <target>)` when read.
    To(String),
}

/// Per-column staging contract: the Arrow type appended, the staging table
/// column type, and the cast applied by consuming SQL.
#[derive(Clone)]
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

    fn sample_line_column_schemas() -> Vec<ColumnSchema> {
        vec![col("n", Type::INT8), col("t", Type::TEXT)]
    }

    #[test]
    fn specs_with_cdc_append_two() {
        let base = build_staging_specs(&sample_line_column_schemas()).unwrap();
        let cdc = build_staging_specs_with_cdc(&sample_line_column_schemas()).unwrap();
        assert_eq!(cdc.len(), base.len() + 2);
        assert_eq!(cdc[cdc.len() - 2].name, "_etl_version");
        assert_eq!(cdc[cdc.len() - 1].name, "_etl_deleted");
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

/// Converts decimal text (PgNumeric's Display output) into an i128 mantissa
/// at scale 10, rounding the 11th fractional digit half away from zero.
/// Errors when the value exceeds DECIMAL(38, 10) — matching the literal
/// path, where DuckDB's CAST would fail the statement.
///
/// Rounds half-away-from-zero on the 11th fractional digit; assumes
/// terminating scale-10 inputs (PgNumeric Display output). Differs from
/// DuckDB CAST only on non-terminating values whose 11th digit is exactly 5
/// with nonzero trailing digits — not produced by CDC numeric decoding.
pub(super) fn decimal_text_to_i128(text: &str) -> EtlResult<Option<i128>> {
    const SCALE: usize = 10;
    // Maximum absolute mantissa for DECIMAL(38, 10): 10^38 - 1
    const MAX_ABS: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;

    let text = text.trim();
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (int_part, frac_part) = match digits.split_once('.') {
        Some((i, f)) => (i, f),
        None => (digits, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return Ok(None);
    }
    let overflow_err = || {
        etl_error!(
            ErrorKind::ConversionError,
            "Numeric value does not fit DECIMAL(38, 10)"
        )
    };
    let mut mantissa: i128 = 0;
    for ch in int_part.chars() {
        let d = ch.to_digit(10).ok_or_else(overflow_err)? as i128;
        mantissa =
            mantissa.checked_mul(10).and_then(|m| m.checked_add(d)).ok_or_else(overflow_err)?;
    }
    let frac_bytes = frac_part.as_bytes();
    for i in 0..SCALE {
        let d: i128 = if i < frac_bytes.len() {
            let b = frac_bytes[i];
            if b < b'0' || b > b'9' {
                return Err(overflow_err());
            }
            (b - b'0') as i128
        } else {
            0
        };
        mantissa =
            mantissa.checked_mul(10).and_then(|m| m.checked_add(d)).ok_or_else(overflow_err)?;
    }
    // Round on the 11th fractional digit, half away from zero.
    if frac_bytes.len() > SCALE {
        let b = frac_bytes[SCALE];
        if b < b'0' || b > b'9' {
            return Err(overflow_err());
        }
        let d = b - b'0';
        if d >= 5 {
            mantissa = mantissa.checked_add(1).ok_or_else(overflow_err)?;
        }
    }
    if mantissa > MAX_ABS {
        return Err(overflow_err());
    }
    Ok(Some(if negative { -mantissa } else { mantissa }))
}

#[cfg(test)]
mod numeric_tests {
    use super::*;

    #[test]
    fn decimal_text_to_i128_scale10() {
        assert_eq!(decimal_text_to_i128("123.45").unwrap(), Some(1_234_500_000_000));
        assert_eq!(decimal_text_to_i128("-0.0000000001").unwrap(), Some(-1));
        assert_eq!(decimal_text_to_i128("0").unwrap(), Some(0));
        // 11th fractional digit rounds half away from zero (DuckDB CAST behavior)
        assert_eq!(decimal_text_to_i128("0.00000000005").unwrap(), Some(1));
        assert_eq!(decimal_text_to_i128("-0.00000000005").unwrap(), Some(-1));
    }

    #[test]
    fn decimal_text_overflow_errors() {
        // 29 integer digits + scale 10 > precision 38
        let too_big = "1".repeat(29);
        assert!(decimal_text_to_i128(&too_big).is_err());
    }

    #[test]
    fn decimal_text_scientific_notation_errors() {
        assert!(decimal_text_to_i128("1e5").is_err());
    }
}

/// Prepared staging payload reused across retry attempts.
pub(super) struct PreparedRows {
    pub batch: RecordBatch,
}

impl PreparedRows {
    pub fn row_count(&self) -> usize {
        self.batch.num_rows()
    }
}

/// Converts table rows into the staging payload for DuckDB writes.
/// Runs on the async worker, off the DuckDB blocking pool.
pub(super) fn prepare_rows(
    table_rows: Vec<TableRow>,
    column_schemas: &[ColumnSchema],
) -> EtlResult<PreparedRows> {
    let specs = build_staging_specs(column_schemas)?;
    let batch = build_record_batch(&specs, &table_rows)?;
    Ok(PreparedRows { batch })
}

/// Builds one RecordBatch for the staging table from prepared rows.
/// Column-oriented: one typed builder per spec, fed from every row.
pub(super) fn build_record_batch(
    specs: &[StagingColumnSpec],
    rows: &[TableRow],
) -> EtlResult<RecordBatch> {
    let mut fields = Vec::with_capacity(specs.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(specs.len());
    for (index, spec) in specs.iter().enumerate() {
        fields.push(Field::new(spec.name.as_str(), spec.arrow_type.clone(), true));
        arrays.push(build_column(spec, index, rows)?);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|error| {
        etl_error!(
            ErrorKind::ConversionError,
            "Failed to assemble Arrow record batch for staging",
            error.to_string()
        )
    })
}

/// Extends a base set of staging specs with two trailing CDC columns:
/// `_etl_version` (packed u128 version, carried as Decimal128(38,0) and cast
/// to UHUGEINT at read time) and `_etl_deleted` (boolean tombstone flag).
pub(super) fn build_staging_specs_with_cdc(
    column_schemas: &[ColumnSchema],
) -> EtlResult<Vec<StagingColumnSpec>> {
    use super::merge_on_read::{ETL_DELETED_COLUMN, ETL_VERSION_COLUMN, ETL_VERSION_SQL_TYPE};
    let mut specs = build_staging_specs(column_schemas)?;
    specs.push(StagingColumnSpec {
        name: ETL_VERSION_COLUMN.into(),
        arrow_type: DataType::Decimal128(38, 0),
        staging_sql_type: "DECIMAL(38,0)".into(),
        cast: CastKind::To(ETL_VERSION_SQL_TYPE.into()),
    });
    specs.push(StagingColumnSpec {
        name: ETL_DELETED_COLUMN.into(),
        arrow_type: DataType::Boolean,
        staging_sql_type: "BOOLEAN".into(),
        cast: CastKind::Identity,
    });
    Ok(specs)
}

/// Builds a [`PreparedRows`] payload carrying the two trailing CDC columns.
///
/// `specs` must be CDC-augmented (i.e. produced by
/// [`build_staging_specs_with_cdc`]) so its trailing two columns are
/// `_etl_version` / `_etl_deleted`. Every row in the resulting batch carries the
/// same constant `version` and `deleted` flag. This mirrors [`prepare_rows`]
/// but for the merge-on-read append path.
pub(super) fn prepare_rows_with_cdc(
    table_rows: Vec<TableRow>,
    specs: &[StagingColumnSpec],
    version: u128,
    deleted: bool,
) -> EtlResult<PreparedRows> {
    let batch = build_record_batch_with_cdc(&table_rows, specs, version, deleted)?;
    Ok(PreparedRows { batch })
}

/// Builds a RecordBatch with CDC-augmented specs: user columns are built
/// exactly as [`build_record_batch`] does (reusing [`build_column`] per spec),
/// then two constant trailing columns are appended — a Decimal128(38,0) holding
/// `version as i128` for every row, and a Boolean holding `deleted` for every
/// row.
///
/// `version` is a packed `u128` (high 64 bits = commit_lsn, low 64 bits =
/// tx_ordinal). Realistic values stay far below `i128::MAX`, so the cast is
/// safe in practice. An explicit guard returns an `EtlResult` error on overflow
/// rather than panicking, in case of future version-encoding changes.
// Consumed by Task 6.
#[allow(dead_code)]
pub(super) fn build_record_batch_with_cdc(
    rows: &[TableRow],
    specs: &[StagingColumnSpec],
    version: u128,
    deleted: bool,
) -> EtlResult<RecordBatch> {
    let version_i128 = i128::try_from(version).map_err(|_| {
        etl_error!(
            ErrorKind::ConversionError,
            "CDC version overflows i128",
            format!("version {version} does not fit in Decimal128(38,0)")
        )
    })?;
    let row_count = rows.len();
    let user_count = specs.len().saturating_sub(2);
    let mut fields = Vec::with_capacity(specs.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(specs.len());
    for (index, spec) in specs[..user_count].iter().enumerate() {
        fields.push(Field::new(spec.name.as_str(), spec.arrow_type.clone(), true));
        arrays.push(build_column(spec, index, rows)?);
    }
    let version_spec = &specs[user_count];
    fields.push(Field::new(version_spec.name.as_str(), version_spec.arrow_type.clone(), true));
    let mut version_builder = decimal_builder(38, 0)?;
    for _ in 0..row_count {
        version_builder.append_value(version_i128);
    }
    arrays.push(Arc::new(version_builder.finish()) as ArrayRef);
    let deleted_spec = &specs[user_count + 1];
    fields.push(Field::new(deleted_spec.name.as_str(), deleted_spec.arrow_type.clone(), true));
    let mut deleted_builder = duckdb::arrow::array::BooleanBuilder::with_capacity(row_count);
    for _ in 0..row_count {
        deleted_builder.append_value(deleted);
    }
    arrays.push(Arc::new(deleted_builder.finish()) as ArrayRef);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|error| {
        etl_error!(
            ErrorKind::ConversionError,
            "Failed to assemble CDC Arrow record batch for staging",
            error.to_string()
        )
    })
}

/// Builds the typed Arrow array for one staged column across all rows.
fn build_column(spec: &StagingColumnSpec, index: usize, rows: &[TableRow]) -> EtlResult<ArrayRef> {
    let name = spec.name.as_str();
    macro_rules! primitive_column {
        ($builder:expr, $variant:ident, $conv:expr) => {
            build_primitive_column(rows, index, name, $builder, |cell| match cell {
                Cell::Null => Ok(None),
                Cell::$variant(value) => Ok(Some($conv(value))),
                other => Err(unexpected_cell(name, other)),
            })
        };
    }
    match &spec.arrow_type {
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                match cell_at(row, index, name)? {
                    Cell::Null => builder.append_null(),
                    Cell::Bool(value) => builder.append_value(*value),
                    other => return Err(unexpected_cell(name, other)),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int16 => primitive_column!(Int16Builder::new(), I16, |v: &i16| *v),
        DataType::Int32 => primitive_column!(Int32Builder::new(), I32, |v: &i32| *v),
        DataType::UInt32 => primitive_column!(UInt32Builder::new(), U32, |v: &u32| *v),
        DataType::Int64 => primitive_column!(Int64Builder::new(), I64, |v: &i64| *v),
        DataType::Float32 => primitive_column!(Float32Builder::new(), F32, |v: &f32| *v),
        DataType::Float64 => primitive_column!(Float64Builder::new(), F64, |v: &f64| *v),
        DataType::Decimal128(precision, scale) => build_primitive_column(
            rows,
            index,
            name,
            decimal_builder(*precision, *scale)?,
            |cell| match cell {
                Cell::Null => Ok(None),
                Cell::Numeric(numeric) => numeric_to_mantissa(numeric),
                other => Err(unexpected_cell(name, other)),
            },
        ),
        DataType::Date32 => primitive_column!(Date32Builder::new(), Date, date_to_days),
        DataType::Time64(TimeUnit::Microsecond) => {
            primitive_column!(Time64MicrosecondBuilder::new(), Time, time_to_micros)
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => primitive_column!(
            TimestampMicrosecondBuilder::new(),
            Timestamp,
            |dt: &NaiveDateTime| dt.and_utc().timestamp_micros()
        ),
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => primitive_column!(
            TimestampMicrosecondBuilder::new().with_timezone(Arc::clone(tz)),
            TimestampTz,
            |dt: &DateTime<Utc>| dt.timestamp_micros()
        ),
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for row in rows {
                match cell_at(row, index, name)? {
                    Cell::Null => builder.append_null(),
                    Cell::String(value) => builder.append_value(value),
                    Cell::Uuid(value) => builder.append_value(value.to_string()),
                    Cell::Json(value) => builder.append_value(value.to_string()),
                    other => return Err(unexpected_cell(name, other)),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Binary => {
            let mut builder = BinaryBuilder::new();
            for row in rows {
                match cell_at(row, index, name)? {
                    Cell::Null => builder.append_null(),
                    Cell::Bytes(value) => builder.append_value(value),
                    other => return Err(unexpected_cell(name, other)),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Struct(fields) => build_range_struct_column(name, fields, index, rows),
        DataType::List(item) => match item.data_type() {
            DataType::Struct(fields) => build_range_list_column(name, item, fields, index, rows),
            element => build_scalar_list_column(name, item, element, index, rows),
        },
        other => Err(unsupported_arrow_type(name, other)),
    }
}

/// Builds a primitive column by extracting one native value per row.
fn build_primitive_column<T: ArrowPrimitiveType>(
    rows: &[TableRow],
    index: usize,
    name: &str,
    mut builder: PrimitiveBuilder<T>,
    extract: impl Fn(&Cell) -> EtlResult<Option<T::Native>>,
) -> EtlResult<ArrayRef> {
    for row in rows {
        match extract(cell_at(row, index, name)?)? {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Builds a list column by appending the elements of each row's ArrayCell.
fn build_list_column<B: ArrayBuilder>(
    rows: &[TableRow],
    index: usize,
    name: &str,
    item: Arc<Field>,
    values: B,
    append_elements: impl Fn(&mut B, &ArrayCell) -> EtlResult<()>,
) -> EtlResult<ArrayRef> {
    let mut builder = ListBuilder::new(values).with_field(item);
    for row in rows {
        match cell_at(row, index, name)? {
            Cell::Null => builder.append(false),
            Cell::Array(array) => {
                append_elements(builder.values(), array)?;
                builder.append(true);
            }
            other => return Err(unexpected_cell(name, other)),
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Builds a list column for scalar (non-range) element types.
fn build_scalar_list_column(
    name: &str,
    item: &Arc<Field>,
    element: &DataType,
    index: usize,
    rows: &[TableRow],
) -> EtlResult<ArrayRef> {
    macro_rules! primitive_list_column {
        ($builder:expr, $variant:ident, $conv:expr) => {
            build_list_column(rows, index, name, Arc::clone(item), $builder, |values, array| {
                match array {
                    ArrayCell::$variant(elements) => {
                        for element in elements {
                            match element {
                                Some(value) => values.append_value($conv(value)),
                                None => values.append_null(),
                            }
                        }
                        Ok(())
                    }
                    other => Err(unexpected_cell_in_array(name, other)),
                }
            })
        };
    }
    match element {
        DataType::Boolean => {
            primitive_list_column!(BooleanBuilder::new(), Bool, |v: &bool| *v)
        }
        DataType::Int16 => primitive_list_column!(Int16Builder::new(), I16, |v: &i16| *v),
        DataType::Int32 => primitive_list_column!(Int32Builder::new(), I32, |v: &i32| *v),
        DataType::UInt32 => primitive_list_column!(UInt32Builder::new(), U32, |v: &u32| *v),
        DataType::Int64 => primitive_list_column!(Int64Builder::new(), I64, |v: &i64| *v),
        DataType::Float32 => primitive_list_column!(Float32Builder::new(), F32, |v: &f32| *v),
        DataType::Float64 => primitive_list_column!(Float64Builder::new(), F64, |v: &f64| *v),
        DataType::Decimal128(precision, scale) => build_list_column(
            rows,
            index,
            name,
            Arc::clone(item),
            decimal_builder(*precision, *scale)?,
            |values, array| match array {
                ArrayCell::Numeric(elements) => {
                    for element in elements {
                        match element.as_ref().map(numeric_to_mantissa).transpose()?.flatten() {
                            Some(mantissa) => values.append_value(mantissa),
                            None => values.append_null(),
                        }
                    }
                    Ok(())
                }
                other => Err(unexpected_cell_in_array(name, other)),
            },
        ),
        DataType::Date32 => primitive_list_column!(Date32Builder::new(), Date, date_to_days),
        DataType::Time64(TimeUnit::Microsecond) => {
            primitive_list_column!(Time64MicrosecondBuilder::new(), Time, time_to_micros)
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => primitive_list_column!(
            TimestampMicrosecondBuilder::new(),
            Timestamp,
            |dt: &NaiveDateTime| dt.and_utc().timestamp_micros()
        ),
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => primitive_list_column!(
            TimestampMicrosecondBuilder::new().with_timezone(Arc::clone(tz)),
            TimestampTz,
            |dt: &DateTime<Utc>| dt.timestamp_micros()
        ),
        DataType::Utf8 => build_list_column(
            rows,
            index,
            name,
            Arc::clone(item),
            StringBuilder::new(),
            |values, array| {
                match array {
                    ArrayCell::String(elements) => {
                        for element in elements {
                            values.append_option(element.as_deref());
                        }
                    }
                    ArrayCell::Uuid(elements) => {
                        for element in elements {
                            values.append_option(element.map(|value| value.to_string()));
                        }
                    }
                    ArrayCell::Json(elements) => {
                        for element in elements {
                            values.append_option(element.as_ref().map(|value| value.to_string()));
                        }
                    }
                    other => return Err(unexpected_cell_in_array(name, other)),
                }
                Ok(())
            },
        ),
        DataType::Binary => build_list_column(
            rows,
            index,
            name,
            Arc::clone(item),
            BinaryBuilder::new(),
            |values, array| match array {
                ArrayCell::Bytes(elements) => {
                    for element in elements {
                        match element {
                            Some(value) => values.append_value(value),
                            None => values.append_null(),
                        }
                    }
                    Ok(())
                }
                other => Err(unexpected_cell_in_array(name, other)),
            },
        ),
        other => Err(unsupported_arrow_type(name, other)),
    }
}

/// Builds a struct column for a scalar range type.
fn build_range_struct_column(
    name: &str,
    fields: &Fields,
    index: usize,
    rows: &[TableRow],
) -> EtlResult<ArrayRef> {
    let bound_type = fields[0].data_type().clone();
    let mut builder = StructBuilder::from_fields(fields.clone(), rows.len());
    for row in rows {
        match cell_at(row, index, name)? {
            Cell::Null => append_null_range_struct(&mut builder, &bound_type, name)?,
            Cell::String(text) => {
                append_parsed_range(&mut builder, &bound_type, parse_range_text(text), name)?;
            }
            other => return Err(unexpected_cell(name, other)),
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Builds a list-of-struct column for a range array type.
fn build_range_list_column(
    name: &str,
    item: &Arc<Field>,
    fields: &Fields,
    index: usize,
    rows: &[TableRow],
) -> EtlResult<ArrayRef> {
    let bound_type = fields[0].data_type().clone();
    let mut builder = ListBuilder::new(StructBuilder::from_fields(fields.clone(), 0))
        .with_field(Arc::clone(item));
    for row in rows {
        match cell_at(row, index, name)? {
            Cell::Null => builder.append(false),
            Cell::String(text) => {
                for parsed in parse_range_array_text(text) {
                    append_parsed_range(builder.values(), &bound_type, parsed, name)?;
                }
                builder.append(true);
            }
            // CDC decodes range arrays as ArrayCell::String where each
            // element is a single range text like "[lower,upper)".
            Cell::Array(ArrayCell::String(elements)) => {
                for element in elements {
                    match element {
                        Some(text) => append_parsed_range(
                            builder.values(),
                            &bound_type,
                            parse_range_text(text),
                            name,
                        )?,
                        None => append_null_range_struct(builder.values(), &bound_type, name)?,
                    }
                }
                builder.append(true);
            }
            other => return Err(unexpected_cell(name, other)),
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Appends a parsed range to a range struct builder: empty ranges become
/// NULL structs, unbounded sides become NULL fields.
fn append_parsed_range(
    builder: &mut StructBuilder,
    bound_type: &DataType,
    parsed: ParsedRange,
    name: &str,
) -> EtlResult<()> {
    match parsed {
        ParsedRange::Empty => append_null_range_struct(builder, bound_type, name),
        ParsedRange::Bounds(lower, upper) => {
            append_range_bound(builder, 0, bound_type, lower.as_deref(), name)?;
            append_range_bound(builder, 1, bound_type, upper.as_deref(), name)?;
            builder.append(true);
            Ok(())
        }
    }
}

/// Appends a NULL struct, keeping child builder lengths aligned.
fn append_null_range_struct(
    builder: &mut StructBuilder,
    bound_type: &DataType,
    name: &str,
) -> EtlResult<()> {
    append_range_bound(builder, 0, bound_type, None, name)?;
    append_range_bound(builder, 1, bound_type, None, name)?;
    builder.append(false);
    Ok(())
}

/// Appends one range bound (lower = field 0, upper = field 1), parsing the
/// Postgres bound text into the struct field's arrow type.
fn append_range_bound(
    builder: &mut StructBuilder,
    field_index: usize,
    bound_type: &DataType,
    bound: Option<&str>,
    name: &str,
) -> EtlResult<()> {
    match bound_type {
        DataType::Int32 => {
            let parsed = bound
                .map(|text| text.trim().parse::<i32>().map_err(|_| invalid_range_bound(name)))
                .transpose()?;
            range_bound_builder::<Int32Builder>(builder, field_index, name)?
                .append_option(parsed);
        }
        DataType::Int64 => {
            let parsed = bound
                .map(|text| text.trim().parse::<i64>().map_err(|_| invalid_range_bound(name)))
                .transpose()?;
            range_bound_builder::<Int64Builder>(builder, field_index, name)?
                .append_option(parsed);
        }
        DataType::Decimal128(_, _) => {
            let parsed = bound.map(decimal_text_to_i128).transpose()?.flatten();
            range_bound_builder::<Decimal128Builder>(builder, field_index, name)?
                .append_option(parsed);
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let parsed = bound
                .map(|text| parse_timestamp_bound(text).ok_or_else(|| invalid_range_bound(name)))
                .transpose()?;
            range_bound_builder::<TimestampMicrosecondBuilder>(builder, field_index, name)?
                .append_option(parsed);
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            let parsed = bound
                .map(|text| parse_timestamptz_bound(text).ok_or_else(|| invalid_range_bound(name)))
                .transpose()?;
            range_bound_builder::<TimestampMicrosecondBuilder>(builder, field_index, name)?
                .append_option(parsed);
        }
        DataType::Date32 => {
            let parsed = bound
                .map(|text| {
                    NaiveDate::parse_from_str(text.trim(), "%Y-%m-%d")
                        .map(|date| date_to_days(&date))
                        .map_err(|_| invalid_range_bound(name))
                })
                .transpose()?;
            range_bound_builder::<Date32Builder>(builder, field_index, name)?
                .append_option(parsed);
        }
        other => return Err(unsupported_arrow_type(name, other)),
    }
    Ok(())
}

/// Downcasts a struct child builder, erroring instead of panicking on an
/// internal type mismatch.
fn range_bound_builder<'a, B: ArrayBuilder>(
    builder: &'a mut StructBuilder,
    field_index: usize,
    name: &str,
) -> EtlResult<&'a mut B> {
    builder.field_builder::<B>(field_index).ok_or_else(|| {
        etl_error!(
            ErrorKind::ConversionError,
            "Arrow staging range builder mismatch",
            format!("column `{name}` range bound builder does not match its arrow type")
        )
    })
}

/// Parses a Postgres tstzrange bound (e.g. `2026-01-28 01:17:00+00`).
fn parse_timestamptz_bound(text: &str) -> Option<i64> {
    let trimmed = text.trim();
    DateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S%#z")
        .or_else(|_| DateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S%.f%#z"))
        .ok()
        .map(|dt| dt.timestamp_micros())
}

/// Parses a Postgres tsrange bound (e.g. `2026-01-28 01:17:00`).
fn parse_timestamp_bound(text: &str) -> Option<i64> {
    let trimmed = text.trim();
    NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S%.f"))
        .ok()
        .map(|dt| dt.and_utc().timestamp_micros())
}

/// Converts a PgNumeric to a Decimal128(38, 10) mantissa, coercing
/// NaN/Infinity to NULL — matching the literal path's behavior.
fn numeric_to_mantissa(numeric: &PgNumeric) -> EtlResult<Option<i128>> {
    match numeric {
        PgNumeric::NaN | PgNumeric::PositiveInfinity | PgNumeric::NegativeInfinity => Ok(None),
        PgNumeric::Value { .. } => decimal_text_to_i128(&numeric.to_string()),
    }
}

fn decimal_builder(precision: u8, scale: i8) -> EtlResult<Decimal128Builder> {
    Decimal128Builder::new().with_precision_and_scale(precision, scale).map_err(|error| {
        etl_error!(
            ErrorKind::ConversionError,
            "Invalid decimal precision/scale for Arrow staging",
            error.to_string()
        )
    })
}

fn date_to_days(date: &NaiveDate) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    date.signed_duration_since(epoch).num_days() as i32
}

fn time_to_micros(time: &NaiveTime) -> i64 {
    let midnight = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
    time.signed_duration_since(midnight).num_microseconds().unwrap_or(0)
}

fn cell_at<'a>(row: &'a TableRow, index: usize, name: &str) -> EtlResult<&'a Cell> {
    row.values().get(index).ok_or_else(|| {
        etl_error!(
            ErrorKind::ConversionError,
            "Row is missing a value for a staged column",
            format!("column `{name}` has no value at index {index}")
        )
    })
}

fn unexpected_cell(name: &str, cell: &Cell) -> etl::error::EtlError {
    etl_error!(
        ErrorKind::ConversionError,
        "Unexpected cell variant for Arrow staging",
        format!("column `{name}` received cell variant {}", cell_variant_name(cell))
    )
}

fn unexpected_cell_in_array(name: &str, array: &ArrayCell) -> etl::error::EtlError {
    etl_error!(
        ErrorKind::ConversionError,
        "Unexpected cell variant for Arrow staging",
        format!("column `{name}` received cell variant {}", array_cell_variant_name(array))
    )
}

fn invalid_range_bound(name: &str) -> etl::error::EtlError {
    etl_error!(
        ErrorKind::ConversionError,
        "Invalid range bound for Arrow staging",
        format!("column `{name}` contains a range bound that does not parse as its bound type")
    )
}

fn unsupported_arrow_type(name: &str, arrow_type: &DataType) -> etl::error::EtlError {
    etl_error!(
        ErrorKind::ConversionError,
        "Unsupported arrow type for Arrow staging",
        format!("column `{name}` maps to unsupported arrow type {arrow_type:?}")
    )
}

fn cell_variant_name(cell: &Cell) -> &'static str {
    match cell {
        Cell::Null => "Null",
        Cell::Bool(_) => "Bool",
        Cell::String(_) => "String",
        Cell::I16(_) => "I16",
        Cell::I32(_) => "I32",
        Cell::U32(_) => "U32",
        Cell::I64(_) => "I64",
        Cell::F32(_) => "F32",
        Cell::F64(_) => "F64",
        Cell::Numeric(_) => "Numeric",
        Cell::Date(_) => "Date",
        Cell::Time(_) => "Time",
        Cell::Timestamp(_) => "Timestamp",
        Cell::TimestampTz(_) => "TimestampTz",
        Cell::Uuid(_) => "Uuid",
        Cell::Json(_) => "Json",
        Cell::Bytes(_) => "Bytes",
        Cell::Array(array) => array_cell_variant_name(array),
    }
}

fn array_cell_variant_name(array: &ArrayCell) -> &'static str {
    match array {
        ArrayCell::Bool(_) => "Array(Bool)",
        ArrayCell::String(_) => "Array(String)",
        ArrayCell::I16(_) => "Array(I16)",
        ArrayCell::I32(_) => "Array(I32)",
        ArrayCell::U32(_) => "Array(U32)",
        ArrayCell::I64(_) => "Array(I64)",
        ArrayCell::F32(_) => "Array(F32)",
        ArrayCell::F64(_) => "Array(F64)",
        ArrayCell::Numeric(_) => "Array(Numeric)",
        ArrayCell::Date(_) => "Array(Date)",
        ArrayCell::Time(_) => "Array(Time)",
        ArrayCell::Timestamp(_) => "Array(Timestamp)",
        ArrayCell::TimestampTz(_) => "Array(TimestampTz)",
        ArrayCell::Uuid(_) => "Array(Uuid)",
        ArrayCell::Json(_) => "Array(Json)",
        ArrayCell::Bytes(_) => "Array(Bytes)",
    }
}

#[cfg(test)]
mod batch_tests {
    use super::*;
    use duckdb::arrow::array::{
        Array, Int32Array, ListArray, StringArray, StructArray, TimestampMicrosecondArray,
    };
    use etl::types::{ArrayCell, Cell, ColumnSchema, PgNumeric, TableRow, Type};
    use std::str::FromStr;

    fn lines_like_schema() -> Vec<ColumnSchema> {
        vec![
            ColumnSchema::new("id".into(), Type::UUID, -1, 1, Some(1), false),
            ColumnSchema::new("debit".into(), Type::NUMERIC, -1, 2, None, true),
            ColumnSchema::new("tag_ids".into(), Type::UUID_ARRAY, -1, 3, None, true),
            ColumnSchema::new("effective_range".into(), Type::TSTZ_RANGE_ARRAY, -1, 4, None, true),
            ColumnSchema::new("created_at".into(), Type::TIMESTAMPTZ, -1, 5, None, true),
            ColumnSchema::new("description".into(), Type::TEXT, -1, 6, None, true),
        ]
    }

    fn sample_rows() -> Vec<TableRow> {
        vec![
            TableRow::new(vec![
                Cell::Uuid(uuid::Uuid::from_str("01234567-89ab-cdef-0123-456789abcdef").unwrap()),
                Cell::Numeric(PgNumeric::from_str("123.45").unwrap()),
                Cell::Array(ArrayCell::Uuid(vec![
                    Some(uuid::Uuid::from_str("11111111-1111-1111-1111-111111111111").unwrap()),
                    None,
                ])),
                Cell::Array(ArrayCell::String(vec![Some(
                    r#"["2026-01-28 01:17:00+00","2026-01-28 05:25:00+00")"#.to_owned(),
                )])),
                Cell::TimestampTz("2026-01-01T00:00:00Z".parse().unwrap()),
                Cell::String("hello".into()),
            ]),
            TableRow::new(vec![
                Cell::Uuid(uuid::Uuid::nil()),
                Cell::Numeric(PgNumeric::NaN), // NaN coerces to NULL
                Cell::Null,
                Cell::Array(ArrayCell::String(vec![])), // empty range array
                Cell::Null,
                Cell::Null,
            ]),
        ]
    }

    /// Stage two lines-shaped rows through a real DuckDB temp table created
    /// from the spec DDL, then read them back through the cast expressions.
    #[test]
    fn record_batch_roundtrip_through_staging_table() {
        let schemas = lines_like_schema();
        let specs = build_staging_specs(&schemas).unwrap();
        let batch: RecordBatch = build_record_batch(&specs, &sample_rows()).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let conn = duckdb::Connection::open_in_memory().unwrap();
        let columns_ddl = specs
            .iter()
            .map(|s| format!("{} {}", quote_identifier(&s.name), s.staging_sql_type))
            .collect::<Vec<_>>()
            .join(", ");
        // Pin the session timezone so TIMESTAMPTZ::VARCHAR is deterministic.
        conn.execute_batch(&format!(
            "SET TimeZone = 'UTC'; CREATE TEMP TABLE staging_rt ({columns_ddl});"
        ))
        .unwrap();
        let mut appender = conn.appender("staging_rt").unwrap();
        appender.append_record_batch(batch).unwrap();
        appender.flush().unwrap();

        let select_list =
            specs.iter().map(|s| s.select_expr()).collect::<Vec<_>>().join(", ");
        let (id, debit, tag_count, lower, desc): (String, Option<String>, i64, String, String) =
            conn.query_row(
                &format!(
                    "SELECT \"id\"::VARCHAR, \"debit\"::VARCHAR, len(\"tag_ids\"),
                            \"effective_range\"[1].\"lower\"::VARCHAR, \"description\"
                     FROM (SELECT {select_list} FROM staging_rt) WHERE \"description\" IS NOT NULL"
                ),
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .unwrap();
        assert_eq!(id, "01234567-89ab-cdef-0123-456789abcdef");
        assert_eq!(debit.as_deref(), Some("123.4500000000"));
        assert_eq!(tag_count, 2);
        assert!(lower.starts_with("2026-01-28 01:17:00"), "lower was {lower}");
        assert_eq!(desc, "hello");

        let (nan_debit, empty_ranges): (Option<String>, i64) = conn
            .query_row(
                &format!(
                    "SELECT \"debit\"::VARCHAR, len(\"effective_range\")
                     FROM (SELECT {select_list} FROM staging_rt) WHERE \"description\" IS NULL"
                ),
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(nan_debit, None);
        assert_eq!(empty_ranges, 0);
    }

    /// Exercises the production dedup SQL shape used by `apply_deduped_upsert`
    /// / `apply_merge_mutation`: a `QUALIFY ROW_NUMBER() OVER (PARTITION BY
    /// <identity> ORDER BY rowid DESC) = 1` inner subquery feeding a
    /// cast-applying outer SELECT. Proves rowid is valid inside the inner
    /// subquery and that the last-appended row wins, surviving the cast.
    #[test]
    fn deduped_insert_keeps_last_appended_per_identity() {
        // UUID id forces a CAST column in the outer select; INT4 n is identity.
        let schemas = vec![
            ColumnSchema::new("id".into(), Type::UUID, -1, 1, Some(1), false),
            ColumnSchema::new("n".into(), Type::INT4, -1, 2, None, true),
        ];
        let specs = build_staging_specs(&schemas).unwrap();
        let id_a = "11111111-1111-1111-1111-111111111111";
        let id_b = "22222222-2222-2222-2222-222222222222";
        // Append order matters: for id_a, n=20 is appended last and must win.
        let rows = vec![
            TableRow::new(vec![Cell::String(id_a.to_owned()), Cell::I32(10)]),
            TableRow::new(vec![Cell::String(id_a.to_owned()), Cell::I32(20)]),
            TableRow::new(vec![Cell::String(id_b.to_owned()), Cell::I32(30)]),
        ];
        let batch = build_record_batch(&specs, &rows).unwrap();
        assert_eq!(batch.num_rows(), 3);

        let conn = duckdb::Connection::open_in_memory().unwrap();
        let staging_ddl = specs
            .iter()
            .map(|s| format!("{} {}", quote_identifier(&s.name), s.staging_sql_type))
            .collect::<Vec<_>>()
            .join(", ");
        conn.execute_batch(&format!(
            "CREATE TEMP TABLE staging_dedup ({staging_ddl}); \
             CREATE TEMP TABLE target_dedup (\"id\" UUID, \"n\" INTEGER);"
        ))
        .unwrap();

        let mut appender = conn.appender("staging_dedup").unwrap();
        appender.append_record_batch(batch).unwrap();
        appender.flush().unwrap();
        drop(appender);

        let select_exprs =
            specs.iter().map(|s| s.select_expr()).collect::<Vec<_>>().join(", ");
        let insert_columns =
            specs.iter().map(|s| quote_identifier(&s.name)).collect::<Vec<_>>().join(", ");
        // Mirrors the production dedup INSERT shape exactly.
        conn.execute_batch(&format!(
            "INSERT INTO target_dedup ({insert_columns}) \
             SELECT {select_exprs} FROM (SELECT * FROM staging_dedup \
             QUALIFY ROW_NUMBER() OVER (PARTITION BY \"id\" ORDER BY rowid DESC) = 1)"
        ))
        .unwrap();

        let total: i64 =
            conn.query_row("SELECT COUNT(*) FROM target_dedup", [], |row| row.get(0)).unwrap();
        assert_eq!(total, 2, "one row per identity after dedup");

        let n_a: i32 = conn
            .query_row(
                &format!("SELECT \"n\" FROM target_dedup WHERE \"id\" = '{id_a}'"),
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n_a, 20, "last-appended row wins for id_a");

        let n_b: i32 = conn
            .query_row(
                &format!("SELECT \"n\" FROM target_dedup WHERE \"id\" = '{id_b}'"),
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n_b, 30);
    }

    #[test]
    fn scalar_tstz_range_builds_nullable_structs() {
        let schemas = vec![ColumnSchema::new("r".into(), Type::TSTZ_RANGE, -1, 1, None, true)];
        let specs = build_staging_specs(&schemas).unwrap();
        let rows = vec![
            TableRow::new(vec![Cell::String(r#"["2026-01-28 01:17:00+00",)"#.to_owned())]),
            TableRow::new(vec![Cell::String("empty".to_owned())]),
            TableRow::new(vec![Cell::Null]),
        ];
        let batch = build_record_batch(&specs, &rows).unwrap();
        let ranges = batch.column(0).as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(ranges.len(), 3);
        assert!(ranges.is_valid(0));
        assert!(ranges.is_null(1)); // empty range -> NULL struct
        assert!(ranges.is_null(2)); // NULL cell -> NULL struct
        let lower =
            ranges.column(0).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        let upper =
            ranges.column(1).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        let expected: DateTime<Utc> = "2026-01-28T01:17:00Z".parse().unwrap();
        assert_eq!(lower.value(0), expected.timestamp_micros());
        assert!(upper.is_null(0)); // unbounded upper -> NULL field
    }

    #[test]
    fn int_array_preserves_null_elements() {
        let schemas = vec![ColumnSchema::new("xs".into(), Type::INT4_ARRAY, -1, 1, None, true)];
        let specs = build_staging_specs(&schemas).unwrap();
        let rows = vec![
            TableRow::new(vec![Cell::Array(ArrayCell::I32(vec![Some(1), None, Some(3)]))]),
            TableRow::new(vec![Cell::Null]),
        ];
        let batch = build_record_batch(&specs, &rows).unwrap();
        let lists = batch.column(0).as_any().downcast_ref::<ListArray>().unwrap();
        assert!(lists.is_valid(0));
        assert!(lists.is_null(1));
        let first = lists.value(0);
        let first = first.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(first.len(), 3);
        assert_eq!(first.value(0), 1);
        assert!(first.is_null(1));
        assert_eq!(first.value(2), 3);
    }

    #[test]
    fn json_column_serializes_to_text() {
        let schemas =
            vec![ColumnSchema::new("payload".into(), Type::JSONB, -1, 1, None, true)];
        let specs = build_staging_specs(&schemas).unwrap();
        let rows = vec![
            TableRow::new(vec![Cell::Json(serde_json::json!({"a": 1}))]),
            TableRow::new(vec![Cell::Null]),
        ];
        let batch = build_record_batch(&specs, &rows).unwrap();
        let texts = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(texts.value(0), r#"{"a":1}"#);
        assert!(texts.is_null(1));
    }

    #[test]
    fn wrong_cell_variant_names_column_and_variant_only() {
        let schemas = vec![ColumnSchema::new("debit".into(), Type::NUMERIC, -1, 1, None, true)];
        let specs = build_staging_specs(&schemas).unwrap();
        let rows = vec![TableRow::new(vec![Cell::Bool(true)])];
        let error = build_record_batch(&specs, &rows).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("debit"), "missing column name: {message}");
        assert!(message.contains("Bool"), "missing variant name: {message}");
        assert!(!message.contains("true"), "leaked cell value: {message}");
    }

    #[test]
    fn record_batch_with_cdc_has_version_and_deleted() {
        use duckdb::arrow::array::{BooleanArray, Decimal128Array};
        let specs = build_staging_specs_with_cdc(&lines_like_schema()).unwrap();
        let rows = sample_rows();
        let batch = build_record_batch_with_cdc(&rows, &specs, 42u128, false).unwrap();
        assert_eq!(batch.num_columns(), specs.len());
        assert_eq!(batch.num_rows(), rows.len());
        let version_col = batch
            .column(specs.len() - 2)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("_etl_version should be Decimal128Array");
        for i in 0..rows.len() {
            assert_eq!(version_col.value(i), 42i128, "row {i}: expected version 42");
        }
        let deleted_col = batch
            .column(specs.len() - 1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("_etl_deleted should be BooleanArray");
        for i in 0..rows.len() {
            assert!(!deleted_col.value(i), "row {i}: expected deleted=false");
        }
    }

    /// Builds the staging payload for a single-column schema, creates a real
    /// DuckDB temp table from the spec DDL, appends the batch, and returns the
    /// open connection plus the `SELECT` list that applies the read-time casts.
    /// The session timezone is pinned to UTC so TIMESTAMPTZ rendering is
    /// deterministic (mirrors `record_batch_roundtrip_through_staging_table`).
    fn stage_single_column(
        schema: ColumnSchema,
        rows: Vec<TableRow>,
    ) -> (duckdb::Connection, String) {
        let specs = build_staging_specs(&[schema]).unwrap();
        let batch = build_record_batch(&specs, &rows).unwrap();
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let columns_ddl = specs
            .iter()
            .map(|s| format!("{} {}", quote_identifier(&s.name), s.staging_sql_type))
            .collect::<Vec<_>>()
            .join(", ");
        conn.execute_batch(&format!(
            "SET TimeZone = 'UTC'; CREATE TEMP TABLE staging_one ({columns_ddl});"
        ))
        .unwrap();
        let mut appender = conn.appender("staging_one").unwrap();
        appender.append_record_batch(batch).unwrap();
        appender.flush().unwrap();
        drop(appender);
        let select_list = specs.iter().map(|s| s.select_expr()).collect::<Vec<_>>().join(", ");
        (conn, select_list)
    }

    /// Reads `r."lower"::VARCHAR` and `r."upper"::VARCHAR` for the single
    /// staged scalar-range row, through the cast-applying SELECT.
    fn read_scalar_bounds(
        conn: &duckdb::Connection,
        select_list: &str,
    ) -> (Option<String>, Option<String>) {
        conn.query_row(
            &format!(
                "SELECT \"r\".\"lower\"::VARCHAR, \"r\".\"upper\"::VARCHAR
                 FROM (SELECT {select_list} FROM staging_one)"
            ),
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap()
    }

    /// Round-trips every scalar range bound type through a real staging table
    /// and asserts the rendered lower/upper bounds. Exercises the
    /// `build_range_struct_column` path plus each `append_range_bound` arm.
    #[test]
    fn scalar_ranges_roundtrip_all_bound_types() {
        // int4
        let (conn, sel) = stage_single_column(
            ColumnSchema::new("r".into(), Type::INT4_RANGE, -1, 1, None, true),
            vec![TableRow::new(vec![Cell::String("[1,10)".into())])],
        );
        let (lower, upper) = read_scalar_bounds(&conn, &sel);
        assert_eq!(lower.as_deref(), Some("1"));
        assert_eq!(upper.as_deref(), Some("10"));

        // int8
        let (conn, sel) = stage_single_column(
            ColumnSchema::new("r".into(), Type::INT8_RANGE, -1, 1, None, true),
            vec![TableRow::new(vec![Cell::String("[100,200)".into())])],
        );
        let (lower, upper) = read_scalar_bounds(&conn, &sel);
        assert_eq!(lower.as_deref(), Some("100"));
        assert_eq!(upper.as_deref(), Some("200"));

        // numeric — DECIMAL(38,10) renders with trailing zeros to scale 10.
        let (conn, sel) = stage_single_column(
            ColumnSchema::new("r".into(), Type::NUM_RANGE, -1, 1, None, true),
            vec![TableRow::new(vec![Cell::String("[1.50,3.25)".into())])],
        );
        let (lower, upper) = read_scalar_bounds(&conn, &sel);
        assert_eq!(lower.as_deref(), Some("1.5000000000"));
        assert_eq!(upper.as_deref(), Some("3.2500000000"));

        // date
        let (conn, sel) = stage_single_column(
            ColumnSchema::new("r".into(), Type::DATE_RANGE, -1, 1, None, true),
            vec![TableRow::new(vec![Cell::String("[2026-01-01,2026-02-01)".into())])],
        );
        let (lower, upper) = read_scalar_bounds(&conn, &sel);
        assert_eq!(lower.as_deref(), Some("2026-01-01"));
        assert_eq!(upper.as_deref(), Some("2026-02-01"));

        // ts (no tz)
        let (conn, sel) = stage_single_column(
            ColumnSchema::new("r".into(), Type::TS_RANGE, -1, 1, None, true),
            vec![TableRow::new(vec![Cell::String(
                r#"["2026-01-28 01:17:00","2026-01-28 05:25:00")"#.into(),
            )])],
        );
        let (lower, upper) = read_scalar_bounds(&conn, &sel);
        assert!(
            lower.as_deref().unwrap().starts_with("2026-01-28 01:17:00"),
            "ts lower was {lower:?}"
        );
        assert!(
            upper.as_deref().unwrap().starts_with("2026-01-28 05:25:00"),
            "ts upper was {upper:?}"
        );

        // tstz — UTC-pinned session makes the rendering deterministic.
        let (conn, sel) = stage_single_column(
            ColumnSchema::new("r".into(), Type::TSTZ_RANGE, -1, 1, None, true),
            vec![TableRow::new(vec![Cell::String(
                r#"["2026-01-28 01:17:00+00","2026-01-28 05:25:00+00")"#.into(),
            )])],
        );
        let (lower, upper) = read_scalar_bounds(&conn, &sel);
        assert!(
            lower.as_deref().unwrap().starts_with("2026-01-28 01:17:00"),
            "tstz lower was {lower:?}"
        );
        assert!(
            upper.as_deref().unwrap().starts_with("2026-01-28 05:25:00"),
            "tstz upper was {upper:?}"
        );
    }

    /// Round-trips scalar-range edge cases (empty, unbounded side, NULL cell)
    /// through a real staging table, using int4 as the representative bound.
    #[test]
    fn scalar_range_edge_cases_roundtrip() {
        // empty range -> NULL struct
        let (conn, sel) = stage_single_column(
            ColumnSchema::new("r".into(), Type::INT4_RANGE, -1, 1, None, true),
            vec![TableRow::new(vec![Cell::String("empty".into())])],
        );
        let null_count: i64 = conn
            .query_row(
                &format!("SELECT count(*) FROM (SELECT {sel} FROM staging_one) WHERE \"r\" IS NULL"),
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(null_count, 1, "empty range should round-trip as a NULL struct");

        // unbounded upper -> lower=5, upper IS NULL
        let (conn, sel) = stage_single_column(
            ColumnSchema::new("r".into(), Type::INT4_RANGE, -1, 1, None, true),
            vec![TableRow::new(vec![Cell::String("[5,)".into())])],
        );
        let (lower, upper) = read_scalar_bounds(&conn, &sel);
        assert_eq!(lower.as_deref(), Some("5"));
        assert_eq!(upper, None, "unbounded upper should be NULL");

        // NULL cell -> NULL struct
        let (conn, sel) = stage_single_column(
            ColumnSchema::new("r".into(), Type::INT4_RANGE, -1, 1, None, true),
            vec![TableRow::new(vec![Cell::Null])],
        );
        let null_count: i64 = conn
            .query_row(
                &format!("SELECT count(*) FROM (SELECT {sel} FROM staging_one) WHERE \"r\" IS NULL"),
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(null_count, 1, "NULL cell should round-trip as a NULL struct");
    }

    /// Round-trips a non-tstz range array (int4) through a real staging table,
    /// covering the whole-array text form, the per-element `ArrayCell::String`
    /// form (including a NULL element), and the empty-array form. Exercises
    /// `build_range_list_column`.
    #[test]
    fn int4_range_array_roundtrips() {
        // Whole-array text form: {"[1,2)","[3,4)"}
        let (conn, sel) = stage_single_column(
            ColumnSchema::new("r".into(), Type::INT4_RANGE_ARRAY, -1, 1, None, true),
            vec![TableRow::new(vec![Cell::String(r#"{"[1,2)","[3,4)"}"#.into())])],
        );
        let (len, e1_lower, e2_upper): (i64, Option<String>, Option<String>) = conn
            .query_row(
                &format!(
                    "SELECT len(\"r\"), \"r\"[1].\"lower\"::VARCHAR, \"r\"[2].\"upper\"::VARCHAR
                     FROM (SELECT {sel} FROM staging_one)"
                ),
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(len, 2);
        assert_eq!(e1_lower.as_deref(), Some("1"));
        assert_eq!(e2_upper.as_deref(), Some("4"));

        // Per-element form with a NULL element -> NULL struct in the list.
        let (conn, sel) = stage_single_column(
            ColumnSchema::new("r".into(), Type::INT4_RANGE_ARRAY, -1, 1, None, true),
            vec![TableRow::new(vec![Cell::Array(ArrayCell::String(vec![
                Some("[1,2)".into()),
                None,
            ]))])],
        );
        let (len, e1_lower, e2_is_null): (i64, Option<String>, bool) = conn
            .query_row(
                &format!(
                    "SELECT len(\"r\"), \"r\"[1].\"lower\"::VARCHAR, \"r\"[2] IS NULL
                     FROM (SELECT {sel} FROM staging_one)"
                ),
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(len, 2);
        assert_eq!(e1_lower.as_deref(), Some("1"));
        assert!(e2_is_null, "None element should round-trip as a NULL struct");

        // Empty array -> len 0.
        let (conn, sel) = stage_single_column(
            ColumnSchema::new("r".into(), Type::INT4_RANGE_ARRAY, -1, 1, None, true),
            vec![TableRow::new(vec![Cell::Array(ArrayCell::String(vec![]))])],
        );
        let len: i64 = conn
            .query_row(
                &format!("SELECT len(\"r\") FROM (SELECT {sel} FROM staging_one)"),
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(len, 0);
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
