use chrono::{NaiveDate, NaiveTime};
use duckdb::types::{TimeUnit, Value};
use etl::types::{ArrayCell, Cell, ColumnSchema, TableRow, Type, is_range_array_type, is_range_type};
use pg_escape::quote_literal;

use crate::ducklake::schema::duckdb_range_bound_type;

/// Prepared row payload reused across retry attempts.
pub(super) enum PreparedRows {
    Appender(Vec<Vec<Value>>),
    SqlLiterals(Vec<String>),
}

/// Converts table rows into a retryable payload for DuckDB writes.
pub(super) fn prepare_rows(
    table_rows: Vec<TableRow>,
    column_schemas: &[ColumnSchema],
) -> PreparedRows {
    let has_ranges = column_schemas
        .iter()
        .any(|c| is_range_type(&c.typ) || is_range_array_type(&c.typ));

    if has_ranges
        || table_rows
            .iter()
            .any(|row| row.values().iter().any(cell_requires_sql_literals))
    {
        return PreparedRows::SqlLiterals(
            table_rows
                .into_iter()
                .map(|row| table_row_to_sql_literal_typed(&row, column_schemas))
                .collect(),
        );
    }

    PreparedRows::Appender(
        table_rows
            .into_iter()
            .map(|row| row.into_values().into_iter().map(cell_to_value).collect())
            .collect(),
    )
}

/// Serializes a borrowed row into a SQL `VALUES (...)` tuple.
pub(super) fn table_row_to_sql_literal_ref(row: &TableRow) -> String {
    format!("({})", row.values().iter().map(cell_to_sql_literal_ref).collect::<Vec<_>>().join(", "))
}

/// Serializes a borrowed cell into a DuckDB SQL literal expression.
pub(super) fn cell_to_sql_literal_ref(cell: &Cell) -> String {
    cell_to_sql_literal(cell_to_owned(cell))
}

/// Returns whether a cell must bypass the DuckDB appender path.
fn cell_requires_sql_literals(cell: &Cell) -> bool {
    matches!(cell, Cell::Array(_) | Cell::Numeric(_))
}

/// Converts a [`Cell`] into a DuckDB SQL literal expression.
fn cell_to_sql_literal(cell: Cell) -> String {
    match cell {
        Cell::Null => "NULL".to_owned(),
        Cell::Bool(b) => {
            if b {
                "TRUE".to_owned()
            } else {
                "FALSE".to_owned()
            }
        }
        Cell::String(s) => quote_literal(&s),
        Cell::I16(i) => i.to_string(),
        Cell::I32(i) => i.to_string(),
        Cell::U32(u) => u.to_string(),
        Cell::I64(i) => i.to_string(),
        Cell::F32(f) => float_literal(f as f64, false),
        Cell::F64(f) => float_literal(f, true),
        Cell::Numeric(n) => numeric_to_decimal_literal(&n),
        Cell::Date(d) => format!("DATE '{}'", d.format("%Y-%m-%d")),
        Cell::Time(t) => format!("TIME '{}'", t.format("%H:%M:%S%.6f")),
        Cell::Timestamp(dt) => {
            format!("TIMESTAMP '{}'", dt.format("%Y-%m-%d %H:%M:%S%.6f"))
        }
        Cell::TimestampTz(dt) => {
            format!("TIMESTAMPTZ '{}'", dt.format("%Y-%m-%d %H:%M:%S%.6f%:z"))
        }
        Cell::Uuid(u) => format!("CAST({} AS UUID)", quote_literal(&u.to_string())),
        Cell::Json(j) => format!("CAST({} AS JSON)", quote_literal(&j.to_string())),
        Cell::Bytes(b) => format!("from_hex('{}')", encode_hex(&b)),
        Cell::Array(arr) => array_cell_to_sql_literal(arr),
    }
}

/// Clones a [`Cell`] from a borrowed row reference.
fn cell_to_owned(cell: &Cell) -> Cell {
    match cell {
        Cell::Null => Cell::Null,
        Cell::Bool(value) => Cell::Bool(*value),
        Cell::String(value) => Cell::String(value.clone()),
        Cell::I16(value) => Cell::I16(*value),
        Cell::I32(value) => Cell::I32(*value),
        Cell::U32(value) => Cell::U32(*value),
        Cell::I64(value) => Cell::I64(*value),
        Cell::F32(value) => Cell::F32(*value),
        Cell::F64(value) => Cell::F64(*value),
        Cell::Numeric(value) => Cell::Numeric(value.clone()),
        Cell::Date(value) => Cell::Date(*value),
        Cell::Time(value) => Cell::Time(*value),
        Cell::Timestamp(value) => Cell::Timestamp(*value),
        Cell::TimestampTz(value) => Cell::TimestampTz(*value),
        Cell::Uuid(value) => Cell::Uuid(*value),
        Cell::Json(value) => Cell::Json(value.clone()),
        Cell::Bytes(value) => Cell::Bytes(value.clone()),
        Cell::Array(value) => Cell::Array(array_cell_to_owned(value)),
    }
}

/// Clones an [`ArrayCell`] from a borrowed row reference.
fn array_cell_to_owned(cell: &ArrayCell) -> ArrayCell {
    match cell {
        ArrayCell::Bool(values) => ArrayCell::Bool(values.clone()),
        ArrayCell::String(values) => ArrayCell::String(values.clone()),
        ArrayCell::I16(values) => ArrayCell::I16(values.clone()),
        ArrayCell::I32(values) => ArrayCell::I32(values.clone()),
        ArrayCell::U32(values) => ArrayCell::U32(values.clone()),
        ArrayCell::I64(values) => ArrayCell::I64(values.clone()),
        ArrayCell::F32(values) => ArrayCell::F32(values.clone()),
        ArrayCell::F64(values) => ArrayCell::F64(values.clone()),
        ArrayCell::Numeric(values) => ArrayCell::Numeric(values.clone()),
        ArrayCell::Date(values) => ArrayCell::Date(values.clone()),
        ArrayCell::Time(values) => ArrayCell::Time(values.clone()),
        ArrayCell::Timestamp(values) => ArrayCell::Timestamp(values.clone()),
        ArrayCell::TimestampTz(values) => ArrayCell::TimestampTz(values.clone()),
        ArrayCell::Uuid(values) => ArrayCell::Uuid(values.clone()),
        ArrayCell::Json(values) => ArrayCell::Json(values.clone()),
        ArrayCell::Bytes(values) => ArrayCell::Bytes(values.clone()),
    }
}

/// Converts an [`ArrayCell`] into a DuckDB list literal expression.
fn array_cell_to_sql_literal(arr: ArrayCell) -> String {
    let values: Vec<String> = match arr {
        ArrayCell::Bool(v) => v
            .into_iter()
            .map(|o| {
                o.map_or_else(
                    || "NULL".to_owned(),
                    |value| if value { "TRUE" } else { "FALSE" }.to_owned(),
                )
            })
            .collect(),
        ArrayCell::String(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| quote_literal(&value)))
            .collect(),
        ArrayCell::I16(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| value.to_string()))
            .collect(),
        ArrayCell::I32(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| value.to_string()))
            .collect(),
        ArrayCell::U32(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| value.to_string()))
            .collect(),
        ArrayCell::I64(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| value.to_string()))
            .collect(),
        ArrayCell::F32(v) => v
            .into_iter()
            .map(|o| {
                o.map_or_else(|| "NULL".to_owned(), |value| float_literal(value as f64, false))
            })
            .collect(),
        ArrayCell::F64(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| float_literal(value, true)))
            .collect(),
        ArrayCell::Numeric(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| quote_literal(&value.to_string())))
            .collect(),
        ArrayCell::Date(v) => v
            .into_iter()
            .map(|o| {
                o.map_or_else(
                    || "NULL".to_owned(),
                    |value| format!("DATE '{}'", value.format("%Y-%m-%d")),
                )
            })
            .collect(),
        ArrayCell::Time(v) => v
            .into_iter()
            .map(|o| {
                o.map_or_else(
                    || "NULL".to_owned(),
                    |value| format!("TIME '{}'", value.format("%H:%M:%S%.6f")),
                )
            })
            .collect(),
        ArrayCell::Timestamp(v) => v
            .into_iter()
            .map(|o| {
                o.map_or_else(
                    || "NULL".to_owned(),
                    |value| format!("TIMESTAMP '{}'", value.format("%Y-%m-%d %H:%M:%S%.6f")),
                )
            })
            .collect(),
        ArrayCell::TimestampTz(v) => v
            .into_iter()
            .map(|o| {
                o.map_or_else(
                    || "NULL".to_owned(),
                    |value| format!("TIMESTAMPTZ '{}'", value.format("%Y-%m-%d %H:%M:%S%.6f%:z")),
                )
            })
            .collect(),
        ArrayCell::Uuid(v) => v
            .into_iter()
            .map(|o| {
                o.map_or_else(
                    || "NULL".to_owned(),
                    |value| format!("CAST({} AS UUID)", quote_literal(&value.to_string())),
                )
            })
            .collect(),
        ArrayCell::Json(v) => v
            .into_iter()
            .map(|o| {
                o.map_or_else(
                    || "NULL".to_owned(),
                    |value| format!("CAST({} AS JSON)", quote_literal(&value.to_string())),
                )
            })
            .collect(),
        ArrayCell::Bytes(v) => v
            .into_iter()
            .map(|o| {
                o.map_or_else(
                    || "NULL".to_owned(),
                    |value| format!("from_hex('{}')", encode_hex(&value)),
                )
            })
            .collect(),
    };

    format!("[{}]", values.join(", "))
}

/// Returns a DuckDB SQL literal for a floating-point value.
fn float_literal(value: f64, is_double: bool) -> String {
    if value.is_nan() {
        return if is_double {
            "CAST('NaN' AS DOUBLE)".to_owned()
        } else {
            "CAST('NaN' AS FLOAT)".to_owned()
        };
    }
    if value == f64::INFINITY {
        return if is_double {
            "CAST('Infinity' AS DOUBLE)".to_owned()
        } else {
            "CAST('Infinity' AS FLOAT)".to_owned()
        };
    }
    if value == f64::NEG_INFINITY {
        return if is_double {
            "CAST('-Infinity' AS DOUBLE)".to_owned()
        } else {
            "CAST('-Infinity' AS FLOAT)".to_owned()
        };
    }

    value.to_string()
}

/// Formats a CAST literal for parameterized types like DECIMAL.
fn format_typed_literal(value: &str, sql_type: &str) -> String {
    format!("CAST('{}' AS {})", value.replace('\'', "''"), sql_type)
}

/// Converts a PgNumeric to a DECIMAL(38,10) SQL literal, coercing NaN/Infinity to NULL.
fn numeric_to_decimal_literal(n: &etl::types::PgNumeric) -> String {
    use etl::types::PgNumeric;
    match n {
        PgNumeric::NaN | PgNumeric::PositiveInfinity | PgNumeric::NegativeInfinity => {
            "NULL".to_owned()
        }
        PgNumeric::Value { .. } => format_typed_literal(&n.to_string(), "DECIMAL(38, 10)"),
    }
}

/// Serializes a row using column type info for range-aware encoding.
fn table_row_to_sql_literal_typed(row: &TableRow, column_schemas: &[ColumnSchema]) -> String {
    let values: Vec<String> = row
        .values()
        .iter()
        .enumerate()
        .map(|(i, cell)| {
            if let Some(schema) = column_schemas.get(i) {
                cell_to_sql_literal_typed(cell, &schema.typ)
            } else {
                cell_to_sql_literal_ref(cell)
            }
        })
        .collect();
    format!("({})", values.join(", "))
}

/// Encodes a cell using column type info for range-aware conversion.
fn cell_to_sql_literal_typed(cell: &Cell, typ: &Type) -> String {
    if let Some(bound_type) = duckdb_range_bound_type(typ) {
        if is_range_type(typ) {
            return match cell {
                Cell::Null => "NULL".to_owned(),
                Cell::String(s) => range_text_to_struct_literal(s, bound_type),
                other => cell_to_sql_literal_ref(other),
            };
        }
        if is_range_array_type(typ) {
            return match cell {
                Cell::Null => "NULL".to_owned(),
                Cell::String(s) => range_array_text_to_list_literal(s, bound_type),
                // CDC decodes tstzrange[] as ArrayCell::String where each
                // element is a single range text like "[lower,upper)".
                Cell::Array(ArrayCell::String(elements)) => {
                    let structs: Vec<String> = elements
                        .iter()
                        .map(|elem| match elem {
                            Some(s) => range_text_to_struct_literal(s, bound_type),
                            None => "NULL".to_owned(),
                        })
                        .collect();
                    format!("[{}]", structs.join(", "))
                }
                other => cell_to_sql_literal_ref(other),
            };
        }
    }
    cell_to_sql_literal_ref(cell)
}

/// Parses Postgres range text (e.g. `[2024-01-01,2024-12-31)`) into a DuckDB STRUCT literal.
fn range_text_to_struct_literal(text: &str, bound_type: &str) -> String {
    let trimmed = text.trim();
    if trimmed == "empty" || trimmed.is_empty() {
        return "NULL".to_owned();
    }

    let inner = &trimmed[1..trimmed.len() - 1];
    let (lower, upper) = match inner.split_once(',') {
        Some(parts) => parts,
        None => return "NULL".to_owned(),
    };

    let lower_literal = if lower.is_empty() {
        "NULL".to_owned()
    } else {
        format_typed_literal(lower, bound_type)
    };
    let upper_literal = if upper.is_empty() {
        "NULL".to_owned()
    } else {
        format_typed_literal(upper, bound_type)
    };

    format!("{{'lower': {lower_literal}, 'upper': {upper_literal}}}")
}

/// Parses Postgres range array text (e.g. `{"[1,2)","[3,4)"}`) into a DuckDB list-of-struct literal.
fn range_array_text_to_list_literal(text: &str, bound_type: &str) -> String {
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed == "{}" {
        return "[]".to_owned();
    }

    let inner = &trimmed[1..trimmed.len() - 1];
    let elements: Vec<String> = split_pg_array_elements(inner)
        .iter()
        .map(|elem| {
            let unquoted = elem.trim_matches('"');
            range_text_to_struct_literal(unquoted, bound_type)
        })
        .collect();

    format!("[{}]", elements.join(", "))
}

/// Quote-aware comma splitting for Postgres array interior strings.
fn split_pg_array_elements(s: &str) -> Vec<String> {
    let mut elements = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;

    for ch in s.chars() {
        match ch {
            '"' => {
                in_quotes = !in_quotes;
                current.push(ch);
            }
            ',' if !in_quotes => {
                elements.push(std::mem::take(&mut current));
            }
            _ => current.push(ch),
        }
    }
    if !current.is_empty() {
        elements.push(current);
    }
    elements
}

/// Encodes bytes as uppercase hexadecimal for DuckDB's `from_hex`.
fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02X}")).collect()
}

/// Converts a [`Cell`] to a [`duckdb::types::Value`] for use with parameterized
/// INSERT statements.
fn cell_to_value(cell: Cell) -> Value {
    let epoch_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let epoch_time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();

    match cell {
        Cell::Null => Value::Null,
        Cell::Bool(b) => Value::Boolean(b),
        Cell::String(s) => Value::Text(s),
        Cell::I16(i) => Value::SmallInt(i),
        Cell::I32(i) => Value::Int(i),
        Cell::U32(u) => Value::UInt(u),
        Cell::I64(i) => Value::BigInt(i),
        Cell::F32(f) => Value::Float(f),
        Cell::F64(f) => Value::Double(f),
        // Numeric goes through SQL literal path (cell_requires_sql_literals); this arm is fallback.
        Cell::Numeric(n) => Value::Text(n.to_string()),
        Cell::Date(d) => Value::Date32(d.signed_duration_since(epoch_date).num_days() as i32),
        Cell::Time(t) => {
            let micros = t.signed_duration_since(epoch_time).num_microseconds().unwrap_or(0);
            Value::Time64(TimeUnit::Microsecond, micros)
        }
        Cell::Timestamp(dt) => {
            Value::Timestamp(TimeUnit::Microsecond, dt.and_utc().timestamp_micros())
        }
        Cell::TimestampTz(dt) => Value::Timestamp(TimeUnit::Microsecond, dt.timestamp_micros()),
        // UUID stored as text; DuckDB casts VARCHAR → UUID automatically.
        Cell::Uuid(u) => Value::Text(u.to_string()),
        // JSON serialised as text.
        Cell::Json(j) => Value::Text(j.to_string()),
        Cell::Bytes(b) => Value::Blob(b),
        Cell::Array(arr) => array_cell_to_value(arr),
    }
}

/// Converts an [`ArrayCell`] (with nullable elements) to a `Value::List`.
fn array_cell_to_value(arr: ArrayCell) -> Value {
    let values = match arr {
        ArrayCell::Bool(v) => {
            v.into_iter().map(|o| o.map_or(Value::Null, Value::Boolean)).collect()
        }
        ArrayCell::String(v) => v.into_iter().map(|o| o.map_or(Value::Null, Value::Text)).collect(),
        ArrayCell::I16(v) => {
            v.into_iter().map(|o| o.map_or(Value::Null, Value::SmallInt)).collect()
        }
        ArrayCell::I32(v) => v.into_iter().map(|o| o.map_or(Value::Null, Value::Int)).collect(),
        ArrayCell::U32(v) => v.into_iter().map(|o| o.map_or(Value::Null, Value::UInt)).collect(),
        ArrayCell::I64(v) => v.into_iter().map(|o| o.map_or(Value::Null, Value::BigInt)).collect(),
        ArrayCell::F32(v) => v.into_iter().map(|o| o.map_or(Value::Null, Value::Float)).collect(),
        ArrayCell::F64(v) => v.into_iter().map(|o| o.map_or(Value::Null, Value::Double)).collect(),
        ArrayCell::Numeric(v) => {
            v.into_iter().map(|o| o.map_or(Value::Null, |n| Value::Text(n.to_string()))).collect()
        }
        ArrayCell::Date(v) => {
            let epoch_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            v.into_iter()
                .map(|o| {
                    o.map_or(Value::Null, |d| {
                        Value::Date32(d.signed_duration_since(epoch_date).num_days() as i32)
                    })
                })
                .collect()
        }
        ArrayCell::Time(v) => {
            let epoch_time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
            v.into_iter()
                .map(|o| {
                    o.map_or(Value::Null, |t| {
                        let micros =
                            t.signed_duration_since(epoch_time).num_microseconds().unwrap_or(0);
                        Value::Time64(TimeUnit::Microsecond, micros)
                    })
                })
                .collect()
        }
        ArrayCell::Timestamp(v) => v
            .into_iter()
            .map(|o| {
                o.map_or(Value::Null, |dt| {
                    Value::Timestamp(TimeUnit::Microsecond, dt.and_utc().timestamp_micros())
                })
            })
            .collect(),
        ArrayCell::TimestampTz(v) => v
            .into_iter()
            .map(|o| {
                o.map_or(Value::Null, |dt| {
                    Value::Timestamp(TimeUnit::Microsecond, dt.timestamp_micros())
                })
            })
            .collect(),
        ArrayCell::Uuid(v) => {
            v.into_iter().map(|o| o.map_or(Value::Null, |u| Value::Text(u.to_string()))).collect()
        }
        ArrayCell::Json(v) => {
            v.into_iter().map(|o| o.map_or(Value::Null, |j| Value::Text(j.to_string()))).collect()
        }
        ArrayCell::Bytes(v) => v.into_iter().map(|o| o.map_or(Value::Null, Value::Blob)).collect(),
    };
    Value::List(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cell_to_value_primitives() {
        assert_eq!(cell_to_value(Cell::Null), Value::Null);
        assert_eq!(cell_to_value(Cell::Bool(true)), Value::Boolean(true));
        assert_eq!(
            cell_to_value(Cell::String("hello".to_owned())),
            Value::Text("hello".to_owned())
        );
        assert_eq!(cell_to_value(Cell::I32(42)), Value::Int(42));
        assert_eq!(cell_to_value(Cell::I64(-1)), Value::BigInt(-1));
        assert_eq!(cell_to_value(Cell::F64(3.46)), Value::Double(3.46));
    }

    #[test]
    fn array_cell_to_sql_literal_preserves_nulls() {
        assert_eq!(
            array_cell_to_sql_literal(ArrayCell::I32(vec![Some(1), None, Some(3)])),
            "[1, NULL, 3]"
        );
        assert_eq!(
            array_cell_to_sql_literal(ArrayCell::Json(vec![
                Some(serde_json::json!({"a": 1})),
                None,
            ])),
            "[CAST('{\"a\":1}' AS JSON), NULL]"
        );
    }

    #[test]
    fn prepare_rows_uses_sql_literals_for_arrays() {
        let schemas = vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, Some(1), false),
            ColumnSchema::new("arr".to_owned(), Type::INT4_ARRAY, -1, 2, None, true),
        ];
        let prepared = prepare_rows(
            vec![TableRow::new(vec![
                Cell::I32(1),
                Cell::Array(ArrayCell::I32(vec![Some(1), None, Some(3)])),
            ])],
            &schemas,
        );

        match prepared {
            PreparedRows::SqlLiterals(rows) => {
                assert_eq!(rows, vec!["(1, [1, NULL, 3])"]);
            }
            PreparedRows::Appender(_) => panic!("expected sql literal fallback"),
        }
    }
}
