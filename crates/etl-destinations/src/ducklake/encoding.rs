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

/// A parsed Postgres range: either empty or raw bound strings
/// (None = unbounded side).
#[derive(Debug, PartialEq)]
pub(super) enum ParsedRange {
    Empty,
    Bounds(Option<String>, Option<String>),
}

/// Parses Postgres range text (e.g. `[a,b)`) into raw bound strings.
pub(super) fn parse_range_text(text: &str) -> ParsedRange {
    let trimmed = text.trim();
    if trimmed == "empty" || trimmed.is_empty() {
        return ParsedRange::Empty;
    }
    let inner = &trimmed[1..trimmed.len() - 1];
    match split_range_bounds(inner) {
        Some((lower, upper)) => ParsedRange::Bounds(
            (!lower.is_empty()).then_some(lower),
            (!upper.is_empty()).then_some(upper),
        ),
        None => ParsedRange::Empty,
    }
}

/// Parses Postgres range-array text (e.g. `{"[1,2)","[3,4)"}`) into parsed
/// ranges, unescaping element quoting.
pub(super) fn parse_range_array_text(text: &str) -> Vec<ParsedRange> {
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed == "{}" {
        return Vec::new();
    }
    let inner = &trimmed[1..trimmed.len() - 1];
    split_pg_array_elements(inner)
        .iter()
        .map(|elem| {
            let unquoted = elem.trim_matches('"');
            let unescaped = unquoted.replace("\\\"", "\"");
            parse_range_text(&unescaped)
        })
        .collect()
}

/// Renders a [`ParsedRange`] as a DuckDB STRUCT literal or NULL.
fn parsed_range_to_struct_literal(parsed: ParsedRange, bound_type: &str) -> String {
    match parsed {
        ParsedRange::Empty => "NULL".to_owned(),
        ParsedRange::Bounds(lower, upper) => {
            let lower_literal = lower
                .map_or_else(|| "NULL".to_owned(), |v| format_typed_literal(&v, bound_type));
            let upper_literal = upper
                .map_or_else(|| "NULL".to_owned(), |v| format_typed_literal(&v, bound_type));
            format!("{{'lower': {lower_literal}, 'upper': {upper_literal}}}")
        }
    }
}

/// Parses Postgres range text (e.g. `[2024-01-01,2024-12-31)`) into a DuckDB STRUCT literal.
fn range_text_to_struct_literal(text: &str, bound_type: &str) -> String {
    parsed_range_to_struct_literal(parse_range_text(text), bound_type)
}

/// Splits the interior of a Postgres range into (lower, upper) bounds,
/// handling double-quoted values that may contain commas.
/// Input example: `"2026-01-28 01:17:00+00","2026-01-28 05:25:00+00"`
/// Returns unquoted bound strings.
fn split_range_bounds(inner: &str) -> Option<(String, String)> {
    if inner.starts_with('"') {
        // Quoted lower bound — find the closing quote, then expect a comma.
        let rest = &inner[1..];
        let close = rest.find('"')?;
        let lower = &rest[..close];
        let after = &rest[close + 1..];
        if after.is_empty() {
            // Unbounded upper: e.g. `"value",)`  but inner is `"value"`
            return Some((lower.to_owned(), String::new()));
        }
        let after = after.strip_prefix(',')?;
        let upper = after.trim_matches('"');
        Some((lower.to_owned(), upper.to_owned()))
    } else {
        // Unquoted lower bound — simple comma split.
        let (lower, upper) = inner.split_once(',')?;
        Some((
            lower.trim_matches('"').to_owned(),
            upper.trim_matches('"').to_owned(),
        ))
    }
}

/// Parses Postgres range array text (e.g. `{"[1,2)","[3,4)"}`) into a DuckDB list-of-struct literal.
fn range_array_text_to_list_literal(text: &str, bound_type: &str) -> String {
    let elements: Vec<String> = parse_range_array_text(text)
        .into_iter()
        .map(|p| parsed_range_to_struct_literal(p, bound_type))
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

    // --- Typed range parser tests ---

    #[test]
    fn parse_range_text_returns_raw_bounds() {
        assert_eq!(
            parse_range_text("[1,10)"),
            ParsedRange::Bounds(Some("1".to_owned()), Some("10".to_owned()))
        );
        assert_eq!(
            parse_range_text(r#"["2026-01-28 01:17:00+00",)"#),
            ParsedRange::Bounds(Some("2026-01-28 01:17:00+00".to_owned()), None)
        );
        assert_eq!(parse_range_text("empty"), ParsedRange::Empty);
        assert_eq!(parse_range_text(""), ParsedRange::Empty);
    }

    #[test]
    fn parse_range_array_text_splits_elements() {
        let elements = parse_range_array_text(r#"{"[1,2)","[3,4)"}"#);
        assert_eq!(elements.len(), 2);
        assert_eq!(
            elements[0],
            ParsedRange::Bounds(Some("1".to_owned()), Some("2".to_owned()))
        );
    }

    // --- Range conversion tests ---

    #[test]
    fn split_range_bounds_unquoted() {
        assert_eq!(
            split_range_bounds("1,10"),
            Some(("1".to_owned(), "10".to_owned()))
        );
    }

    #[test]
    fn split_range_bounds_quoted_timestamps() {
        assert_eq!(
            split_range_bounds(r#""2026-01-28 01:17:00+00","2026-01-28 05:25:00+00""#),
            Some((
                "2026-01-28 01:17:00+00".to_owned(),
                "2026-01-28 05:25:00+00".to_owned()
            ))
        );
    }

    #[test]
    fn split_range_bounds_unbounded_upper() {
        assert_eq!(
            split_range_bounds("1,"),
            Some(("1".to_owned(), String::new()))
        );
    }

    #[test]
    fn split_range_bounds_unbounded_lower() {
        assert_eq!(
            split_range_bounds(",10"),
            Some((String::new(), "10".to_owned()))
        );
    }

    #[test]
    fn range_text_to_struct_literal_unquoted_integers() {
        assert_eq!(
            range_text_to_struct_literal("[1,10)", "INTEGER"),
            "{'lower': CAST('1' AS INTEGER), 'upper': CAST('10' AS INTEGER)}"
        );
    }

    #[test]
    fn range_text_to_struct_literal_quoted_timestamps() {
        assert_eq!(
            range_text_to_struct_literal(
                r#"["2026-01-28 01:17:00+00","2026-01-28 05:25:00+00")"#,
                "TIMESTAMPTZ"
            ),
            "{'lower': CAST('2026-01-28 01:17:00+00' AS TIMESTAMPTZ), 'upper': CAST('2026-01-28 05:25:00+00' AS TIMESTAMPTZ)}"
        );
    }

    #[test]
    fn range_text_to_struct_literal_empty() {
        assert_eq!(range_text_to_struct_literal("empty", "INTEGER"), "NULL");
    }

    #[test]
    fn range_text_to_struct_literal_unbounded_upper() {
        assert_eq!(
            range_text_to_struct_literal("[1,)", "INTEGER"),
            "{'lower': CAST('1' AS INTEGER), 'upper': NULL}"
        );
    }

    #[test]
    fn range_array_text_to_list_literal_pg_format() {
        // Postgres array text: {"[1,2)","[3,4)"}
        assert_eq!(
            range_array_text_to_list_literal(r#"{"[1,2)","[3,4)"}"#, "INTEGER"),
            "[{'lower': CAST('1' AS INTEGER), 'upper': CAST('2' AS INTEGER)}, {'lower': CAST('3' AS INTEGER), 'upper': CAST('4' AS INTEGER)}]"
        );
    }

    #[test]
    fn range_array_text_to_list_literal_pg_quoted_timestamps() {
        // Postgres array text with quoted range bounds containing spaces:
        // {"[\"2026-01-28 01:17:00+00\",\"2026-01-28 05:25:00+00\")"}
        let pg_text = r#"{"[\"2026-01-28 01:17:00+00\",\"2026-01-28 05:25:00+00\")"}"#;
        let result = range_array_text_to_list_literal(pg_text, "TIMESTAMPTZ");
        assert_eq!(
            result,
            "[{'lower': CAST('2026-01-28 01:17:00+00' AS TIMESTAMPTZ), 'upper': CAST('2026-01-28 05:25:00+00' AS TIMESTAMPTZ)}]"
        );
    }

    #[test]
    fn range_array_text_to_list_literal_empty() {
        assert_eq!(range_array_text_to_list_literal("{}", "INTEGER"), "[]");
    }

    #[test]
    fn cell_to_sql_literal_typed_scalar_range() {
        let cell = Cell::String("[1,10)".to_owned());
        assert_eq!(
            cell_to_sql_literal_typed(&cell, &Type::INT4_RANGE),
            "{'lower': CAST('1' AS integer), 'upper': CAST('10' AS integer)}"
        );
    }

    #[test]
    fn cell_to_sql_literal_typed_range_array_as_array_cell_string() {
        // CDC delivers tstzrange[] as ArrayCell::String
        let cell = Cell::Array(ArrayCell::String(vec![
            Some(r#"["2026-01-28 01:17:00+00","2026-01-28 05:25:00+00")"#.to_owned()),
            None,
        ]));
        let result = cell_to_sql_literal_typed(&cell, &Type::TSTZ_RANGE_ARRAY);
        assert_eq!(
            result,
            "[{'lower': CAST('2026-01-28 01:17:00+00' AS timestamptz), 'upper': CAST('2026-01-28 05:25:00+00' AS timestamptz)}, NULL]"
        );
    }

    #[test]
    fn cell_to_sql_literal_typed_range_array_empty() {
        let cell = Cell::Array(ArrayCell::String(vec![]));
        let result = cell_to_sql_literal_typed(&cell, &Type::TSTZ_RANGE_ARRAY);
        assert_eq!(result, "[]");
    }

    #[test]
    fn prepare_rows_range_array_forces_sql_literals() {
        let schemas = vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, Some(1), false),
            ColumnSchema::new("r".to_owned(), Type::TSTZ_RANGE_ARRAY, -1, 2, None, true),
        ];
        let prepared = prepare_rows(
            vec![TableRow::new(vec![
                Cell::I32(1),
                Cell::Array(ArrayCell::String(vec![Some(
                    r#"["2026-01-28 01:17:00+00","2026-01-28 05:25:00+00")"#.to_owned(),
                )])),
            ])],
            &schemas,
        );
        match prepared {
            PreparedRows::SqlLiterals(rows) => {
                assert_eq!(
                    rows,
                    vec!["(1, [{'lower': CAST('2026-01-28 01:17:00+00' AS timestamptz), 'upper': CAST('2026-01-28 05:25:00+00' AS timestamptz)}])"]
                );
            }
            PreparedRows::Appender(_) => panic!("expected sql literal path for range arrays"),
        }
    }
}
