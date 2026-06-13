use etl::types::{ArrayCell, Cell, TableRow};
use pg_escape::quote_literal;

/// Serializes a borrowed row into a SQL `VALUES (...)` tuple.
pub(super) fn table_row_to_sql_literal_ref(row: &TableRow) -> String {
    format!("({})", row.values().iter().map(cell_to_sql_literal_ref).collect::<Vec<_>>().join(", "))
}

/// Serializes a borrowed cell into a DuckDB SQL literal expression.
pub(super) fn cell_to_sql_literal_ref(cell: &Cell) -> String {
    cell_to_sql_literal(cell.clone())
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

#[cfg(test)]
mod tests {
    use super::*;

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

}
