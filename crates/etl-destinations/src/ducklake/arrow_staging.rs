//! Arrow RecordBatch staging for DuckLake batch writes.

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
