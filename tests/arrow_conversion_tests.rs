//! Tests for Arrow conversion utilities (RecordBatch ↔ RowBatch)

#[cfg(feature = "parquet")]
use arrow_array::{Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, BinaryArray};
#[cfg(feature = "parquet")]
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
#[cfg(feature = "parquet")]
use std::sync::Arc;

use emsqrt_core::schema::{DataType, Field, Schema};
use emsqrt_core::types::{Column, RowBatch, Scalar};
use emsqrt_io::arrow_convert::{emsqrt_to_arrow_schema, record_batch_to_row_batch, row_batch_to_record_batch};

#[cfg(feature = "parquet")]
fn create_test_row_batch() -> RowBatch {
    RowBatch {
        columns: vec![
            Column {
                name: "id".to_string(),
                values: vec![Scalar::I32(1), Scalar::I32(2), Scalar::I32(3)],
            },
            Column {
                name: "name".to_string(),
                values: vec![
                    Scalar::Str("Alice".to_string()),
                    Scalar::Str("Bob".to_string()),
                    Scalar::Str("Charlie".to_string()),
                ],
            },
            Column {
                name: "score".to_string(),
                values: vec![Scalar::F64(95.5), Scalar::F64(87.0), Scalar::F64(92.3)],
            },
            Column {
                name: "active".to_string(),
                values: vec![Scalar::Bool(true), Scalar::Bool(false), Scalar::Bool(true)],
            },
        ],
    }
}

#[cfg(feature = "parquet")]
#[test]
fn test_record_batch_to_row_batch_all_types() {
    use arrow_array::builder::{BooleanBuilder, Int32Builder, Int64Builder, Float32Builder, Float64Builder, StringBuilder, BinaryBuilder};
    
    // Create Arrow RecordBatch with all types
    let schema = ArrowSchema::new(vec![
        ArrowField::new("bool_col", ArrowDataType::Boolean, true),
        ArrowField::new("i32_col", ArrowDataType::Int32, true),
        ArrowField::new("i64_col", ArrowDataType::Int64, true),
        ArrowField::new("f32_col", ArrowDataType::Float32, true),
        ArrowField::new("f64_col", ArrowDataType::Float64, true),
        ArrowField::new("str_col", ArrowDataType::Utf8, true),
        ArrowField::new("bin_col", ArrowDataType::Binary, true),
    ]);
    
    let mut bool_builder = BooleanBuilder::new();
    bool_builder.append_value(true);
    bool_builder.append_value(false);
    bool_builder.append_null();
    
    let mut i32_builder = Int32Builder::new();
    i32_builder.append_value(42);
    i32_builder.append_value(-100);
    i32_builder.append_value(0);
    
    let mut i64_builder = Int64Builder::new();
    i64_builder.append_value(123456789);
    i64_builder.append_value(-987654321);
    i64_builder.append_value(0);
    
    let mut f32_builder = Float32Builder::new();
    f32_builder.append_value(3.14);
    f32_builder.append_value(-2.5);
    f32_builder.append_value(0.0);
    
    let mut f64_builder = Float64Builder::new();
    f64_builder.append_value(3.14159);
    f64_builder.append_value(-2.71828);
    f64_builder.append_value(0.0);
    
    let mut str_builder = StringBuilder::new();
    str_builder.append_value("hello");
    str_builder.append_value("world");
    str_builder.append_value("test");
    
    let mut bin_builder = BinaryBuilder::new();
    bin_builder.append_value(b"binary");
    bin_builder.append_value(b"data");
    bin_builder.append_value(b"test");
    
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(bool_builder.finish()),
        Arc::new(i32_builder.finish()),
        Arc::new(i64_builder.finish()),
        Arc::new(f32_builder.finish()),
        Arc::new(f64_builder.finish()),
        Arc::new(str_builder.finish()),
        Arc::new(bin_builder.finish()),
    ];
    
    let record_batch = RecordBatch::try_new(Arc::new(schema), arrays).unwrap();
    
    // Convert to RowBatch
    let row_batch = record_batch_to_row_batch(&record_batch).unwrap();
    
    // Verify conversion
    assert_eq!(row_batch.num_rows(), 3);
    assert_eq!(row_batch.columns.len(), 7);
    
    // Check bool column
    assert_eq!(row_batch.columns[0].name, "bool_col");
    assert_eq!(row_batch.columns[0].values[0], Scalar::Bool(true));
    assert_eq!(row_batch.columns[0].values[1], Scalar::Bool(false));
    assert_eq!(row_batch.columns[0].values[2], Scalar::Null);
    
    // Check i32 column
    assert_eq!(row_batch.columns[1].values[0], Scalar::I32(42));
    assert_eq!(row_batch.columns[1].values[1], Scalar::I32(-100));
    
    // Check i64 column
    assert_eq!(row_batch.columns[2].values[0], Scalar::I64(123456789));
    
    // Check f32 column
    assert_eq!(row_batch.columns[3].values[0], Scalar::F32(3.14));
    
    // Check f64 column
    assert_eq!(row_batch.columns[4].values[0], Scalar::F64(3.14159));
    
    // Check string column
    assert_eq!(row_batch.columns[5].values[0], Scalar::Str("hello".to_string()));
    
    // Check binary column
    assert_eq!(row_batch.columns[6].values[0], Scalar::Bin(b"binary".to_vec()));
}

#[cfg(feature = "parquet")]
#[test]
fn test_row_batch_to_record_batch() {
    let row_batch = create_test_row_batch();
    
    // Create Arrow schema
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, true),
        ArrowField::new("name", ArrowDataType::Utf8, true),
        ArrowField::new("score", ArrowDataType::Float64, true),
        ArrowField::new("active", ArrowDataType::Boolean, true),
    ]));
    
    // Convert to RecordBatch
    let record_batch = row_batch_to_record_batch(&row_batch, arrow_schema.clone()).unwrap();
    
    // Verify
    assert_eq!(record_batch.num_rows(), 3);
    assert_eq!(record_batch.num_columns(), 4);
    assert_eq!(record_batch.schema(), arrow_schema);
    
    // Check values
    let id_array = record_batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(id_array.value(0), 1);
    assert_eq!(id_array.value(1), 2);
    
    let name_array = record_batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(name_array.value(0), "Alice");
    
    let score_array = record_batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(score_array.value(0), 95.5);
    
    let active_array = record_batch.column(3).as_any().downcast_ref::<BooleanArray>().unwrap();
    assert_eq!(active_array.value(0), true);
}

#[cfg(feature = "parquet")]
#[test]
fn test_round_trip_conversion() {
    let original = create_test_row_batch();
    
    // Convert to Arrow schema
    let emsqrt_schema = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
    ]);
    
    let arrow_schema = Arc::new(emsqrt_to_arrow_schema(&emsqrt_schema));
    
    // RowBatch -> RecordBatch -> RowBatch
    let record_batch = row_batch_to_record_batch(&original, arrow_schema.clone()).unwrap();
    let converted_back = record_batch_to_row_batch(&record_batch).unwrap();
    
    // Verify round-trip
    assert_eq!(original.num_rows(), converted_back.num_rows());
    assert_eq!(original.columns.len(), converted_back.columns.len());
    
    for (orig_col, conv_col) in original.columns.iter().zip(converted_back.columns.iter()) {
        assert_eq!(orig_col.name, conv_col.name);
        assert_eq!(orig_col.values.len(), conv_col.values.len());
        for (orig_val, conv_val) in orig_col.values.iter().zip(conv_col.values.iter()) {
            assert_eq!(orig_val, conv_val);
        }
    }
}

#[cfg(feature = "parquet")]
#[test]
fn test_empty_batch() {
    // Empty RowBatch
    let empty_batch = RowBatch { columns: vec![] };
    
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("col1", ArrowDataType::Int32, true),
    ]));
    
    let record_batch = row_batch_to_record_batch(&empty_batch, arrow_schema.clone()).unwrap();
    assert_eq!(record_batch.num_rows(), 0);
    assert_eq!(record_batch.num_columns(), 1);
    
    // Empty RecordBatch
    let empty_record_batch = RecordBatch::new_empty(arrow_schema.clone());
    let row_batch = record_batch_to_row_batch(&empty_record_batch).unwrap();
    assert_eq!(row_batch.num_rows(), 0);
    assert_eq!(row_batch.columns.len(), 1);
    assert_eq!(row_batch.columns[0].name, "col1");
}

#[cfg(feature = "parquet")]
#[test]
fn test_nullable_fields() {
    let row_batch = RowBatch {
        columns: vec![
            Column {
                name: "nullable_int".to_string(),
                values: vec![Scalar::I32(1), Scalar::Null, Scalar::I32(3)],
            },
        ],
    };
    
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("nullable_int", ArrowDataType::Int32, true),
    ]));
    
    let record_batch = row_batch_to_record_batch(&row_batch, arrow_schema.clone()).unwrap();
    assert_eq!(record_batch.num_rows(), 3);
    
    let array = record_batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(array.value(0), 1);
    assert!(array.is_null(1));
    assert_eq!(array.value(2), 3);
}

#[cfg(feature = "parquet")]
#[test]
fn test_schema_mismatch_error() {
    let row_batch = RowBatch {
        columns: vec![
            Column {
                name: "col1".to_string(),
                values: vec![Scalar::I32(1)],
            },
        ],
    };
    
    // Schema with wrong number of columns
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("col1", ArrowDataType::Int32, true),
        ArrowField::new("col2", ArrowDataType::Int32, true),
    ]));
    
    let result = row_batch_to_record_batch(&row_batch, arrow_schema);
    assert!(result.is_err());
}

#[cfg(feature = "parquet")]
#[test]
fn test_emsqrt_to_arrow_schema() {
    let emsqrt_schema = Schema::new(vec![
        Field::new("bool_col", DataType::Boolean, false),
        Field::new("int_col", DataType::Int32, true),
        Field::new("float_col", DataType::Float64, false),
        Field::new("str_col", DataType::Utf8, true),
        Field::new("bin_col", DataType::Binary, false),
    ]);
    
    let arrow_schema = emsqrt_to_arrow_schema(&emsqrt_schema);
    
    assert_eq!(arrow_schema.fields().len(), 5);
    assert_eq!(arrow_schema.field(0).name(), "bool_col");
    assert_eq!(arrow_schema.field(0).data_type(), &ArrowDataType::Boolean);
    assert!(!arrow_schema.field(0).is_nullable());
    
    assert_eq!(arrow_schema.field(1).name(), "int_col");
    assert!(arrow_schema.field(1).is_nullable());
}

#[cfg(not(feature = "parquet"))]
#[test]
fn test_parquet_feature_required() {
    // This test file requires the parquet feature to be enabled
    // When the feature is not enabled, compilation will fail
    // This is expected behavior
}

