//! Tests for Parquet I/O integration

#[cfg(feature = "parquet")]
use emsqrt_core::schema::{DataType, Field, Schema};
#[cfg(feature = "parquet")]
use emsqrt_core::types::{Column, RowBatch, Scalar};
#[cfg(feature = "parquet")]
use emsqrt_io::readers::parquet::ParquetReader;
#[cfg(feature = "parquet")]
use emsqrt_io::writers::parquet::{ParquetCompression, ParquetWriter};
#[cfg(feature = "parquet")]
use std::fs;
#[cfg(feature = "parquet")]
use std::path::Path;
mod test_data_gen;
#[cfg(feature = "parquet")]
use test_data_gen::create_temp_spill_dir;

#[cfg(feature = "parquet")]
fn create_test_data() -> RowBatch {
    RowBatch {
        columns: vec![
            Column {
                name: "id".to_string(),
                values: vec![
                    Scalar::I32(1),
                    Scalar::I32(2),
                    Scalar::I32(3),
                    Scalar::I32(4),
                    Scalar::I32(5),
                ],
            },
            Column {
                name: "name".to_string(),
                values: vec![
                    Scalar::Str("Alice".to_string()),
                    Scalar::Str("Bob".to_string()),
                    Scalar::Str("Charlie".to_string()),
                    Scalar::Str("David".to_string()),
                    Scalar::Str("Eve".to_string()),
                ],
            },
            Column {
                name: "score".to_string(),
                values: vec![
                    Scalar::F64(95.5),
                    Scalar::F64(87.0),
                    Scalar::F64(92.3),
                    Scalar::F64(78.9),
                    Scalar::F64(88.1),
                ],
            },
            Column {
                name: "active".to_string(),
                values: vec![
                    Scalar::Bool(true),
                    Scalar::Bool(false),
                    Scalar::Bool(true),
                    Scalar::Bool(true),
                    Scalar::Bool(false),
                ],
            },
        ],
    }
}

#[cfg(feature = "parquet")]
#[test]
fn test_parquet_write_and_read() {
    let temp_dir = create_temp_spill_dir();
    let parquet_file = format!("{}/test.parquet", temp_dir);
    
    fs::create_dir_all(&temp_dir).expect("Failed to create temp dir");
    
    let test_data = create_test_data();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
    ]);
    
    // Write Parquet file
    {
        let mut writer = ParquetWriter::from_emsqrt_schema(&parquet_file, &schema)
            .expect("Failed to create Parquet writer");
        writer.write_row_batch(&test_data)
            .expect("Failed to write RowBatch");
        writer.close().expect("Failed to close writer");
    }
    
    // Verify file exists
    assert!(Path::new(&parquet_file).exists());
    
    // Read Parquet file
    let mut reader = ParquetReader::from_path(&parquet_file, None, 1000)
        .expect("Failed to create Parquet reader");
    
    let read_batch = reader.next_batch()
        .expect("Failed to read batch")
        .expect("Expected batch but got None");
    
    // Verify data
    assert_eq!(read_batch.num_rows(), 5);
    assert_eq!(read_batch.columns.len(), 4);
    
    // Check values
    assert_eq!(read_batch.columns[0].name, "id");
    assert_eq!(read_batch.columns[0].values[0], Scalar::I32(1));
    assert_eq!(read_batch.columns[0].values[1], Scalar::I32(2));
    
    assert_eq!(read_batch.columns[1].name, "name");
    assert_eq!(read_batch.columns[1].values[0], Scalar::Str("Alice".to_string()));
    
    assert_eq!(read_batch.columns[2].name, "score");
    assert_eq!(read_batch.columns[2].values[0], Scalar::F64(95.5));
    
    assert_eq!(read_batch.columns[3].name, "active");
    assert_eq!(read_batch.columns[3].values[0], Scalar::Bool(true));
    
    // Should be end of file
    assert!(reader.next_batch().expect("Failed to read").is_none());
}

#[cfg(feature = "parquet")]
#[test]
fn test_parquet_projection() {
    let temp_dir = create_temp_spill_dir();
    let parquet_file = format!("{}/test_projection.parquet", temp_dir);
    
    fs::create_dir_all(&temp_dir).expect("Failed to create temp dir");
    
    let test_data = create_test_data();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
    ]);
    
    // Write Parquet file
    {
        let mut writer = ParquetWriter::from_emsqrt_schema(&parquet_file, &schema)
            .expect("Failed to create Parquet writer");
        writer.write_row_batch(&test_data).expect("Failed to write");
        writer.close().expect("Failed to close");
    }
    
    // Read with projection (only id and name)
    let projection = vec!["id".to_string(), "name".to_string()];
    let mut reader = ParquetReader::from_path(&parquet_file, Some(projection), 1000)
        .expect("Failed to create Parquet reader");
    
    let read_batch = reader.next_batch()
        .expect("Failed to read")
        .expect("Expected batch");
    
    // Verify only projected columns are present
    assert_eq!(read_batch.columns.len(), 2);
    assert_eq!(read_batch.columns[0].name, "id");
    assert_eq!(read_batch.columns[1].name, "name");
    assert_eq!(read_batch.num_rows(), 5);
}

#[cfg(feature = "parquet")]
#[test]
fn test_parquet_compression() {
    let temp_dir = create_temp_spill_dir();
    let parquet_file = format!("{}/test_compression.parquet", temp_dir);
    
    fs::create_dir_all(&temp_dir).expect("Failed to create temp dir");
    
    // Create test data matching the schema (only id and name)
    let test_data = RowBatch {
        columns: vec![
            Column {
                name: "id".to_string(),
                values: vec![
                    Scalar::I32(1),
                    Scalar::I32(2),
                    Scalar::I32(3),
                ],
            },
            Column {
                name: "name".to_string(),
                values: vec![
                    Scalar::Str("Alice".to_string()),
                    Scalar::Str("Bob".to_string()),
                    Scalar::Str("Charlie".to_string()),
                ],
            },
        ],
    };
    
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    
    // Write with ZSTD compression
    {
        let mut writer = ParquetWriter::from_emsqrt_schema_with_options(
            &parquet_file,
            &schema,
            ParquetCompression::Zstd,
            None,
        ).expect("Failed to create writer");
        writer.write_row_batch(&test_data).expect("Failed to write");
        writer.close().expect("Failed to close");
    }
    
    // Verify file exists and can be read
    assert!(Path::new(&parquet_file).exists());
    
    let mut reader = ParquetReader::from_path(&parquet_file, None, 1000)
        .expect("Failed to create reader");
    let read_batch = reader.next_batch()
        .expect("Failed to read")
        .expect("Expected batch");
    
    assert_eq!(read_batch.num_rows(), 3);
    assert_eq!(read_batch.columns.len(), 2);
}

#[cfg(feature = "parquet")]
#[test]
fn test_parquet_empty_batch() {
    let temp_dir = create_temp_spill_dir();
    let parquet_file = format!("{}/test_empty.parquet", temp_dir);
    
    fs::create_dir_all(&temp_dir).expect("Failed to create temp dir");
    
    let empty_batch = RowBatch {
        columns: vec![
            Column {
                name: "id".to_string(),
                values: vec![],
            },
        ],
    };
    
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
    ]);
    
    // Write empty batch
    {
        let mut writer = ParquetWriter::from_emsqrt_schema(&parquet_file, &schema)
            .expect("Failed to create writer");
        writer.write_row_batch(&empty_batch).expect("Failed to write");
        writer.close().expect("Failed to close");
    }
    
    // Read empty file
    // Note: Parquet files with empty batches may return None or an empty batch
    let mut reader = ParquetReader::from_path(&parquet_file, None, 1000)
        .expect("Failed to create reader");
    
    let read_batch_opt = reader.next_batch()
        .expect("Failed to read");
    
    // Parquet reader may return None for empty files, or an empty batch
    if let Some(read_batch) = read_batch_opt {
        assert_eq!(read_batch.num_rows(), 0);
        assert_eq!(read_batch.columns.len(), 1);
        assert_eq!(read_batch.columns[0].name, "id");
    } else {
        // Empty file returns None - this is also valid
        // The file was created successfully, just has no data
    }
}

#[cfg(feature = "parquet")]
#[test]
fn test_parquet_multi_batch_read() {
    let temp_dir = create_temp_spill_dir();
    let parquet_file = format!("{}/test_multi.parquet", temp_dir);
    
    fs::create_dir_all(&temp_dir).expect("Failed to create temp dir");
    
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("value", DataType::Utf8, true),
    ]);
    
    // Write multiple batches (simulated by creating a larger batch)
    let large_batch = RowBatch {
        columns: vec![
            Column {
                name: "id".to_string(),
                values: (0..1000).map(|i| Scalar::I32(i)).collect(),
            },
            Column {
                name: "value".to_string(),
                values: (0..1000).map(|i| Scalar::Str(format!("value{}", i))).collect(),
            },
        ],
    };
    
    {
        let mut writer = ParquetWriter::from_emsqrt_schema(&parquet_file, &schema)
            .expect("Failed to create writer");
        writer.write_row_batch(&large_batch).expect("Failed to write");
        writer.close().expect("Failed to close");
    }
    
    // Read with small batch size to test multi-batch reading
    let mut reader = ParquetReader::from_path(&parquet_file, None, 100)
        .expect("Failed to create reader");
    
    let mut total_rows = 0;
    while let Some(batch) = reader.next_batch().expect("Failed to read") {
        total_rows += batch.num_rows();
        assert_eq!(batch.columns.len(), 2);
    }
    
    assert_eq!(total_rows, 1000);
}

#[cfg(not(feature = "parquet"))]
#[test]
fn test_parquet_feature_required() {
    // This test file requires the parquet feature to be enabled
}

