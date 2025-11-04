//! Arrow conversion utilities for Parquet I/O boundaries.
//!
//! Converts between Arrow RecordBatch and emsqrt-core RowBatch.
//! This is feature-gated and only compiled when `--features parquet` is enabled.

#[cfg(feature = "parquet")]
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray, BinaryArray, RecordBatch,
};
#[cfg(feature = "parquet")]
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef};
#[cfg(feature = "parquet")]
use std::sync::Arc;

use emsqrt_core::schema::DataType;
use emsqrt_core::types::{Column, RowBatch, Scalar};

use crate::error::{Error, Result};

/// Convert an Arrow RecordBatch to a RowBatch.
///
/// Handles all supported Scalar types and nullable fields.
pub fn record_batch_to_row_batch(batch: &RecordBatch) -> Result<RowBatch> {
    let schema = batch.schema();
    let num_rows = batch.num_rows();
    
    if num_rows == 0 {
        // Empty batch - create columns with correct names but no values
        let columns: Vec<Column> = schema
            .fields()
            .iter()
            .map(|field| Column {
                name: field.name().clone(),
                values: Vec::new(),
            })
            .collect();
        return Ok(RowBatch { columns });
    }
    
    let mut columns = Vec::with_capacity(schema.fields().len());
    
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let array = batch.column(col_idx);
        let mut values = Vec::with_capacity(num_rows);
        
        // Convert each row's value based on Arrow array type
        for row_idx in 0..num_rows {
            if array.is_null(row_idx) {
                values.push(Scalar::Null);
            } else {
                let scalar = arrow_value_to_scalar(array, row_idx)?;
                values.push(scalar);
            }
        }
        
        columns.push(Column {
            name: field.name().clone(),
            values,
        });
    }
    
    Ok(RowBatch { columns })
}

/// Convert a RowBatch to an Arrow RecordBatch.
///
/// Requires an Arrow schema to match the RowBatch structure.
/// The schema should match the RowBatch's column names and types.
pub fn row_batch_to_record_batch(
    batch: &RowBatch,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    if batch.columns.is_empty() {
        // Empty batch - create empty RecordBatch with schema
        return Ok(RecordBatch::new_empty(schema));
    }
    
    let _num_rows = batch.num_rows();
    let mut arrays = Vec::with_capacity(batch.columns.len());
    
    for (col_idx, column) in batch.columns.iter().enumerate() {
        let field = schema.fields().get(col_idx).ok_or_else(|| {
            Error::Schema(format!(
                "Schema has {} fields but RowBatch has {} columns (tried to access index {})",
                schema.fields().len(),
                batch.columns.len(),
                col_idx
            ))
        })?;
        
        // Verify column name matches
        if field.name() != &column.name {
            return Err(Error::Schema(format!(
                "Column name mismatch at index {}: schema expects '{}' but RowBatch has '{}'",
                col_idx,
                field.name(),
                column.name
            )));
        }
        
        // Convert column values to Arrow array
        let array = scalar_column_to_arrow_array(&column.values, field.data_type(), field.is_nullable())?;
        arrays.push(array);
    }
    
    RecordBatch::try_new(schema, arrays)
        .map_err(|e| Error::Other(format!("Failed to create RecordBatch: {}", e)))
}

/// Convert a single Arrow array value to a Scalar.
fn arrow_value_to_scalar(array: &ArrayRef, row_idx: usize) -> Result<Scalar> {
    match array.data_type() {
        ArrowDataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                Error::Other("Failed to cast to BooleanArray".to_string())
            })?;
            Ok(Scalar::Bool(arr.value(row_idx)))
        }
        ArrowDataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                Error::Other("Failed to cast to Int32Array".to_string())
            })?;
            Ok(Scalar::I32(arr.value(row_idx)))
        }
        ArrowDataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                Error::Other("Failed to cast to Int64Array".to_string())
            })?;
            Ok(Scalar::I64(arr.value(row_idx)))
        }
        ArrowDataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                Error::Other("Failed to cast to Float32Array".to_string())
            })?;
            Ok(Scalar::F32(arr.value(row_idx)))
        }
        ArrowDataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                Error::Other("Failed to cast to Float64Array".to_string())
            })?;
            Ok(Scalar::F64(arr.value(row_idx)))
        }
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                Error::Other("Failed to cast to StringArray".to_string())
            })?;
            Ok(Scalar::Str(arr.value(row_idx).to_string()))
        }
        ArrowDataType::Binary | ArrowDataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
                Error::Other("Failed to cast to BinaryArray".to_string())
            })?;
            Ok(Scalar::Bin(arr.value(row_idx).to_vec()))
        }
        _ => Err(Error::Other(format!(
            "Unsupported Arrow data type: {:?}",
            array.data_type()
        ))),
    }
}

/// Convert a column of Scalar values to an Arrow array.
fn scalar_column_to_arrow_array(
    values: &[Scalar],
    data_type: &ArrowDataType,
    nullable: bool,
) -> Result<ArrayRef> {
    match data_type {
        ArrowDataType::Boolean => {
            let mut builder = arrow_array::builder::BooleanBuilder::with_capacity(values.len());
            for val in values {
                match val {
                    Scalar::Null => {
                        if nullable {
                            builder.append_null();
                        } else {
                            return Err(Error::Schema("Null value in non-nullable Boolean column".to_string()));
                        }
                    }
                    Scalar::Bool(b) => builder.append_value(*b),
                    _ => return Err(Error::Schema(format!(
                        "Type mismatch: expected Boolean, got {:?}",
                        val
                    ))),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::Int32 => {
            let mut builder = arrow_array::builder::Int32Builder::with_capacity(values.len());
            for val in values {
                match val {
                    Scalar::Null => {
                        if nullable {
                            builder.append_null();
                        } else {
                            return Err(Error::Schema("Null value in non-nullable Int32 column".to_string()));
                        }
                    }
                    Scalar::I32(i) => builder.append_value(*i),
                    Scalar::I64(i) => {
                        // Allow narrowing conversion
                        builder.append_value(*i as i32);
                    }
                    _ => return Err(Error::Schema(format!(
                        "Type mismatch: expected Int32, got {:?}",
                        val
                    ))),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::Int64 => {
            let mut builder = arrow_array::builder::Int64Builder::with_capacity(values.len());
            for val in values {
                match val {
                    Scalar::Null => {
                        if nullable {
                            builder.append_null();
                        } else {
                            return Err(Error::Schema("Null value in non-nullable Int64 column".to_string()));
                        }
                    }
                    Scalar::I32(i) => builder.append_value(*i as i64),
                    Scalar::I64(i) => builder.append_value(*i),
                    _ => return Err(Error::Schema(format!(
                        "Type mismatch: expected Int64, got {:?}",
                        val
                    ))),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::Float32 => {
            let mut builder = arrow_array::builder::Float32Builder::with_capacity(values.len());
            for val in values {
                match val {
                    Scalar::Null => {
                        if nullable {
                            builder.append_null();
                        } else {
                            return Err(Error::Schema("Null value in non-nullable Float32 column".to_string()));
                        }
                    }
                    Scalar::F32(f) => builder.append_value(*f),
                    Scalar::F64(f) => {
                        // Allow narrowing conversion
                        builder.append_value(*f as f32);
                    }
                    Scalar::I32(i) => builder.append_value(*i as f32),
                    Scalar::I64(i) => builder.append_value(*i as f32),
                    _ => return Err(Error::Schema(format!(
                        "Type mismatch: expected Float32, got {:?}",
                        val
                    ))),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::Float64 => {
            let mut builder = arrow_array::builder::Float64Builder::with_capacity(values.len());
            for val in values {
                match val {
                    Scalar::Null => {
                        if nullable {
                            builder.append_null();
                        } else {
                            return Err(Error::Schema("Null value in non-nullable Float64 column".to_string()));
                        }
                    }
                    Scalar::F32(f) => builder.append_value(*f as f64),
                    Scalar::F64(f) => builder.append_value(*f),
                    Scalar::I32(i) => builder.append_value(*i as f64),
                    Scalar::I64(i) => builder.append_value(*i as f64),
                    _ => return Err(Error::Schema(format!(
                        "Type mismatch: expected Float64, got {:?}",
                        val
                    ))),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => {
            let mut builder = arrow_array::builder::StringBuilder::with_capacity(values.len(), 0);
            for val in values {
                match val {
                    Scalar::Null => {
                        if nullable {
                            builder.append_null();
                        } else {
                            return Err(Error::Schema("Null value in non-nullable Utf8 column".to_string()));
                        }
                    }
                    Scalar::Str(s) => builder.append_value(s),
                    _ => return Err(Error::Schema(format!(
                        "Type mismatch: expected Utf8, got {:?}",
                        val
                    ))),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::Binary | ArrowDataType::LargeBinary => {
            let mut builder = arrow_array::builder::BinaryBuilder::with_capacity(values.len(), 0);
            for val in values {
                match val {
                    Scalar::Null => {
                        if nullable {
                            builder.append_null();
                        } else {
                            return Err(Error::Schema("Null value in non-nullable Binary column".to_string()));
                        }
                    }
                    Scalar::Bin(b) => builder.append_value(b),
                    _ => return Err(Error::Schema(format!(
                        "Type mismatch: expected Binary, got {:?}",
                        val
                    ))),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(Error::Other(format!(
            "Unsupported Arrow data type for conversion: {:?}",
            data_type
        ))),
    }
}

/// Convert emsqrt-core DataType to Arrow DataType.
pub fn emsqrt_to_arrow_data_type(dtype: &DataType) -> ArrowDataType {
    match dtype {
        DataType::Boolean => ArrowDataType::Boolean,
        DataType::Int32 => ArrowDataType::Int32,
        DataType::Int64 => ArrowDataType::Int64,
        DataType::Float32 => ArrowDataType::Float32,
        DataType::Float64 => ArrowDataType::Float64,
        DataType::Utf8 => ArrowDataType::Utf8,
        DataType::Binary => ArrowDataType::Binary,
        DataType::Date64 => ArrowDataType::Date64,
        DataType::Decimal128 => ArrowDataType::Decimal128(10, 2), // Default precision/scale
    }
}

/// Convert emsqrt-core Schema to Arrow Schema.
pub fn emsqrt_to_arrow_schema(schema: &emsqrt_core::schema::Schema) -> ArrowSchema {
    let fields: Vec<ArrowField> = schema
        .fields
        .iter()
        .map(|field| {
            ArrowField::new(
                field.name.clone(),
                emsqrt_to_arrow_data_type(&field.data_type),
                field.nullable,
            )
        })
        .collect();
    ArrowSchema::new(fields)
}

#[cfg(not(feature = "parquet"))]
compile_error!("arrow_convert.rs was compiled without the `parquet` feature; enable `--features parquet` or exclude this module.");

