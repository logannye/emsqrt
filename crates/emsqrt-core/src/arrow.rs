//! Arrow integration for columnar processing.
//!
//! Provides conversions between `RowBatch` (row-oriented) and Arrow `RecordBatch` (columnar).
//! This module is feature-gated and only compiled when the `arrow` feature is enabled.

#[cfg(not(feature = "arrow"))]
compile_error!("arrow module requires 'arrow' feature to be enabled");

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray,
};
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
    StringBuilder,
};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};

use crate::schema::{DataType, Field, Schema};
use crate::types::{Column, RowBatch, Scalar};

/// Convert an EM-√ `Schema` to an Arrow `Schema`.
pub fn schema_to_arrow(schema: &Schema) -> ArrowSchema {
    let fields: Vec<ArrowField> = schema
        .fields
        .iter()
        .map(|f| {
            ArrowField::new(
                f.name.clone(),
                data_type_to_arrow(&f.data_type),
                f.nullable,
            )
        })
        .collect();
    ArrowSchema::new(fields)
}

/// Convert an EM-√ `DataType` to an Arrow `DataType`.
pub fn data_type_to_arrow(dt: &DataType) -> ArrowDataType {
    match dt {
        DataType::Boolean => ArrowDataType::Boolean,
        DataType::Int32 => ArrowDataType::Int32,
        DataType::Int64 => ArrowDataType::Int64,
        DataType::Float32 => ArrowDataType::Float32,
        DataType::Float64 => ArrowDataType::Float64,
        DataType::Utf8 => ArrowDataType::Utf8,
        DataType::Binary => ArrowDataType::Binary,
        DataType::Date64 => ArrowDataType::Date64,
        DataType::Decimal128 => ArrowDataType::Decimal128(38, 10), // Default precision/scale
    }
}

/// Convert a `RowBatch` to an Arrow `RecordBatch`.
///
/// This conversion is efficient for small batches but may be slow for large datasets
/// due to the row-oriented to columnar transformation.
pub fn row_batch_to_arrow(batch: &RowBatch) -> Result<RecordBatch, String> {
    if batch.columns.is_empty() {
        return Err("Cannot convert empty RowBatch to RecordBatch".into());
    }

    let num_rows = batch.num_rows();
    if num_rows == 0 {
        // Return empty batch with schema
        let schema = Schema {
            fields: batch
                .columns
                .iter()
                .map(|c| Field {
                    name: c.name.clone(),
                    data_type: DataType::Utf8, // Placeholder
                    nullable: true,
                })
                .collect(),
        };
        let arrow_schema = schema_to_arrow(&schema);
        return Ok(RecordBatch::new_empty(Arc::new(arrow_schema)));
    }

    // Build Arrow schema from first row (or use a provided schema)
    let arrow_schema = Arc::new(schema_to_arrow(&batch_to_schema(batch)));

    // Convert each column to Arrow array
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(batch.columns.len());

    for column in &batch.columns {
        let array = column_to_arrow_array(column, num_rows)?;
        arrays.push(array);
    }

    RecordBatch::try_new(arrow_schema, arrays)
        .map_err(|e| format!("Failed to create RecordBatch: {}", e))
}

/// Convert an Arrow `RecordBatch` to a `RowBatch`.
///
/// This conversion materializes all rows, which may be memory-intensive for large batches.
pub fn arrow_to_row_batch(batch: &RecordBatch) -> Result<RowBatch, String> {
    let schema = batch.schema();
    let num_rows = batch.num_rows();
    let num_columns = batch.num_columns();

    let mut columns = Vec::with_capacity(num_columns);

    for (i, field) in schema.fields().iter().enumerate() {
        let array = batch.column(i);
        let column = arrow_array_to_column(array, field.name(), num_rows)?;
        columns.push(column);
    }

    Ok(RowBatch { columns })
}

/// Infer schema from a RowBatch (using first row's types).
fn batch_to_schema(batch: &RowBatch) -> Schema {
    let fields: Vec<Field> = batch
        .columns
        .iter()
        .map(|c| {
            let data_type = if c.values.is_empty() {
                DataType::Utf8
            } else {
                c.values[0].data_type()
            };
            Field {
                name: c.name.clone(),
                data_type,
                nullable: true,
            }
        })
        .collect();
    Schema { fields }
}

/// Convert a `Column` to an Arrow `ArrayRef`.
fn column_to_arrow_array(column: &Column, num_rows: usize) -> Result<ArrayRef, String> {
    if column.values.is_empty() {
        return Err("Cannot convert empty column to Arrow array".into());
    }

    // Determine type from first non-null value
    let data_type = column.values[0].data_type();
    let arrow_dt = data_type_to_arrow(&data_type);

    match arrow_dt {
        ArrowDataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(num_rows);
            for scalar in &column.values {
                match scalar {
                    Scalar::Null => builder.append_null(),
                    Scalar::Bool(v) => builder.append_value(*v),
                    _ => return Err(format!("Type mismatch: expected Bool, got {:?}", scalar)),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::Int32 => {
            let mut builder = Int32Builder::with_capacity(num_rows);
            for scalar in &column.values {
                match scalar {
                    Scalar::Null => builder.append_null(),
                    Scalar::I32(v) => builder.append_value(*v),
                    _ => return Err(format!("Type mismatch: expected I32, got {:?}", scalar)),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(num_rows);
            for scalar in &column.values {
                match scalar {
                    Scalar::Null => builder.append_null(),
                    Scalar::I64(v) => builder.append_value(*v),
                    _ => return Err(format!("Type mismatch: expected I64, got {:?}", scalar)),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::Float32 => {
            let mut builder = Float32Builder::with_capacity(num_rows);
            for scalar in &column.values {
                match scalar {
                    Scalar::Null => builder.append_null(),
                    Scalar::F32(v) => builder.append_value(*v),
                    _ => return Err(format!("Type mismatch: expected F32, got {:?}", scalar)),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(num_rows);
            for scalar in &column.values {
                match scalar {
                    Scalar::Null => builder.append_null(),
                    Scalar::F64(v) => builder.append_value(*v),
                    _ => return Err(format!("Type mismatch: expected F64, got {:?}", scalar)),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 10); // Estimate avg string length
            for scalar in &column.values {
                match scalar {
                    Scalar::Null => builder.append_null(),
                    Scalar::Str(v) => builder.append_value(v),
                    _ => return Err(format!("Type mismatch: expected Str, got {:?}", scalar)),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::Binary => {
            let mut builder = BinaryBuilder::with_capacity(num_rows, num_rows * 10); // Estimate avg binary length
            for scalar in &column.values {
                match scalar {
                    Scalar::Null => builder.append_null(),
                    Scalar::Bin(v) => builder.append_value(v),
                    _ => return Err(format!("Type mismatch: expected Bin, got {:?}", scalar)),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(format!("Unsupported Arrow data type: {:?}", arrow_dt)),
    }
}

/// Convert an Arrow `ArrayRef` to a `Column`.
fn arrow_array_to_column(
    array: &ArrayRef,
    name: &str,
    num_rows: usize,
) -> Result<Column, String> {
    let mut values = Vec::with_capacity(num_rows);

    match array.data_type() {
        ArrowDataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            for i in 0..num_rows {
                if arr.is_null(i) {
                    values.push(Scalar::Null);
                } else {
                    values.push(Scalar::Bool(arr.value(i)));
                }
            }
        }
        ArrowDataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..num_rows {
                if arr.is_null(i) {
                    values.push(Scalar::Null);
                } else {
                    values.push(Scalar::I32(arr.value(i)));
                }
            }
        }
        ArrowDataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..num_rows {
                if arr.is_null(i) {
                    values.push(Scalar::Null);
                } else {
                    values.push(Scalar::I64(arr.value(i)));
                }
            }
        }
        ArrowDataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            for i in 0..num_rows {
                if arr.is_null(i) {
                    values.push(Scalar::Null);
                } else {
                    values.push(Scalar::F32(arr.value(i)));
                }
            }
        }
        ArrowDataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            for i in 0..num_rows {
                if arr.is_null(i) {
                    values.push(Scalar::Null);
                } else {
                    values.push(Scalar::F64(arr.value(i)));
                }
            }
        }
        ArrowDataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..num_rows {
                if arr.is_null(i) {
                    values.push(Scalar::Null);
                } else {
                    values.push(Scalar::Str(arr.value(i).to_string()));
                }
            }
        }
        ArrowDataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            for i in 0..num_rows {
                if arr.is_null(i) {
                    values.push(Scalar::Null);
                } else {
                    values.push(Scalar::Bin(arr.value(i).to_vec()));
                }
            }
        }
        _ => return Err(format!("Unsupported Arrow data type: {:?}", array.data_type())),
    }

    Ok(Column {
        name: name.to_string(),
        values,
    })
}

use std::sync::Arc;

