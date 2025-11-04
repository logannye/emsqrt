//! Test data generation utilities for EM-√ test suite

use emsqrt_core::schema::{DataType, Field, Schema};
use emsqrt_core::types::{Column, RowBatch, Scalar};
use std::collections::HashMap;

/// Generate a random RowBatch matching the given schema
pub fn generate_random_batch(rows: usize, schema: &Schema) -> RowBatch {
    let mut columns = Vec::new();
    
    for field in &schema.fields {
        let mut values = Vec::with_capacity(rows);
        
        for i in 0..rows {
            let value = match field.data_type {
                DataType::Boolean => Scalar::Bool(i % 2 == 0),
                DataType::Int32 => Scalar::I32((i % 1000) as i32),
                DataType::Int64 => Scalar::I64(i as i64),
                DataType::Float32 => Scalar::F32((i as f32) * 0.5),
                DataType::Float64 => Scalar::F64((i as f64) * 0.5),
                DataType::Utf8 => Scalar::Str(format!("value_{}", i % 100)),
                DataType::Binary => Scalar::Bin(vec![i as u8; 10]),
                DataType::Date64 => Scalar::I64((i as i64) * 86400000), // Days as ms
                DataType::Decimal128 => Scalar::I64(i as i64), // Simplified
            };
            values.push(value);
        }
        
        columns.push(Column {
            name: field.name.clone(),
            values,
        });
    }
    
    RowBatch { columns }
}

/// Generate a RowBatch that is already sorted by the specified column
pub fn generate_sorted_batch(rows: usize, sort_col: &str, dtype: DataType) -> RowBatch {
    let _schema = Schema::new(vec![
        Field::new(sort_col, dtype.clone(), false),
        Field::new("data", DataType::Utf8, false),
    ]);
    
    let mut sort_values = Vec::with_capacity(rows);
    let mut data_values = Vec::with_capacity(rows);
    
    for i in 0..rows {
        let sort_val = match dtype {
            DataType::Int32 => Scalar::I32(i as i32),
            DataType::Int64 => Scalar::I64(i as i64),
            DataType::Float64 => Scalar::F64(i as f64),
            DataType::Utf8 => Scalar::Str(format!("key_{:05}", i)),
            _ => Scalar::I64(i as i64),
        };
        sort_values.push(sort_val);
        data_values.push(Scalar::Str(format!("data_{}", i)));
    }
    
    RowBatch {
        columns: vec![
            Column {
                name: sort_col.to_string(),
                values: sort_values,
            },
            Column {
                name: "data".to_string(),
                values: data_values,
            },
        ],
    }
}

/// Generate a RowBatch with skewed distribution (Zipfian-like)
pub fn generate_skewed_batch(rows: usize, num_keys: usize, skew_factor: f64) -> RowBatch {
    let mut key_values = Vec::with_capacity(rows);
    let mut value_values = Vec::with_capacity(rows);
    
    // Simple skewed distribution: first few keys appear much more frequently
    for i in 0..rows {
        // Use a simple power law: key frequency proportional to 1/rank^skew_factor
        let rank = ((i as f64 / rows as f64) * (num_keys as f64)).powf(1.0 / skew_factor) as usize;
        let key = rank.min(num_keys - 1);
        
        key_values.push(Scalar::Str(format!("key_{}", key)));
        value_values.push(Scalar::I64(i as i64));
    }
    
    RowBatch {
        columns: vec![
            Column {
                name: "key".to_string(),
                values: key_values,
            },
            Column {
                name: "value".to_string(),
                values: value_values,
            },
        ],
    }
}

/// Generate a batch with controllable null percentage
pub fn generate_batch_with_nulls(rows: usize, null_percentage: f64) -> RowBatch {
    let mut values = Vec::with_capacity(rows);
    
    for i in 0..rows {
        let val = if (i as f64 / rows as f64) < null_percentage {
            Scalar::Null
        } else {
            Scalar::I64(i as i64)
        };
        values.push(val);
    }
    
    RowBatch {
        columns: vec![Column {
            name: "nullable_col".to_string(),
            values,
        }],
    }
}

/// Create a temporary spill directory for testing
pub fn create_temp_spill_dir() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    
    format!("/tmp/emsqrt-test-{}", nanos)
}

/// Generate two batches suitable for join testing
pub fn generate_join_batches(
    left_rows: usize,
    right_rows: usize,
    match_percentage: f64,
) -> (RowBatch, RowBatch) {
    let num_matching_keys = ((left_rows.min(right_rows) as f64) * match_percentage) as usize;
    
    // Left batch
    let mut left_keys = Vec::with_capacity(left_rows);
    let mut left_data = Vec::with_capacity(left_rows);
    
    for i in 0..left_rows {
        let key = i % num_matching_keys.max(1);
        left_keys.push(Scalar::I64(key as i64));
        left_data.push(Scalar::Str(format!("left_{}", i)));
    }
    
    let left = RowBatch {
        columns: vec![
            Column {
                name: "key".to_string(),
                values: left_keys,
            },
            Column {
                name: "left_data".to_string(),
                values: left_data,
            },
        ],
    };
    
    // Right batch
    let mut right_keys = Vec::with_capacity(right_rows);
    let mut right_data = Vec::with_capacity(right_rows);
    
    for i in 0..right_rows {
        let key = i % num_matching_keys.max(1);
        right_keys.push(Scalar::I64(key as i64));
        right_data.push(Scalar::Str(format!("right_{}", i)));
    }
    
    let right = RowBatch {
        columns: vec![
            Column {
                name: "key".to_string(),
                values: right_keys,
            },
            Column {
                name: "right_data".to_string(),
                values: right_data,
            },
        ],
    };
    
    (left, right)
}

/// Generate a batch for aggregation testing
pub fn generate_aggregate_batch(rows: usize, num_groups: usize) -> RowBatch {
    let mut group_keys = Vec::with_capacity(rows);
    let mut amounts = Vec::with_capacity(rows);
    
    for i in 0..rows {
        let group = i % num_groups;
        group_keys.push(Scalar::Str(format!("group_{}", group)));
        amounts.push(Scalar::I64((i + 1) as i64)); // Non-zero for easier testing
    }
    
    RowBatch {
        columns: vec![
            Column {
                name: "group_key".to_string(),
                values: group_keys,
            },
            Column {
                name: "amount".to_string(),
                values: amounts,
            },
        ],
    }
}

/// Calculate expected aggregate results for verification
pub fn calculate_expected_aggregates(batch: &RowBatch, group_col: &str, agg_col: &str) -> HashMap<String, (u64, i64, i64, i64)> {
    // Returns HashMap<group_key, (count, sum, min, max)>
    let mut results: HashMap<String, (u64, i64, i64, i64)> = HashMap::new();
    
    let group_idx = batch.columns.iter().position(|c| c.name == group_col).unwrap();
    let agg_idx = batch.columns.iter().position(|c| c.name == agg_col).unwrap();
    
    for row_idx in 0..batch.num_rows() {
        let group_key = match &batch.columns[group_idx].values[row_idx] {
            Scalar::Str(s) => s.clone(),
            _ => panic!("Expected string group key"),
        };
        
        let value = match &batch.columns[agg_idx].values[row_idx] {
            Scalar::I64(v) => *v,
            _ => panic!("Expected i64 value"),
        };
        
        let entry = results.entry(group_key).or_insert((0, 0, i64::MAX, i64::MIN));
        entry.0 += 1; // count
        entry.1 += value; // sum
        entry.2 = entry.2.min(value); // min
        entry.3 = entry.3.max(value); // max
    }
    
    results
}

