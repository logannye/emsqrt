//! Merge join for pre-sorted inputs.
//!
//! Implements a streaming merge join algorithm that efficiently joins two
//! sorted RowBatches on specified join keys. Supports INNER, LEFT, RIGHT, and FULL joins.
//!
//! Precondition: inputs must be pre-sorted on the join keys (enforced by planner/TE).

use std::cmp::Ordering;

use emsqrt_core::prelude::Schema;
use emsqrt_core::types::{RowBatch, Scalar};

use crate::plan::{Footprint, OpPlan};
use crate::traits::{MemoryBudget, OpError, Operator};

#[derive(Default)]
pub struct MergeJoin {
    pub on: Vec<(String, String)>, // (left_col, right_col)
    pub join_type: String,         // "inner", "left", "right", "full"
}

impl Operator for MergeJoin {
    fn name(&self) -> &'static str {
        "join_merge"
    }

    fn memory_need(&self, _rows: u64, _bytes: u64) -> Footprint {
        // Merge join is streaming; small overhead for buffers.
        Footprint {
            bytes_per_row: 1,
            overhead_bytes: 64 * 1024,
        }
    }

    fn plan(&self, input_schemas: &[Schema]) -> Result<OpPlan, OpError> {
        if input_schemas.len() != 2 {
            return Err(OpError::Plan("merge join expects two inputs".into()));
        }

        // Derive output schema by concatenating left + right schemas
        let left_schema = &input_schemas[0];
        let right_schema = &input_schemas[1];

        let mut fields = Vec::new();

        // Add left fields
        for field in &left_schema.fields {
            fields.push(field.clone());
        }

        // Add right fields (with suffix if name conflicts)
        for field in &right_schema.fields {
            let mut new_field = field.clone();
            if left_schema.fields.iter().any(|f| f.name == field.name) {
                new_field.name = format!("{}_right", field.name);
            }
            fields.push(new_field);
        }

        let out_schema = Schema::new(fields);
        Ok(OpPlan::new(out_schema, self.memory_need(0, 0)))
    }

    fn eval_block(
        &self,
        inputs: &[RowBatch],
        _budget: &dyn MemoryBudget<Guard = emsqrt_mem::guard::BudgetGuardImpl>,
    ) -> Result<RowBatch, OpError> {
        if inputs.len() != 2 {
            return Err(OpError::Exec("merge join needs two block inputs".into()));
        }

        let left = &inputs[0];
        let right = &inputs[1];

        // Extract join key column indices
        let left_keys: Vec<usize> = self
            .on
            .iter()
            .map(|(left_col, _)| {
                left.columns
                    .iter()
                    .position(|c| &c.name == left_col)
                    .ok_or_else(|| OpError::Exec(format!("left join key '{}' not found", left_col)))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let right_keys: Vec<usize> = self
            .on
            .iter()
            .map(|(_, right_col)| {
                right
                    .columns
                    .iter()
                    .position(|c| &c.name == right_col)
                    .ok_or_else(|| OpError::Exec(format!("right join key '{}' not found", right_col)))
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Perform streaming merge join
        let join_type = parse_join_type(&self.join_type)?;
        merge_join_sorted(left, right, &left_keys, &right_keys, join_type)
    }
}

/// Parse join type string.
fn parse_join_type(s: &str) -> Result<JoinType, OpError> {
    match s.to_lowercase().as_str() {
        "inner" => Ok(JoinType::Inner),
        "left" => Ok(JoinType::Left),
        "right" => Ok(JoinType::Right),
        "full" => Ok(JoinType::Full),
        _ => Err(OpError::Exec(format!("unknown join type: {}", s))),
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

/// Perform streaming merge join on two sorted RowBatches.
fn merge_join_sorted(
    left: &RowBatch,
    right: &RowBatch,
    left_keys: &[usize],
    right_keys: &[usize],
    join_type: JoinType,
) -> Result<RowBatch, OpError> {
    use std::cmp::Ordering;

    let left_rows = left.num_rows();
    let right_rows = right.num_rows();

    if left_rows == 0 && right_rows == 0 {
        return Ok(RowBatch { columns: Vec::new() });
    }

    // Prepare output columns
    let mut output_cols = Vec::new();
    
    // Initialize left columns
    for col in &left.columns {
        output_cols.push(emsqrt_core::types::Column {
            name: col.name.clone(),
            values: Vec::new(),
        });
    }

    // Initialize right columns (with suffix if name conflicts)
    for col in &right.columns {
        let mut new_col = emsqrt_core::types::Column {
            name: col.name.clone(),
            values: Vec::new(),
        };
        if left.columns.iter().any(|c| c.name == col.name) {
            new_col.name = format!("{}_right", col.name);
        }
        output_cols.push(new_col);
    }

    // Two-pointer merge algorithm
    let mut left_idx = 0;
    let mut right_idx = 0;

    while left_idx < left_rows && right_idx < right_rows {
        let left_key = extract_join_key(left, left_idx, left_keys)?;
        let right_key = extract_join_key(right, right_idx, right_keys)?;

        match compare_scalar_tuples(&left_key, &right_key) {
            Ordering::Less => {
                // Left key < right key
                match join_type {
                    JoinType::Left | JoinType::Full => {
                        // Emit left row with nulls for right
                        emit_row(left, left_idx, &mut output_cols, 0, left.columns.len());
                        emit_nulls(&mut output_cols, left.columns.len(), right.columns.len());
                    }
                    _ => {}
                }
                left_idx += 1;
            }
            Ordering::Greater => {
                // Left key > right key
                match join_type {
                    JoinType::Right | JoinType::Full => {
                        // Emit right row with nulls for left
                        emit_nulls(&mut output_cols, 0, left.columns.len());
                        emit_row(right, right_idx, &mut output_cols, left.columns.len(), right.columns.len());
                    }
                    _ => {}
                }
                right_idx += 1;
            }
            Ordering::Equal => {
                // Keys match - emit cartesian product for all matching rows
                // Find all rows with the same key on both sides
                let mut left_match_end = left_idx;
                while left_match_end < left_rows {
                    let key = extract_join_key(left, left_match_end, left_keys)?;
                    if compare_scalar_tuples(&key, &left_key) == Ordering::Equal {
                        left_match_end += 1;
                    } else {
                        break;
                    }
                }

                let mut right_match_end = right_idx;
                while right_match_end < right_rows {
                    let key = extract_join_key(right, right_match_end, right_keys)?;
                    if compare_scalar_tuples(&key, &right_key) == Ordering::Equal {
                        right_match_end += 1;
                    } else {
                        break;
                    }
                }

                // Emit cartesian product
                for l in left_idx..left_match_end {
                    for r in right_idx..right_match_end {
                        emit_row(left, l, &mut output_cols, 0, left.columns.len());
                        emit_row(right, r, &mut output_cols, left.columns.len(), right.columns.len());
                    }
                }

                left_idx = left_match_end;
                right_idx = right_match_end;
            }
        }
    }

    // Handle remaining rows for LEFT/FULL joins
    while left_idx < left_rows {
        match join_type {
            JoinType::Left | JoinType::Full => {
                emit_row(left, left_idx, &mut output_cols, 0, left.columns.len());
                emit_nulls(&mut output_cols, left.columns.len(), right.columns.len());
            }
            _ => {}
        }
        left_idx += 1;
    }

    // Handle remaining rows for RIGHT/FULL joins
    while right_idx < right_rows {
        match join_type {
            JoinType::Right | JoinType::Full => {
                emit_nulls(&mut output_cols, 0, left.columns.len());
                emit_row(right, right_idx, &mut output_cols, left.columns.len(), right.columns.len());
            }
            _ => {}
        }
        right_idx += 1;
    }

    Ok(RowBatch {
        columns: output_cols,
    })
}

/// Extract join key tuple for a row.
fn extract_join_key(
    batch: &RowBatch,
    row_idx: usize,
    key_indices: &[usize],
) -> Result<Vec<Scalar>, OpError> {
    let mut key = Vec::with_capacity(key_indices.len());
    for &col_idx in key_indices {
        if col_idx >= batch.columns.len() {
            return Err(OpError::Exec(format!("column index {} out of bounds", col_idx)));
        }
        if row_idx >= batch.columns[col_idx].values.len() {
            return Err(OpError::Exec(format!("row index {} out of bounds", row_idx)));
        }
        key.push(batch.columns[col_idx].values[row_idx].clone());
    }
    Ok(key)
}

/// Compare two scalar tuples for ordering.
fn compare_scalar_tuples(a: &[Scalar], b: &[Scalar]) -> Ordering {
    use emsqrt_core::types::Scalar::*;
    
    for (x, y) in a.iter().zip(b.iter()) {
        match (x, y) {
            (Null, Null) => continue,
            (Null, _) => return Ordering::Less,
            (_, Null) => return Ordering::Greater,
            (Bool(x), Bool(y)) => {
                let cmp = x.cmp(y);
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            (I32(x), I32(y)) => {
                let cmp = x.cmp(y);
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            (I64(x), I64(y)) => {
                let cmp = x.cmp(y);
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            (F32(x), F32(y)) => {
                let cmp = x.partial_cmp(y).unwrap_or(Ordering::Equal);
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            (F64(x), F64(y)) => {
                let cmp = x.partial_cmp(y).unwrap_or(Ordering::Equal);
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            (Str(x), Str(y)) => {
                let cmp = x.cmp(y);
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            (Bin(x), Bin(y)) => {
                let cmp = x.cmp(y);
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            _ => {
                // Mixed types: compare by type order
                let x_order = scalar_type_order(x);
                let y_order = scalar_type_order(y);
                return x_order.cmp(&y_order);
            }
        }
    }
    a.len().cmp(&b.len())
}

/// Get type order for scalar (for mixed-type comparisons).
fn scalar_type_order(s: &Scalar) -> u8 {
    use emsqrt_core::types::Scalar::*;
    match s {
        Null => 0,
        Bool(_) => 1,
        I32(_) => 2,
        I64(_) => 3,
        F32(_) => 4,
        F64(_) => 5,
        Str(_) => 6,
        Bin(_) => 7,
    }
}

/// Emit a row from source batch to output columns.
fn emit_row(
    source: &RowBatch,
    row_idx: usize,
    output_cols: &mut [emsqrt_core::types::Column],
    start_col: usize,
    num_cols: usize,
) {
    for (i, source_col) in source.columns.iter().take(num_cols).enumerate() {
        if start_col + i < output_cols.len() && row_idx < source_col.values.len() {
            output_cols[start_col + i]
                .values
                .push(source_col.values[row_idx].clone());
        }
    }
}

/// Emit null values for a range of columns.
fn emit_nulls(
    output_cols: &mut [emsqrt_core::types::Column],
    start_col: usize,
    num_cols: usize,
) {
    for i in 0..num_cols {
        if start_col + i < output_cols.len() {
            output_cols[start_col + i].values.push(Scalar::Null);
        }
    }
}
