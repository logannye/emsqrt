//! Grace-partitioned hash join with build/probe phases.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use emsqrt_core::budget::MemoryBudget;
use emsqrt_core::prelude::Schema;
use emsqrt_core::types::{Column, RowBatch, Scalar};
use emsqrt_mem::guard::BudgetGuardImpl;
use emsqrt_mem::SpillManager;

use crate::plan::{Footprint, OpPlan};
use crate::traits::{OpError, Operator};

/// Join type enumeration.
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

impl JoinType {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "inner" => Ok(JoinType::Inner),
            "left" => Ok(JoinType::Left),
            "right" => Ok(JoinType::Right),
            "full" => Ok(JoinType::Full),
            _ => Err(format!("unknown join type: {}", s)),
        }
    }
}

pub struct HashJoin {
    pub on: Vec<(String, String)>, // (left_col, right_col)
    pub join_type: String,         // "inner", "left", "right", "full"
    pub spill_mgr: Option<Arc<Mutex<SpillManager>>>,
}

impl Default for HashJoin {
    fn default() -> Self {
        Self {
            on: Vec::new(),
            join_type: "inner".to_string(),
            spill_mgr: None,
        }
    }
}

impl Operator for HashJoin {
    fn name(&self) -> &'static str {
        "join_hash"
    }

    fn memory_need(&self, _rows: u64, _bytes: u64) -> Footprint {
        // Hash join needs hash table buffers + partitions.
        Footprint {
            bytes_per_row: 2,
            overhead_bytes: 512 * 1024,
        }
    }

    fn plan(&self, input_schemas: &[Schema]) -> Result<OpPlan, OpError> {
        if input_schemas.len() != 2 {
            return Err(OpError::Plan("hash join expects two inputs".into()));
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
            if fields.iter().any(|f| f.name == field.name) {
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
        _budget: &dyn MemoryBudget<Guard = BudgetGuardImpl>,
    ) -> Result<RowBatch, OpError> {
        if inputs.len() != 2 {
            return Err(OpError::Exec("hash join needs two block inputs".into()));
        }

        let left = &inputs[0];
        let right = &inputs[1];

        let join_type = JoinType::parse(&self.join_type)
            .map_err(|e| OpError::Exec(format!("invalid join type: {}", e)))?;

        // Simple in-memory hash join (no partitioning/spill for now)
        self.simple_hash_join(left, right, join_type)
    }
}

impl HashJoin {
    /// Simple in-memory hash join (build + probe).
    fn simple_hash_join(
        &self,
        left: &RowBatch,
        right: &RowBatch,
        join_type: JoinType,
    ) -> Result<RowBatch, OpError> {
        if self.on.is_empty() {
            return Err(OpError::Exec("join keys are empty".into()));
        }

        // Extract join key columns
        let (left_key_name, right_key_name) = &self.on[0]; // Support single key for now

        let left_key_col = left
            .columns
            .iter()
            .find(|c| &c.name == left_key_name)
            .ok_or_else(|| OpError::Exec(format!("left join key '{}' not found", left_key_name)))?;

        let right_key_col = right
            .columns
            .iter()
            .find(|c| &c.name == right_key_name)
            .ok_or_else(|| OpError::Exec(format!("right join key '{}' not found", right_key_name)))?;

        // Build phase: hash table on right side
        let mut hash_table: HashMap<String, Vec<usize>> = HashMap::new();

        for (row_idx, val) in right_key_col.values.iter().enumerate() {
            let key_str = scalar_to_string(val);
            hash_table.entry(key_str).or_default().push(row_idx);
        }

        // Probe phase: scan left side and emit matches
        let mut output_rows: Vec<(usize, Option<usize>)> = Vec::new(); // (left_idx, right_idx)

        for (left_idx, left_val) in left_key_col.values.iter().enumerate() {
            let key_str = scalar_to_string(left_val);

            if let Some(right_indices) = hash_table.get(&key_str) {
                // Match found: emit (left_idx, right_idx) for each match
                for &right_idx in right_indices {
                    output_rows.push((left_idx, Some(right_idx)));
                }
            } else {
                // No match
                if join_type == JoinType::Left || join_type == JoinType::Full {
                    output_rows.push((left_idx, None));
                }
            }
        }

        // Handle right-only rows for right/full joins
        if join_type == JoinType::Right || join_type == JoinType::Full {
            let mut matched_right: Vec<bool> = vec![false; right.num_rows()];
            for (_, right_idx) in &output_rows {
                if let Some(idx) = right_idx {
                    matched_right[*idx] = true;
                }
            }

            for (right_idx, &matched) in matched_right.iter().enumerate() {
                if !matched {
                    output_rows.push((0, Some(right_idx))); // Use dummy left_idx for right-only
                }
            }
        }

        // Build output columns
        let mut output_cols = Vec::new();

        // Left columns
        for col in &left.columns {
            let mut new_col = Column {
                name: col.name.clone(),
                values: Vec::with_capacity(output_rows.len()),
            };

            for (left_idx, right_idx) in &output_rows {
                if right_idx.is_none() || join_type != JoinType::Right {
                    new_col.values.push(col.values[*left_idx].clone());
                } else {
                    new_col.values.push(Scalar::Null); // Right-only row
                }
            }

            output_cols.push(new_col);
        }

        // Right columns (with suffix if name conflicts)
        for col in &right.columns {
            let col_name = if left.columns.iter().any(|c| c.name == col.name) {
                format!("{}_right", col.name)
            } else {
                col.name.clone()
            };

            let mut new_col = Column {
                name: col_name,
                values: Vec::with_capacity(output_rows.len()),
            };

            for (_, right_idx) in &output_rows {
                if let Some(idx) = right_idx {
                    new_col.values.push(col.values[*idx].clone());
                } else {
                    new_col.values.push(Scalar::Null); // Left-only row
                }
            }

            output_cols.push(new_col);
        }

        Ok(RowBatch {
            columns: output_cols,
        })
    }
}

/// Convert a scalar to a string for hash key (simplified).
fn scalar_to_string(s: &Scalar) -> String {
    match s {
        Scalar::Null => "NULL".to_string(),
        Scalar::Bool(b) => b.to_string(),
        Scalar::I32(i) => i.to_string(),
        Scalar::I64(i) => i.to_string(),
        Scalar::F32(f) => f.to_string(),
        Scalar::F64(f) => f.to_string(),
        Scalar::Str(s) => s.clone(),
        Scalar::Bin(b) => format!("[binary {} bytes]", b.len()),
    }
}
