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
#[derive(Debug, Clone, Copy, PartialEq)]
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

    fn memory_need(&self, rows: u64, bytes: u64) -> Footprint {
        // Hash join needs:
        // - Hash table for build side (right): ~2x bytes per row
        // - Probe buffer for probe side (left): ~1x bytes per row
        // - Partition buffers for Grace join: ~2x bytes per row (worst case)
        // - Overhead for partition management
        //
        // For Grace join, we partition into smaller chunks, so peak memory
        // is limited to one partition pair at a time.
        let bytes_per_row = if rows > 100_000 {
            // Large inputs likely use Grace join - partition overhead
            2
        } else {
            // Small inputs use simple hash join - hash table overhead
            3 // build + probe
        };
        
        Footprint {
            bytes_per_row,
            overhead_bytes: 1024 * 1024, // 1MB overhead for partition management
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
        budget: &dyn MemoryBudget<Guard = BudgetGuardImpl>,
    ) -> Result<RowBatch, OpError> {
        if inputs.len() != 2 {
            return Err(OpError::Exec("hash join needs two block inputs".into()));
        }

        let left = &inputs[0];
        let right = &inputs[1];

        let join_type = JoinType::parse(&self.join_type)
            .map_err(|e| OpError::Exec(format!("invalid join type: {}", e)))?;

        // Decide between simple hash join and Grace hash join
        // Use simple join if:
        // 1. No spill manager available (can't partition)
        // 2. Inputs are small (< 100k rows each)
        // Otherwise use Grace hash join with partitioning
        
        let right_rows = right.num_rows() as u64;
        let left_rows = left.num_rows() as u64;
        
        // Use simple join for small inputs or when no spill manager
        if self.spill_mgr.is_none() || (right_rows < 100_000 && left_rows < 100_000) {
            self.simple_hash_join(left, right, join_type)
        } else {
            // Large inputs and spill manager available - use Grace hash join
            self.grace_hash_join(left, right, join_type, budget)
        }
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

    /// Partition a RowBatch into multiple partitions based on join keys.
    ///
    /// Returns a vector of RowBatches, one per partition.
    fn partition_batch(
        &self,
        batch: &RowBatch,
        join_key_names: &[String],
        num_partitions: usize,
    ) -> Result<Vec<RowBatch>, OpError> {
        // Compute partition indices for each row
        let partition_indices = batch
            .hash_columns(join_key_names, num_partitions)
            .map_err(|e| OpError::Exec(format!("partition failed: {}", e)))?;

        // Initialize empty batches for each partition
        let mut partitions: Vec<RowBatch> = (0..num_partitions)
            .map(|_| RowBatch {
                columns: batch.columns.iter()
                    .map(|col| Column {
                        name: col.name.clone(),
                        values: Vec::new(),
                    })
                    .collect(),
            })
            .collect();

        // Distribute rows to partitions
        for (row_idx, &part_idx) in partition_indices.iter().enumerate() {
            for (col_idx, col) in batch.columns.iter().enumerate() {
                partitions[part_idx].columns[col_idx].values.push(col.values[row_idx].clone());
            }
        }

        Ok(partitions)
    }

    /// Grace hash join with partitioning for large datasets.
    ///
    /// Algorithm:
    /// 1. Partition both inputs by join keys into N partitions
    /// 2. Spill partitions to disk
    /// 3. For each partition pair (left[i], right[i]):
    ///    - Load left partition into memory (build hash table)
    ///    - Stream right partition (probe phase)
    ///    - Merge results
    fn grace_hash_join(
        &self,
        left: &RowBatch,
        right: &RowBatch,
        join_type: JoinType,
        budget: &dyn MemoryBudget<Guard = BudgetGuardImpl>,
    ) -> Result<RowBatch, OpError> {
        if self.on.is_empty() {
            return Err(OpError::Exec("join keys are empty".into()));
        }

        let spill_mgr = self.spill_mgr.as_ref()
            .ok_or_else(|| OpError::Exec("Grace hash join requires spill manager".into()))?;

        // Extract join key names
        let left_key_names: Vec<String> = self.on.iter().map(|(l, _)| l.clone()).collect();
        let right_key_names: Vec<String> = self.on.iter().map(|(_, r)| r.clone()).collect();

        // Determine number of partitions (aim for partitions that fit in memory)
        // Use a conservative estimate: each partition should be < 1MB
        let estimated_bytes_per_row = 64;
        let left_total_bytes = (left.num_rows() as u64) * estimated_bytes_per_row;
        let right_total_bytes = (right.num_rows() as u64) * estimated_bytes_per_row;
        
        // Target partition size: try to keep each partition under 1MB
        let target_partition_bytes = 1024 * 1024; // 1MB
        let num_partitions = ((left_total_bytes.max(right_total_bytes) / target_partition_bytes).max(1) as usize)
            .min(256); // Cap at 256 partitions

        // Partition both inputs
        let left_partitions = self.partition_batch(left, &left_key_names, num_partitions)?;
        let right_partitions = self.partition_batch(right, &right_key_names, num_partitions)?;

        // Spill partitions to disk
        let mut left_segments: Vec<Vec<emsqrt_mem::spill::SegmentMeta>> = Vec::new();
        let mut right_segments: Vec<Vec<emsqrt_mem::spill::SegmentMeta>> = Vec::new();

        let mut spill_mgr_guard = spill_mgr.lock().unwrap();
        let spill_id = emsqrt_core::id::SpillId::new(1); // Use a fixed ID for this join

        for (part_idx, left_part) in left_partitions.iter().enumerate() {
            if left_part.num_rows() > 0 {
                let run_idx = spill_mgr_guard.next_run_index();
                let meta = spill_mgr_guard.write_batch(left_part, spill_id, run_idx)
                    .map_err(|e| OpError::Exec(format!("failed to spill left partition {}: {}", part_idx, e)))?;
                if left_segments.len() <= part_idx {
                    left_segments.resize(part_idx + 1, Vec::new());
                }
                left_segments[part_idx].push(meta);
            }
        }

        for (part_idx, right_part) in right_partitions.iter().enumerate() {
            if right_part.num_rows() > 0 {
                let run_idx = spill_mgr_guard.next_run_index();
                let meta = spill_mgr_guard.write_batch(right_part, spill_id, run_idx)
                    .map_err(|e| OpError::Exec(format!("failed to spill right partition {}: {}", part_idx, e)))?;
                if right_segments.len() <= part_idx {
                    right_segments.resize(part_idx + 1, Vec::new());
                }
                right_segments[part_idx].push(meta);
            }
        }

        drop(spill_mgr_guard);

        // Join each partition pair
        let mut all_results = Vec::new();

        for part_idx in 0..num_partitions {
            // Load left partition(s) into memory (build phase)
            let mut left_build = RowBatch { columns: Vec::new() };
            
            if part_idx < left_segments.len() {
                let spill_mgr_guard = spill_mgr.lock().unwrap();
                for segment_meta in &left_segments[part_idx] {
                    let batch = spill_mgr_guard.read_batch(segment_meta, budget)
                        .map_err(|e| OpError::Exec(format!("failed to read left partition {}: {}", part_idx, e)))?;
                    
                    if left_build.columns.is_empty() {
                        left_build = batch;
                    } else {
                        // Concatenate batches (append rows)
                        // Append rows from batch to left_build
                        for (col_idx, col) in batch.columns.iter().enumerate() {
                            left_build.columns[col_idx].values.extend_from_slice(&col.values);
                        }
                    }
                }
                drop(spill_mgr_guard);
            }

            // If left partition is empty, skip (no matches possible for inner/left joins)
            if left_build.columns.is_empty() {
                if join_type == JoinType::Right || join_type == JoinType::Full {
                    // For right/full joins, we need to output unmatched right rows
                    // Load right partition and output all rows with NULL left side
                    if part_idx < right_segments.len() {
                        let spill_mgr_guard = spill_mgr.lock().unwrap();
                        for segment_meta in &right_segments[part_idx] {
                            let right_batch = spill_mgr_guard.read_batch(segment_meta, budget)
                                .map_err(|e| OpError::Exec(format!("failed to read right partition {}: {}", part_idx, e)))?;
                            
                            // Create result with NULL left columns
                            let mut result_cols = Vec::new();
                            
                            // Left columns (all NULL)
                            for col in &left.columns {
                                result_cols.push(Column {
                                    name: col.name.clone(),
                                    values: vec![Scalar::Null; right_batch.num_rows()],
                                });
                            }
                            
                            // Right columns
                            for col in &right_batch.columns {
                                let col_name = if left.columns.iter().any(|c| c.name == col.name) {
                                    format!("{}_right", col.name)
                                } else {
                                    col.name.clone()
                                };
                                result_cols.push(Column {
                                    name: col_name,
                                    values: col.values.clone(),
                                });
                            }
                            
                            all_results.push(RowBatch { columns: result_cols });
                        }
                        drop(spill_mgr_guard);
                    }
                }
                continue;
            }

            // Stream right partition(s) and probe (probe phase)
            if part_idx < right_segments.len() {
                let spill_mgr_guard = spill_mgr.lock().unwrap();
                for segment_meta in &right_segments[part_idx] {
                    let right_probe = spill_mgr_guard.read_batch(segment_meta, budget)
                        .map_err(|e| OpError::Exec(format!("failed to read right partition {}: {}", part_idx, e)))?;
                    
                    // Perform hash join on this partition pair
                    let partition_result = self.simple_hash_join(&left_build, &right_probe, join_type)?;
                    all_results.push(partition_result);
                }
                drop(spill_mgr_guard);
            } else if join_type == JoinType::Left || join_type == JoinType::Full {
                // Right partition is empty but left has rows - output left rows with NULL right
                let mut result_cols = Vec::new();
                
                // Left columns
                for col in &left_build.columns {
                    result_cols.push(col.clone());
                }
                
                // Right columns (all NULL)
                for col in &right.columns {
                    let col_name = if left.columns.iter().any(|c| c.name == col.name) {
                        format!("{}_right", col.name)
                    } else {
                        col.name.clone()
                    };
                    result_cols.push(Column {
                        name: col_name,
                        values: vec![Scalar::Null; left_build.num_rows()],
                    });
                }
                
                all_results.push(RowBatch { columns: result_cols });
            }
        }

        // Merge all results into a single batch
        if all_results.is_empty() {
            // Return empty batch with correct schema
            let mut columns = Vec::new();
            for col in &left.columns {
                columns.push(Column {
                    name: col.name.clone(),
                    values: Vec::new(),
                });
            }
            for col in &right.columns {
                let col_name = if left.columns.iter().any(|c| c.name == col.name) {
                    format!("{}_right", col.name)
                } else {
                    col.name.clone()
                };
                columns.push(Column {
                    name: col_name,
                    values: Vec::new(),
                });
            }
            return Ok(RowBatch { columns });
        }

        // Concatenate all result batches
        let mut merged = all_results[0].clone();
        for result in all_results.iter().skip(1) {
            // Concatenate rows (append to each column)
            if merged.columns.len() != result.columns.len() {
                return Err(OpError::Exec("schema mismatch in merged results".into()));
            }
            
            for (col_idx, col) in result.columns.iter().enumerate() {
                merged.columns[col_idx].values.extend_from_slice(&col.values);
            }
        }

        Ok(merged)
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
