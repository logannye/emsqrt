//! Runtime: execute PhysicalProgram in TE order and emit a RunManifest.
//!
//! Starter behavior:
//! - Instantiates operators via `emsqrt-operators::registry`.
//! - Special-cases "source" and "sink" keys with placeholder ops.
//! - Walks `TePlan.order` sequentially; respects dependencies.
//! - Enforces a hard memory ceiling via `emsqrt-mem::MemoryBudgetImpl`.
//! - Emits a `RunManifest` with stable plan/TE hashes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

use emsqrt_core::config::EngineConfig;
use emsqrt_core::hash::{hash_serde, Hash256};
use emsqrt_core::manifest::RunManifest;
use emsqrt_core::prelude::Schema;
use emsqrt_core::types::RowBatch;

use emsqrt_mem::guard::MemoryBudgetImpl;
use emsqrt_mem::{Codec, SpillManager};

use emsqrt_io::storage::FsStorage;

use emsqrt_operators::registry::Registry;
use emsqrt_operators::traits::{OpError, Operator}; // placeholder alias (Vec<RowBatch>)

use emsqrt_planner::physical::PhysicalProgram;
use emsqrt_te::tree_eval::TePlan;

use emsqrt_io::writers::csv::CsvWriter;

#[derive(Debug, Error)]
pub enum ExecError {
    #[error("operator registry: {0}")]
    Registry(String),
    #[error("operator exec: {0}")]
    Operator(String),
    #[error("invalid plan: {0}")]
    Invalid(String),
    #[error("hashing error: {0}")]
    Hash(String),
}

/// Engine owns the memory budget, operator registry, and spill manager.
pub struct Engine {
    cfg: EngineConfig,
    budget: MemoryBudgetImpl,
    registry: Registry,
    spill_mgr: Arc<Mutex<SpillManager>>,
}

impl Engine {
    pub fn new(cfg: EngineConfig) -> Self {
        let cap = cfg.mem_cap_bytes;
        let spill_dir = cfg.spill_dir.clone();

        // Create spill manager with FsStorage
        let storage = Box::new(FsStorage::new());
        let codec = Codec::None; // Default to no compression; can be made configurable
        let spill_mgr = SpillManager::new(storage, codec, spill_dir);

        Self {
            cfg,
            budget: MemoryBudgetImpl::new(cap),
            registry: Registry::new(),
            spill_mgr: Arc::new(Mutex::new(spill_mgr)),
        }
    }

    /// Execute a prepared `PhysicalProgram` under `TePlan` and return a manifest.
    pub fn run(
        &mut self,
        program: &PhysicalProgram,
        te: &TePlan,
    ) -> Result<RunManifest, ExecError> {
        // Hash inputs deterministically (logical → physical handled earlier).
        let plan_hash = hash_serde(&program.plan).map_err(|e| ExecError::Hash(e.to_string()))?;
        let bindings_hash =
            hash_serde(&program.bindings).map_err(|e| ExecError::Hash(e.to_string()))?;
        let te_hash = hash_serde(&te.order).map_err(|e| ExecError::Hash(e.to_string()))?;

        // Merge hashes (simple xor of bytes) to capture bindings+plan.
        let plan_hash = xor_hashes(plan_hash, bindings_hash);

        // Instantiate operator table keyed by OpId.
        let mut ops: HashMap<u64, Box<dyn Operator>> = HashMap::new();
        for (op_id, binding) in &program.bindings {
            let key = binding.key.as_str();
            let config = &binding.config;
            let inst: Box<dyn Operator> = match key {
                "source" => {
                    let source_uri = config.get("source")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| ExecError::Registry("source operator missing 'source' in config".into()))?;
                    
                    // Get schema from config or use default
                    let schema: Schema = if let Some(schema_val) = config.get("schema") {
                        serde_json::from_value(schema_val.clone())
                            .unwrap_or_else(|_| Schema::new(vec![]))
                    } else {
                        Schema::new(vec![])
                    };
                    
                    Box::new(SourceOp {
                        source_uri: source_uri.to_string(),
                        schema,
                        file_position: Arc::new(Mutex::new(0)),
                    })
                },
                "sink" => {
                    let destination = config.get("destination")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let format = config.get("format")
                        .and_then(|v| v.as_str())
                        .unwrap_or("csv");
                    
                    Box::new(SinkOp {
                        destination: destination.to_string(),
                        format: format.to_string(),
                        writer_initialized: std::sync::Arc::new(std::sync::Mutex::new(false)),
                    })
                },
                "filter" => {
                    let mut op = emsqrt_operators::filter::Filter::default();
                    if let Some(expr) = config.get("expr").and_then(|v| v.as_str()) {
                        op.expr = Some(expr.to_string());
                    }
                    Box::new(op)
                }
                "project" => {
                    let mut op = emsqrt_operators::project::Project::default();
                    if let Some(cols) = config.get("columns").and_then(|v| v.as_array()) {
                        op.columns = cols
                            .iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect();
                    }
                    Box::new(op)
                }
                "map" => {
                    // Map currently doesn't use config, but we could parse renames here
                    Box::new(emsqrt_operators::map::Map::default())
                }
                "aggregate" => {
                    let mut op = emsqrt_operators::agregate::Aggregate::default();
                    op.spill_mgr = Some(self.spill_mgr.clone());
                    // Parse group_by and aggs from config if provided
                    if let Some(group_by) = config.get("group_by").and_then(|v| v.as_array()) {
                        op.group_by = group_by
                            .iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect();
                    }
                    if let Some(aggs) = config.get("aggs").and_then(|v| v.as_array()) {
                        op.aggs = aggs
                            .iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect();
                    }
                    Box::new(op)
                }
                "sort_external" => {
                    let mut op = emsqrt_operators::sort::external::ExternalSort::default();
                    op.spill_mgr = Some(self.spill_mgr.clone());
                    // Parse sort keys from config if provided
                    if let Some(keys) = config.get("by").and_then(|v| v.as_array()) {
                        op.by = keys
                            .iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect();
                    }
                    Box::new(op)
                }
                "join_hash" => {
                    let mut op = emsqrt_operators::join::hash::HashJoin::default();
                    op.spill_mgr = Some(self.spill_mgr.clone());
                    // Parse join keys from config if provided
                    if let Some(on) = config.get("on").and_then(|v| v.as_array()) {
                        op.on = on
                            .iter()
                            .filter_map(|v| {
                                if let Some(pair) = v.as_array() {
                                    if pair.len() == 2 {
                                        let left = pair[0].as_str()?.to_string();
                                        let right = pair[1].as_str()?.to_string();
                                        return Some((left, right));
                                    }
                                }
                                None
                            })
                            .collect();
                    }
                    if let Some(join_type) = config.get("join_type").and_then(|v| v.as_str()) {
                        op.join_type = join_type.to_string();
                    }
                    Box::new(op)
                }
                other => self.registry.make(other).ok_or_else(|| {
                    ExecError::Registry(format!("unknown operator key '{other}'"))
                })?,
            };
            ops.insert(op_id.get(), inst);
        }

        // Map: BlockId → RowBatch result
        let mut results: HashMap<u64, RowBatch> = HashMap::new();

        // Start manifest
        let now_ms = now_millis();
        let mut manifest = RunManifest::new(plan_hash, te_hash, now_ms);

        // Sequential TE order (starter).
        for b in &te.order {
            // Gather input batches from deps in order.
            let mut inputs: Vec<RowBatch> = Vec::with_capacity(b.deps.len());
            for dep in &b.deps {
                let key = dep.get();
                let batch = results.remove(&key).ok_or_else(|| {
                    ExecError::Invalid(format!("missing dependency block result for {}", key))
                })?;
                inputs.push(batch);
            }

            // Dispatch to the operator by op id.
            let op = ops.get(&b.op.get()).ok_or_else(|| {
                ExecError::Invalid(format!("no operator bound for op id {}", b.op))
            })?;

            // Calculate input sizes for error context
            let input_rows: usize = inputs.iter().map(|batch| batch.num_rows()).sum();
            let input_bytes: usize = inputs.iter().map(|batch| {
                batch.columns.iter().map(|col| col.values.len() * 8).sum::<usize>()
            }).sum();

            // Build error context with operator and block information
            let operator_name = op.name();
            let context = format!(
                "operator '{}' (op_id={}, block_id={}, input_rows={}, input_bytes={})",
                operator_name, b.op.get(), b.id.get(), input_rows, input_bytes
            );

            // Try to execute with retry logic for recoverable errors
            let out = match self.execute_block_with_retry(op.as_ref(), &inputs, &context, 3) {
                Ok(batch) => batch,
                Err(e) => {
                    // Enhance error with context and suggestions
                    let mut error_msg = format!("{}: {}", context, e);
                    if let OpError::Schema(_) | OpError::Exec(_) = e {
                        let suggestions = e.suggestions();
                        if !suggestions.is_empty() {
                            error_msg.push_str("\nSuggestions:");
                            for suggestion in suggestions {
                                error_msg.push_str(&format!("\n  - {}", suggestion));
                            }
                        }
                    }
                    return Err(ExecError::Operator(error_msg));
                }
            };

            // Store the result for this block (downstream deps will consume/remove it).
            results.insert(b.id.get(), out);

            #[cfg(feature = "tracing")]
            tracing::trace!(block = %b.id.get(), op = %b.op.get(), deps = b.deps.len(), "executed block");
        }

        // TODO: compute outputs digest (e.g., sinks) once sinks actually write data.
        let outputs_digest = None;

        manifest = manifest.finish(now_millis(), outputs_digest);
        Ok(manifest)
    }

    /// Execute a block with retry logic for recoverable errors.
    ///
    /// Retries up to `max_retries` times for recoverable errors.
    fn execute_block_with_retry(
        &self,
        op: &dyn Operator,
        inputs: &[RowBatch],
        context: &str,
        max_retries: u32,
    ) -> Result<RowBatch, OpError> {
        let mut last_error = None;
        
        for attempt in 0..=max_retries {
            match op.eval_block(inputs, &self.budget) {
                Ok(batch) => return Ok(batch),
                Err(e) => {
                    if e.is_recoverable() && attempt < max_retries {
                        // Exponential backoff: wait 2^attempt milliseconds
                        let delay_ms = 2_u64.pow(attempt);
                        std::thread::sleep(std::time::Duration::from_millis(delay_ms));
                        last_error = Some(e);
                        continue;
                    } else {
                        // Non-recoverable or max retries reached
                        return Err(e.with_context(context));
                    }
                }
            }
        }
        
        // Should not reach here, but handle gracefully
        match last_error {
            Some(e) => Err(e.with_context(context)),
            None => Err(OpError::Exec(format!("execution failed after {} retries", max_retries))
                .with_context(context)),
        }
    }
}

// --- helpers ---

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn xor_hashes(a: Hash256, b: Hash256) -> Hash256 {
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = a.0[i] ^ b.0[i];
    }
    Hash256(out)
}

// --- placeholder source/sink operators (until real IO is wired) ---

struct SourceOp {
    source_uri: String,
    schema: Schema,
    // Track file position for multi-block reading
    file_position: Arc<Mutex<usize>>,
}

impl Operator for SourceOp {
    fn name(&self) -> &'static str {
        "source"
    }
    fn memory_need(&self, _rows: u64, _bytes: u64) -> emsqrt_operators::plan::Footprint {
        emsqrt_operators::plan::Footprint {
            bytes_per_row: 1,
            overhead_bytes: 0,
        }
    }
    fn plan(&self, _input_schemas: &[Schema]) -> Result<emsqrt_operators::plan::OpPlan, OpError> {
        Err(OpError::Plan(
            "source.plan should not be called at exec time".into(),
        ))
    }
    fn eval_block(
        &self,
        _inputs: &[RowBatch],
        _budget: &dyn emsqrt_core::budget::MemoryBudget<Guard = emsqrt_mem::guard::BudgetGuardImpl>,
    ) -> Result<RowBatch, OpError> {
        // Strip file:// prefix if present
        let file_path = if self.source_uri.starts_with("file://") {
            &self.source_uri[7..]
        } else {
            &self.source_uri
        };
        
        // Read CSV file with provided schema
        use std::fs::File;
        use emsqrt_core::types::{Column, Scalar};
        
        let file = File::open(file_path)
            .map_err(|e| OpError::Exec(format!("failed to open CSV file '{}': {}", file_path, e)))?;
        
        let mut rdr = ::csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(file);
        
        // Build column index mapping from schema field names
        let headers = rdr.headers()
            .map_err(|e| OpError::Exec(format!("failed to read CSV headers: {}", e)))?;
        
        let col_indices: Vec<Option<usize>> = self.schema.fields.iter()
            .map(|field| {
                headers.iter().position(|h| h.trim() == field.name.trim())
            })
            .collect();
        
        // Verify all required columns are found
        for (field, col_idx_opt) in self.schema.fields.iter().zip(col_indices.iter()) {
            if col_idx_opt.is_none() {
                return Err(OpError::Exec(format!(
                    "CSV file missing required column '{}'. Available columns: {:?}",
                    field.name,
                    headers.iter().collect::<Vec<_>>()
                )));
            }
        }
        
        // Initialize columns based on schema
        let mut columns: Vec<Column> = self.schema.fields.iter()
            .map(|field| Column {
                name: field.name.clone(),
                values: Vec::new(),
            })
            .collect();
        
        // Read rows and populate columns
        // Skip rows that were already read by previous blocks
        let mut file_pos = self.file_position.lock().unwrap();
        let skip_rows = *file_pos;
        
        // Skip header + already-read rows
        let mut row_count = 0;
        let mut skipped = 0;
        for result in rdr.records() {
            // Skip rows that were read in previous blocks
            if skipped < skip_rows {
                skipped += 1;
                continue; // Skip this row, don't process it
            }
            
            // We've skipped enough - now process this row
            
            let record = result
                .map_err(|e| OpError::Exec(format!("failed to read CSV record: {}", e)))?;
            
            for (col_idx, field) in self.schema.fields.iter().enumerate() {
                let value = if let Some(csv_col_idx) = col_indices[col_idx] {
                    record.get(csv_col_idx).unwrap_or("")
                } else {
                    ""
                };
                
                // Parse value based on schema type
                let scalar = match field.data_type {
                    emsqrt_core::schema::DataType::Int32 => {
                        value.parse::<i32>()
                            .map(Scalar::I32)
                            .unwrap_or(Scalar::Null)
                    }
                    emsqrt_core::schema::DataType::Int64 => {
                        value.parse::<i64>()
                            .map(Scalar::I64)
                            .unwrap_or(Scalar::Null)
                    }
                    emsqrt_core::schema::DataType::Float32 => {
                        value.parse::<f32>()
                            .map(Scalar::F32)
                            .unwrap_or(Scalar::Null)
                    }
                    emsqrt_core::schema::DataType::Float64 => {
                        value.parse::<f64>()
                            .map(Scalar::F64)
                            .unwrap_or(Scalar::Null)
                    }
                    emsqrt_core::schema::DataType::Boolean => {
                        value.parse::<bool>()
                            .map(Scalar::Bool)
                            .unwrap_or(Scalar::Null)
                    }
                    _ => Scalar::Str(value.to_string()),
                };
                
                columns[col_idx].values.push(scalar);
            }
            
            row_count += 1;
            if row_count >= 10000 {
                break; // Limit batch size
            }
        }
        
        // Update file position for next block
        *file_pos += row_count;
        
        // Ensure all columns have the same number of values
        let num_rows = columns.first().map(|c| c.values.len()).unwrap_or(0);
        for col in &mut columns {
            if col.values.len() != num_rows {
                return Err(OpError::Exec(format!(
                    "column '{}' has {} values but expected {}",
                    col.name, col.values.len(), num_rows
                )));
            }
        }
        
        // If we skipped rows but didn't read any new ones, we've reached the end
        // This is fine - return empty batch with correct column structure
        if row_count == 0 {
            // If we've already read some rows in previous blocks, it's OK to return empty
            // But we still need to return columns with the correct names so downstream operators work
            if skip_rows > 0 {
                // Return empty batch with correct schema (columns exist but empty)
                return Ok(RowBatch { columns });
            }
            // Otherwise, this is the first read and we got nothing - that's an error
            return Err(OpError::Exec("no data in CSV file".into()));
        }
        
        Ok(RowBatch { columns })
    }
}

struct SinkOp {
    destination: String,
    format: String,
    writer_initialized: std::sync::Arc<std::sync::Mutex<bool>>,
}

impl Operator for SinkOp {
    fn name(&self) -> &'static str {
        "sink"
    }
    fn memory_need(&self, _rows: u64, _bytes: u64) -> emsqrt_operators::plan::Footprint {
        emsqrt_operators::plan::Footprint {
            bytes_per_row: 0,
            overhead_bytes: 0,
        }
    }
    fn plan(&self, _input_schemas: &[Schema]) -> Result<emsqrt_operators::plan::OpPlan, OpError> {
        Err(OpError::Plan(
            "sink.plan should not be called at exec time".into(),
        ))
    }
    fn eval_block(
        &self,
        inputs: &[RowBatch],
        _budget: &dyn emsqrt_core::budget::MemoryBudget<Guard = emsqrt_mem::guard::BudgetGuardImpl>,
    ) -> Result<RowBatch, OpError> {
        let input = inputs.get(0)
            .ok_or_else(|| OpError::Exec("sink requires one input".into()))?;
        
        // Check if input is empty (shouldn't happen, but handle gracefully)
        if input.num_rows() == 0 {
            // Empty batch - still write to ensure file exists, but skip if no columns
            if input.columns.is_empty() {
                return Ok(RowBatch { columns: vec![] });
            }
        }
        
        // Strip file:// prefix if present
        let file_path = if self.destination.starts_with("file://") {
            &self.destination[7..]
        } else {
            &self.destination
        };
        
        // Write based on format
        // For CSV, we need to append to the file if it already exists (for multiple blocks)
        match self.format.as_str() {
            "csv" => {
                use std::fs::OpenOptions;
                use std::io::Write;
                
                let mut initialized = self.writer_initialized.lock().unwrap();
                
                // Determine if this is the first write or a subsequent append
                let is_first_write = !*initialized;
                
                let file = if *initialized {
                    // Append mode for subsequent blocks
                    OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(file_path)
                        .map_err(|e| OpError::Exec(format!("failed to open CSV file for append '{}': {}", file_path, e)))?
                } else {
                    // Create/truncate for first block
                    *initialized = true;
                    std::fs::File::create(file_path)
                        .map_err(|e| OpError::Exec(format!("failed to create CSV file '{}': {}", file_path, e)))?
                };
                
                // Only write header on first write
                let mut writer = if is_first_write {
                    CsvWriter::to_writer(file)
                } else {
                    CsvWriter::to_writer_skip_header(file)
                };
                
                // Always write the batch - CsvWriter handles headers and empty batches correctly
                // If this is the first write, header will be written
                // If this is a subsequent write and batch is empty, nothing happens (which is fine)
                writer.write_batch(input)
                    .map_err(|e| OpError::Exec(format!("failed to write CSV batch with {} rows, {} cols: {}", input.num_rows(), input.columns.len(), e)))?;
                
                // CsvWriter already flushes in write_batch, so data should be written
            }
            _ => {
                return Err(OpError::Exec(format!("unsupported sink format: {}", self.format)));
            }
        }
        
        // Return empty batch (sink is terminal)
        Ok(RowBatch { columns: vec![] })
    }
}
