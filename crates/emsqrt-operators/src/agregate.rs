//! Aggregate operator with budget-aware hash table and spill support.
//!
//! Implements partitioned aggregation: hash group keys to partitions,
//! spill when budget exceeded, final merge phase.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use emsqrt_core::budget::MemoryBudget;
use emsqrt_core::id::SpillId;
use emsqrt_core::prelude::{DataType, Field, Schema};
use emsqrt_core::types::{Column, RowBatch, Scalar};
use emsqrt_mem::guard::BudgetGuardImpl;
use emsqrt_mem::SpillManager;

use crate::plan::{Footprint, OpPlan};
use crate::traits::{OpError, Operator};

/// Aggregation function specification.
#[derive(Debug, Clone)]
pub enum AggFunc {
    Count,
    Sum { column: String },
    Min { column: String },
    Max { column: String },
    Avg { column: String },
}

impl AggFunc {
    /// Parse from string like "count", "sum:sales", "max:price".
    pub fn parse(s: &str) -> Result<Self, String> {
        if s == "count" {
            return Ok(AggFunc::Count);
        }
        if let Some((func, col)) = s.split_once(':') {
            match func {
                "sum" => Ok(AggFunc::Sum {
                    column: col.to_string(),
                }),
                "min" => Ok(AggFunc::Min {
                    column: col.to_string(),
                }),
                "max" => Ok(AggFunc::Max {
                    column: col.to_string(),
                }),
                "avg" => Ok(AggFunc::Avg {
                    column: col.to_string(),
                }),
                _ => Err(format!("unknown agg function: {}", func)),
            }
        } else {
            Err(format!("invalid agg spec: {}", s))
        }
    }

    /// Output field for this aggregation.
    pub fn output_field(&self) -> Field {
        match self {
            AggFunc::Count => Field::new("count", DataType::Int64, false),
            AggFunc::Sum { column } => Field::new(format!("sum_{}", column), DataType::Float64, true),
            AggFunc::Min { column } => Field::new(format!("min_{}", column), DataType::Float64, true),
            AggFunc::Max { column } => Field::new(format!("max_{}", column), DataType::Float64, true),
            AggFunc::Avg { column } => Field::new(format!("avg_{}", column), DataType::Float64, true),
        }
    }
}

/// Aggregate value accumulator.
#[derive(Debug, Clone)]
pub struct AggValue {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}

impl Default for AggValue {
    fn default() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }
}

impl AggValue {
    pub fn update(&mut self, val: f64) {
        self.count += 1;
        self.sum += val;
        if val < self.min {
            self.min = val;
        }
        if val > self.max {
            self.max = val;
        }
    }

    pub fn merge(&mut self, other: &AggValue) {
        self.count += other.count;
        self.sum += other.sum;
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }
    }

    pub fn avg(&self) -> f64 {
        if self.count > 0 {
            self.sum / (self.count as f64)
        } else {
            0.0
        }
    }
}

pub struct Aggregate {
    pub group_by: Vec<String>,
    pub aggs: Vec<String>, // e.g., "count", "sum:col"
    pub spill_mgr: Option<Arc<Mutex<SpillManager>>>,
}

impl Default for Aggregate {
    fn default() -> Self {
        Self {
            group_by: Vec::new(),
            aggs: Vec::new(),
            spill_mgr: None,
        }
    }
}

impl Operator for Aggregate {
    fn name(&self) -> &'static str {
        "aggregate"
    }

    fn memory_need(&self, _rows: u64, _bytes: u64) -> Footprint {
        // Aggregations need overhead for hash tables.
        Footprint {
            bytes_per_row: 1,
            overhead_bytes: 128 * 1024,
        }
    }

    fn plan(&self, input_schemas: &[Schema]) -> Result<OpPlan, OpError> {
        let input_schema = input_schemas
            .get(0)
            .ok_or_else(|| OpError::Plan("aggregate expects one input".into()))?;

        // Build output schema: group_by columns + aggregation result columns
        let mut fields = Vec::new();

        for key in &self.group_by {
            let field = input_schema
                .fields
                .iter()
                .find(|f| &f.name == key)
                .ok_or_else(|| OpError::Plan(format!("group key '{}' not in input schema", key)))?;
            fields.push(field.clone());
        }

        // Add aggregation result columns
        for agg_str in &self.aggs {
            let agg_func = AggFunc::parse(agg_str)
                .map_err(|e| OpError::Plan(format!("invalid agg: {}", e)))?;
            fields.push(agg_func.output_field());
        }

        let schema = Schema::new(fields);
        Ok(OpPlan::new(schema, self.memory_need(0, 0)))
    }

    fn eval_block(
        &self,
        inputs: &[RowBatch],
        budget: &dyn MemoryBudget<Guard = BudgetGuardImpl>,
    ) -> Result<RowBatch, OpError> {
        let input = inputs
            .get(0)
            .ok_or_else(|| OpError::Exec("missing input".into()))?;

        // Parse aggregation functions
        let agg_funcs: Vec<AggFunc> = self
            .aggs
            .iter()
            .map(|s| AggFunc::parse(s).map_err(|e| OpError::Exec(e)))
            .collect::<Result<Vec<_>, _>>()?;

        // Simple case: no spill manager, do in-memory aggregation
        if self.spill_mgr.is_none() || self.group_by.len() != 1 {
            return self.simple_aggregate(input, &agg_funcs);
        }

        // Partitioned aggregation with spill support
        self.partitioned_aggregate(input, &agg_funcs, budget)
    }
}

impl Aggregate {
    /// Simple in-memory aggregation (no spill).
    fn simple_aggregate(
        &self,
        input: &RowBatch,
        agg_funcs: &[AggFunc],
    ) -> Result<RowBatch, OpError> {
        if self.group_by.is_empty() {
            return Err(OpError::Exec("group_by is empty".into()));
        }

        let key_col_name = &self.group_by[0];
        let key_col = input
            .columns
            .iter()
            .find(|c| &c.name == key_col_name)
            .ok_or_else(|| {
                OpError::Exec(format!("group key column '{}' not found", key_col_name))
            })?;

        // Build hash map: group key -> AggValue
        let mut groups: HashMap<String, AggValue> = HashMap::new();

        for row_idx in 0..input.num_rows() {
            let key_str = match &key_col.values[row_idx] {
                Scalar::Str(s) => s.clone(),
                Scalar::Null => "NULL".to_string(),
                other => format!("{:?}", other),
            };

            let agg = groups.entry(key_str).or_default();

            // Update aggregations
            for func in agg_funcs {
                match func {
                    AggFunc::Count => {} // Count is tracked in AggValue automatically
                    AggFunc::Sum { column }
                    | AggFunc::Min { column }
                    | AggFunc::Max { column }
                    | AggFunc::Avg { column } => {
                        let val_col = input
                            .columns
                            .iter()
                            .find(|c| &c.name == column)
                            .ok_or_else(|| OpError::Exec(format!("agg column '{}' not found", column)))?;

                        let val_f64 = match &val_col.values[row_idx] {
                            Scalar::I32(i) => *i as f64,
                            Scalar::I64(i) => *i as f64,
                            Scalar::F32(f) => *f as f64,
                            Scalar::F64(f) => *f,
                            _ => 0.0,
                        };

                        agg.update(val_f64);
                    }
                }
            }
        }

        // Convert hash map to output columns
        let mut output_cols = Vec::new();

        // Group key column
        let mut key_col_out = Column {
            name: key_col_name.clone(),
            values: Vec::with_capacity(groups.len()),
        };

        for key in groups.keys() {
            key_col_out.values.push(Scalar::Str(key.clone()));
        }
        output_cols.push(key_col_out);

        // Aggregation result columns
        for func in agg_funcs {
            let mut agg_col = Column {
                name: func.output_field().name,
                values: Vec::with_capacity(groups.len()),
            };

            for (key, agg_val) in &groups {
                let result = match func {
                    AggFunc::Count => Scalar::I64(agg_val.count as i64),
                    AggFunc::Sum { .. } => Scalar::F64(agg_val.sum),
                    AggFunc::Min { .. } => Scalar::F64(agg_val.min),
                    AggFunc::Max { .. } => Scalar::F64(agg_val.max),
                    AggFunc::Avg { .. } => Scalar::F64(agg_val.avg()),
                };
                agg_col.values.push(result);
            }

            output_cols.push(agg_col);
        }

        Ok(RowBatch {
            columns: output_cols,
        })
    }

    /// Partitioned aggregation with spill support (future enhancement).
    fn partitioned_aggregate(
        &self,
        input: &RowBatch,
        agg_funcs: &[AggFunc],
        _budget: &dyn MemoryBudget<Guard = BudgetGuardImpl>,
    ) -> Result<RowBatch, OpError> {
        // For now, fall back to simple aggregation
        // TODO: Implement partitioning, spill when hash table exceeds budget, merge phase
        self.simple_aggregate(input, agg_funcs)
    }
}
