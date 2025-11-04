//! Project operator (starter).
//!
//! Implements a simple column selection by name on the `RowBatch` placeholder.
//! TODOs:
//! - Handle renames/expressions.
//! - Switch to columnar (Arrow) arrays internally.

use emsqrt_core::prelude::{Field, Schema};
use emsqrt_core::types::{Column, RowBatch};

use crate::plan::{Footprint, OpPlan};
use crate::traits::{MemoryBudget, OpError, Operator};

#[derive(Default)]
pub struct Project {
    pub columns: Vec<String>,
}

impl Operator for Project {
    fn name(&self) -> &'static str {
        "project"
    }

    fn memory_need(&self, _rows: u64, _bytes: u64) -> Footprint {
        // Projection just forwards a subset of columns.
        Footprint {
            bytes_per_row: 1,
            overhead_bytes: 0,
        }
    }

    fn plan(&self, input_schemas: &[Schema]) -> Result<OpPlan, OpError> {
        let input = input_schemas
            .get(0)
            .ok_or_else(|| OpError::Plan("project expects one input".into()))?;
        if self.columns.is_empty() {
            return Ok(OpPlan::new(input.clone(), self.memory_need(0, 0)));
        }
        let mut fields = Vec::with_capacity(self.columns.len());
        for name in &self.columns {
            let idx = input
                .index_of(name)
                .ok_or_else(|| OpError::Schema(format!("unknown column '{name}'")))?;
            fields.push(input.fields[idx].clone());
        }
        Ok(OpPlan::new(Schema::new(fields), self.memory_need(0, 0)))
    }

    fn eval_block(
        &self,
        inputs: &[RowBatch],
        _budget: &dyn MemoryBudget<Guard = emsqrt_mem::guard::BudgetGuardImpl>,
    ) -> Result<RowBatch, OpError> {
        let input = inputs
            .get(0)
            .ok_or_else(|| OpError::Exec("missing input".into()))?;
        if self.columns.is_empty() {
            return Ok(input.clone());
        }
        // Build projected batch
        let mut out_cols: Vec<Column> = Vec::with_capacity(self.columns.len());
        for name in &self.columns {
            let idx = input
                .columns
                .iter()
                .position(|c| c.name == *name)
                .ok_or_else(|| OpError::Schema(format!("unknown column '{name}'")))?;
            out_cols.push(input.columns[idx].clone());
        }
        Ok(RowBatch { columns: out_cols })
    }
}
