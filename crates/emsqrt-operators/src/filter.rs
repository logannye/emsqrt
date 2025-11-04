//! Filter operator with predicate evaluation.
//!
//! Supports SQL-like expressions using the expression engine.
//! Simple predicates: "col OP literal" where OP ∈ {==, !=, <, <=, >, >=}
//! Complex predicates: "col1 > 10 AND col2 == 'active'"
//!
//! When the `arrow` feature is enabled, uses Arrow compute kernels for better performance.

#[cfg(feature = "arrow")]
use arrow_array::{Array, BooleanArray, RecordBatch};
#[cfg(feature = "arrow")]
use arrow_schema::SchemaRef;
#[cfg(feature = "arrow")]
use std::sync::Arc;

use emsqrt_core::expr::Expr;
use emsqrt_core::prelude::Schema;
use emsqrt_core::types::{Column, RowBatch, Scalar};

use crate::plan::{Footprint, OpPlan};
use crate::traits::{MemoryBudget, OpError, Operator};

#[derive(Default)]
pub struct Filter {
    /// Predicate expression string (parsed into Expr on demand)
    pub expr: Option<String>,
}

impl Operator for Filter {
    fn name(&self) -> &'static str {
        "filter"
    }

    fn memory_need(&self, _rows: u64, _bytes: u64) -> Footprint {
        // Filtering is streaming and should be close to input size.
        Footprint {
            bytes_per_row: 1,
            overhead_bytes: 0,
        }
    }

    fn plan(&self, input_schemas: &[Schema]) -> Result<OpPlan, OpError> {
        let schema = input_schemas
            .get(0)
            .ok_or_else(|| OpError::Plan("filter expects one input".into()))?
            .clone();
        Ok(OpPlan::new(schema, self.memory_need(0, 0)))
    }

    fn eval_block(
        &self,
        inputs: &[RowBatch],
        _budget: &dyn MemoryBudget<Guard = emsqrt_mem::guard::BudgetGuardImpl>,
    ) -> Result<RowBatch, OpError> {
        let input = inputs
            .get(0)
            .ok_or_else(|| OpError::Exec("missing input".into()))?;

        // If no expression, pass through
        let Some(ref expr_str) = self.expr else {
            return Ok(input.clone());
        };

        // Parse expression string into Expr AST
        let expr = Expr::parse(expr_str)
            .map_err(|e| OpError::Exec(format!("failed to parse expression '{}': {}", expr_str, e)))?;

        // Evaluate expression for each row
        let num_rows = input.num_rows();
        let mut keep = Vec::with_capacity(num_rows);
        
        for row_idx in 0..num_rows {
            match expr.evaluate_bool(input, row_idx) {
                Ok(b) => keep.push(b),
                Err(e) => {
                    // If evaluation fails, return error instead of silently filtering
                    // This helps catch bugs during development
                    return Err(OpError::Exec(format!(
                        "expression evaluation failed at row {}: {}",
                        row_idx, e
                    )));
                }
            }
        }

        // Filter all columns
        let mut filtered_cols = Vec::new();
        for input_col in &input.columns {
            let mut new_col = Column {
                name: input_col.name.clone(),
                values: Vec::new(),
            };
            for (i, val) in input_col.values.iter().enumerate() {
                if i < keep.len() && keep[i] {
                    new_col.values.push(val.clone());
                }
            }
            filtered_cols.push(new_col);
        }

        Ok(RowBatch {
            columns: filtered_cols,
        })
    }
}

