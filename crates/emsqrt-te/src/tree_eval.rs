//! TE block plan and naive order builder.
//!
//! The actual TE transformation (bounded fan-in decomposition) will live here.
//! For now, we create a placeholder linear order and carry deps so the engine
//! can execute deterministically and tests can be written.
//!
//! TODOs:
//! - Transform PhysicalPlan into a TE DAG with bounded fan-in (block decomposition).
//! - Use `BlockSizeHint` to cut streams into approximately equal row/byte blocks.
//! - Emit dependency edges to ensure correctness and bounded frontier.

use emsqrt_core::dag::PhysicalPlan;
use emsqrt_core::id::{BlockId, OpId};
use emsqrt_core::prelude::Schema;
use serde::{Deserialize, Serialize};

use crate::cost::WorkEstimate;
use crate::schedule::{choose_block_size, BlockSizeHint};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeBlock {
    /// Unique block identifier.
    pub id: BlockId,
    /// Owning operator (for execution routing in the engine).
    pub op: OpId,
    /// Logical schema (useful for verification/instrumentation).
    pub schema: Schema,
    /// Upstream block dependencies (bounded fan-in in real TE).
    pub deps: Vec<BlockId>,
    /// Optional [start,end) row offsets (planner-supplied / estimated).
    pub range_rows: Option<(u64, u64)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TePlan {
    /// Chosen block size hint used by this plan.
    pub block_size: BlockSizeHint,
    /// Blocks in an order that respects dependencies (topological).
    pub order: Vec<TeBlock>,
    /// Optional frontier bound computed for this order (advisory).
    pub max_frontier_hint: Option<usize>,
}

impl TePlan {
    pub fn empty() -> Self {
        Self {
            block_size: BlockSizeHint { rows_per_block: 1 },
            order: Vec::new(),
            max_frontier_hint: None,
        }
    }
}

/// Multi-block TE planner with bounded fan-in.
///
/// Each PhysicalPlan node is decomposed into multiple blocks based on row count estimates.
/// - For unary nodes: chunks pipeline 1-to-1 (chunk i depends on input chunk i)
/// - For binary nodes: chunks are aligned (join chunk i depends on left[i] and right[i])
/// - Each block gets a monotonic row range hint.
pub fn plan_te(
    phys: &PhysicalPlan,
    est: &WorkEstimate,
    mem_cap_bytes: usize,
) -> Result<TePlan, PlanError> {
    let b = choose_block_size(mem_cap_bytes, est);
    let mut order = Vec::<TeBlock>::new();
    let mut next_block_id = 0u64;

    // Helper structure to track which blocks were created for each node
    struct BlockRange {
        blocks: Vec<BlockId>,
        estimated_rows: u64,
    }

    fn walk(
        node: &PhysicalPlan,
        order: &mut Vec<TeBlock>,
        next_block_id: &mut u64,
        rows_per_block: u64,
        est: &WorkEstimate,
    ) -> Result<BlockRange, PlanError> {
        use PhysicalPlan::*;
        match node {
            Source { op, schema } => {
                // Estimate: use total_rows from work estimate divided by number of sources
                // For now, assume single source gets all rows
                let estimated_rows = est.total_rows.max(rows_per_block);
                let num_blocks = ((estimated_rows + rows_per_block - 1) / rows_per_block).max(1);

                let mut blocks = Vec::new();
                for i in 0..num_blocks {
                    let start = i * rows_per_block;
                    let end = ((i + 1) * rows_per_block).min(estimated_rows);

                    let id = BlockId::new(*next_block_id);
                    *next_block_id += 1;

                    order.push(TeBlock {
                        id,
                        op: *op,
                        schema: schema.clone(),
                        deps: vec![],
                        range_rows: Some((start, end)),
                    });
                    blocks.push(id);
                }

                Ok(BlockRange {
                    blocks,
                    estimated_rows,
                })
            }
            Unary { op, input, schema } => {
                let child_range = walk(input, order, next_block_id, rows_per_block, est)?;

                // Create same number of blocks as input (1-to-1 pipeline)
                let estimated_rows = child_range.estimated_rows; // Pass through for unary

                let mut blocks = Vec::new();
                for (i, &input_block) in child_range.blocks.iter().enumerate() {
                    let start = (i as u64) * rows_per_block;
                    let end = ((i as u64 + 1) * rows_per_block).min(estimated_rows);

                    let id = BlockId::new(*next_block_id);
                    *next_block_id += 1;

                    order.push(TeBlock {
                        id,
                        op: *op,
                        schema: schema.clone(),
                        deps: vec![input_block],
                        range_rows: Some((start, end)),
                    });
                    blocks.push(id);
                }

                Ok(BlockRange {
                    blocks,
                    estimated_rows,
                })
            }
            Binary {
                op,
                left,
                right,
                schema,
            } => {
                let left_range = walk(left, order, next_block_id, rows_per_block, est)?;
                let right_range = walk(right, order, next_block_id, rows_per_block, est)?;

                // Align chunks: create blocks matching the max of left/right block counts
                // For simplicity, each join block depends on corresponding left/right blocks
                let num_blocks = left_range.blocks.len().max(right_range.blocks.len());
                let estimated_rows = left_range.estimated_rows.max(right_range.estimated_rows);

                let mut blocks = Vec::new();
                for i in 0..num_blocks {
                    let start = (i as u64) * rows_per_block;
                    let end = ((i as u64 + 1) * rows_per_block).min(estimated_rows);

                    let id = BlockId::new(*next_block_id);
                    *next_block_id += 1;

                    // Depend on corresponding blocks from left and right
                    let mut deps = Vec::new();
                    if i < left_range.blocks.len() {
                        deps.push(left_range.blocks[i]);
                    }
                    if i < right_range.blocks.len() {
                        deps.push(right_range.blocks[i]);
                    }

                    order.push(TeBlock {
                        id,
                        op: *op,
                        schema: schema.clone(),
                        deps,
                        range_rows: Some((start, end)),
                    });
                    blocks.push(id);
                }

                Ok(BlockRange {
                    blocks,
                    estimated_rows,
                })
            }
            Sink { op, input } => {
                let child_range = walk(input, order, next_block_id, rows_per_block, est)?;

                // Sink typically processes each input block (1-to-1)
                let mut blocks = Vec::new();
                for (i, &input_block) in child_range.blocks.iter().enumerate() {
                    let start = (i as u64) * rows_per_block;
                    let end = ((i as u64 + 1) * rows_per_block).min(child_range.estimated_rows);

                    let id = BlockId::new(*next_block_id);
                    *next_block_id += 1;

                    order.push(TeBlock {
                        id,
                        op: *op,
                        schema: Schema::new(vec![]), // sinks don't produce rows
                        deps: vec![input_block],
                        range_rows: Some((start, end)),
                    });
                    blocks.push(id);
                }

                Ok(BlockRange {
                    blocks,
                    estimated_rows: child_range.estimated_rows,
                })
            }
        }
    }

    let _ = walk(phys, &mut order, &mut next_block_id, b.rows_per_block, est)?;

    // Compute frontier bound using the new compute_max_frontier helper
    use crate::frontier::compute_max_frontier;
    let order_with_deps: Vec<(BlockId, Vec<BlockId>)> = order
        .iter()
        .map(|block| (block.id, block.deps.clone()))
        .collect();
    let max_frontier_hint = Some(compute_max_frontier(&order_with_deps));

    Ok(TePlan {
        block_size: b,
        order,
        max_frontier_hint,
    })
}

/// Planning errors local to TE; map to core::Error in the executor if needed.
#[derive(thiserror::Error, Debug)]
pub enum PlanError {
    #[error("Invalid PhysicalPlan structure: {0}")]
    InvalidPlan(String),
}
