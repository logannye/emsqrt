//! Lowering from (optimized) logical plan to a physical program:
//! - assign stable `OpId`s
//! - build a `PhysicalPlan` tree
//! - emit bindings `OpId → {key, config}`
//!
//! The actual operator instances will be constructed later by the exec
//! using `emsqrt-operators`' registry and the `key` + `config` here.

use std::collections::BTreeMap;

use emsqrt_core::dag::{LogicalPlan, PhysicalPlan};
use emsqrt_core::id::OpId;
use emsqrt_core::schema::Schema;

use crate::physical::{OperatorBinding, PhysicalProgram};

/// Lower a logical plan into a `PhysicalProgram`.
/// Strategy:
/// - Assign an OpId per node.
/// - Pick a default operator key based on node kind (e.g., "filter").
/// - Propagate schemas in a simplistic way (filter/map preserve; join uses left).
pub fn lower_to_physical(lp: &LogicalPlan) -> PhysicalProgram {
    let mut next_id = 1u64;
    let mut bindings = BTreeMap::<OpId, OperatorBinding>::new();

    fn alloc_id(next_id: &mut u64) -> OpId {
        let id = OpId::new(*next_id);
        *next_id += 1;
        id
    }

    fn schema_of(lp: &LogicalPlan) -> Schema {
        use LogicalPlan::*;
        match lp {
            Scan { schema, .. } => schema.clone(),
            Filter { input, .. }
            | Map { input, .. }
            | Project { input, .. }
            | Aggregate { input, .. }
            | Sink { input, .. } => schema_of(input),
            Join { left, .. } => schema_of(left), // TODO: real join schema
        }
    }

    fn lower_rec(
        lp: &LogicalPlan,
        next_id: &mut u64,
        bindings: &mut BTreeMap<OpId, OperatorBinding>,
    ) -> PhysicalPlan {
        use LogicalPlan::*;
        match lp {
            Scan { source, schema } => {
                let op = alloc_id(next_id);
                bindings.insert(
                    op,
                    OperatorBinding {
                        key: "source".to_string(),
                        config: serde_json::json!({
                            "source": source,
                            "schema": serde_json::to_value(&schema).unwrap_or(serde_json::json!({}))
                        }),
                    },
                );
                PhysicalPlan::Source {
                    op,
                    schema: schema.clone(),
                }
            }
            Filter { input, expr } => {
                let child = lower_rec(input, next_id, bindings);
                let op = alloc_id(next_id);
                bindings.insert(
                    op,
                    OperatorBinding {
                        key: "filter".to_string(),
                        config: serde_json::json!({ "expr": expr }),
                    },
                );
                PhysicalPlan::Unary {
                    op,
                    input: Box::new(child),
                    schema: schema_of(lp),
                }
            }
            Map { input, expr } => {
                let child = lower_rec(input, next_id, bindings);
                let op = alloc_id(next_id);
                bindings.insert(
                    op,
                    OperatorBinding {
                        key: "map".to_string(),
                        config: serde_json::json!({ "expr": expr }),
                    },
                );
                PhysicalPlan::Unary {
                    op,
                    input: Box::new(child),
                    schema: schema_of(lp),
                }
            }
            Project { input, columns } => {
                let child = lower_rec(input, next_id, bindings);
                let op = alloc_id(next_id);
                bindings.insert(
                    op,
                    OperatorBinding {
                        key: "project".to_string(),
                        config: serde_json::json!({ "columns": columns }),
                    },
                );
                PhysicalPlan::Unary {
                    op,
                    input: Box::new(child),
                    schema: schema_of(lp),
                }
            }
            Aggregate { input, group_by, aggs } => {
                let child = lower_rec(input, next_id, bindings);
                let op = alloc_id(next_id);
                
                // Serialize aggs to strings (format expected by Aggregate::parse)
                let aggs_str: Vec<String> = aggs.iter().map(|a| {
                    match a {
                        emsqrt_core::dag::Aggregation::Count => "count".to_string(),
                        emsqrt_core::dag::Aggregation::Sum(col) => format!("sum:{}", col),
                        emsqrt_core::dag::Aggregation::Avg(col) => format!("avg:{}", col),
                        emsqrt_core::dag::Aggregation::Min(col) => format!("min:{}", col),
                        emsqrt_core::dag::Aggregation::Max(col) => format!("max:{}", col),
                    }
                }).collect();
                
                bindings.insert(
                    op,
                    OperatorBinding {
                        key: "aggregate".to_string(),
                        config: serde_json::json!({
                            "group_by": group_by,
                            "aggs": aggs_str
                        }),
                    },
                );
                PhysicalPlan::Unary {
                    op,
                    input: Box::new(child),
                    schema: schema_of(lp),
                }
            }
            Join { left, right, .. } => {
                let l = lower_rec(left, next_id, bindings);
                let r = lower_rec(right, next_id, bindings);
                let op = alloc_id(next_id);
                bindings.insert(
                    op,
                    OperatorBinding {
                        key: "join_hash".to_string(), // default to hash join; rules may switch to merge later
                        config: serde_json::json!({}),
                    },
                );
                PhysicalPlan::Binary {
                    op,
                    left: Box::new(l),
                    right: Box::new(r),
                    schema: schema_of(lp),
                }
            }
            Sink { input, destination, format } => {
                let child = lower_rec(input, next_id, bindings);
                let op = alloc_id(next_id);
                bindings.insert(
                    op,
                    OperatorBinding {
                        key: "sink".to_string(),
                        config: serde_json::json!({
                            "destination": destination,
                            "format": format
                        }),
                    },
                );
                PhysicalPlan::Sink {
                    op,
                    input: Box::new(child),
                }
            }
        }
    }

    let plan = lower_rec(lp, &mut next_id, &mut bindings);
    PhysicalProgram::new(plan, bindings)
}
