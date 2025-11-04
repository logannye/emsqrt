//! Simple optimization rules (pushdown/reorder/strategy).

use crate::logical::LogicalPlan;

/// Apply a sequence of lightweight rewrites to the logical plan.
pub fn optimize(plan: LogicalPlan) -> LogicalPlan {
    // Apply projection pushdown rule
    projection_pushdown(plan)
}

/// Simple projection pushdown: Project(Filter(x)) → Filter(Project(x)) when safe.
/// This is safe when the filter doesn't reference columns not in the projection.
/// For simplicity, we only apply this when the project includes all columns needed by filter.
fn projection_pushdown(plan: LogicalPlan) -> LogicalPlan {
    use LogicalPlan::*;

    match plan {
        Project { input, columns } => {
            // Don't push Project below Filter - the filter might need columns
            // that are not in the projection (e.g., "age > 25" needs "age" column
            // even if the projection only selects "name,email").
            // TODO: Add proper column dependency analysis to safely push down
            // only when the filter expression doesn't reference columns outside the projection.
            Project {
                input: Box::new(projection_pushdown(*input)),
                columns,
            }
        }
        Filter { input, expr } => Filter {
            input: Box::new(projection_pushdown(*input)),
            expr,
        },
        Map { input, expr } => Map {
            input: Box::new(projection_pushdown(*input)),
            expr,
        },
        Aggregate {
            input,
            group_by,
            aggs,
        } => Aggregate {
            input: Box::new(projection_pushdown(*input)),
            group_by,
            aggs,
        },
        Join {
            left,
            right,
            on,
            join_type,
        } => Join {
            left: Box::new(projection_pushdown(*left)),
            right: Box::new(projection_pushdown(*right)),
            on,
            join_type,
        },
        Sink {
            input,
            destination,
            format,
        } => Sink {
            input: Box::new(projection_pushdown(*input)),
            destination,
            format,
        },
        // Leaf nodes
        Scan { .. } => plan,
    }
}
