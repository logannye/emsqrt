//! Coarse work estimation for TE planning.
//!
//! We compute a very rough `WorkEstimate` by walking the logical plan. In real
//! deployments, this should be informed by stats (filesize, row count) and/or
//! operator-specific models.
//!
//! Now enhanced with column statistics for better selectivity estimation.

use emsqrt_core::dag::LogicalPlan;
use emsqrt_core::schema::Schema;
use emsqrt_te::WorkEstimate;
use serde::{Deserialize, Serialize};

/// Optional hints you can pass in when estimating work.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkHint {
    /// Rows at sources (if known); map by source URI.
    pub source_rows: Vec<(String, u64)>,
    /// Bytes at sources (if known); map by source URI.
    pub source_bytes: Vec<(String, u64)>,
}

pub fn estimate_work(plan: &LogicalPlan, hints: Option<&WorkHint>) -> WorkEstimate {
    let mut total_rows = 0u64;
    let mut total_bytes = 0u64;
    let mut max_fan_in = 1u32;

    fn schema_size_bytes(_schema: &Schema) -> u64 {
        // TODO: derive from field types; placeholder per-row byte guess
        1
    }

    fn walk(
        lp: &LogicalPlan,
        hints: Option<&WorkHint>,
        acc_rows: &mut u64,
        acc_bytes: &mut u64,
        max_fan_in: &mut u32,
    ) -> u64 {
        use LogicalPlan::*;
        match lp {
            Scan { source, schema } => {
                // Use hints if available; otherwise guess 0 (unknown).
                let rows = hints
                    .and_then(|h| h.source_rows.iter().find(|(s, _)| s == source))
                    .map(|(_, r)| *r)
                    .unwrap_or(0);

                let bytes = hints
                    .and_then(|h| h.source_bytes.iter().find(|(s, _)| s == source))
                    .map(|(_, b)| *b)
                    .unwrap_or(rows * schema_size_bytes(schema));

                *acc_rows += rows;
                *acc_bytes += bytes;
                rows
            }
            Filter { input, expr } => {
                let in_rows = walk(input, hints, acc_rows, acc_bytes, max_fan_in);
                
                // Try to estimate selectivity using statistics
                let selectivity = estimate_filter_selectivity(expr, input);
                let out_rows = ((in_rows as f64) * selectivity) as u64;
                out_rows.max(1)
            }
            Map { input, .. } | Project { input, .. } => {
                walk(input, hints, acc_rows, acc_bytes, max_fan_in)
            }
            Join { left, right, on, .. } => {
                *max_fan_in = (*max_fan_in).max(2);
                let l = walk(left, hints, acc_rows, acc_bytes, max_fan_in);
                let r = walk(right, hints, acc_rows, acc_bytes, max_fan_in);
                
                // Try to estimate join cardinality using statistics
                let join_card = estimate_join_cardinality(left, right, on, l, r);
                join_card.max(1)
            }
            Aggregate { input, group_by, .. } => {
                let in_rows = walk(input, hints, acc_rows, acc_bytes, max_fan_in);
                
                // Try to estimate groups using statistics
                let groups = estimate_aggregate_groups(input, group_by, in_rows);
                groups.max(1)
            }
            Sink { input, .. } => walk(input, hints, acc_rows, acc_bytes, max_fan_in),
        }
    }

    let _rows_out = walk(
        plan,
        hints,
        &mut total_rows,
        &mut total_bytes,
        &mut max_fan_in,
    );
    WorkEstimate {
        total_rows,
        total_bytes,
        max_fan_in,
    }
}

/// Estimate filter selectivity (fraction of rows that pass the filter).
///
/// Uses column statistics if available, otherwise falls back to heuristics.
fn estimate_filter_selectivity(expr: &str, input_plan: &LogicalPlan) -> f64 {
    // Simple heuristic: try to parse the expression and use stats if available
    // For now, parse simple predicates like "col OP literal"
    let ops = ["==", "!=", "<=", ">=", "<", ">"];
    
    for op in &ops {
        if let Some(pos) = expr.find(op) {
            let col_name = expr[..pos].trim();
            let literal_str = expr[pos + op.len()..].trim();
            
            // Try to get schema from input plan
            if let Some(schema) = get_schema_from_plan(input_plan) {
                if let Some(stats_opt) = &schema.stats {
                    if let Some(col_stats) = stats_opt.get(col_name) {
                        match *op {
                            "==" => return col_stats.estimate_equality_selectivity(),
                            "!=" => return 1.0 - col_stats.estimate_equality_selectivity(),
                            "<" | "<=" | ">" | ">=" => {
                                // Try to parse literal as Scalar for range estimation
                                if let Ok(scalar) = parse_literal_as_scalar(literal_str) {
                                    let (min_val, max_val) = match *op {
                                        "<" => (None, Some(&scalar)),
                                        "<=" => (None, Some(&scalar)),
                                        ">" => (Some(&scalar), None),
                                        ">=" => (Some(&scalar), None),
                                        _ => (None, None),
                                    };
                                    return col_stats.estimate_range_selectivity(min_val, max_val);
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }
    
    // Fallback: conservative 50% selectivity
    0.5
}

/// Estimate join cardinality (number of output rows).
///
/// Uses distinct_count from statistics if available, otherwise uses heuristics.
fn estimate_join_cardinality(
    left_plan: &LogicalPlan,
    right_plan: &LogicalPlan,
    on: &[(String, String)],
    left_rows: u64,
    right_rows: u64,
) -> u64 {
    // Get schemas from plans
    let left_schema = get_schema_from_plan(left_plan);
    let right_schema = get_schema_from_plan(right_plan);
    
    // Try to use distinct_count from statistics
    if let (Some(left_schema), Some(right_schema)) = (left_schema, right_schema) {
        if let (Some(left_stats), Some(right_stats)) = (&left_schema.stats, &right_schema.stats) {
            if let Some((left_col, right_col)) = on.first() {
                if let (Some(left_col_stats), Some(right_col_stats)) = 
                    (left_stats.get(left_col), right_stats.get(right_col)) {
                    
                    // Use distinct_count if available
                    let left_distinct = left_col_stats.distinct_count.unwrap_or(left_rows);
                    let right_distinct = right_col_stats.distinct_count.unwrap_or(right_rows);
                    
                    // Estimate: rows * rows / max(distinct_left, distinct_right)
                    // This is a simplified model assuming uniform distribution
                    let max_distinct = left_distinct.max(right_distinct);
                    if max_distinct > 0 {
                        return (left_rows * right_rows / max_distinct).min(left_rows * right_rows);
                    }
                }
            }
        }
    }
    
    // Fallback: conservative estimate (min of left and right)
    left_rows.min(right_rows)
}

/// Estimate number of groups for an aggregate operation.
///
/// Uses distinct_count from statistics if available.
fn estimate_aggregate_groups(
    input_plan: &LogicalPlan,
    group_by: &[String],
    input_rows: u64,
) -> u64 {
    if group_by.is_empty() {
        return 1; // No grouping, single aggregate row
    }
    
    // Get schema from input plan
    if let Some(schema) = get_schema_from_plan(input_plan) {
        if let Some(stats) = &schema.stats {
            // Estimate groups using distinct_count of group_by columns
            let mut estimated_groups = 1u64;
            
            for col_name in group_by {
                if let Some(col_stats) = stats.get(col_name) {
                    if let Some(distinct) = col_stats.distinct_count {
                        estimated_groups *= distinct.max(1);
                        // Cap at input_rows to avoid overestimation
                        estimated_groups = estimated_groups.min(input_rows);
                    }
                }
            }
            
            if estimated_groups > 1 {
                return estimated_groups.min(input_rows);
            }
        }
    }
    
    // Fallback: assume 10% reduction (conservative)
    (input_rows / 10).max(1)
}

/// Helper to extract schema from a LogicalPlan.
fn get_schema_from_plan(plan: &LogicalPlan) -> Option<&Schema> {
    use LogicalPlan::*;
    match plan {
        Scan { schema, .. } => Some(schema),
        Filter { input, .. } => get_schema_from_plan(input),
        Map { input, .. } | Project { input, .. } => get_schema_from_plan(input),
        Join { left, .. } => get_schema_from_plan(left), // Use left schema as approximation
        Aggregate { input, .. } => get_schema_from_plan(input),
        Sink { input, .. } => get_schema_from_plan(input),
    }
}

/// Parse a literal string as a Scalar value.
fn parse_literal_as_scalar(literal: &str) -> Result<emsqrt_core::types::Scalar, String> {
    use emsqrt_core::types::Scalar;
    
    // Try to parse as different types
    if let Ok(i) = literal.parse::<i32>() {
        return Ok(Scalar::I32(i));
    }
    if let Ok(i) = literal.parse::<i64>() {
        return Ok(Scalar::I64(i));
    }
    if let Ok(f) = literal.parse::<f32>() {
        return Ok(Scalar::F32(f));
    }
    if let Ok(f) = literal.parse::<f64>() {
        return Ok(Scalar::F64(f));
    }
    if let Ok(b) = literal.parse::<bool>() {
        return Ok(Scalar::Bool(b));
    }
    
    // Try removing quotes for strings
    let trimmed = literal.trim();
    if (trimmed.starts_with('"') && trimmed.ends_with('"')) ||
       (trimmed.starts_with('\'') && trimmed.ends_with('\'')) {
        let unquoted = &trimmed[1..trimmed.len()-1];
        return Ok(Scalar::Str(unquoted.to_string()));
    }
    
    // Default to string
    Ok(Scalar::Str(literal.to_string()))
}
