//! Column statistics for cost estimation and optimization.
//!
//! Tracks min, max, null_count, distinct_count, and total_count for columns.
//! Used by the planner for better cost estimation and selectivity modeling.

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

use crate::types::Scalar;

/// Statistics for a single column.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnStats {
    /// Minimum value observed (None if no non-null values)
    pub min: Option<Scalar>,
    /// Maximum value observed (None if no non-null values)
    pub max: Option<Scalar>,
    /// Number of null values
    pub null_count: u64,
    /// Estimated distinct count (None if not computed)
    pub distinct_count: Option<u64>,
    /// Total number of values (including nulls)
    pub total_count: u64,
}

impl ColumnStats {
    /// Create empty statistics.
    pub fn new() -> Self {
        Self {
            min: None,
            max: None,
            null_count: 0,
            distinct_count: None,
            total_count: 0,
        }
    }

    /// Update statistics with a new value.
    pub fn update(&mut self, value: &Scalar) {
        self.total_count += 1;
        
        match value {
            Scalar::Null => {
                self.null_count += 1;
            }
            val => {
                // Update min
                match &mut self.min {
                    Some(min) => {
                        if scalar_cmp(val, min).is_lt() {
                            *min = val.clone();
                        }
                    }
                    None => {
                        self.min = Some(val.clone());
                    }
                }
                
                // Update max
                match &mut self.max {
                    Some(max) => {
                        if scalar_cmp(val, max).is_gt() {
                            *max = val.clone();
                        }
                    }
                    None => {
                        self.max = Some(val.clone());
                    }
                }
            }
        }
    }

    /// Merge statistics from another ColumnStats into this one.
    ///
    /// Used when combining stats from multiple partitions or batches.
    pub fn merge(&self, other: &ColumnStats) -> ColumnStats {
        let merged = ColumnStats {
            min: match (&self.min, &other.min) {
                (Some(a), Some(b)) => {
                    if scalar_cmp(a, b).is_le() {
                        Some(a.clone())
                    } else {
                        Some(b.clone())
                    }
                }
                (Some(a), None) => Some(a.clone()),
                (None, Some(b)) => Some(b.clone()),
                (None, None) => None,
            },
            max: match (&self.max, &other.max) {
                (Some(a), Some(b)) => {
                    if scalar_cmp(a, b).is_ge() {
                        Some(a.clone())
                    } else {
                        Some(b.clone())
                    }
                }
                (Some(a), None) => Some(a.clone()),
                (None, Some(b)) => Some(b.clone()),
                (None, None) => None,
            },
            null_count: self.null_count + other.null_count,
            distinct_count: None, // Merging distinct counts is complex, set to None
            total_count: self.total_count + other.total_count,
        };

        merged
    }

    /// Get the number of non-null values.
    pub fn non_null_count(&self) -> u64 {
        self.total_count - self.null_count
    }

    /// Estimate selectivity for a range predicate (min <= value <= max).
    ///
    /// Returns a value between 0.0 and 1.0 representing the fraction of rows
    /// that would match the predicate.
    pub fn estimate_range_selectivity(&self, min_val: Option<&Scalar>, max_val: Option<&Scalar>) -> f64 {
        if self.non_null_count() == 0 {
            return 0.0;
        }

        // If we don't have min/max stats, assume uniform distribution
        if self.min.is_none() || self.max.is_none() {
            return 0.5; // Conservative estimate
        }

        // For now, use simple heuristics
        // TODO: Implement more sophisticated selectivity estimation
        match (min_val, max_val) {
            (Some(min), Some(max)) => {
                // Range predicate: estimate based on overlap
                if scalar_cmp(min, &self.max.as_ref().unwrap()).is_gt() ||
                   scalar_cmp(max, &self.min.as_ref().unwrap()).is_lt() {
                    return 0.0; // No overlap
                }
                // Simple estimate: assume uniform distribution
                0.3 // Conservative estimate
            }
            (Some(min), None) => {
                // value >= min
                if scalar_cmp(min, &self.max.as_ref().unwrap()).is_gt() {
                    return 0.0;
                }
                if scalar_cmp(min, &self.min.as_ref().unwrap()).is_le() {
                    return 1.0;
                }
                0.5 // Conservative estimate
            }
            (None, Some(max)) => {
                // value <= max
                if scalar_cmp(max, &self.min.as_ref().unwrap()).is_lt() {
                    return 0.0;
                }
                if scalar_cmp(max, &self.max.as_ref().unwrap()).is_ge() {
                    return 1.0;
                }
                0.5 // Conservative estimate
            }
            (None, None) => 1.0,
        }
    }

    /// Estimate selectivity for an equality predicate.
    ///
    /// Uses distinct_count if available, otherwise returns a conservative estimate.
    pub fn estimate_equality_selectivity(&self) -> f64 {
        if self.non_null_count() == 0 {
            return 0.0;
        }

        if let Some(distinct) = self.distinct_count {
            if distinct > 0 {
                return 1.0 / (distinct as f64);
            }
        }

        // Conservative estimate: assume high cardinality
        0.01
    }
}

impl Default for ColumnStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics for all columns in a schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaStats {
    /// Map from column name to its statistics
    pub column_stats: HashMap<String, ColumnStats>,
}

impl SchemaStats {
    /// Create empty schema statistics.
    pub fn new() -> Self {
        Self {
            column_stats: HashMap::new(),
        }
    }

    /// Get statistics for a column by name.
    pub fn get(&self, column_name: &str) -> Option<&ColumnStats> {
        self.column_stats.get(column_name)
    }

    /// Get or create statistics for a column.
    pub fn get_or_create(&mut self, column_name: String) -> &mut ColumnStats {
        self.column_stats.entry(column_name).or_insert_with(ColumnStats::new)
    }

    /// Merge statistics from another SchemaStats into this one.
    pub fn merge(&self, other: &SchemaStats) -> SchemaStats {
        let mut merged = SchemaStats::new();
        
        // Merge stats for all columns present in either schema
        let mut all_columns: Vec<String> = self.column_stats.keys()
            .chain(other.column_stats.keys())
            .cloned()
            .collect();
        all_columns.sort();
        all_columns.dedup();

        for col_name in all_columns {
            let stats = match (self.column_stats.get(&col_name), other.column_stats.get(&col_name)) {
                (Some(a), Some(b)) => a.merge(b),
                (Some(a), None) => a.clone(),
                (None, Some(b)) => b.clone(),
                (None, None) => continue,
            };
            merged.column_stats.insert(col_name, stats);
        }

        merged
    }
}

impl Default for SchemaStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Compare two scalars for ordering (helper for stats).
fn scalar_cmp(a: &Scalar, b: &Scalar) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    use Scalar::*;
    
    match (a, b) {
        (Null, Null) => Ordering::Equal,
        (Null, _) => Ordering::Less,
        (_, Null) => Ordering::Greater,
        (Bool(x), Bool(y)) => x.cmp(y),
        (I32(x), I32(y)) => x.cmp(y),
        (I64(x), I64(y)) => x.cmp(y),
        (F32(x), F32(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (F64(x), F64(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Str(x), Str(y)) => x.cmp(y),
        (Bin(x), Bin(y)) => x.cmp(y),
        _ => {
            // Mixed types: compare by type discriminant
            let a_order = scalar_type_order(a);
            let b_order = scalar_type_order(b);
            a_order.cmp(&b_order)
        }
    }
}

/// Get type order for scalar (for mixed-type comparisons).
fn scalar_type_order(s: &Scalar) -> u8 {
    use Scalar::*;
    match s {
        Null => 0,
        Bool(_) => 1,
        I32(_) => 2,
        I64(_) => 3,
        F32(_) => 4,
        F64(_) => 5,
        Str(_) => 6,
        Bin(_) => 7,
    }
}

