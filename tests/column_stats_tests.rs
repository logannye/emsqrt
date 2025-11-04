//! Column statistics tests

use emsqrt_core::stats::{ColumnStats, SchemaStats};
use emsqrt_core::types::Scalar;

#[test]
fn test_column_stats_new() {
    let stats = ColumnStats::new();
    assert_eq!(stats.total_count, 0);
    assert_eq!(stats.null_count, 0);
    assert!(stats.min.is_none());
    assert!(stats.max.is_none());
    assert!(stats.distinct_count.is_none());
}

#[test]
fn test_column_stats_update_int() {
    let mut stats = ColumnStats::new();
    
    stats.update(&Scalar::I32(10));
    stats.update(&Scalar::I32(20));
    stats.update(&Scalar::I32(5));
    stats.update(&Scalar::I32(15));
    
    assert_eq!(stats.total_count, 4);
    assert_eq!(stats.null_count, 0);
    assert_eq!(stats.min, Some(Scalar::I32(5)));
    assert_eq!(stats.max, Some(Scalar::I32(20)));
}

#[test]
fn test_column_stats_update_with_null() {
    let mut stats = ColumnStats::new();
    
    stats.update(&Scalar::I32(10));
    stats.update(&Scalar::Null);
    stats.update(&Scalar::I32(20));
    
    assert_eq!(stats.total_count, 3);
    assert_eq!(stats.null_count, 1);
    assert_eq!(stats.min, Some(Scalar::I32(10)));
    assert_eq!(stats.max, Some(Scalar::I32(20)));
}

#[test]
fn test_column_stats_update_float() {
    let mut stats = ColumnStats::new();
    
    stats.update(&Scalar::F64(10.5));
    stats.update(&Scalar::F64(20.3));
    stats.update(&Scalar::F64(5.1));
    
    assert_eq!(stats.total_count, 3);
    assert_eq!(stats.non_null_count(), 3);
    assert!(matches!(stats.min, Some(Scalar::F64(v)) if (v - 5.1).abs() < 0.001));
    assert!(matches!(stats.max, Some(Scalar::F64(v)) if (v - 20.3).abs() < 0.001));
}

#[test]
fn test_column_stats_update_string() {
    let mut stats = ColumnStats::new();
    
    stats.update(&Scalar::Str("Alice".to_string()));
    stats.update(&Scalar::Str("Bob".to_string()));
    stats.update(&Scalar::Str("Charlie".to_string()));
    
    assert_eq!(stats.total_count, 3);
    assert_eq!(stats.min, Some(Scalar::Str("Alice".to_string())));
    assert_eq!(stats.max, Some(Scalar::Str("Charlie".to_string())));
}

#[test]
fn test_column_stats_merge() {
    let mut stats1 = ColumnStats::new();
    stats1.update(&Scalar::I32(10));
    stats1.update(&Scalar::I32(20));
    
    let mut stats2 = ColumnStats::new();
    stats2.update(&Scalar::I32(5));
    stats2.update(&Scalar::I32(25));
    
    let merged = stats1.merge(&stats2);
    
    assert_eq!(merged.total_count, 4);
    assert_eq!(merged.min, Some(Scalar::I32(5)));
    assert_eq!(merged.max, Some(Scalar::I32(25)));
}

#[test]
fn test_column_stats_merge_with_nulls() {
    let mut stats1 = ColumnStats::new();
    stats1.update(&Scalar::I32(10));
    stats1.update(&Scalar::Null);
    
    let mut stats2 = ColumnStats::new();
    stats2.update(&Scalar::I32(20));
    stats2.update(&Scalar::Null);
    
    let merged = stats1.merge(&stats2);
    
    assert_eq!(merged.total_count, 4);
    assert_eq!(merged.null_count, 2);
}

#[test]
fn test_column_stats_equality_selectivity() {
    let mut stats = ColumnStats::new();
    stats.distinct_count = Some(100);
    stats.total_count = 1000;
    
    let selectivity = stats.estimate_equality_selectivity();
    assert!((selectivity - 0.01).abs() < 0.001); // 1/100
}

#[test]
fn test_column_stats_equality_selectivity_no_distinct() {
    let mut stats = ColumnStats::new();
    stats.total_count = 1000;
    // distinct_count is None
    
    let selectivity = stats.estimate_equality_selectivity();
    assert!((selectivity - 0.01).abs() < 0.001); // Conservative estimate
}

#[test]
fn test_column_stats_range_selectivity() {
    let mut stats = ColumnStats::new();
    stats.min = Some(Scalar::I32(0));
    stats.max = Some(Scalar::I32(100));
    stats.total_count = 1000;
    
    // Range query: value >= 50
    let selectivity = stats.estimate_range_selectivity(Some(&Scalar::I32(50)), None);
    assert!(selectivity > 0.0 && selectivity <= 1.0);
}

#[test]
fn test_schema_stats_new() {
    let stats = SchemaStats::new();
    assert_eq!(stats.column_stats.len(), 0);
}

#[test]
fn test_schema_stats_get_or_create() {
    let mut stats = SchemaStats::new();
    let col_stats = stats.get_or_create("age".to_string());
    col_stats.update(&Scalar::I32(25));
    
    assert!(stats.get("age").is_some());
    assert_eq!(stats.get("age").unwrap().total_count, 1);
}

#[test]
fn test_schema_stats_merge() {
    let mut stats1 = SchemaStats::new();
    let col1 = stats1.get_or_create("age".to_string());
    col1.update(&Scalar::I32(25));
    
    let mut stats2 = SchemaStats::new();
    let col2 = stats2.get_or_create("age".to_string());
    col2.update(&Scalar::I32(30));
    
    let merged = stats1.merge(&stats2);
    assert_eq!(merged.get("age").unwrap().total_count, 2);
}

