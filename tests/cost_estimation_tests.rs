//! Cost estimation with statistics tests

use emsqrt_core::dag::{JoinType, LogicalPlan as L};
use emsqrt_core::schema::{DataType, Field, Schema};
use emsqrt_core::stats::{ColumnStats, SchemaStats};
use emsqrt_planner::{estimate_work, WorkHint};

fn create_schema_with_stats() -> Schema {
    let mut stats = SchemaStats::new();
    
    // Age column stats
    let mut age_stats = ColumnStats::new();
    age_stats.min = Some(emsqrt_core::types::Scalar::I32(18));
    age_stats.max = Some(emsqrt_core::types::Scalar::I32(65));
    age_stats.total_count = 1000;
    age_stats.distinct_count = Some(48);
    stats.column_stats.insert("age".to_string(), age_stats);
    
    // Status column stats
    let mut status_stats = ColumnStats::new();
    status_stats.total_count = 1000;
    status_stats.distinct_count = Some(3); // active, inactive, pending
    stats.column_stats.insert("status".to_string(), status_stats);
    
    Schema::new_with_stats(
        vec![
            Field::new("age", DataType::Int32, false),
            Field::new("status", DataType::Utf8, false),
        ],
        Some(stats),
    )
}

#[test]
fn test_filter_selectivity_with_stats() {
    let schema = create_schema_with_stats();
    let plan = L::Filter {
        input: Box::new(L::Scan {
            source: "test.csv".to_string(),
            schema,
        }),
        expr: "age > 30".to_string(),
    };
    
    let hints = WorkHint {
        source_rows: vec![("test.csv".to_string(), 1000)],
        source_bytes: vec![],
    };
    
    let work = estimate_work(&plan, Some(&hints));
    // Should use stats to estimate selectivity (should be less than 1000)
    assert!(work.total_rows > 0);
    assert!(work.total_rows < 1000); // Selectivity should reduce rows
}

#[test]
fn test_filter_equality_selectivity() {
    let schema = create_schema_with_stats();
    let plan = L::Filter {
        input: Box::new(L::Scan {
            source: "test.csv".to_string(),
            schema,
        }),
        expr: "status == \"active\"".to_string(),
    };
    
    let hints = WorkHint {
        source_rows: vec![("test.csv".to_string(), 1000)],
        source_bytes: vec![],
    };
    
    let work = estimate_work(&plan, Some(&hints));
    // Equality on status with distinct_count=3 should give ~1/3 selectivity
    assert!(work.total_rows > 0);
    assert!(work.total_rows < 1000); // Should be less than input
}

#[test]
fn test_join_cardinality_with_stats() {
    let schema1 = create_schema_with_stats();
    let schema2 = create_schema_with_stats();
    
    let plan = L::Join {
        left: Box::new(L::Scan {
            source: "left.csv".to_string(),
            schema: schema1,
        }),
        right: Box::new(L::Scan {
            source: "right.csv".to_string(),
            schema: schema2,
        }),
        on: vec![("age".to_string(), "age".to_string())],
        join_type: JoinType::Inner,
    };
    
    let hints = WorkHint {
        source_rows: vec![
            ("left.csv".to_string(), 100),
            ("right.csv".to_string(), 200),
        ],
        source_bytes: vec![],
    };
    
    let work = estimate_work(&plan, Some(&hints));
    // Should use distinct_count from stats to estimate join cardinality
    assert!(work.total_rows > 0);
    assert!(work.max_fan_in >= 2); // Join increases fan-in
}

#[test]
fn test_aggregate_groups_with_stats() {
    let schema = create_schema_with_stats();
    let plan = L::Aggregate {
        input: Box::new(L::Scan {
            source: "test.csv".to_string(),
            schema,
        }),
        group_by: vec!["status".to_string()],
        aggs: vec![emsqrt_core::dag::Aggregation::Count],
    };
    
    let hints = WorkHint {
        source_rows: vec![("test.csv".to_string(), 1000)],
        source_bytes: vec![],
    };
    
    let work = estimate_work(&plan, Some(&hints));
    // Should use distinct_count=3 for status to estimate ~3 groups
    assert!(work.total_rows > 0);
    assert!(work.total_rows <= 1000); // Should be less than input
}

#[test]
fn test_cost_estimation_without_stats() {
    // Plan without statistics should still work with heuristics
    let schema = Schema::new(vec![
        Field::new("age", DataType::Int32, false),
    ]);
    
    let plan = L::Filter {
        input: Box::new(L::Scan {
            source: "test.csv".to_string(),
            schema,
        }),
        expr: "age > 30".to_string(),
    };
    
    let hints = WorkHint {
        source_rows: vec![("test.csv".to_string(), 1000)],
        source_bytes: vec![],
    };
    
    let work = estimate_work(&plan, Some(&hints));
    // Should fall back to 50% selectivity heuristic
    assert_eq!(work.total_rows, 500); // 1000 * 0.5
}

#[test]
fn test_cost_estimation_with_hints() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
    ]);
    
    let plan = L::Scan {
        source: "test.csv".to_string(),
        schema,
    };
    
    let hints = WorkHint {
        source_rows: vec![("test.csv".to_string(), 5000)],
        source_bytes: vec![("test.csv".to_string(), 100000)],
    };
    
    let work = estimate_work(&plan, Some(&hints));
    assert_eq!(work.total_rows, 5000);
    assert_eq!(work.total_bytes, 100000);
}

