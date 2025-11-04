//! Merge join operator tests

use emsqrt_core::dag::JoinType;
use emsqrt_core::schema::{DataType, Field, Schema};
use emsqrt_core::types::{Column, RowBatch, Scalar};
use emsqrt_mem::guard::MemoryBudgetImpl;
use emsqrt_operators::join::merge::MergeJoin;
use emsqrt_operators::traits::Operator;

fn create_sorted_left_batch() -> RowBatch {
    RowBatch {
        columns: vec![
            Column {
                name: "id".to_string(),
                values: vec![
                    Scalar::I32(1),
                    Scalar::I32(2),
                    Scalar::I32(3),
                    Scalar::I32(4),
                ],
            },
            Column {
                name: "name".to_string(),
                values: vec![
                    Scalar::Str("Alice".to_string()),
                    Scalar::Str("Bob".to_string()),
                    Scalar::Str("Charlie".to_string()),
                    Scalar::Str("David".to_string()),
                ],
            },
        ],
    }
}

fn create_sorted_right_batch() -> RowBatch {
    RowBatch {
        columns: vec![
            Column {
                name: "id".to_string(),
                values: vec![
                    Scalar::I32(2),
                    Scalar::I32(3),
                    Scalar::I32(5),
                ],
            },
            Column {
                name: "value".to_string(),
                values: vec![
                    Scalar::F64(10.5),
                    Scalar::F64(20.0),
                    Scalar::F64(30.0),
                ],
            },
        ],
    }
}

#[test]
fn test_merge_join_inner() {
    let mut join = MergeJoin::default();
    join.on = vec![("id".to_string(), "id".to_string())];
    join.join_type = "inner".to_string();
    
    let left = create_sorted_left_batch();
    let right = create_sorted_right_batch();
    
    let budget = MemoryBudgetImpl::new(10 * 1024 * 1024);
    let result = join.eval_block(&[left, right], &budget).unwrap();
    
    // Inner join should produce 2 rows (id=2 and id=3 match)
    assert_eq!(result.num_rows(), 2);
    assert_eq!(result.columns.len(), 4); // 2 from left + 2 from right
}

#[test]
fn test_merge_join_left() {
    let mut join = MergeJoin::default();
    join.on = vec![("id".to_string(), "id".to_string())];
    join.join_type = "left".to_string();
    
    let left = create_sorted_left_batch();
    let right = create_sorted_right_batch();
    
    let budget = MemoryBudgetImpl::new(10 * 1024 * 1024);
    let result = join.eval_block(&[left, right], &budget).unwrap();
    
    // Left join should produce 4 rows (all from left, with nulls for non-matching right)
    assert_eq!(result.num_rows(), 4);
}

#[test]
fn test_merge_join_right() {
    let mut join = MergeJoin::default();
    join.on = vec![("id".to_string(), "id".to_string())];
    join.join_type = "right".to_string();
    
    let left = create_sorted_left_batch();
    let right = create_sorted_right_batch();
    
    let budget = MemoryBudgetImpl::new(10 * 1024 * 1024);
    let result = join.eval_block(&[left, right], &budget).unwrap();
    
    // Right join should produce 3 rows (all from right, with nulls for non-matching left)
    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_merge_join_full() {
    let mut join = MergeJoin::default();
    join.on = vec![("id".to_string(), "id".to_string())];
    join.join_type = "full".to_string();
    
    let left = create_sorted_left_batch();
    let right = create_sorted_right_batch();
    
    let budget = MemoryBudgetImpl::new(10 * 1024 * 1024);
    let result = join.eval_block(&[left, right], &budget).unwrap();
    
    // Full join should produce all rows from both sides
    assert!(result.num_rows() >= 4);
}

#[test]
fn test_merge_join_duplicate_keys() {
    let left = RowBatch {
        columns: vec![
            Column {
                name: "id".to_string(),
                values: vec![Scalar::I32(1), Scalar::I32(1), Scalar::I32(2)],
            },
            Column {
                name: "name".to_string(),
                values: vec![
                    Scalar::Str("A".to_string()),
                    Scalar::Str("B".to_string()),
                    Scalar::Str("C".to_string()),
                ],
            },
        ],
    };
    
    let right = RowBatch {
        columns: vec![
            Column {
                name: "id".to_string(),
                values: vec![Scalar::I32(1), Scalar::I32(2)],
            },
            Column {
                name: "value".to_string(),
                values: vec![Scalar::F64(10.0), Scalar::F64(20.0)],
            },
        ],
    };
    
    let mut join = MergeJoin::default();
    join.on = vec![("id".to_string(), "id".to_string())];
    join.join_type = "inner".to_string();
    
    let budget = MemoryBudgetImpl::new(10 * 1024 * 1024);
    let result = join.eval_block(&[left, right], &budget).unwrap();
    
    // Should produce cartesian product: 2 left rows * 1 right row for id=1, plus 1 for id=2
    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_merge_join_empty_inputs() {
    // Create empty batches with proper schema structure
    let left = RowBatch {
        columns: vec![Column {
            name: "id".to_string(),
            values: vec![],
        }],
    };
    let right = RowBatch {
        columns: vec![Column {
            name: "id".to_string(),
            values: vec![],
        }],
    };
    
    let mut join = MergeJoin::default();
    join.on = vec![("id".to_string(), "id".to_string())];
    join.join_type = "inner".to_string();
    
    let budget = MemoryBudgetImpl::new(10 * 1024 * 1024);
    let result = join.eval_block(&[left, right], &budget).unwrap();
    
    assert_eq!(result.num_rows(), 0);
}

