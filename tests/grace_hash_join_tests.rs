//! Tests for Grace hash join implementation

mod test_data_gen;

use emsqrt_core::config::EngineConfig;
use emsqrt_core::schema::{DataType, Field, Schema};
use emsqrt_core::types::{Column, RowBatch, Scalar};
use emsqrt_mem::spill::{Codec, SpillManager};
use emsqrt_operators::join::hash::HashJoin;
use emsqrt_operators::traits::Operator;
use std::sync::{Arc, Mutex};
use test_data_gen::create_temp_spill_dir;
use emsqrt_io::storage::FsStorage;
use emsqrt_mem::guard::MemoryBudgetImpl;

fn create_left_batch() -> RowBatch {
    RowBatch {
        columns: vec![
            Column {
                name: "id".to_string(),
                values: vec![
                    Scalar::I32(1),
                    Scalar::I32(2),
                    Scalar::I32(3),
                    Scalar::I32(4),
                    Scalar::I32(5),
                ],
            },
            Column {
                name: "name".to_string(),
                values: vec![
                    Scalar::Str("Alice".to_string()),
                    Scalar::Str("Bob".to_string()),
                    Scalar::Str("Charlie".to_string()),
                    Scalar::Str("David".to_string()),
                    Scalar::Str("Eve".to_string()),
                ],
            },
        ],
    }
}

fn create_right_batch() -> RowBatch {
    RowBatch {
        columns: vec![
            Column {
                name: "id".to_string(),
                values: vec![
                    Scalar::I32(2),
                    Scalar::I32(4),
                    Scalar::I32(6),
                    Scalar::I32(8),
                ],
            },
            Column {
                name: "score".to_string(),
                values: vec![
                    Scalar::F64(95.0),
                    Scalar::F64(87.0),
                    Scalar::F64(92.0),
                    Scalar::F64(78.0),
                ],
            },
        ],
    }
}

#[test]
fn test_simple_hash_join_fallback() {
    // Small inputs should use simple hash join
    let mut join = HashJoin::default();
    join.on = vec![("id".to_string(), "id".to_string())];
    join.join_type = "inner".to_string();
    
    let left = create_left_batch();
    let right = create_right_batch();
    
    // Create a budget
    let config = EngineConfig::default();
    let budget = MemoryBudgetImpl::new(config.mem_cap_bytes);
    
    // Without spill manager, should use simple join
    let result = join.eval_block(&[left.clone(), right.clone()], &budget)
        .expect("Join should succeed");
    
    // Verify inner join results (id=2 and id=4 match)
    assert_eq!(result.num_rows(), 2);
    assert_eq!(result.columns.len(), 4); // id (left), name, id_right, score
    
    // Check that id=2 and id=4 are in results
    let id_col = &result.columns[0];
    let ids: Vec<i32> = id_col.values.iter()
        .filter_map(|v| {
            if let Scalar::I32(x) = v {
                Some(*x)
            } else {
                None
            }
        })
        .collect();
    assert!(ids.contains(&2));
    assert!(ids.contains(&4));
}

#[test]
fn test_grace_hash_join_with_spill_manager() {
    // Create spill manager
    let temp_dir = create_temp_spill_dir();
    let spill_dir = format!("{}/spill", temp_dir);
    std::fs::create_dir_all(&spill_dir).expect("Failed to create spill dir");
    
    let storage = Box::new(FsStorage::new());
    let spill_mgr = Arc::new(Mutex::new(SpillManager::new(
        storage,
        Codec::None, // Use None codec for tests (works without feature flags)
        spill_dir.clone(),
    )));
    
    let mut join = HashJoin::default();
    join.on = vec![("id".to_string(), "id".to_string())];
    join.join_type = "inner".to_string();
    join.spill_mgr = Some(spill_mgr);
    
    // Create large batches to trigger Grace join
    let large_left = RowBatch {
        columns: vec![
            Column {
                name: "id".to_string(),
                values: (0..200_000).map(|i| Scalar::I32(i)).collect(),
            },
            Column {
                name: "name".to_string(),
                values: (0..200_000).map(|i| Scalar::Str(format!("name{}", i))).collect(),
            },
        ],
    };
    
    let large_right = RowBatch {
        columns: vec![
            Column {
                name: "id".to_string(),
                values: (100_000..300_000).map(|i| Scalar::I32(i)).collect(),
            },
            Column {
                name: "score".to_string(),
                values: (100_000..300_000).map(|i| Scalar::F64(i as f64)).collect(),
            },
        ],
    };
    
    let config = EngineConfig::default();
    let budget = MemoryBudgetImpl::new(config.mem_cap_bytes);
    
    // Should use Grace join for large inputs
    let result = join.eval_block(&[large_left, large_right], &budget)
        .expect("Grace join should succeed");
    
    // Verify results (should have matches in the overlap range 100k-200k)
    assert!(result.num_rows() > 0);
    assert_eq!(result.columns.len(), 4); // id (left), name, id_right, score
    
    // Verify all results have matching IDs
    let id_col = &result.columns[0];
    let score_col = &result.columns[3]; // score is the 4th column
    for (i, id_val) in id_col.values.iter().enumerate() {
        if let Scalar::I32(id) = id_val {
            // Verify the score matches the ID
            if let Scalar::F64(score) = score_col.values[i] {
                assert_eq!(score, *id as f64);
            }
        }
    }
}

#[test]
fn test_grace_hash_join_left_join() {
    let temp_dir = create_temp_spill_dir();
    let spill_dir = format!("{}/spill", temp_dir);
    std::fs::create_dir_all(&spill_dir).expect("Failed to create spill dir");
    
    let storage = Box::new(FsStorage::new());
    let spill_mgr = Arc::new(Mutex::new(SpillManager::new(
        storage,
        Codec::None,
        spill_dir,
    )));
    
    let mut join = HashJoin::default();
    join.on = vec![("id".to_string(), "id".to_string())];
    join.join_type = "left".to_string();
    join.spill_mgr = Some(spill_mgr);
    
    let left = create_left_batch();
    let right = create_right_batch();
    
    // Create larger batches to trigger Grace join
    let large_left = RowBatch {
        columns: vec![
            Column {
                name: "id".to_string(),
                values: (0..150_000).map(|i| Scalar::I32(i)).collect(),
            },
            Column {
                name: "name".to_string(),
                values: (0..150_000).map(|i| Scalar::Str(format!("name{}", i))).collect(),
            },
        ],
    };
    
    let large_right = RowBatch {
        columns: vec![
            Column {
                name: "id".to_string(),
                values: (100_000..150_000).map(|i| Scalar::I32(i)).collect(),
            },
            Column {
                name: "score".to_string(),
                values: (100_000..150_000).map(|i| Scalar::F64(i as f64)).collect(),
            },
        ],
    };
    
    let config = EngineConfig::default();
    let budget = MemoryBudgetImpl::new(config.mem_cap_bytes);
    
    let result = join.eval_block(&[large_left, large_right], &budget)
        .expect("Left join should succeed");
    
    // Left join should have all rows from left (150k)
    // Some will have matching scores, some will have NULL
    assert_eq!(result.num_rows(), 150_000);
    assert_eq!(result.columns.len(), 4); // id (left), name, id_right, score
    
    // Check that unmatched rows have NULL scores
    let score_col = &result.columns[3]; // score is the 4th column
    let null_count = score_col.values.iter()
        .filter(|v| matches!(v, Scalar::Null))
        .count();
    
    // First 100k rows should have NULL scores (no match in right)
    assert_eq!(null_count, 100_000);
}

#[test]
fn test_grace_hash_join_partitioning() {
    // Test that partitioning works correctly by using Grace join
    // Partitioning is tested indirectly through the join operation
    let temp_dir = create_temp_spill_dir();
    let spill_dir = format!("{}/spill", temp_dir);
    std::fs::create_dir_all(&spill_dir).expect("Failed to create spill dir");
    
    let storage = Box::new(FsStorage::new());
    let spill_mgr = Arc::new(Mutex::new(SpillManager::new(
        storage,
        Codec::None, // Use None codec for tests (works without feature flags)
        spill_dir.clone(),
    )));
    
    let mut join = HashJoin::default();
    join.on = vec![("id".to_string(), "id".to_string())];
    join.join_type = "inner".to_string();
    join.spill_mgr = Some(spill_mgr);
    
    // Create batches large enough to trigger Grace join
    let large_left = RowBatch {
        columns: vec![
            Column {
                name: "id".to_string(),
                values: (0..150_000).map(|i| Scalar::I32(i)).collect(),
            },
            Column {
                name: "value".to_string(),
                values: (0..150_000).map(|i| Scalar::Str(format!("val{}", i))).collect(),
            },
        ],
    };
    
    let large_right = RowBatch {
        columns: vec![
            Column {
                name: "id".to_string(),
                values: (100_000..200_000).map(|i| Scalar::I32(i)).collect(),
            },
            Column {
                name: "extra".to_string(),
                values: (100_000..200_000).map(|i| Scalar::Str(format!("extra{}", i))).collect(),
            },
        ],
    };
    
    let config = EngineConfig::default();
    let budget = MemoryBudgetImpl::new(config.mem_cap_bytes);
    
    // Grace join should partition and join correctly
    let result = join.eval_block(&[large_left, large_right], &budget)
        .expect("Grace join should succeed");
    
    // Verify results (should have matches in overlap range 100k-150k)
    assert_eq!(result.num_rows(), 50_000); // Overlap range
    assert_eq!(result.columns.len(), 4); // id (left), value, id_right, extra
    
    // Verify all result IDs are in the overlap range
    let id_col = &result.columns[0];
    for id_val in &id_col.values {
        if let Scalar::I32(id) = id_val {
            assert!(*id >= 100_000 && *id < 150_000);
        }
    }
}

#[test]
fn test_grace_hash_join_memory_constraint() {
    // Test that Grace join respects memory budget
    let temp_dir = create_temp_spill_dir();
    let spill_dir = format!("{}/spill", temp_dir);
    std::fs::create_dir_all(&spill_dir).expect("Failed to create spill dir");
    
    let storage = Box::new(FsStorage::new());
    let spill_mgr = Arc::new(Mutex::new(SpillManager::new(
        storage,
        Codec::None,
        spill_dir,
    )));
    
    let mut join = HashJoin::default();
    join.on = vec![("id".to_string(), "id".to_string())];
    join.join_type = "inner".to_string();
    join.spill_mgr = Some(spill_mgr);
    
    // Create batches that exceed a small memory budget
    let large_left = RowBatch {
        columns: vec![
            Column {
                name: "id".to_string(),
                values: (0..500_000).map(|i| Scalar::I32(i)).collect(),
            },
            Column {
                name: "data".to_string(),
                values: (0..500_000).map(|i| Scalar::Str(format!("data{}", i))).collect(),
            },
        ],
    };
    
    let large_right = RowBatch {
        columns: vec![
            Column {
                name: "id".to_string(),
                values: (250_000..750_000).map(|i| Scalar::I32(i)).collect(),
            },
            Column {
                name: "extra".to_string(),
                values: (250_000..750_000).map(|i| Scalar::Str(format!("extra{}", i))).collect(),
            },
        ],
    };
    
    // Use a small memory budget to force partitioning
    let mut config = EngineConfig::default();
    config.mem_cap_bytes = 10 * 1024 * 1024; // 10MB
    let budget = MemoryBudgetImpl::new(config.mem_cap_bytes);
    
    // Should succeed with Grace join (partitioning)
    let result = join.eval_block(&[large_left, large_right], &budget)
        .expect("Grace join should handle memory constraints");
    
    // Verify results
    assert!(result.num_rows() > 0);
    assert_eq!(result.columns.len(), 4); // id (left), data, id_right, extra
}

