//! SpillManager unit tests

mod test_data_gen;

use emsqrt_core::id::SpillId;
use emsqrt_core::schema::{DataType, Field, Schema};
use emsqrt_core::types::RowBatch;
use emsqrt_mem::{Codec, MemoryBudgetImpl, SpillManager, Storage};
use emsqrt_io::storage::FsStorage;
use test_data_gen::{create_temp_spill_dir, generate_random_batch};

fn setup_spill_manager(codec: Codec) -> (SpillManager, String) {
    let spill_dir = create_temp_spill_dir();
    let storage = Box::new(FsStorage::new());
    let mgr = SpillManager::new(storage, codec, format!("{}/test-spills", spill_dir));
    (mgr, spill_dir)
}

fn cleanup_spill_dir(dir: &str) {
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_spill_write_read_cycle() {
    let (mut mgr, spill_dir) = setup_spill_manager(Codec::None);
    let budget = MemoryBudgetImpl::new(10 * 1024 * 1024); // 10MB budget
    
    // Generate test data
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]);
    let original_batch = generate_random_batch(100, &schema);
    
    // Write batch to spill
    let spill_id = SpillId::new(12345);
    let run_index = 0;
    let meta = mgr
        .write_batch(&original_batch, spill_id, run_index)
        .expect("Failed to write batch");
    
    // Verify metadata
    assert!(meta.uncompressed_len > 0);
    assert!(meta.compressed_len > 0);
    
    // Read batch back
    let read_batch = mgr
        .read_batch(&meta, &budget)
        .expect("Failed to read batch");
    
    // Verify equality
    assert_eq!(original_batch.num_rows(), read_batch.num_rows());
    assert_eq!(original_batch.columns.len(), read_batch.columns.len());
    
    for (orig_col, read_col) in original_batch.columns.iter().zip(read_batch.columns.iter()) {
        assert_eq!(orig_col.name, read_col.name);
        assert_eq!(orig_col.values.len(), read_col.values.len());
        for (orig_val, read_val) in orig_col.values.iter().zip(read_col.values.iter()) {
            assert_eq!(orig_val, read_val, "Value mismatch in column {}", orig_col.name);
        }
    }
    
    cleanup_spill_dir(&spill_dir);
}

#[test]
fn test_spill_checksum_validation() {
    let (mut mgr, spill_dir) = setup_spill_manager(Codec::None);
    let budget = MemoryBudgetImpl::new(10 * 1024 * 1024);
    
    let schema = Schema::new(vec![
        Field::new("data", DataType::Int64, false)
    ]);
    let batch = generate_random_batch(50, &schema);
    
    // Write batch
    let spill_id = SpillId::new(99999);
    let meta = mgr.write_batch(&batch, spill_id, 0).expect("Write failed");
    
    // Corrupt the segment file on disk
    let segment_path = format!("{}/test-spills/{}.seg", spill_dir, meta.name.0);
    let mut corrupted_data = std::fs::read(&segment_path).expect("Failed to read segment");
    
    // Corrupt some bytes in the middle (past the header)
    if corrupted_data.len() > 100 {
        corrupted_data[100] ^= 0xFF;
        corrupted_data[101] ^= 0xFF;
        std::fs::write(&segment_path, corrupted_data).expect("Failed to write corrupted data");
    }
    
    // Attempt to read - should fail checksum validation
    let result = mgr.read_batch(&meta, &budget);
    assert!(result.is_err(), "Expected checksum validation to fail");
    
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(
        err_msg.to_lowercase().contains("checksum") || err_msg.to_lowercase().contains("hash"),
        "Error should mention checksum/hash: {}",
        err_msg
    );
    
    cleanup_spill_dir(&spill_dir);
}

#[test]
fn test_spill_multiple_segments() {
    let (mut mgr, spill_dir) = setup_spill_manager(Codec::None);
    let budget = MemoryBudgetImpl::new(20 * 1024 * 1024);
    
    let schema = Schema::new(vec![
        Field::new("seq", DataType::Int64, false)
    ]);
    
    let num_segments = 10;
    let mut metas = Vec::new();
    let mut original_batches = Vec::new();
    
    // Write multiple segments
    for i in 0..num_segments {
        let batch = generate_random_batch(10 + i * 5, &schema); // Varying sizes
        let spill_id = SpillId::new(1000 + i as u64);
        let meta = mgr.write_batch(&batch, spill_id, i as u32).expect("Write failed");
        
        metas.push(meta);
        original_batches.push(batch);
    }
    
    // Read them all back in order
    for (i, (meta, original)) in metas.iter().zip(original_batches.iter()).enumerate() {
        let read_batch = mgr.read_batch(meta, &budget).expect("Read failed");
        
        assert_eq!(
            original.num_rows(),
            read_batch.num_rows(),
            "Segment {} row count mismatch",
            i
        );
        assert_eq!(
            original.columns.len(),
            read_batch.columns.len(),
            "Segment {} column count mismatch",
            i
        );
    }
    
    // Verify segment listing
    let all_segments = mgr.list_segments();
    assert_eq!(all_segments.len(), num_segments, "Segment count mismatch");
    
    cleanup_spill_dir(&spill_dir);
}

#[cfg(feature = "zstd")]
#[test]
fn test_spill_compression() {
    // Test with no compression
    let (mut mgr_none, spill_dir_none) = setup_spill_manager(Codec::None);
    let schema = Schema::new(vec![
        Field::new("repeated", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
    ]);
    
    // Create highly compressible data
    let mut batch = generate_random_batch(1000, &schema);
    // Make first column very repetitive
    for i in 0..batch.columns[0].values.len() {
        batch.columns[0].values[i] = emsqrt_core::types::Scalar::Str("repeated_value_for_compression".to_string());
    }
    
    let spill_id = SpillId::new(7777);
    let meta_none = mgr_none.write_batch(&batch, spill_id, 0).expect("Write failed");
    let size_none = meta_none.compressed_len;
    
    // Test with zstd compression
    let (mut mgr_zstd, spill_dir_zstd) = setup_spill_manager(Codec::Zstd);
    let meta_zstd = mgr_zstd.write_batch(&batch, spill_id, 0).expect("Write failed");
    let size_zstd = meta_zstd.compressed_len;
    
    // Compressed should be smaller
    assert!(
        size_zstd < size_none,
        "Compressed size {} should be less than uncompressed {}",
        size_zstd,
        size_none
    );
    
    // Verify decompression works
    let budget = MemoryBudgetImpl::new(20 * 1024 * 1024);
    let read_batch = mgr_zstd.read_batch(&meta_zstd, &budget).expect("Read failed");
    assert_eq!(batch.num_rows(), read_batch.num_rows());
    
    cleanup_spill_dir(&spill_dir_none);
    cleanup_spill_dir(&spill_dir_zstd);
}

#[test]
fn test_spill_budget_enforcement() {
    let (mut mgr, spill_dir) = setup_spill_manager(Codec::None);
    
    let schema = Schema::new(vec![
        Field::new("big_data", DataType::Utf8, false),
    ]);
    let batch = generate_random_batch(1000, &schema);
    
    let spill_id = SpillId::new(5555);
    let meta = mgr.write_batch(&batch, spill_id, 0).expect("Write failed");
    
    // Try to read with insufficient budget
    let tiny_budget = MemoryBudgetImpl::new(100); // Only 100 bytes
    let result = mgr.read_batch(&meta, &tiny_budget);
    
    // Should fail due to insufficient budget
    assert!(
        result.is_err(),
        "Expected read to fail with insufficient budget"
    );
    
    cleanup_spill_dir(&spill_dir);
}

#[test]
fn test_spill_segment_metadata() {
    let (mut mgr, spill_dir) = setup_spill_manager(Codec::None);
    
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Utf8, false),
    ]);
    let batch = generate_random_batch(250, &schema);
    
    let spill_id = SpillId::new(3333);
    let run_index = 42;
    let meta = mgr.write_batch(&batch, spill_id, run_index).expect("Write failed");
    
    // Verify metadata fields
    assert!(meta.uncompressed_len > 0, "Uncompressed length should be positive");
    assert!(meta.compressed_len > 0, "Compressed length should be positive");
    
    // For uncompressed codec, sizes should be equal
    assert_eq!(
        meta.uncompressed_len, meta.compressed_len,
        "With Codec::None, sizes should match"
    );
    
    // Verify segment name contains spill_id and run_index
    assert!(meta.name.0.contains("3333"));
    assert!(meta.name.0.contains("42"));
    
    // Verify segment can be retrieved
    let retrieved = mgr.get_segment(&meta.name);
    assert!(retrieved.is_some(), "Segment should be retrievable");
    
    cleanup_spill_dir(&spill_dir);
}

#[test]
fn test_spill_delete_segment() {
    let (mut mgr, spill_dir) = setup_spill_manager(Codec::None);
    
    let schema = Schema::new(vec![
        Field::new("x", DataType::Int64, false)
    ]);
    let batch = generate_random_batch(100, &schema);
    
    let spill_id = SpillId::new(8888);
    let meta = mgr.write_batch(&batch, spill_id, 0).expect("Write failed");
    
    // Verify segment exists
    assert!(mgr.get_segment(&meta.name).is_some());
    
    // Delete segment
    mgr.delete_segment(&meta.name).expect("Delete failed");
    
    // Verify segment no longer exists
    assert!(mgr.get_segment(&meta.name).is_none());
    
    cleanup_spill_dir(&spill_dir);
}

#[test]
fn test_spill_empty_batch() {
    let (mut mgr, spill_dir) = setup_spill_manager(Codec::None);
    let budget = MemoryBudgetImpl::new(10 * 1024 * 1024);
    
    // Create empty batch
    let empty_batch = RowBatch { columns: vec![] };
    
    let spill_id = SpillId::new(0);
    let meta = mgr.write_batch(&empty_batch, spill_id, 0).expect("Write failed");
    
    assert!(meta.uncompressed_len >= 0);
    
    let read_batch = mgr.read_batch(&meta, &budget).expect("Read failed");
    assert_eq!(read_batch.num_rows(), 0);
    
    cleanup_spill_dir(&spill_dir);
}

