# EM-√ (EM-Sqrt): External-Memory ETL Engine

## **Process any dataset size with a fixed, small memory footprint.**

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.70+-orange.svg)](https://www.rust-lang.org/)


EM-√ is an external-memory ETL/log processing engine with **hard peak-RAM guarantees**. Unlike traditional systems that "try" to stay within memory limits, EM-√ **enforces** a strict memory cap, enabling you to process arbitrarily large datasets using small memory footprints.

## Key Features

- **Hard Memory Guarantees**: Never exceeds the configured memory cap (e.g., 100MB). All allocations are tracked via RAII guards.
- **External-Memory Operators**: Sort, join, and aggregate operations automatically spill to disk when memory limits are hit.
- **Tree Evaluation (TE) Scheduling**: Principled execution schedule that decomposes plans into blocks with bounded fan-in to control peak memory.
- **Cloud-Ready**: Spill segments support local filesystem, S3, and GCS (planned) with checksums and compression.
- **Deterministic Execution**: Stable plan hashing for reproducibility and auditability.
- **Memory-Constrained Environments**: Designed for edge computing, serverless, embedded systems, and containerized deployments.

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/logannye/emsqrt.git
cd emsqrt

# Build the project
cargo build --release

# Run tests
cargo test
```

### Basic Usage

#### Programmatic API

```rust
use emsqrt_core::schema::{Field, DataType, Schema};
use emsqrt_core::dag::LogicalPlan as L;
use emsqrt_core::config::EngineConfig;
use emsqrt_planner::{rules, lower_to_physical, estimate_work};
use emsqrt_te::plan_te;
use emsqrt_exec::Engine;

// Define your schema
let schema = Schema {
    fields: vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
    ],
};

// Build a logical plan: scan → filter → project → sink
let scan = L::Scan {
    source: "file:///path/to/input.csv".to_string(),
    schema: schema.clone(),
};

let filter = L::Filter {
    input: Box::new(scan),
    expr: "age > 25".to_string(),
};

let project = L::Project {
    input: Box::new(filter),
    columns: vec!["name".to_string(), "age".to_string()],
};

let sink = L::Sink {
    input: Box::new(project),
    destination: "file:///path/to/output.csv".to_string(),
    format: "csv".to_string(),
};

// Optimize and execute
let optimized = rules::optimize(sink);
let phys_prog = lower_to_physical(&optimized);
let work = estimate_work(&optimized, None);
let te = plan_te(&phys_prog.plan, &work, 64 * 1024 * 1024)?; // 64MB memory cap

// Configure and run
let mut config = EngineConfig::default();
config.mem_cap_bytes = 64 * 1024 * 1024; // 64MB
config.spill_dir = "/tmp/emsqrt-spill".to_string();

let mut engine = Engine::new(config);
let manifest = engine.run(&phys_prog, &te)?;

println!("Execution completed in {}ms", manifest.finished_ms - manifest.started_ms);
```

#### YAML DSL (Planned)

```yaml
steps:
  - scan:
      source: "data/logs.csv"
      schema:
        - { name: "ts", type: "Utf8", nullable: false }
        - { name: "uid", type: "Utf8", nullable: false }
        - { name: "amount", type: "Float64", nullable: true }
  
  - filter:
      expr: "amount > 1000"
  
  - aggregate:
      group_by: ["uid"]
      aggs: ["COUNT(*)", "SUM(amount)"]
  
  - sort:
      by: ["uid", "amount"]
  
  - sink:
      destination: "results/summary.csv"
      format: "csv"
```

## Examples of Practical Use Cases

### 1. Serverless Data Pipelines

Process large datasets in AWS Lambda, Google Cloud Functions, or Azure Functions with strict memory limits:

```rust
// Process 100GB dataset in a 512MB Lambda
let config = EngineConfig {
    mem_cap_bytes: 512 * 1024 * 1024, // 512MB
    spill_dir: "s3://my-bucket/spill/".to_string(),
    ..Default::default()
};
```

**Value**: 10-100x cost reduction vs. large EC2 instances or EMR clusters.

### 2. Edge Data Processing

Aggregate sensor data on IoT gateways or embedded devices with limited RAM:

```rust
// Process 1M sensor readings on a Raspberry Pi with 256MB RAM
let config = EngineConfig {
    mem_cap_bytes: 128 * 1024 * 1024, // Use only 128MB
    spill_dir: "/tmp/sensor-spill".to_string(),
    ..Default::default()
};
```

**Value**: Enable edge analytics without hardware upgrades.

### 3. Multi-Tenant Data Platforms

Run customer queries with isolated memory budgets:

```rust
// Each customer gets a memory budget
let config = EngineConfig {
    mem_cap_bytes: customer_memory_budget,
    spill_dir: format!("s3://platform/spill/customer-{}", customer_id),
    ..Default::default()
};
```

**Value**: Predictable performance, resource isolation, accurate cost attribution.

### 4. Cost-Optimized Analytics

Use smaller, cheaper instances by trading I/O for memory:

```rust
// Process 500GB dataset on a 4GB RAM instance instead of 64GB
let config = EngineConfig {
    mem_cap_bytes: 4 * 1024 * 1024 * 1024, // 4GB
    spill_dir: "/fast-nvme/spill".to_string(),
    ..Default::default()
};
```

**Value**: 10x cost reduction for memory-bound workloads.

## Architecture

EM-√ is built as a modular Rust workspace with the following crates:

```
emsqrt-core/      - Core types, schemas, DAGs, memory budget traits
emsqrt-te/        - Tree Evaluation planner (bounded fan-in decomposition)
emsqrt-mem/       - Memory budget implementation, spill manager, buffer pool
emsqrt-io/        - I/O adapters (CSV, JSONL, Parquet, storage backends)
emsqrt-operators/ - Query operators (filter, project, sort, join, aggregate)
emsqrt-planner/   - Logical/physical planning, optimization, YAML DSL
emsqrt-exec/      - Execution runtime, scheduler, engine
emsqrt-tests/     - Comprehensive test suite
```

### Execution Flow

1. **Planning**: YAML/Logical plan → Optimized logical plan → Physical plan with operator bindings
2. **TE Scheduling**: Physical plan → Tree Evaluation blocks with bounded fan-in
3. **Execution**: Blocks executed in dependency order, respecting memory budget
4. **Spilling**: Operators automatically spill to disk when memory limits are hit
5. **Manifest**: Deterministic execution manifest with plan hashes for reproducibility

### Memory Management

- **MemoryBudget**: RAII guards track all allocations
- **SpillManager**: Checksummed, compressed segments for external-memory operations
- **TE Frontier**: Bounded live blocks guarantee peak memory ≤ cap

## Configuration

### EngineConfig

```rust
pub struct EngineConfig {
    /// Hard memory cap (bytes). The engine must NEVER exceed this.
    pub mem_cap_bytes: usize,
    
    /// Optional block-size hint (TE planner may override)
    pub block_size_hint: Option<usize>,
    
    /// Max on-disk spill concurrency
    pub max_spill_concurrency: usize,
    
    /// Optional seed for deterministic shuffles
    pub seed: Option<u64>,
    
    /// Execution parallelism
    pub max_parallel_tasks: usize,
    
    /// Directory for spill files
    pub spill_dir: String,
}
```

### Environment Variables

```bash
export EMSQRT_MEM_CAP_BYTES=536870912  # 512MB
export EMSQRT_SPILL_DIR=/tmp/emsqrt-spill
export EMSQRT_MAX_PARALLEL_TASKS=4
```

### Default Configuration

```rust
EngineConfig::default()
// mem_cap_bytes: 512 MiB
// max_spill_concurrency: 4
// max_parallel_tasks: 4
// spill_dir: "/tmp/emsqrt-spill"
```

## Building & Testing

### Build

```bash
# Debug build
cargo build

# Release build (optimized)
cargo build --release

# Build specific crate
cargo build -p emsqrt-exec
```

### Run Tests

```bash
# All tests
cargo test

# Specific test suite
cargo test -p emsqrt-tests --test integration_tests

# Run comprehensive test suite
./scripts/run_all_tests.sh
```

### Test Coverage

The project includes:
- **Unit Tests**: SpillManager, RowBatch helpers, Memory budget, External sort
- **Integration Tests**: Full pipeline tests (scan, filter, project, sort, aggregate, sink)
- **E2E Tests**: End-to-end smoke tests

## Supported Operations

### Currently Implemented

- ✅ **Scan**: Read CSV files with schema inference
- ✅ **Filter**: Predicate filtering (e.g., `age > 25`, `name == "Alice"`)
- ✅ **Project**: Column selection and renaming
- ✅ **Map**: Column renaming (e.g., `old_name AS new_name`)
- ✅ **Sort**: External sort with k-way merge
- ✅ **Aggregate**: Group-by with COUNT, SUM, AVG, MIN, MAX
- ✅ **Join**: Hash join (Grace hash join planned)
- ✅ **Sink**: Write CSV files

### Planned Features

- 🔄 **Parquet I/O**: Native columnar read/write
- 🔄 **Arrow Integration**: Columnar processing with SIMD
- 🔄 **Cloud Storage**: S3, GCS adapters for spill segments
- 🔄 **Merge Join**: Sorted merge join for pre-sorted inputs
- 🔄 **Expression Engine**: Full SQL-like expressions
- 🔄 **Statistics**: Column statistics for better cost estimation

## How It Works

### Tree Evaluation (TE)

Tree Evaluation is a principled execution scheduling approach that:

1. **Decomposes plans into blocks** with bounded fan-in (e.g., each join block depends on at most K input blocks)
2. **Controls the live frontier** (the set of materialized blocks at any time)
3. **Guarantees peak memory** ≤ `K * block_size + overhead`

### External-Memory Operators

When memory limits are hit, operators automatically:

1. **Spill to disk**: Write intermediate results to checksummed, compressed segments
2. **Partition**: Divide work into smaller chunks that fit in memory
3. **Merge**: Combine results from multiple partitions/runs

Example: External sort generates sorted runs, then performs k-way merge.

### Memory Budget Enforcement

Every allocation requires a `BudgetGuard`:

```rust
let guard = budget.try_acquire(bytes, "my-buffer")?;
// Allocate memory...
// Guard automatically releases bytes on drop (RAII)
```

If `try_acquire` returns `None`, the operator must spill or partition.

## Performance

### Benchmarks (Planned)

- Sort 10GB with 100MB memory
- Join 1GB × 1GB with 50MB memory
- Aggregate 1M groups with 20MB memory
- TPC-H queries (Q1, Q3, Q6)

### Expected Characteristics

- **Throughput**: 10-100x slower than in-memory systems (by design)
- **Memory**: **Guaranteed** to never exceed cap (unlike other systems)
- **Scalability**: Can process datasets 100-1000x larger than available RAM

## Development

### Project Structure

```
emsqrt/
├── Cargo.toml              # Workspace configuration
├── crates/
│   ├── emsqrt-core/       # Core types and traits
│   ├── emsqrt-te/          # Tree Evaluation planner
│   ├── emsqrt-mem/         # Memory budget and spill manager
│   ├── emsqrt-io/          # I/O adapters
│   ├── emsqrt-operators/   # Query operators
│   ├── emsqrt-planner/     # Planning and optimization
│   ├── emsqrt-exec/        # Execution runtime
│   └── tests/              # Test suite
├── tests/                  # Integration tests
├── scripts/                # Utility scripts
└── README.md               # This file
```

### Adding a New Operator

1. Implement the `Operator` trait in `emsqrt-operators/src/`
2. Register in `emsqrt-operators/src/registry.rs`
3. Add to planner lowering in `emsqrt-planner/src/lower.rs`
4. Add tests in `tests/`

### Code Style

- Follow Rust standard formatting: `cargo fmt`
- All code must compile with `#![forbid(unsafe_code)]`
- Use `thiserror` for error types
- Use `serde` for serialization

## Contributing

Contributions are welcome! Areas of particular interest:

- Cloud storage adapters (S3, GCS, Azure)
- Parquet and Arrow integration
- Additional operators (merge join, window functions)
- Performance optimizations
- Documentation improvements

<<<<<<< HEAD
## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

=======
>>>>>>> 477c699 (Fix projection pushdown bug and enhance error handling)
## Acknowledgments

This project implements Tree Evaluation (TE) scheduling for external-memory query processing, enabling predictable memory usage in constrained environments.

---

**Remember**: EM-√ trades throughput for guaranteed memory bounds. Use it when memory constraints are more important than raw speed.

Repo is a dynamic work, please be aware that it will evolve and further develop over time.
