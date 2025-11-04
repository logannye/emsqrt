# EM-√ Pipeline Examples

This directory contains example YAML pipeline definitions for the EM-√ engine.

## Running Examples

```bash
# Validate a pipeline
emsqrt validate --pipeline examples/simple_pipeline.yaml

# Show execution plan
emsqrt explain --pipeline examples/simple_pipeline.yaml --memory-cap 536870912

# Execute a pipeline
emsqrt run --pipeline examples/simple_pipeline.yaml
```

## Pipeline Structure

All pipelines follow this structure:

```yaml
steps:
  - op: scan
    source: "..."
    schema: [...]
  - op: filter
    expr: "..."
  - op: project
    columns: [...]
  - op: aggregate
    group_by: [...]
    aggs: [...]
  - op: sink
    destination: "..."
    format: "csv"
```

Note: Each step must have an `op` field that specifies the operator type.

## Available Operators

### Scan
Read data from a source file. Supports CSV and Parquet formats (Parquet requires `--features parquet`).

```yaml
- op: scan
  source: "path/to/file.csv"  # or "path/to/file.parquet"
  schema:
    - name: "column_name"
      type: "Int64"  # or Utf8, Float64, Bool, Int32
      nullable: false
```

**Parquet Support**: Parquet files are automatically detected by extension (`.parquet`, `.parq`). The engine uses Arrow integration for efficient columnar reading.

### Filter
Filter rows based on a predicate expression.

```yaml
- op: filter
  expr: "column_name > 100"
```

Supported operators: `=`, `!=`, `<`, `<=`, `>`, `>=`

### Project
Select and reorder columns.

```yaml
- op: project
  columns:
    - "col1"
    - "col2"
```

### Aggregate
Group by columns and compute aggregations.

```yaml
- op: aggregate
  group_by:
    - "category"
  aggs:
    - "SUM(sales)"
    - "COUNT(*)"
    - "AVG(price)"
```

Aggregation functions: `SUM(column)`, `COUNT(*)`, `AVG(column)`, `MIN(column)`, `MAX(column)`

### Map
Rename columns.

```yaml
- op: map
  expr: "old_name AS new_name, other_col"
```

### Sink
Write results to a destination. Supports CSV, JSONL, and Parquet formats (Parquet requires `--features parquet`).

```yaml
- op: sink
  destination: "output/result.csv"
  format: "csv"  # or "jsonl" or "parquet"
```

**Parquet Support**: When writing Parquet files, the engine automatically infers the schema from the first batch and uses Arrow integration for efficient columnar writing.

## Examples

- **simple_pipeline.yaml**: Basic scan → filter → project → sink
- **aggregate_pipeline.yaml**: Aggregation with grouping
- **join_pipeline.yaml**: Join operation between two data sources
- **parquet_pipeline.yaml**: CSV to Parquet conversion with filtering
- **parquet_scan_pipeline.yaml**: Read Parquet, transform, write Parquet

### Running Parquet Examples

Parquet examples require the `parquet` feature to be enabled:

```bash
# Build with Parquet support
cargo build --release --features parquet

# Run Parquet pipeline
emsqrt run --pipeline examples/parquet_pipeline.yaml --features parquet
```

## Configuration

You can override configuration via command-line flags:

```bash
emsqrt run \
  --pipeline examples/simple_pipeline.yaml \
  --memory-cap 1073741824 \
  --spill-dir /tmp/emsqrt-spill \
  --max-parallel 4
```

Or via environment variables:

```bash
export EMSQRT_MEM_CAP_BYTES=1073741824
export EMSQRT_SPILL_DIR=/tmp/emsqrt-spill
export EMSQRT_MAX_PARALLEL_TASKS=4

emsqrt run --pipeline examples/simple_pipeline.yaml
```

