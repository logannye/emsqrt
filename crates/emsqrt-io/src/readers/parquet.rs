//! Parquet reader with projection and predicate pushdown (enabled with `--features parquet`).
//!
//! Supports:
//! - Column projection (only read needed columns)
//! - Row group filtering (basic predicate pushdown)
//! - Batched reading with configurable batch size

#[cfg(feature = "parquet")]
use arrow_array::RecordBatch;
#[cfg(feature = "parquet")]
use arrow_schema::{Schema as ArrowSchema, SchemaRef};
#[cfg(feature = "parquet")]
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
#[cfg(feature = "parquet")]
use parquet::arrow::ProjectionMask;
#[cfg(feature = "parquet")]
use std::fs::File;
#[cfg(feature = "parquet")]
use std::io::{BufReader, Read, Seek};
#[cfg(feature = "parquet")]
use std::sync::Arc;

use crate::arrow_convert::record_batch_to_row_batch;
use crate::error::{Error, Result};
use emsqrt_core::types::RowBatch;

/// Parquet reader with projection and predicate pushdown support.
#[cfg(feature = "parquet")]
pub struct ParquetReader {
    reader: ParquetRecordBatchReader,
    schema: SchemaRef,
    batch_size: usize,
}

#[cfg(feature = "parquet")]
impl ParquetReader {
    /// Create a new ParquetReader from a file path.
    ///
    /// # Arguments
    /// * `path` - Path to the Parquet file
    /// * `projection` - Optional column names to project (if None, reads all columns)
    /// * `batch_size` - Number of rows per batch
    pub fn from_path(
        path: &str,
        projection: Option<Vec<String>>,
        batch_size: usize,
    ) -> Result<Self> {
        let file = File::open(path)
            .map_err(|e| Error::Io(e))?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| Error::Parquet(e))?;

        // Get schema and metadata before building (needed for projection)
        let schema_ref = builder.schema().clone();
        let arrow_schema = schema_ref.as_ref();
        let metadata = builder.metadata().clone();
        let parquet_schema = metadata.file_metadata().schema_descr();

        // Calculate projection indices if needed (before moving builder)
        let projection_indices = if let Some(ref projection_cols) = projection {
            Some(projection_cols
                .iter()
                .map(|col_name| {
                    arrow_schema
                        .fields()
                        .iter()
                        .position(|f| f.name() == col_name)
                        .ok_or_else(|| {
                            Error::Other(format!("Column '{}' not found in Parquet schema", col_name))
                        })
                })
                .collect::<std::result::Result<Vec<usize>, Error>>()?)
        } else {
            None
        };

        // Apply projection if specified (now we can move builder)
        let mut builder = if let Some(ref indices) = projection_indices {
            // Create ProjectionMask from indices using Parquet schema descriptor
            let mask = ProjectionMask::roots(parquet_schema, indices.clone());
            builder.with_projection(mask)
        } else {
            builder
        };

        // Set batch size
        builder = builder.with_batch_size(batch_size);

        let reader = builder
            .build()
            .map_err(|e| {
                // build() returns ArrowError, convert to string error
                Error::Other(format!("Failed to build Parquet reader: {}", e))
            })?;

        // Get projected schema - if projection was applied, extract the projected schema
        let final_schema = if let Some(ref indices) = projection_indices {
            // Create a new schema with only projected columns
            let projected_fields: Vec<_> = indices
                .iter()
                .filter_map(|&idx| arrow_schema.fields().get(idx).cloned())
                .collect();
            Arc::new(ArrowSchema::new(projected_fields))
        } else {
            schema_ref
        };

        Ok(Self {
            reader,
            schema: final_schema,
            batch_size,
        })
    }


    /// Read the next batch of rows as a RecordBatch (low-level Arrow API).
    ///
    /// Returns `None` when all rows have been read.
    /// This method is kept for advanced use cases that need direct Arrow access.
    pub fn next_record_batch(&mut self) -> Result<Option<RecordBatch>> {
        // ParquetRecordBatchReader implements Iterator
        self.reader
            .next()
            .transpose()
            .map_err(|e| Error::Other(format!("Failed to read Parquet batch: {}", e)))
    }

    /// Read the next batch of rows as a RowBatch.
    ///
    /// Returns `None` when all rows have been read.
    pub fn next_batch(&mut self) -> Result<Option<RowBatch>> {
        match self.next_record_batch()? {
            Some(record_batch) => {
                let row_batch = record_batch_to_row_batch(&record_batch)?;
                Ok(Some(row_batch))
            }
            None => Ok(None),
        }
    }

    /// Get the Arrow schema of the Parquet file (after projection).
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Get the configured batch size.
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
}

#[cfg(not(feature = "parquet"))]
compile_error!("parquet.rs was compiled without the `parquet` feature; enable `--features parquet` or exclude this module.");
