//! Parquet writer with compression support (enabled with `--features parquet`).
//!
//! Supports:
//! - Writing Arrow RecordBatch to Parquet files
//! - Compression codecs (SNAPPY, GZIP, ZSTD, LZ4, UNCOMPRESSED)
//! - Configurable row group size
//! - Schema writing

#[cfg(feature = "parquet")]
use arrow_array::RecordBatch;
#[cfg(feature = "parquet")]
use arrow_schema::SchemaRef;
#[cfg(feature = "parquet")]
use parquet::arrow::ArrowWriter;
#[cfg(feature = "parquet")]
use parquet::basic::{Compression, ZstdLevel};
#[cfg(feature = "parquet")]
use parquet::file::properties::WriterProperties;
#[cfg(feature = "parquet")]
use parquet::file::properties::WriterPropertiesBuilder;
#[cfg(feature = "parquet")]
use std::fs::File;
#[cfg(feature = "parquet")]
use std::sync::Arc;

use crate::error::{Error, Result};

/// Compression codec for Parquet files.
#[cfg(feature = "parquet")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParquetCompression {
    /// No compression
    Uncompressed,
    /// Snappy compression (fast, good compression)
    Snappy,
    /// GZIP compression (good compression ratio)
    Gzip,
    /// ZSTD compression (excellent compression ratio)
    Zstd,
    /// LZ4 compression (very fast)
    Lz4,
}

#[cfg(feature = "parquet")]
impl Default for ParquetCompression {
    fn default() -> Self {
        ParquetCompression::Snappy
    }
}

#[cfg(feature = "parquet")]
impl ParquetCompression {
    /// Convert to Parquet's Compression enum.
    fn to_parquet_compression(self) -> Compression {
        match self {
            ParquetCompression::Uncompressed => Compression::UNCOMPRESSED,
            ParquetCompression::Snappy => Compression::SNAPPY,
            ParquetCompression::Gzip => Compression::GZIP(Default::default()),
            ParquetCompression::Zstd => Compression::ZSTD(ZstdLevel::default()),
            ParquetCompression::Lz4 => Compression::LZ4,
        }
    }
}

/// Parquet writer with compression support.
#[cfg(feature = "parquet")]
pub struct ParquetWriter {
    writer: ArrowWriter<File>,
    schema: SchemaRef,
}

#[cfg(feature = "parquet")]
impl ParquetWriter {
    /// Create a new ParquetWriter with default settings (Snappy compression, 128MB row groups).
    pub fn to_path(path: &str, schema: SchemaRef) -> Result<Self> {
        Self::to_path_with_options(path, schema, ParquetCompression::default(), None)
    }

    /// Create a new ParquetWriter with custom compression and row group size.
    ///
    /// # Arguments
    /// * `path` - Path to the Parquet file
    /// * `schema` - Arrow schema for the data
    /// * `compression` - Compression codec to use
    /// * `row_group_size` - Optional row group size in bytes (default: 128MB)
    pub fn to_path_with_options(
        path: &str,
        schema: SchemaRef,
        compression: ParquetCompression,
        row_group_size: Option<usize>,
    ) -> Result<Self> {
        let file = File::create(path)
            .map_err(|e| Error::Io(e))?;

        // Build writer properties
        let mut props_builder = WriterProperties::builder()
            .set_compression(compression.to_parquet_compression());

        // Set row group size if specified
        if let Some(size_bytes) = row_group_size {
            props_builder = props_builder.set_max_row_group_size(size_bytes);
        } else {
            // Default: 128MB row groups
            props_builder = props_builder.set_max_row_group_size(128 * 1024 * 1024);
        }

        let props = props_builder.build();

        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .map_err(|e| Error::Other(format!("Failed to create Parquet writer: {}", e)))?;

        Ok(Self {
            writer,
            schema,
        })
    }

    /// Write a RecordBatch to the Parquet file.
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer
            .write(batch)
            .map_err(|e| Error::Other(format!("Failed to write Parquet batch: {}", e)))?;
        Ok(())
    }

    /// Close the writer and finalize the Parquet file.
    ///
    /// This consumes the writer and writes the file footer.
    pub fn close(self) -> Result<()> {
        self.writer
            .close()
            .map_err(|e| Error::Other(format!("Failed to close Parquet writer: {}", e)))?;
        Ok(())
    }

    /// Get the schema of the Parquet file.
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(not(feature = "parquet"))]
compile_error!("parquet.rs was compiled without the `parquet` feature; enable `--features parquet` or exclude this module.");
