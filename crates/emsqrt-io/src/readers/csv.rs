//! Streaming CSV reader → `RowBatch`.
//!
//! Caveats:
//! - No type inference (everything is Utf8 Scalar by default).
//! - Suitable as a starter; replace with Arrow-based scans later.

use std::fs::File;
use std::io::Read;

use csv as csv_crate;
use emsqrt_core::schema::{DataType, Field, Schema};
use emsqrt_core::types::{Column, RowBatch, Scalar};

use crate::error::{Error, Result};

pub struct CsvReader<R: Read> {
    rdr: csv_crate::Reader<R>,
    schema: Schema,
}

impl CsvReader<File> {
    pub fn from_path(path: &str, has_headers: bool) -> Result<Self> {
        let file = File::open(path)?;
        Self::from_reader(file, has_headers)
    }
}

impl<R: Read> CsvReader<R> {
    pub fn from_reader(reader: R, has_headers: bool) -> Result<Self> {
        let mut rdr = csv_crate::ReaderBuilder::new()
            .has_headers(has_headers)
            .flexible(true)
            .from_reader(reader);

        let headers: Vec<String> = if has_headers {
            rdr.headers()?.iter().map(|s| s.to_string()).collect()
        } else {
            // For headerless CSV, use from_reader_with_schema instead
            return Err(Error::Schema(
                "CSV without headers is not yet supported (use from_reader_with_schema)".into(),
            ));
        };

        let schema = Schema::new(
            headers
                .into_iter()
                .map(|h| Field::new(h, DataType::Utf8, true))
                .collect(),
        );

        Ok(Self { rdr, schema })
    }

    /// Create a CSV reader with an explicit schema (for headerless CSV).
    pub fn from_reader_with_schema(reader: R, schema: Schema) -> Result<Self> {
        let rdr = csv_crate::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(reader);

        Ok(Self { rdr, schema })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Read up to `limit_rows` rows into a `RowBatch`.
    pub fn next_batch(&mut self, limit_rows: usize) -> Result<Option<RowBatch>> {
        if limit_rows == 0 {
            return Ok(Some(RowBatch { columns: vec![] }));
        }

        let ncols = self.schema.fields.len();
        let mut cols: Vec<Column> = self
            .schema
            .fields
            .iter()
            .map(|f| Column {
                name: f.name.clone(),
                values: Vec::with_capacity(limit_rows),
            })
            .collect();

        let mut read_rows = 0usize;
        for rec in self.rdr.records() {
            let rec = rec?;
            if rec.len() != ncols {
                // Flexible CSV may have variable length rows; pad with Nulls.
                for (i, col) in cols.iter_mut().enumerate() {
                    let v = rec
                        .get(i)
                        .map(|s| Scalar::Str(s.to_string()))
                        .unwrap_or(Scalar::Null);
                    col.values.push(v);
                }
            } else {
                for (i, col) in cols.iter_mut().enumerate() {
                    col.values
                        .push(Scalar::Str(rec.get(i).unwrap().to_string()));
                }
            }
            read_rows += 1;
            if read_rows >= limit_rows {
                break;
            }
        }

        if read_rows == 0 {
            return Ok(None);
        }

        Ok(Some(RowBatch { columns: cols }))
    }
}
