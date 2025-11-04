//! Streaming CSV writer from `RowBatch`.
//!
//! Placeholder implementation: writes header on first batch; all values via `to_string()`.

use std::fs::File;
use std::io::Write;

use csv as csv_crate;
use emsqrt_core::types::RowBatch;

use crate::error::{Error, Result};

pub struct CsvWriter<W: Write> {
    wtr: csv_crate::Writer<W>,
    wrote_header: bool,
}

impl CsvWriter<File> {
    pub fn to_path(path: &str) -> Result<Self> {
        let file = File::create(path)?;
        Ok(Self::to_writer(file))
    }
}

impl<W: Write> CsvWriter<W> {
    pub fn to_writer(writer: W) -> Self {
        Self {
            wtr: csv_crate::Writer::from_writer(writer),
            wrote_header: false,
        }
    }

    /// Create a writer that assumes headers have already been written
    pub fn to_writer_skip_header(writer: W) -> Self {
        Self {
            wtr: csv_crate::Writer::from_writer(writer),
            wrote_header: true,
        }
    }

    pub fn write_batch(&mut self, batch: &RowBatch) -> Result<()> {
        let ncols = batch.columns.len();
        if !self.wrote_header {
            let headers: Vec<&str> = batch.columns.iter().map(|c| c.name.as_str()).collect();
            self.wtr.write_record(headers)?;
            self.wtr.flush()?;
            self.wrote_header = true;
        }
        let nrows = batch.num_rows();
        for row_idx in 0..nrows {
            let mut row = Vec::with_capacity(ncols);
            for c in &batch.columns {
                let s = batch_value_to_string(&c.values[row_idx]);
                row.push(s);
            }
            self.wtr.write_record(&row)?;
        }
        self.wtr.flush()?;
        Ok(())
    }
}

fn batch_value_to_string(v: &emsqrt_core::types::Scalar) -> String {
    use emsqrt_core::types::Scalar::*;
    match v {
        Null => "".to_string(),
        Bool(b) => b.to_string(),
        I32(i) => i.to_string(),
        I64(i) => i.to_string(),
        F32(f) => f.to_string(),
        F64(f) => f.to_string(),
        Str(s) => s.clone(),
        Bin(b) => format!("[binary {} bytes]", b.len()), // base64 not available
    }
}
