//! Streaming NDJSON reader → `RowBatch`.
//!
//! Caveats:
//! - Builds the column set from the union of keys seen so far.
//! - All scalars are mapped to a small set of types; complex values become strings.

use std::fs::File;
use std::io::{BufRead, BufReader, Read};

use emsqrt_core::schema::{DataType, Field, Schema};
use emsqrt_core::types::{Column, RowBatch, Scalar};
use serde_json::Value;

use crate::error::{Error, Result};

pub struct JsonlReader<R: Read> {
    reader: BufReader<R>,
    // We grow the schema as we see new keys (simple prototype behavior).
    schema: Schema,
}

impl JsonlReader<File> {
    pub fn from_path(path: &str) -> Result<Self> {
        let f = File::open(path)?;
        Self::from_reader(f)
    }
}

impl<R: Read> JsonlReader<R> {
    pub fn from_reader(reader: R) -> Result<Self> {
        Ok(Self {
            reader: BufReader::new(reader),
            schema: Schema::new(vec![]),
        })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn next_batch(&mut self, limit_rows: usize) -> Result<Option<RowBatch>> {
        if limit_rows == 0 {
            return Ok(Some(RowBatch { columns: vec![] }));
        }

        let mut lines = Vec::with_capacity(limit_rows);
        for _ in 0..limit_rows {
            let mut s = String::new();
            let n = self.reader.read_line(&mut s)?;
            if n == 0 {
                break;
            }
            if s.trim().is_empty() {
                continue;
            }
            lines.push(s);
        }
        if lines.is_empty() {
            return Ok(None);
        }

        // Discover union of keys
        use std::collections::BTreeSet;
        let mut keys = BTreeSet::<String>::new();
        let mut parsed = Vec::with_capacity(lines.len());
        for s in lines {
            let v: Value = serde_json::from_str(&s)?;
            if let Value::Object(map) = &v {
                for k in map.keys() {
                    keys.insert(k.clone());
                }
            }
            parsed.push(v);
        }

        // Ensure schema covers all keys
        for k in keys.iter() {
            if self.schema.index_of(k).is_none() {
                self.schema
                    .fields
                    .push(Field::new(k.clone(), DataType::Utf8, true));
            }
        }

        // Build columns
        let mut cols: Vec<Column> = self
            .schema
            .fields
            .iter()
            .map(|f| Column {
                name: f.name.clone(),
                values: Vec::with_capacity(parsed.len()),
            })
            .collect();

        for v in parsed {
            match v {
                Value::Object(map) => {
                    for (i, f) in self.schema.fields.iter().enumerate() {
                        let s = map.get(&f.name).cloned().unwrap_or(Value::Null);
                        cols[i].values.push(to_scalar(s));
                    }
                }
                _ => {
                    // Non-object line → place into a single "value" column if present,
                    // else discard with Nulls (prototype behavior).
                    for col in cols.iter_mut() {
                        col.values.push(Scalar::Null);
                    }
                }
            }
        }

        Ok(Some(RowBatch { columns: cols }))
    }
}

fn to_scalar(v: Value) -> Scalar {
    use Scalar::*;
    match v {
        Value::Null => Null,
        Value::Bool(b) => Bool(b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Scalar::I64(i)
            } else if let Some(f) = n.as_f64() {
                Scalar::F64(f)
            } else {
                Str(n.to_string())
            }
        }
        Value::String(s) => Str(s),
        // Arrays/objects → stringified for now
        other => Str(other.to_string()),
    }
}
