//! Logical schema types. Pure data; no Arrow dependency here.
//!
//! The `types.rs` module contains lightweight `Scalar`/`Column` placeholders.
//! In `emsqrt-operators`, you'll likely convert to Arrow arrays for execution.

use serde::{Deserialize, Serialize};

use crate::stats::SchemaStats;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Int32,
    Int64,
    Float32,
    Float64,
    Utf8,
    Binary,
    Date64,
    Decimal128,
    // TODO: Add Time/Struct/List as needed.
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl Field {
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub fields: Vec<Field>,
    /// Optional column statistics for cost estimation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<SchemaStats>,
}

impl PartialEq for Schema {
    fn eq(&self, other: &Self) -> bool {
        self.fields == other.fields
        // Note: stats are not compared for equality (HashMap + floats make this complex)
    }
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Self {
        Self {
            fields,
            stats: None,
        }
    }

    pub fn new_with_stats(fields: Vec<Field>, stats: Option<SchemaStats>) -> Self {
        Self { fields, stats }
    }

    pub fn field(&self, idx: usize) -> Option<&Field> {
        self.fields.get(idx)
    }

    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == name)
    }
}
