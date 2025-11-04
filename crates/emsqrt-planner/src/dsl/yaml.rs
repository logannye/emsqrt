//! Minimal YAML → LogicalPlan parser for *linear* pipelines.
//!
//! Example:
//! ```yaml
//! steps:
//!   - scan: { source: "data/logs.csv", schema:
//!       [ {name: "ts",  type: "Utf8",  nullable: false},
//!         {name: "uid", type: "Utf8",  nullable: false},
//!         {name: "lat", type: "Float64", nullable: true} ] }
//!   - filter: { expr: "uid != ''" }
//!   - project: { columns: ["ts","uid"] }
//!   - sink: { destination: "out/filtered.csv", format: "csv" }
//! ```

use serde::{Deserialize, Serialize};
use serde_yaml;

use emsqrt_core::dag::LogicalPlan;
use emsqrt_core::schema::{DataType, Field, Schema};

use crate::logical::LogicalPlan as L;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", tag = "op")]
pub enum Step {
    #[serde(rename = "scan")]
    Scan {
        source: String,
        schema: Vec<FieldDef>,
    },

    #[serde(rename = "filter")]
    Filter { expr: String },

    #[serde(rename = "project")]
    Project { columns: Vec<String> },

    #[serde(rename = "map")]
    Map { expr: String },

    #[serde(rename = "sink")]
    Sink { destination: String, format: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDef {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    #[serde(default)]
    pub nullable: bool,
}

fn parse_dtype(s: &str) -> DataType {
    match s {
        "Boolean" | "bool" => DataType::Boolean,
        "Int32" | "i32" => DataType::Int32,
        "Int64" | "i64" => DataType::Int64,
        "Float32" | "f32" => DataType::Float32,
        "Float64" | "f64" => DataType::Float64,
        "Binary" | "bytes" => DataType::Binary,
        _ => DataType::Utf8,
    }
}

fn to_schema(fields: &[FieldDef]) -> Schema {
    Schema::new(
        fields
            .iter()
            .map(|f| Field {
                name: f.name.clone(),
                data_type: parse_dtype(&f.data_type),
                nullable: f.nullable,
            })
            .collect(),
    )
}

/// Parse YAML string into a `LogicalPlan`.
/// This supports *linear* pipelines only; joins/branches not yet supported.
pub fn parse_yaml_pipeline(yaml_src: &str) -> Result<LogicalPlan, serde_yaml::Error> {
    let doc: Pipeline = serde_yaml::from_str(yaml_src)?;
    let mut cur: Option<LogicalPlan> = None;

    for step in doc.steps {
        cur = Some(match (step, cur) {
            (Step::Scan { source, schema }, None) => L::Scan {
                source,
                schema: to_schema(&schema),
            },
            (Step::Scan { .. }, Some(_)) => {
                // serde_yaml::Error doesn't have a custom method, so we'll just parse error
                return Err(
                    serde_yaml::from_str::<()>("invalid: multiple scans not supported")
                        .unwrap_err(),
                );
            }
            (Step::Filter { expr }, Some(input)) => L::Filter {
                input: Box::new(input),
                expr,
            },
            (Step::Project { columns }, Some(input)) => L::Project {
                input: Box::new(input),
                columns,
            },
            (Step::Map { expr }, Some(input)) => L::Map {
                input: Box::new(input),
                expr,
            },
            (
                Step::Sink {
                    destination,
                    format,
                },
                Some(input),
            ) => L::Sink {
                input: Box::new(input),
                destination,
                format,
            },
            (s, None) => {
                // Any non-scan step without a prior plan is invalid in linear pipelines.
                // Return a parse error since serde_yaml::Error doesn't have a constructor
                return Err(serde_yaml::from_str::<()>(&format!(
                    "invalid: first step must be 'scan', got {:?}",
                    s
                ))
                .unwrap_err());
            }
        });
    }

    cur.ok_or_else(|| serde_yaml::from_str::<()>("invalid: empty pipeline").unwrap_err())
}
