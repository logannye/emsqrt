//! Minimal operator registry for planner/exec wiring.
//!
//! This is intentionally simple: it maps string keys to boxed operator instances.
//! Replace with a richer factory when adding config params per operator.

use std::collections::HashMap;

use crate::agregate::Aggregate;
use crate::filter::Filter;
use crate::map::Map;
use crate::project::Project;
use crate::traits::Operator;

pub struct Registry {
    makers: HashMap<&'static str, fn() -> Box<dyn Operator>>,
}

impl Registry {
    pub fn new() -> Self {
        let mut r = Self {
            makers: HashMap::new(),
        };
        r.register("filter", || Box::new(Filter::default()));
        r.register("map", || Box::new(Map::default()));
        r.register("project", || Box::new(Project::default()));
        r.register("aggregate", || Box::new(Aggregate::default()));
        r.register("sort_external", || Box::new(crate::sort::external::ExternalSort::default()));
        r.register("join_hash", || Box::new(crate::join::hash::HashJoin::default()));
        r.register("join_merge", || Box::new(crate::join::merge::MergeJoin::default()));
        r
    }

    pub fn register(&mut self, key: &'static str, f: fn() -> Box<dyn Operator>) {
        self.makers.insert(key, f);
    }

    pub fn make(&self, key: &str) -> Option<Box<dyn Operator>> {
        self.makers.get(key).map(|f| f())
    }
}
