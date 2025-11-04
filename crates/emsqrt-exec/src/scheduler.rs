//! DAG scheduler primitives (starter).
//!
//! The initial engine runs sequentially to keep things simple. This module
//! sketches bounded-channel types for future parallelization.

use std::collections::VecDeque;

/// A tiny bounded queue used as a placeholder for future mpsc channels.
/// Replace with `tokio::sync::mpsc` or crossbeam once we go async.
pub struct BoundedQueue<T> {
    cap: usize,
    q: VecDeque<T>,
}

impl<T> BoundedQueue<T> {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            q: VecDeque::new(),
        }
    }

    pub fn try_push(&mut self, v: T) -> Result<(), T> {
        if self.q.len() >= self.cap {
            Err(v)
        } else {
            self.q.push_back(v);
            Ok(())
        }
    }

    pub fn try_pop(&mut self) -> Option<T> {
        self.q.pop_front()
    }

    pub fn len(&self) -> usize {
        self.q.len()
    }
    pub fn is_empty(&self) -> bool {
        self.q.is_empty()
    }
}
