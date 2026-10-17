//! Rebuild lock (Phase 3). The full `RebuildService` (wipe, two replay passes, progress) arrives in
//! Phase 6; the write side only needs to know whether the box office is "closed for a moment".
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Default)]
pub struct RebuildService {
    locked: AtomicBool,
}

impl RebuildService {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_locked(&self) -> bool {
        self.locked.load(Ordering::SeqCst)
    }
}
