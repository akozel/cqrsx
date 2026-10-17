//! What every read model shares: event-handler decorators (activity notifications after a live
//! update, progress and slow motion during a replay) and maintenance (wipe, rebuild, verify).
pub mod maintenance;
pub mod notifying;
pub mod progress;
