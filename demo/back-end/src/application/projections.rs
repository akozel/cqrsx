//! What every read model shares: the event-handler decorator for activity notifications after a
//! live update, and maintenance (wipe, rebuild, verify; replay progress and slow motion come from
//! the cqrsx `EventReplay`).
pub mod maintenance;
pub mod notifying;
