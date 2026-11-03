//! Event repository persistence, snapshots, histories, the feed and concurrent writes.
use super::*;
#[path = "event_repository/concurrency.rs"]
mod concurrency;
#[path = "event_repository/feed.rs"]
mod feed;
#[path = "event_repository/history.rs"]
mod history;
#[path = "event_repository/persistence.rs"]
mod persistence;
#[path = "event_repository/snapshots.rs"]
mod snapshots;
#[path = "event_repository/totals.rs"]
mod totals;

#[path = "event_repository/repository.rs"]
mod repository;
