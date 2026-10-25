//! Event repository persistence, snapshots, histories, the feed and concurrent writes.
use super::*;
#[path = "event_store/concurrency.rs"]
mod concurrency;
#[path = "event_store/feed.rs"]
mod feed;
#[path = "event_store/history.rs"]
mod history;
#[path = "event_store/persistence.rs"]
mod persistence;
#[path = "event_store/snapshots.rs"]
mod snapshots;
#[path = "event_store/totals.rs"]
mod totals;

#[path = "event_store/repository.rs"]
mod repository;
