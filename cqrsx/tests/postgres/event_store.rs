//! Event repository persistence, snapshots, streams and concurrent writes.
use super::*;
#[path = "event_store/concurrency.rs"]
mod concurrency;
#[path = "event_store/feed.rs"]
mod feed;
#[path = "event_store/persistence.rs"]
mod persistence;
#[path = "event_store/snapshots.rs"]
mod snapshots;
#[path = "event_store/streams.rs"]
mod streams;
#[path = "event_store/totals.rs"]
mod totals;

#[path = "event_store/repository.rs"]
mod repository;
