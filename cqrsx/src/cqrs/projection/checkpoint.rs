//! The checkpoint of a projection: one row per projection and aggregate type that remembers how far the
//! projection has read the journal of that type, who owns it right now and the state of a rebuild.
//!
//! A `ProjectionProcessor` claims the row under a lease ([`CheckpointStore::claim`]), extends the lease
//! while it works, moves the position after every page it has applied and releases the row when it stops.
//! Every write of an owner carries its [`LeaseToken`]; a write that finds another token (`false` or `None`)
//! means the lease was lost, and the owner stops at once. The lease compares against the clock of the
//! store, so the clocks of the nodes do not matter; an owner keeps a conservative local deadline anchored
//! before each request, as the task queue does.
use crate::cqrs::{aggregate::Aggregate, projection::View, store::feed::Position};
use chrono::{DateTime, Utc};
use std::{error::Error, future::Future, time::Duration};
use thiserror::Error;
use uuid::Uuid;

/// The row of one projection over one aggregate type; ordered by the projection, then the aggregate type.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CheckpointKey {
    projection: String,
    aggregate_type: &'static str,
}

impl CheckpointKey {
    /// `projection` names the projection (its handler name, e.g. a collection); `aggregate_type` is the
    /// `Aggregate::TYPE` of the journal it reads.
    pub fn new(projection: impl Into<String>, aggregate_type: &'static str) -> Self {
        Self {
            projection: projection.into(),
            aggregate_type,
        }
    }

    /// The row of the projection of the view `V` over the journal of `A`: the view's name and the aggregate's type.
    pub fn of<V: View<A>, A: Aggregate>() -> Self {
        Self::new(V::NAME, A::TYPE)
    }

    pub fn projection(&self) -> &str {
        &self.projection
    }

    pub fn aggregate_type(&self) -> &'static str {
        self.aggregate_type
    }
}

/// The credential of one lease: every write of the owner carries it, and a write that meets another token
/// is refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LeaseToken(Uuid);

impl LeaseToken {
    pub const fn new(value: Uuid) -> Self {
        Self(value)
    }

    pub const fn value(self) -> Uuid {
        self.0
    }
}

/// What a successful claim hands the new owner: where to continue, how many failures in a row the row
/// counts, and a rebuild that was begun and not finished.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Claimed {
    pub token: LeaseToken,
    /// Everything up to and including this position is applied.
    pub position: Position,
    /// Failed rounds since the last advance; the owner continues the backoff from here.
    pub failures: u32,
    pub rebuild: Option<RebuildState>,
}

/// A rebuild in progress: the last key whose rebuild is recorded (`""` right after the start), the pause
/// between events of one key and the keys rebuilt so far.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RebuildState {
    pub cursor: String,
    pub pace: Duration,
    pub rebuilt: u64,
}

/// The answer to a rebuild request: one rebuild at a time per row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RebuildRequest {
    Accepted,
    AlreadyRunning,
}

/// What the owner takes from the row on one poll: a requested rebuild of the whole journal with its pace,
/// and the keys requested one by one (removed from the row by the take).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RebuildRequests {
    pub rebuild: Option<Duration>,
    pub keys: Vec<String>,
}

/// The row as an operator sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointStatus {
    pub projection: String,
    pub aggregate_type: String,
    pub position: Position,
    /// Events delivered live since the row was created.
    pub applied: u64,
    pub rebuild_requested: bool,
    pub rebuild_pace: Duration,
    /// `None`: no rebuild runs; `Some("")`: one has begun; otherwise the last key whose rebuild is recorded.
    pub rebuild_cursor: Option<String>,
    /// Keys rebuilt by the running or the last rebuild.
    pub rebuilt: u64,
    pub owner: Option<String>,
    pub locked_until: Option<DateTime<Utc>>,
    pub failures: u32,
    pub last_error: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum CheckpointError {
    /// The store could not be reached or refused the statement; the operation may or may not have happened.
    #[error("checkpoint storage failed: {0}")]
    Storage(#[source] Box<dyn Error + Send + Sync + 'static>),
    /// A row holds a value this version cannot read.
    #[error("checkpoint row is corrupt: {0}")]
    Corrupt(#[source] Box<dyn Error + Send + Sync + 'static>),
    /// The request itself is wrong: a pace beyond the row's range, a projection nobody registered.
    #[error("invalid checkpoint request: {0}")]
    Invalid(&'static str),
}

impl CheckpointError {
    /// A fixed word for logs and the row's `last_error`.
    pub fn category(&self) -> &'static str {
        match self {
            Self::Storage(_) => "storage",
            Self::Corrupt(_) => "corrupt",
            Self::Invalid(_) => "invalid",
        }
    }
}

/// The checkpoint rows of a store. Every operation is one statement (on PostgreSQL one autocommitted
/// statement under the pool's `StatementTimeout`), and everything that writes a row is fenced by the lease
/// token: `false` or `None` from such an operation means the lease is lost. `advance` and `advance_rebuild`
/// are idempotent, so an owner that lost the reply may repeat them.
pub trait CheckpointStore: Send + Sync {
    /// Creates the row at [`Position::START`] when it does not exist yet; an existing row stays as it is.
    fn register(&self, key: &CheckpointKey) -> impl Future<Output = Result<(), CheckpointError>> + Send;

    /// Takes the row when nobody holds it or the holder's lease has run out (by the store's clock), for
    /// `lease` from now; `None` when another owner holds it.
    fn claim(
        &self,
        key: &CheckpointKey,
        owner: &str,
        lease: Duration,
    ) -> impl Future<Output = Result<Option<Claimed>, CheckpointError>> + Send;

    /// Renews the lease for `lease` from now; `false` when the token no longer owns the row.
    fn extend(
        &self,
        key: &CheckpointKey,
        token: LeaseToken,
        lease: Duration,
    ) -> impl Future<Output = Result<bool, CheckpointError>> + Send;

    /// Records that everything up to `to` is applied and `applied` more events were delivered; clears the
    /// failure count. Only forward: a position already reached is accepted without a change, one behind the
    /// row's is refused like a lost lease.
    fn advance(
        &self,
        key: &CheckpointKey,
        token: LeaseToken,
        to: Position,
        applied: u64,
    ) -> impl Future<Output = Result<bool, CheckpointError>> + Send;

    /// Counts one more failed round and keeps `error` (a category and a handler name, never a payload).
    fn record_failure(
        &self,
        key: &CheckpointKey,
        token: LeaseToken,
        error: &str,
    ) -> impl Future<Output = Result<bool, CheckpointError>> + Send;

    /// Asks the owner, whoever it is or will be, to rebuild every document of the projection with `pace`
    /// between the events of one key; one rebuild at a time.
    fn request_rebuild(
        &self,
        key: &CheckpointKey,
        pace: Duration,
    ) -> impl Future<Output = Result<RebuildRequest, CheckpointError>> + Send;

    /// Asks the owner to rebuild the document of one aggregate; a repeated request for the same key is one.
    fn request_rebuild_key(
        &self,
        key: &CheckpointKey,
        aggregate_id: &str,
    ) -> impl Future<Output = Result<(), CheckpointError>> + Send;

    /// What was requested: the rebuild flag with its pace and up to `limit` requested keys in request
    /// order, which the take removes. `None` when the token no longer owns the row; then nothing is removed.
    fn take_requests(
        &self,
        key: &CheckpointKey,
        token: LeaseToken,
        limit: usize,
    ) -> impl Future<Output = Result<Option<RebuildRequests>, CheckpointError>> + Send;

    /// Starts the requested rebuild: the flag is cleared, the cursor is `""`, the count is zero.
    fn begin_rebuild(&self, key: &CheckpointKey, token: LeaseToken)
    -> impl Future<Output = Result<bool, CheckpointError>> + Send;

    /// Records a page of rebuilt keys: the cursor moves to `cursor` and `keys` are counted, once per cursor.
    fn advance_rebuild(
        &self,
        key: &CheckpointKey,
        token: LeaseToken,
        cursor: &str,
        keys: u64,
    ) -> impl Future<Output = Result<bool, CheckpointError>> + Send;

    /// Ends the rebuild: the cursor is cleared, the count stays for the status.
    fn finish_rebuild(
        &self,
        key: &CheckpointKey,
        token: LeaseToken,
    ) -> impl Future<Output = Result<bool, CheckpointError>> + Send;

    /// Gives the row back at once, so the next claim need not wait for the lease to run out.
    fn release(&self, key: &CheckpointKey, token: LeaseToken) -> impl Future<Output = Result<(), CheckpointError>> + Send;

    /// The rows of one projection, or of all of them, ordered by projection and aggregate type.
    fn status(&self, projection: Option<&str>) -> impl Future<Output = Result<Vec<CheckpointStatus>, CheckpointError>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cqrs::{aggregate::tests::Ledger, event::EventEnvelope};
    use serde::{Deserialize, Serialize};
    use std::collections::HashSet;

    #[test]
    fn the_key_of_a_view_is_its_name_and_the_type_of_its_aggregate() {
        #[derive(Debug, Default, Serialize, Deserialize)]
        struct Wallets;
        impl View<Ledger> for Wallets {
            const NAME: &'static str = "wallets";
            fn update(&mut self, _: &EventEnvelope<Ledger>) {}
        }
        assert_eq!(
            CheckpointKey::of::<Wallets, Ledger>(),
            CheckpointKey::new("wallets", Ledger::TYPE)
        );
    }

    #[test]
    fn keys_tokens_and_requests_describe_themselves() {
        let key = CheckpointKey::new("halls", "Concert");
        assert_eq!((key.projection(), key.aggregate_type()), ("halls", "Concert"));
        assert_eq!(key, CheckpointKey::new(String::from("halls"), "Concert"));
        assert_ne!(key, CheckpointKey::new("halls", "Fan"));
        let keys: HashSet<CheckpointKey> = [key.clone(), key.clone()].into_iter().collect();
        assert_eq!(keys.len(), 1);
        let token = LeaseToken::new(Uuid::from_u128(7));
        assert_eq!(token.value(), Uuid::from_u128(7));
        assert_eq!(token, token);
        assert_eq!(
            RebuildRequests::default(),
            RebuildRequests {
                rebuild: None,
                keys: vec![]
            }
        );
        assert_ne!(RebuildRequest::Accepted, RebuildRequest::AlreadyRunning);
        assert!(
            format!(
                "{:?}",
                Claimed {
                    token,
                    position: Position::START,
                    failures: 0,
                    rebuild: Some(RebuildState {
                        cursor: String::new(),
                        pace: Duration::ZERO,
                        rebuilt: 0
                    })
                }
            )
            .contains("RebuildState")
        );
    }

    #[test]
    fn errors_name_their_category_and_keep_the_source() {
        let storage = CheckpointError::Storage(std::io::Error::other("private_source").into());
        assert_eq!(storage.category(), "storage");
        assert_eq!(storage.to_string(), "checkpoint storage failed: private_source");
        assert!(std::error::Error::source(&storage).is_some());
        let corrupt = CheckpointError::Corrupt("bad position".into());
        assert_eq!(
            (corrupt.category(), corrupt.to_string().as_str()),
            ("corrupt", "checkpoint row is corrupt: bad position")
        );
        let invalid = CheckpointError::Invalid("projection is not registered");
        assert_eq!(
            (invalid.category(), invalid.to_string().as_str()),
            ("invalid", "invalid checkpoint request: projection is not registered")
        );
        assert!(std::error::Error::source(&invalid).is_none());
    }
}
