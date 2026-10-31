//! An in-memory `CheckpointStore` with the rules of the PostgreSQL one: for tests of a projection
//! processor without a database.
use crate::{
    cqrs::projection::checkpoint::{
        CheckpointError, CheckpointKey, CheckpointStatus, CheckpointStore, Claimed, LeaseToken, RebuildRequest, RebuildRequests,
        RebuildState,
    },
    journal::feed::Position,
    locking::lock,
};
use chrono::{DateTime, TimeDelta, Utc};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
use tokio::time::Instant;
use uuid::Uuid;

#[derive(Debug)]
struct Lease {
    owner: String,
    token: LeaseToken,
    /// Decides claims, by the test clock.
    until: Instant,
    /// What the status shows: the wall clock when the lease was granted plus its length.
    until_wall: DateTime<Utc>,
}

#[derive(Debug)]
struct Row {
    position: Position,
    applied: u64,
    rebuild_requested: bool,
    pace: Duration,
    cursor: Option<String>,
    rebuilt: u64,
    lease: Option<Lease>,
    failures: u32,
    last_error: Option<String>,
    updated_at: DateTime<Utc>,
}

impl Row {
    fn owned_by(&self, token: LeaseToken) -> bool {
        self.lease.as_ref().is_some_and(|lease| lease.token == token)
    }
}

#[derive(Debug, Default)]
struct State {
    rows: BTreeMap<CheckpointKey, Row>,
    /// Requested keys per row in request order.
    keys: BTreeMap<CheckpointKey, Vec<(u64, String)>>,
    next_request: u64,
    unavailable: bool,
}

/// Checkpoint rows in memory with the rules of `postgres::checkpoint_store::PostgresCheckpointStore`
/// by meaning: a claim takes a free or expired lease (by the clock of `tokio::time`, so a paused test clock
/// drives it), every write is fenced by the token, `advance` and `advance_rebuild` move only forward and
/// accept a repeat, a rebuild request is refused while one is requested or running, and a request for a
/// projection nobody registered is `Invalid`.
///
/// Clones share the rows. For tests: [`set_unavailable`](Self::set_unavailable) makes every operation fail
/// with `CheckpointError::Storage`, [`expire_lease`](Self::expire_lease) lets another owner take a row at once,
/// [`clear`](Self::clear) drops every row as a dropped collection would.
///
/// ```
/// use cqrsx::cqrs::{projection::checkpoint::{CheckpointKey, CheckpointStore}, testing::mem_checkpoint_store::MemCheckpointStore};
/// use std::time::Duration;
///
/// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
/// let store = MemCheckpointStore::default();
/// let key = CheckpointKey::new("halls", "Concert");
/// store.register(&key).await.unwrap();
/// let claimed = store.claim(&key, "node-a", Duration::from_secs(10)).await.unwrap().unwrap();
/// assert!(store.claim(&key, "node-b", Duration::from_secs(10)).await.unwrap().is_none(), "held by node-a");
/// store.release(&key, claimed.token).await.unwrap();
/// assert!(store.claim(&key, "node-b", Duration::from_secs(10)).await.unwrap().is_some());
/// # });
/// ```
#[derive(Clone, Debug, Default)]
pub struct MemCheckpointStore {
    state: Arc<Mutex<State>>,
}

/// Milliseconds as the PostgreSQL row stores them: an `integer` for the pace.
fn pace_fits(pace: Duration) -> Result<(), CheckpointError> {
    i32::try_from(pace.as_millis())
        .map(|_| ())
        .map_err(|_| CheckpointError::Invalid("rebuild pace does not fit the row"))
}

impl MemCheckpointStore {
    /// Every operation fails with `CheckpointError::Storage` while `unavailable` is set.
    pub fn set_unavailable(&self, unavailable: bool) {
        self.state().unavailable = unavailable;
    }

    /// Ends the lease of the row now, so the next claim by anybody succeeds; the old token keeps failing
    /// its fenced writes once somebody else claimed.
    pub fn expire_lease(&self, key: &CheckpointKey) {
        if let Some(row) = self.state().rows.get_mut(key)
            && let Some(lease) = &mut row.lease
        {
            let now = Instant::now();
            lease.until = now.checked_sub(Duration::from_millis(1)).unwrap_or(now);
            lease.until_wall = Utc::now();
        }
    }

    /// Drops every row with its requested keys, as dropping the read side would: the old tokens own nothing,
    /// and the next `register` creates the row again at [`Position::START`].
    pub fn clear(&self) {
        let mut state = self.state();
        state.rows.clear();
        state.keys.clear();
    }

    fn state(&self) -> MutexGuard<'_, State> {
        lock(&self.state, "in-memory checkpoints")
    }

    /// The state, unless the store is unavailable.
    fn available(&self) -> Result<MutexGuard<'_, State>, CheckpointError> {
        let state = self.state();
        if state.unavailable {
            return Err(CheckpointError::Storage("checkpoint store unavailable".into()));
        }
        Ok(state)
    }

    /// Runs `write` on the row when `token` owns it; `false` otherwise or without a row.
    fn fenced(
        &self,
        key: &CheckpointKey,
        token: LeaseToken,
        write: impl FnOnce(&mut Row) -> bool,
    ) -> Result<bool, CheckpointError> {
        let mut state = self.available()?;
        let Some(row) = state.rows.get_mut(key) else {
            return Ok(false);
        };
        if !row.owned_by(token) {
            return Ok(false);
        }
        let changed = write(row);
        if changed {
            row.updated_at = Utc::now();
        }
        Ok(changed)
    }
}

impl CheckpointStore for MemCheckpointStore {
    async fn register(&self, key: &CheckpointKey) -> Result<(), CheckpointError> {
        self.available()?.rows.entry(key.clone()).or_insert_with(|| Row {
            position: Position::START,
            applied: 0,
            rebuild_requested: false,
            pace: Duration::ZERO,
            cursor: None,
            rebuilt: 0,
            lease: None,
            failures: 0,
            last_error: None,
            updated_at: Utc::now(),
        });
        Ok(())
    }

    async fn claim(&self, key: &CheckpointKey, owner: &str, lease: Duration) -> Result<Option<Claimed>, CheckpointError> {
        let mut state = self.available()?;
        let Some(row) = state.rows.get_mut(key) else {
            return Ok(None);
        };
        let now = Instant::now();
        if row.lease.as_ref().is_some_and(|held| held.until >= now) {
            return Ok(None);
        }
        let token = LeaseToken::new(Uuid::new_v4());
        row.lease = Some(Lease {
            owner: owner.to_owned(),
            token,
            until: now + lease,
            until_wall: Utc::now() + TimeDelta::from_std(lease).unwrap_or_default(),
        });
        row.updated_at = Utc::now();
        Ok(Some(Claimed {
            token,
            position: row.position,
            failures: row.failures,
            rebuild: row.cursor.clone().map(|cursor| RebuildState {
                cursor,
                pace: row.pace,
                rebuilt: row.rebuilt,
            }),
        }))
    }

    async fn extend(&self, key: &CheckpointKey, token: LeaseToken, lease: Duration) -> Result<bool, CheckpointError> {
        let until = Instant::now() + lease;
        let until_wall = Utc::now() + TimeDelta::from_std(lease).unwrap_or_default();
        self.fenced(key, token, |row| {
            if let Some(held) = &mut row.lease {
                held.until = until;
                held.until_wall = until_wall;
            }
            true
        })
    }

    async fn advance(&self, key: &CheckpointKey, token: LeaseToken, to: Position, applied: u64) -> Result<bool, CheckpointError> {
        self.fenced(key, token, |row| {
            if row.position > to {
                return false;
            }
            if row.position < to {
                row.applied += applied;
            }
            row.position = to;
            row.failures = 0;
            row.last_error = None;
            true
        })
    }

    async fn record_failure(&self, key: &CheckpointKey, token: LeaseToken, error: &str) -> Result<bool, CheckpointError> {
        self.fenced(key, token, |row| {
            row.failures += 1;
            row.last_error = Some(error.to_owned());
            true
        })
    }

    async fn request_rebuild(&self, key: &CheckpointKey, pace: Duration) -> Result<RebuildRequest, CheckpointError> {
        pace_fits(pace)?;
        let mut state = self.available()?;
        let Some(row) = state.rows.get_mut(key) else {
            return Err(CheckpointError::Invalid("projection is not registered"));
        };
        if row.rebuild_requested || row.cursor.is_some() {
            return Ok(RebuildRequest::AlreadyRunning);
        }
        row.rebuild_requested = true;
        row.pace = pace;
        row.updated_at = Utc::now();
        Ok(RebuildRequest::Accepted)
    }

    async fn request_rebuild_key(&self, key: &CheckpointKey, aggregate_id: &str) -> Result<(), CheckpointError> {
        let mut state = self.available()?;
        if !state.rows.contains_key(key) {
            return Err(CheckpointError::Invalid("projection is not registered"));
        }
        let number = state.next_request;
        let requests = state.keys.entry(key.clone()).or_default();
        if !requests.iter().any(|(_, requested)| requested == aggregate_id) {
            requests.push((number, aggregate_id.to_owned()));
            state.next_request += 1;
        }
        Ok(())
    }

    async fn take_requests(
        &self,
        key: &CheckpointKey,
        token: LeaseToken,
        limit: usize,
    ) -> Result<Option<RebuildRequests>, CheckpointError> {
        let mut state = self.available()?;
        let Some(row) = state.rows.get(key) else {
            return Ok(None);
        };
        if !row.owned_by(token) {
            return Ok(None);
        }
        let rebuild = row.rebuild_requested.then_some(row.pace);
        let requests = state.keys.entry(key.clone()).or_default();
        requests.sort();
        let taken = requests.drain(..limit.min(requests.len())).map(|(_, key)| key).collect();
        Ok(Some(RebuildRequests { rebuild, keys: taken }))
    }

    async fn begin_rebuild(&self, key: &CheckpointKey, token: LeaseToken) -> Result<bool, CheckpointError> {
        self.fenced(key, token, |row| {
            row.rebuild_requested = false;
            row.cursor = Some(String::new());
            row.rebuilt = 0;
            true
        })
    }

    async fn advance_rebuild(
        &self,
        key: &CheckpointKey,
        token: LeaseToken,
        cursor: &str,
        keys: u64,
    ) -> Result<bool, CheckpointError> {
        self.fenced(key, token, |row| {
            let Some(current) = &row.cursor else {
                return false;
            };
            if current.as_str() > cursor {
                return false;
            }
            if current.as_str() < cursor {
                row.rebuilt += keys;
            }
            row.cursor = Some(cursor.to_owned());
            true
        })
    }

    async fn finish_rebuild(&self, key: &CheckpointKey, token: LeaseToken) -> Result<bool, CheckpointError> {
        self.fenced(key, token, |row| {
            row.cursor = None;
            true
        })
    }

    async fn release(&self, key: &CheckpointKey, token: LeaseToken) -> Result<(), CheckpointError> {
        self.fenced(key, token, |row| {
            row.lease = None;
            true
        })?;
        Ok(())
    }

    async fn status(&self, projection: Option<&str>) -> Result<Vec<CheckpointStatus>, CheckpointError> {
        let state = self.available()?;
        Ok(state
            .rows
            .iter()
            .filter(|(key, _)| projection.is_none_or(|wanted| wanted == key.projection()))
            .map(|(key, row)| CheckpointStatus {
                projection: key.projection().to_owned(),
                aggregate_type: key.aggregate_type().to_owned(),
                position: row.position,
                applied: row.applied,
                rebuild_requested: row.rebuild_requested,
                rebuild_pace: row.pace,
                rebuild_cursor: row.cursor.clone(),
                rebuilt: row.rebuilt,
                owner: row.lease.as_ref().map(|lease| lease.owner.clone()),
                locked_until: row.lease.as_ref().map(|lease| lease.until_wall),
                failures: row.failures,
                last_error: row.last_error.clone(),
                updated_at: row.updated_at,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> CheckpointKey {
        CheckpointKey::new("halls", "Concert")
    }

    fn other() -> CheckpointKey {
        CheckpointKey::new("wallets", "Fan")
    }

    const LEASE: Duration = Duration::from_secs(1);

    async fn claimed(store: &MemCheckpointStore, owner: &str) -> Claimed {
        store.claim(&key(), owner, LEASE).await.unwrap().expect("a free row")
    }

    #[tokio::test(start_paused = true)]
    async fn a_row_is_registered_once_claimed_by_one_owner_and_released_or_expires() {
        let store = MemCheckpointStore::default();
        assert!(store.claim(&key(), "a", LEASE).await.unwrap().is_none(), "no row yet");
        store.register(&key()).await.unwrap();
        store.register(&key()).await.unwrap();
        let first = claimed(&store, "a").await;
        assert_eq!((first.position, first.failures, first.rebuild), (Position::START, 0, None));
        assert!(store.claim(&key(), "b", LEASE).await.unwrap().is_none(), "held");
        tokio::time::advance(LEASE).await;
        assert!(
            store.claim(&key(), "b", LEASE).await.unwrap().is_none(),
            "not expired at the exact end"
        );
        tokio::time::advance(Duration::from_millis(1)).await;
        let second = claimed(&store, "b").await;
        assert_ne!(first.token, second.token);
        assert!(
            !store.extend(&key(), first.token, LEASE).await.unwrap(),
            "the old token is fenced out"
        );
        assert!(store.extend(&key(), second.token, LEASE).await.unwrap());
        tokio::time::advance(LEASE).await;
        assert!(store.claim(&key(), "a", LEASE).await.unwrap().is_none(), "extended");
        store.release(&key(), first.token).await.unwrap(); // a stale release changes nothing
        assert!(store.claim(&key(), "a", LEASE).await.unwrap().is_none());
        store.release(&key(), second.token).await.unwrap();
        let third = claimed(&store, "a").await;
        store.expire_lease(&key());
        let fourth = claimed(&store, "c").await;
        assert!(!store.advance(&key(), third.token, Position::new(1), 1).await.unwrap());
        assert!(store.advance(&key(), fourth.token, Position::new(1), 1).await.unwrap());
        store.expire_lease(&other()); // no row: nothing happens
    }

    #[tokio::test(start_paused = true)]
    async fn advance_moves_only_forward_counts_once_and_clears_the_failures() {
        let store = MemCheckpointStore::default();
        store.register(&key()).await.unwrap();
        let owner = claimed(&store, "a").await;
        assert!(store.record_failure(&key(), owner.token, "halls: connection").await.unwrap());
        assert!(store.record_failure(&key(), owner.token, "halls: connection").await.unwrap());
        let row = &store.status(Some("halls")).await.unwrap()[0];
        assert_eq!((row.failures, row.last_error.as_deref()), (2, Some("halls: connection")));
        assert!(store.advance(&key(), owner.token, Position::new(5), 7).await.unwrap());
        assert!(
            store.advance(&key(), owner.token, Position::new(5), 7).await.unwrap(),
            "a repeat is accepted"
        );
        assert!(
            !store.advance(&key(), owner.token, Position::new(4), 1).await.unwrap(),
            "never back"
        );
        assert!(store.advance(&key(), owner.token, Position::new(9), 2).await.unwrap());
        let row = &store.status(None).await.unwrap()[0];
        assert_eq!(
            (row.position, row.applied, row.failures, row.last_error.clone()),
            (Position::new(9), 9, 0, None)
        );
        assert_eq!(
            (row.owner.as_deref(), row.projection.as_str(), row.aggregate_type.as_str()),
            (Some("a"), "halls", "Concert")
        );
        assert!(row.locked_until.unwrap() > Utc::now());
        let stale = LeaseToken::new(Uuid::new_v4());
        assert!(!store.record_failure(&key(), stale, "x").await.unwrap());
        assert!(
            !store.advance(&other(), owner.token, Position::new(1), 1).await.unwrap(),
            "no row"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn rebuild_requests_run_one_at_a_time_and_keys_are_taken_in_request_order_by_the_owner() {
        let store = MemCheckpointStore::default();
        assert!(matches!(
            store.request_rebuild(&key(), Duration::ZERO).await,
            Err(CheckpointError::Invalid("projection is not registered"))
        ));
        assert!(matches!(
            store.request_rebuild_key(&key(), "c1").await,
            Err(CheckpointError::Invalid("projection is not registered"))
        ));
        store.register(&key()).await.unwrap();
        assert!(matches!(
            store
                .request_rebuild(&key(), Duration::from_millis(u64::from(u32::MAX)))
                .await,
            Err(CheckpointError::Invalid(_))
        ));
        assert_eq!(
            store.request_rebuild(&key(), Duration::from_millis(80)).await.unwrap(),
            RebuildRequest::Accepted
        );
        assert_eq!(
            store.request_rebuild(&key(), Duration::ZERO).await.unwrap(),
            RebuildRequest::AlreadyRunning
        );
        for id in ["c2", "c1", "c2", "c3"] {
            store.request_rebuild_key(&key(), id).await.unwrap();
        }
        let owner = claimed(&store, "a").await;
        let stale = LeaseToken::new(Uuid::new_v4());
        assert!(store.take_requests(&key(), stale, 10).await.unwrap().is_none());
        assert!(store.take_requests(&other(), owner.token, 10).await.unwrap().is_none());
        let first = store.take_requests(&key(), owner.token, 2).await.unwrap().unwrap();
        assert_eq!(
            first,
            RebuildRequests {
                rebuild: Some(Duration::from_millis(80)),
                keys: vec!["c2".into(), "c1".into()]
            }
        );
        assert!(store.begin_rebuild(&key(), owner.token).await.unwrap());
        let rest = store.take_requests(&key(), owner.token, 2).await.unwrap().unwrap();
        assert_eq!(
            rest,
            RebuildRequests {
                rebuild: None,
                keys: vec!["c3".into()]
            }
        );
        assert_eq!(
            store.request_rebuild(&key(), Duration::ZERO).await.unwrap(),
            RebuildRequest::AlreadyRunning,
            "running"
        );
        assert!(store.advance_rebuild(&key(), owner.token, "c2", 2).await.unwrap());
        assert!(
            store.advance_rebuild(&key(), owner.token, "c2", 2).await.unwrap(),
            "a repeat counts once"
        );
        assert!(
            !store.advance_rebuild(&key(), owner.token, "c1", 1).await.unwrap(),
            "never back"
        );
        assert!(store.advance_rebuild(&key(), owner.token, "c3", 1).await.unwrap());
        let row = &store.status(None).await.unwrap()[0];
        assert_eq!(
            (
                row.rebuild_cursor.as_deref(),
                row.rebuilt,
                row.rebuild_requested,
                row.rebuild_pace
            ),
            (Some("c3"), 3, false, Duration::from_millis(80))
        );
        store.release(&key(), owner.token).await.unwrap();
        let next = claimed(&store, "b").await;
        assert_eq!(
            next.rebuild,
            Some(RebuildState {
                cursor: "c3".into(),
                pace: Duration::from_millis(80),
                rebuilt: 3
            }),
            "a new owner continues from the cursor"
        );
        assert!(store.finish_rebuild(&key(), next.token).await.unwrap());
        assert!(
            !store.advance_rebuild(&key(), next.token, "c4", 1).await.unwrap(),
            "no rebuild runs"
        );
        assert_eq!(
            store.request_rebuild(&key(), Duration::ZERO).await.unwrap(),
            RebuildRequest::Accepted
        );
        assert!(!store.begin_rebuild(&key(), stale).await.unwrap());
        assert!(!store.finish_rebuild(&key(), stale).await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn status_lists_rows_in_order_and_an_unavailable_store_fails_every_operation() {
        let store = MemCheckpointStore::default();
        store.register(&other()).await.unwrap();
        store.register(&key()).await.unwrap();
        let names: Vec<_> = store
            .status(None)
            .await
            .unwrap()
            .into_iter()
            .map(|row| (row.projection, row.aggregate_type, row.owner, row.locked_until))
            .collect();
        assert_eq!(
            names,
            [
                ("halls".into(), "Concert".into(), None, None),
                ("wallets".into(), "Fan".into(), None, None)
            ]
        );
        assert_eq!(store.status(Some("wallets")).await.unwrap().len(), 1);
        assert!(store.status(Some("none")).await.unwrap().is_empty());
        let shared = store.clone();
        shared.set_unavailable(true);
        let token = LeaseToken::new(Uuid::new_v4());
        let errors = [
            store.register(&key()).await.err(),
            store.claim(&key(), "a", LEASE).await.err(),
            store.extend(&key(), token, LEASE).await.err(),
            store.advance(&key(), token, Position::START, 0).await.err(),
            store.record_failure(&key(), token, "x").await.err(),
            store.request_rebuild(&key(), Duration::ZERO).await.err(),
            store.request_rebuild_key(&key(), "k").await.err(),
            store.take_requests(&key(), token, 1).await.err(),
            store.begin_rebuild(&key(), token).await.err(),
            store.advance_rebuild(&key(), token, "k", 1).await.err(),
            store.finish_rebuild(&key(), token).await.err(),
            store.release(&key(), token).await.err(),
            store.status(None).await.err(),
        ];
        assert!(errors.iter().all(|error| matches!(error, Some(CheckpointError::Storage(_)))));
        shared.set_unavailable(false);
        assert_eq!(store.status(None).await.unwrap().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_cleared_store_forgets_every_row_and_fences_the_old_owner_out() {
        let store = MemCheckpointStore::default();
        store.register(&key()).await.unwrap();
        store.register(&other()).await.unwrap();
        store.request_rebuild_key(&key(), "c1").await.unwrap();
        let owner = claimed(&store, "a").await;
        assert!(store.advance(&key(), owner.token, Position::new(5), 5).await.unwrap());
        store.clear();
        assert!(store.status(None).await.unwrap().is_empty());
        assert!(!store.extend(&key(), owner.token, LEASE).await.unwrap());
        assert!(!store.advance(&key(), owner.token, Position::new(6), 1).await.unwrap());
        assert!(store.take_requests(&key(), owner.token, 10).await.unwrap().is_none());
        assert!(
            store.claim(&key(), "b", LEASE).await.unwrap().is_none(),
            "no row until it is registered"
        );
        store.register(&key()).await.unwrap();
        let next = claimed(&store, "b").await;
        assert_eq!((next.position, next.failures, next.rebuild), (Position::START, 0, None));
        let requests = store.take_requests(&key(), next.token, 10).await.unwrap().unwrap();
        assert!(requests.keys.is_empty(), "the requested keys went with the row");
    }
}
