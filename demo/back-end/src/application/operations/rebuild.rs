//! The read side as an operator sees it: Encore! (every read model rebuilt from the journal while the box
//! office stays open, D-16), the rebuild of one concert hall, and the status of the projections: where their
//! checkpoints stand, how far behind the journal they are and who owns them.
//!
//! A rebuild is a request on the checkpoint rows (`CheckpointStore::request_rebuild`). The owner of each row,
//! on whatever node, walks the keys of the journal and replaces the documents in place through the same key
//! workers as the live events, so commands keep running and nothing is wiped. This service only asks, then
//! watches the rows until both rebuilds have ended and reports what it saw.
use crate::application::{StorageError, activity::ActivityHub, ports::EventLedgerStats};
use async_trait::async_trait;
use chrono::{SecondsFormat, Utc};
use cqrsx::cqrs::projection::checkpoint::{CheckpointError, CheckpointKey, CheckpointStatus, CheckpointStore, RebuildRequest};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Duration,
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// How often a running Encore! reads the checkpoint rows for its progress and its end.
pub const REBUILD_WATCH_INTERVAL: Duration = Duration::from_millis(250);
/// The longest pause between the events of one key a rebuild may ask for (`ENCORE_REPLAY_MAX_PACE_MS`): a
/// longer one would hold its keys, and through the window the whole projection, for hours.
pub const DEFAULT_MAX_PACE: Duration = Duration::from_secs(1);
/// The pause between the events of one key a slow-motion Encore! asks for (`ENCORE_REPLAY_SLOW_DELAY_MS`): the
/// UI sends it as `pace_ms`, so the seats light up one by one.
pub const DEFAULT_SLOW_PACE: Duration = Duration::from_millis(80);
/// How long the status waits for the checkpoint store before it answers with the rows it saw last
/// (`ENCORE_PROJECTION_STATUS_TIMEOUT_MS`): a paused MongoDB must not hang the status page.
pub const DEFAULT_STATUS_TIMEOUT: Duration = Duration::from_secs(1);

/// The bounds of the read side's operations, from the configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadSideLimits {
    /// Events counted at most for one projection's lag.
    pub lag_cap: u64,
    pub max_pace: Duration,
    pub status_timeout: Duration,
}

/// Why a rebuild was not started.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RebuildRefused {
    #[error("a rebuild is already running")]
    AlreadyRunning,
    #[error("the pace must be at most {max_ms} ms")]
    PaceTooLong { max_ms: u64 },
    /// The checkpoint store failed, or the processors have not registered their rows yet.
    #[error("{0}")]
    Unavailable(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RebuildStarted {
    pub rebuild_id: Uuid,
    pub pace: Duration,
}

/// The read side for the HTTP layer, behind a port so the router tests run it over in-memory checkpoints.
#[async_trait]
pub trait ReadSide: Send + Sync {
    /// Every projection's checkpoint, lag and state, plus the rebuild this node started.
    async fn status(&self) -> Result<Value, StorageError>;
    /// Encore!: asks the owners to rebuild every read model with `pace` between the events of one key.
    async fn rebuild(&self, pace: Duration) -> Result<RebuildStarted, RebuildRefused>;
    /// Asks the owner of the concert halls to rebuild one hall from its history.
    async fn rebuild_concert(&self, concert_id: &str) -> Result<(), RebuildRefused>;
}

#[derive(Debug, Clone, Serialize)]
pub struct RebuildResult {
    pub rebuild_id: Uuid,
    /// Documents rebuilt, as the checkpoint rows count them.
    pub keys: u64,
    pub duration_ms: u64,
    pub finished_at: String,
}

#[derive(Debug, Clone)]
struct Running {
    rebuild_id: Uuid,
    pace: Duration,
    started_at: String,
    keys_total: u64,
}

#[derive(Default)]
struct State {
    running: Option<Running>,
    last_result: Option<RebuildResult>,
    /// The rows the last successful read returned, for a status while the store does not answer.
    last_rows: Vec<CheckpointStatus>,
}

/// [`ReadSide`] over the checkpoint rows of `C`.
pub struct RebuildService<C> {
    checkpoints: Arc<C>,
    /// Every read model's row, the concert halls first.
    read_models: Vec<CheckpointKey>,
    ledger: Arc<dyn EventLedgerStats>,
    activity: Arc<ActivityHub>,
    limits: ReadSideLimits,
    shutdown: CancellationToken,
    state: Arc<Mutex<State>>,
}

fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn lock(state: &Mutex<State>) -> MutexGuard<'_, State> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

fn is_row_of(key: &CheckpointKey, row: &CheckpointStatus) -> bool {
    row.projection == key.projection() && row.aggregate_type == key.aggregate_type()
}

fn is_rebuilding(row: &CheckpointStatus) -> bool {
    row.rebuild_requested || row.rebuild_cursor.is_some()
}

/// Documents rebuilt by the row's rebuild; a request its owner has not taken yet counts nothing (the count is
/// still the last rebuild's).
fn rebuilt(row: &CheckpointStatus) -> u64 {
    if row.rebuild_requested && row.rebuild_cursor.is_none() {
        0
    } else {
        row.rebuilt
    }
}

fn unavailable(error: CheckpointError) -> RebuildRefused {
    match error {
        CheckpointError::Invalid(_) => {
            RebuildRefused::Unavailable("the read side is starting: its checkpoints are not registered yet".into())
        }
        other => RebuildRefused::Unavailable(other.to_string()),
    }
}

/// The states of a projection, the worst first: the overall state is the worst of them.
const STATES: [&str; 5] = ["stalled", "unclaimed", "starting", "rebuilding", "live"];

impl<C: CheckpointStore + 'static> RebuildService<C> {
    /// `read_models` are the rows of every read model, the concert halls first.
    pub fn new(
        checkpoints: Arc<C>,
        read_models: Vec<CheckpointKey>,
        ledger: Arc<dyn EventLedgerStats>,
        activity: Arc<ActivityHub>,
        limits: ReadSideLimits,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            checkpoints,
            read_models,
            ledger,
            activity,
            limits,
            shutdown,
            state: Arc::default(),
        }
    }

    /// The rows of the read models, or the ones seen last when the store does not answer in time.
    async fn rows(&self) -> (Vec<CheckpointStatus>, bool) {
        match tokio::time::timeout(self.limits.status_timeout, self.checkpoints.status(None)).await {
            Ok(Ok(rows)) => {
                lock(&self.state).last_rows = rows.clone();
                (rows, false)
            }
            Ok(Err(_)) | Err(_) => (lock(&self.state).last_rows.clone(), true),
        }
    }

    async fn projection(
        &self,
        key: &CheckpointKey,
        row: Option<&CheckpointStatus>,
    ) -> Result<(&'static str, Value), StorageError> {
        let Some(row) = row else {
            let state = "starting";
            return Ok((
                state,
                json!({ "projection": key.projection(), "aggregate_type": key.aggregate_type(), "state": state }),
            ));
        };
        let lag = self
            .ledger
            .count_after(key.aggregate_type(), row.position, self.limits.lag_cap)
            .await?;
        let rebuilding = is_rebuilding(row);
        let claimed = row.owner.is_some() && row.locked_until.is_some_and(|until| until > Utc::now());
        let state = if row.failures > 0 && (lag > 0 || rebuilding) {
            "stalled"
        } else if !claimed {
            "unclaimed"
        } else if rebuilding {
            "rebuilding"
        } else {
            "live"
        };
        Ok((
            state,
            json!({
                "projection": row.projection,
                "aggregate_type": row.aggregate_type,
                "state": state,
                "position": row.position.to_string(),
                "applied": row.applied,
                "lag": lag,
                "lag_capped": lag >= self.limits.lag_cap,
                "owner": row.owner,
                "locked_until": row.locked_until.map(|until| until.to_rfc3339_opts(SecondsFormat::Millis, true)),
                "failures": row.failures,
                "last_error": row.last_error,
                "rebuild": {
                    "requested": row.rebuild_requested,
                    "cursor": row.rebuild_cursor,
                    "rebuilt": row.rebuilt,
                    "pace_ms": u64::try_from(row.rebuild_pace.as_millis()).unwrap_or(u64::MAX),
                },
            }),
        ))
    }
}

/// Watches the rows of a running Encore! until neither is requested nor running (a dropped row counts as
/// ended), publishing the progress as the rows count it and the end.
async fn watch<C: CheckpointStore>(
    checkpoints: Arc<C>,
    keys: Vec<CheckpointKey>,
    activity: Arc<ActivityHub>,
    state: Arc<Mutex<State>>,
    shutdown: CancellationToken,
    rebuild_id: Uuid,
) {
    let started = Instant::now();
    let mut reported = 0;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(REBUILD_WATCH_INTERVAL) => {}
        }
        let Ok(rows) = checkpoints.status(None).await else {
            continue;
        };
        let rows: Vec<&CheckpointStatus> = rows.iter().filter(|row| keys.iter().any(|key| is_row_of(key, row))).collect();
        let keys_done: u64 = rows.iter().map(|row| rebuilt(row)).sum();
        if keys_done != reported {
            reported = keys_done;
            activity.publish("rebuild.progress", json!({ "rebuild_id": rebuild_id, "keys": keys_done }));
        }
        if rows.iter().any(|row| is_rebuilding(row)) {
            continue;
        }
        let result = RebuildResult {
            rebuild_id,
            keys: keys_done,
            duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            finished_at: now(),
        };
        activity.publish(
            "rebuild.finished",
            json!({ "rebuild_id": rebuild_id, "keys": result.keys, "duration_ms": result.duration_ms }),
        );
        let mut state = lock(&state);
        state.running = None;
        state.last_result = Some(result);
        return;
    }
}

#[async_trait]
impl<C: CheckpointStore + 'static> ReadSide for RebuildService<C> {
    async fn status(&self) -> Result<Value, StorageError> {
        let (rows, stale) = self.rows().await;
        let mut projections = Vec::new();
        let mut worst = STATES.len() - 1;
        for key in &self.read_models {
            let (state, projection) = self.projection(key, rows.iter().find(|row| is_row_of(key, row))).await?;
            worst = worst.min(STATES.iter().position(|known| *known == state).unwrap_or(0));
            projections.push(projection);
        }
        let state = lock(&self.state);
        let running = state.running.as_ref().map(|running| {
            json!({
                "rebuild_id": running.rebuild_id,
                "pace_ms": u64::try_from(running.pace.as_millis()).unwrap_or(u64::MAX),
                "started_at": running.started_at,
                "keys_total": running.keys_total,
            })
        });
        Ok(json!({
            "state": STATES[worst],
            "stale": stale,
            "projections": projections,
            "rebuild": running,
            "last_result": state.last_result,
        }))
    }

    async fn rebuild(&self, pace: Duration) -> Result<RebuildStarted, RebuildRefused> {
        if pace > self.limits.max_pace {
            return Err(RebuildRefused::PaceTooLong {
                max_ms: u64::try_from(self.limits.max_pace.as_millis()).unwrap_or(u64::MAX),
            });
        }
        let mut accepted = false;
        for key in &self.read_models {
            match self.checkpoints.request_rebuild(key, pace).await.map_err(unavailable)? {
                RebuildRequest::Accepted => accepted = true,
                RebuildRequest::AlreadyRunning => {}
            }
        }
        if !accepted {
            return Err(RebuildRefused::AlreadyRunning);
        }
        // Progress figures only: a failed count is reported as zero.
        let totals = self.ledger.totals().await.unwrap_or_default();
        let rebuild_id = Uuid::now_v7();
        lock(&self.state).running = Some(Running {
            rebuild_id,
            pace,
            started_at: now(),
            keys_total: totals.aggregates,
        });
        self.activity.publish(
            "rebuild.started",
            json!({
                "rebuild_id": rebuild_id,
                "pace_ms": u64::try_from(pace.as_millis()).unwrap_or(u64::MAX),
                "keys_total": totals.aggregates,
                "events_total": totals.events,
            }),
        );
        tokio::spawn(watch(
            self.checkpoints.clone(),
            self.read_models.clone(),
            self.activity.clone(),
            self.state.clone(),
            self.shutdown.clone(),
            rebuild_id,
        ));
        Ok(RebuildStarted { rebuild_id, pace })
    }

    async fn rebuild_concert(&self, concert_id: &str) -> Result<(), RebuildRefused> {
        let Some(halls) = self.read_models.first() else {
            return Err(RebuildRefused::Unavailable("no read model to rebuild".into()));
        };
        self.checkpoints
            .request_rebuild_key(halls, concert_id)
            .await
            .map_err(unavailable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::ports::LedgerTotals;
    use cqrsx::cqrs::{store::feed::Position, testing::mem_checkpoint_store::MemCheckpointStore};
    use std::collections::HashMap;

    fn halls() -> CheckpointKey {
        CheckpointKey::new("concert_halls", "Concert")
    }

    fn wallets() -> CheckpointKey {
        CheckpointKey::new("fan_wallets", "Fan")
    }

    /// Fixed totals and a lag per aggregate type; `None` fails the count.
    struct Ledger(Mutex<HashMap<&'static str, Option<u64>>>);

    impl Ledger {
        fn new() -> Arc<Self> {
            Arc::new(Self(Mutex::new(HashMap::from([("Concert", Some(0)), ("Fan", Some(0))]))))
        }

        fn set_lag(&self, aggregate_type: &'static str, lag: Option<u64>) {
            self.0.lock().unwrap().insert(aggregate_type, lag);
        }
    }

    #[async_trait]
    impl EventLedgerStats for Ledger {
        async fn totals(&self) -> Result<LedgerTotals, StorageError> {
            Ok(LedgerTotals {
                events: 9,
                aggregates: 3,
            })
        }

        async fn count_after(&self, aggregate_type: &str, _: Position, cap: u64) -> Result<u64, StorageError> {
            match self.0.lock().unwrap().get(aggregate_type).copied().flatten() {
                Some(lag) => Ok(lag.min(cap)),
                None => Err(StorageError("journal unavailable".into())),
            }
        }
    }

    const LIMITS: ReadSideLimits = ReadSideLimits {
        lag_cap: 100,
        max_pace: Duration::from_secs(1),
        status_timeout: Duration::from_secs(1),
    };

    struct Setup {
        service: RebuildService<MemCheckpointStore>,
        store: MemCheckpointStore,
        ledger: Arc<Ledger>,
        activity: Arc<ActivityHub>,
    }

    async fn setup(registered: &[CheckpointKey]) -> Setup {
        let store = MemCheckpointStore::default();
        for key in registered {
            store.register(key).await.unwrap();
        }
        let ledger = Ledger::new();
        let activity = Arc::new(ActivityHub::new(64, 64));
        let service = RebuildService::new(
            Arc::new(store.clone()),
            vec![halls(), wallets()],
            ledger.clone(),
            activity.clone(),
            LIMITS,
            CancellationToken::new(),
        );
        Setup {
            service,
            store,
            ledger,
            activity,
        }
    }

    fn kinds(activity: &ActivityHub) -> Vec<String> {
        activity.subscribe().0.iter().map(|entry| entry.kind.clone()).collect()
    }

    /// Plays the owner of `key`: takes the request, rebuilds `keys` documents and finishes.
    async fn run_rebuild(store: &MemCheckpointStore, key: &CheckpointKey, keys: u64) {
        let owner = store.claim(key, "node-a", Duration::from_secs(10)).await.unwrap().unwrap();
        assert!(
            store
                .take_requests(key, owner.token, 10)
                .await
                .unwrap()
                .unwrap()
                .rebuild
                .is_some()
        );
        assert!(store.begin_rebuild(key, owner.token).await.unwrap());
        if keys > 0 {
            assert!(store.advance_rebuild(key, owner.token, "k", keys).await.unwrap());
        }
        assert!(store.finish_rebuild(key, owner.token).await.unwrap());
        store.release(key, owner.token).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_rebuild_asks_both_rows_and_is_refused_while_they_run_or_with_a_pace_too_long() {
        let s = setup(&[halls(), wallets()]).await;
        let started = s.service.rebuild(Duration::from_millis(80)).await.unwrap();
        assert_eq!(started.pace, Duration::from_millis(80));
        for row in s.store.status(None).await.unwrap() {
            assert_eq!((row.rebuild_requested, row.rebuild_pace), (true, Duration::from_millis(80)));
        }
        assert_eq!(s.service.rebuild(Duration::ZERO).await, Err(RebuildRefused::AlreadyRunning));
        assert_eq!(
            s.service.rebuild(Duration::from_millis(1001)).await,
            Err(RebuildRefused::PaceTooLong { max_ms: 1000 })
        );
        assert_eq!(kinds(&s.activity), ["rebuild.started"]);
        let started_entry = s.activity.subscribe().0[0].clone();
        assert_eq!(
            (
                started_entry.fields["pace_ms"].as_u64(),
                started_entry.fields["keys_total"].as_u64(),
                started_entry.fields["events_total"].as_u64()
            ),
            (Some(80), Some(3), Some(9))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn one_row_already_running_is_enough_and_a_missing_row_or_store_is_unavailable() {
        let s = setup(&[halls(), wallets()]).await;
        assert_eq!(
            s.store.request_rebuild(&wallets(), Duration::ZERO).await.unwrap(),
            RebuildRequest::Accepted
        );
        assert!(s.service.rebuild(Duration::ZERO).await.is_ok(), "the halls were accepted");
        let s = setup(&[]).await;
        assert!(matches!(
            s.service.rebuild(Duration::ZERO).await,
            Err(RebuildRefused::Unavailable(message)) if message.contains("starting")
        ));
        let s = setup(&[halls(), wallets()]).await;
        s.store.set_unavailable(true);
        assert!(matches!(
            s.service.rebuild(Duration::ZERO).await,
            Err(RebuildRefused::Unavailable(_))
        ));
        assert!(matches!(
            s.service.rebuild_concert("c-1").await,
            Err(RebuildRefused::Unavailable(_))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn one_concert_is_rebuilt_on_request_by_the_owner_of_the_halls() {
        let s = setup(&[halls(), wallets()]).await;
        s.service.rebuild_concert("c-1").await.unwrap();
        let owner = s
            .store
            .claim(&halls(), "node-a", Duration::from_secs(10))
            .await
            .unwrap()
            .unwrap();
        let requests = s.store.take_requests(&halls(), owner.token, 10).await.unwrap().unwrap();
        assert_eq!((requests.rebuild, requests.keys), (None, vec!["c-1".to_owned()]));
    }

    #[tokio::test(start_paused = true)]
    async fn the_watcher_waits_for_the_owners_reports_progress_and_the_end_once_both_rows_are_done() {
        let s = setup(&[halls(), wallets()]).await;
        let started = s.service.rebuild(Duration::ZERO).await.unwrap();
        tokio::time::sleep(REBUILD_WATCH_INTERVAL * 3).await;
        assert_eq!(kinds(&s.activity), ["rebuild.started"], "nobody took the requests yet");
        assert_eq!(
            s.service.status().await.unwrap()["rebuild"]["rebuild_id"],
            json!(started.rebuild_id)
        );
        run_rebuild(&s.store, &halls(), 2).await;
        tokio::time::sleep(REBUILD_WATCH_INTERVAL * 2).await;
        assert_eq!(kinds(&s.activity), ["rebuild.started", "rebuild.progress"]);
        run_rebuild(&s.store, &wallets(), 1).await;
        tokio::time::sleep(REBUILD_WATCH_INTERVAL * 2).await;
        assert_eq!(
            kinds(&s.activity),
            ["rebuild.started", "rebuild.progress", "rebuild.progress", "rebuild.finished"]
        );
        let finished = s.activity.subscribe().0.last().unwrap().clone();
        assert_eq!(finished.fields["keys"], json!(3));
        let status = s.service.status().await.unwrap();
        assert_eq!(status["rebuild"], Value::Null);
        assert_eq!(status["last_result"]["keys"], json!(3));
        assert_eq!(status["last_result"]["rebuild_id"], json!(started.rebuild_id));
    }

    #[tokio::test(start_paused = true)]
    async fn a_row_dropped_during_the_rebuild_ends_it_and_a_shutdown_stops_the_watcher() {
        let s = setup(&[halls(), wallets()]).await;
        s.service.rebuild(Duration::ZERO).await.unwrap();
        s.store.clear();
        tokio::time::sleep(REBUILD_WATCH_INTERVAL * 2).await;
        assert_eq!(kinds(&s.activity), ["rebuild.started", "rebuild.finished"]);
        let s = setup(&[halls(), wallets()]).await;
        s.service.rebuild(Duration::ZERO).await.unwrap();
        s.service.shutdown.cancel();
        tokio::time::sleep(REBUILD_WATCH_INTERVAL * 2).await;
        run_rebuild(&s.store, &halls(), 1).await;
        run_rebuild(&s.store, &wallets(), 1).await;
        tokio::time::sleep(REBUILD_WATCH_INTERVAL * 2).await;
        assert_eq!(
            kinds(&s.activity),
            ["rebuild.started"],
            "the watcher stopped with the process"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_status_names_the_state_of_every_projection_and_the_worst_of_them() {
        let s = setup(&[halls()]).await;
        let status = s.service.status().await.unwrap();
        assert_eq!(
            status["projections"][1]["state"], "starting",
            "the wallets' row is not registered yet"
        );
        assert_eq!(status["projections"][0]["state"], "unclaimed", "nobody owns the halls");
        assert_eq!(
            status["state"], "unclaimed",
            "an unowned projection is worse than one starting"
        );
        s.store.register(&wallets()).await.unwrap();
        let halls_owner = s
            .store
            .claim(&halls(), "node-a", Duration::from_secs(10))
            .await
            .unwrap()
            .unwrap();
        let wallets_owner = s
            .store
            .claim(&wallets(), "node-a", Duration::from_secs(10))
            .await
            .unwrap()
            .unwrap();
        let status = s.service.status().await.unwrap();
        assert_eq!(status["state"], "live");
        assert_eq!(
            (
                status["projections"][0]["owner"].as_str(),
                status["projections"][0]["lag"].as_u64()
            ),
            (Some("node-a"), Some(0))
        );
        s.ledger.set_lag("Concert", Some(150));
        s.store
            .record_failure(&halls(), halls_owner.token, "concert_halls: connection")
            .await
            .unwrap();
        let status = s.service.status().await.unwrap();
        let hall = &status["projections"][0];
        assert_eq!(
            (
                hall["state"].as_str(),
                hall["lag"].as_u64(),
                hall["lag_capped"].as_bool(),
                hall["last_error"].as_str()
            ),
            (Some("stalled"), Some(100), Some(true), Some("concert_halls: connection"))
        );
        assert_eq!(status["state"], "stalled");
        s.ledger.set_lag("Concert", Some(0));
        let status = s.service.status().await.unwrap();
        assert_eq!(
            status["projections"][0]["state"], "live",
            "a failure the journal has moved past is no stall"
        );
        s.store.request_rebuild(&wallets(), Duration::from_millis(80)).await.unwrap();
        let status = s.service.status().await.unwrap();
        assert_eq!(status["projections"][1]["state"], "rebuilding");
        assert_eq!(status["projections"][1]["rebuild"]["pace_ms"], 80);
        s.store.expire_lease(&wallets());
        tokio::time::sleep(Duration::from_millis(5)).await;
        let status = s.service.status().await.unwrap();
        assert_eq!(
            status["projections"][1]["state"], "unclaimed",
            "an expired lease has no owner"
        );
        s.store.release(&wallets(), wallets_owner.token).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_store_that_does_not_answer_gives_the_rows_seen_last_and_a_failed_count_is_an_error() {
        let s = setup(&[halls(), wallets()]).await;
        let fresh = s.service.status().await.unwrap();
        assert_eq!(fresh["stale"], false);
        s.store.set_unavailable(true);
        let stale = s.service.status().await.unwrap();
        assert_eq!(stale["stale"], true);
        assert_eq!(stale["projections"], fresh["projections"]);
        s.store.set_unavailable(false);
        s.ledger.set_lag("Fan", None);
        assert_eq!(
            s.service.status().await.unwrap_err(),
            StorageError("journal unavailable".into())
        );
    }
}
