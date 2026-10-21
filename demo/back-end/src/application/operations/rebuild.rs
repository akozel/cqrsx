//! Encore! — rebuild the read models from the ledger, always from scratch (the first step drops
//! them). Dropping them *without* a rebuild is a demo operation and lives in the sidecar.
//!
//! Why a lock instead of a live rebuild: simpler and deterministic for a demo. UI commands get 423
//! while a rebuild runs; the outbox workers keep running (the write side never depends on the
//! read side) and chain tasks that hit the lock simply retry a moment later.
//! Replay is safe because it only calls `View::update`: no commands, no tasks, no e-mails.
use crate::application::{
    activity::ActivityHub,
    lock::{BoxOfficeLock, LockGuard},
    ports::EventLedgerStats,
    projections::maintenance::MaintainedReadModel,
};
use chrono::{SecondsFormat, Utc};
use cqrsx::cqrs::event_handler::replay::{ReplayProgress, ReplayProgressObserver};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Speed {
    Slow,
    Fast,
}

#[derive(Debug, Clone, Serialize)]
pub struct RebuildResult {
    pub rebuild_id: Uuid,
    pub speed: Speed,
    pub ok: bool,
    pub replayed: u64,
    pub aggregates: u64,
    pub duration_ms: u64,
    pub finished_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct Running {
    rebuild_id: Uuid,
    speed: Speed,
    started_at: String,
    total: u64,
}

#[derive(Default)]
struct Status {
    running: Option<Running>,
    last_result: Option<RebuildResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a rebuild is already running")]
pub struct AlreadyRunning;

pub struct RebuildService {
    lock: Arc<BoxOfficeLock>,
    replayed: Arc<AtomicU64>,
    status: Mutex<Status>,
    /// Rebuilt in this order, one pass each (`Concert` first, then `Fan`).
    read_models: Vec<Arc<dyn MaintainedReadModel>>,
    ledger: Arc<dyn EventLedgerStats>,
    activity: Arc<ActivityHub>,
    slow_delay: Duration,
}

/// Lives inside the spawned rebuild task: clears `status.running` and then (field drop order)
/// releases the box-office lock — on every exit path, including a panic inside `run`.
struct RunningGuard {
    service: Arc<RebuildService>,
    _lock: LockGuard,
}

impl Drop for RunningGuard {
    fn drop(&mut self) {
        if let Ok(mut status) = self.service.status.lock() {
            status.running = None;
        }
    }
}

impl RebuildService {
    pub fn new(
        lock: Arc<BoxOfficeLock>,
        read_models: Vec<Arc<dyn MaintainedReadModel>>,
        ledger: Arc<dyn EventLedgerStats>,
        activity: Arc<ActivityHub>,
        slow_delay: Duration,
    ) -> Self {
        Self {
            lock,
            replayed: Arc::new(AtomicU64::new(0)),
            status: Mutex::new(Status::default()),
            read_models,
            ledger,
            activity,
            slow_delay,
        }
    }

    pub fn lock(&self) -> &Arc<BoxOfficeLock> {
        &self.lock
    }

    pub fn is_locked(&self) -> bool {
        self.lock.is_locked()
    }

    pub fn status_json(&self) -> serde_json::Value {
        let (running, last) = self
            .status
            .lock()
            .map(|s| (s.running.clone(), s.last_result.clone()))
            .unwrap_or_default();
        match running {
            Some(running) => json!({
                "state": "running", "rebuild_id": running.rebuild_id, "speed": running.speed,
                "started_at": running.started_at, "total": running.total,
                "replayed": self.replayed.load(Ordering::SeqCst), "last_result": last,
            }),
            None => json!({ "state": "idle", "last_result": last }),
        }
    }

    /// ⏪ Start a rebuild in the background; returns at once with its id.
    pub fn start(self: &Arc<Self>, speed: Speed) -> Result<Uuid, AlreadyRunning> {
        let lock = self.lock.try_acquire().ok_or(AlreadyRunning)?;
        let rebuild_id = Uuid::now_v7();
        self.replayed.store(0, Ordering::SeqCst);
        if let Ok(mut status) = self.status.lock() {
            status.running = Some(Running {
                rebuild_id,
                speed,
                started_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
                total: 0,
            });
        }
        let service = self.clone();
        tokio::spawn(async move {
            let _running = RunningGuard {
                service: service.clone(),
                _lock: lock,
            };
            let result = service.run(rebuild_id, speed).await;
            if let Ok(mut status) = service.status.lock() {
                status.last_result = Some(result);
            }
        });
        Ok(rebuild_id)
    }

    async fn run(&self, rebuild_id: Uuid, speed: Speed) -> RebuildResult {
        let started = Instant::now();
        let finish = |ok: bool, replayed: u64, aggregates: u64, error: Option<String>| RebuildResult {
            rebuild_id,
            speed,
            ok,
            replayed,
            aggregates,
            duration_ms: started.elapsed().as_millis() as u64,
            finished_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            error,
        };
        // Commands admitted before the lock was taken finish first: their projections write into the
        // read models this rebuild is about to wipe. The wait is bounded by one command, its turn included.
        self.lock.wait_idle().await;
        // Progress figures only: a failed count is reported as 0, the replay itself still decides.
        let totals = self.ledger.totals().await.unwrap_or_default();
        let (total, aggregates) = (totals.events, totals.aggregates);
        if let Ok(mut status) = self.status.lock()
            && let Some(running) = status.running.as_mut()
        {
            running.total = total;
        }
        self.activity.publish(
            "rebuild.started",
            json!({ "rebuild_id": rebuild_id, "speed": speed, "total": total, "aggregates": aggregates }),
        );
        // Always from scratch: never trust what a previous (maybe failed) run left behind.
        let mut wiped = Ok(());
        for read_model in &self.read_models {
            wiped = wiped.and(read_model.wipe().await);
        }
        if let Err(error) = wiped {
            let message = format!("wipe failed: {error}");
            self.activity
                .publish("rebuild.failed", json!({ "rebuild_id": rebuild_id, "message": message }));
            return finish(false, 0, 0, Some(message));
        }
        let pace = if speed == Speed::Slow {
            self.slow_delay
        } else {
            Duration::ZERO
        };
        // One pass per read model, i.e. per aggregate type. Within a pass events arrive ordered by
        // `sequence` across aggregates ("seats light up in every hall at once"); per aggregate the
        // order is preserved, which is all LWW needs.
        let mut result = Ok(());
        for read_model in &self.read_models {
            result = self.pass(read_model.as_ref(), rebuild_id, speed, pace).await;
            if result.is_err() {
                break;
            }
        }
        let replayed = self.replayed.load(Ordering::SeqCst);
        match result {
            Ok(()) => {
                let result = finish(true, replayed, aggregates, None);
                self.activity.publish(
                    "rebuild.finished",
                    json!({ "rebuild_id": rebuild_id, "speed": speed, "replayed": replayed, "aggregates": aggregates,
                            "duration_ms": result.duration_ms }),
                );
                result
            }
            Err(message) => {
                self.activity.publish(
                    "rebuild.failed",
                    json!({ "rebuild_id": rebuild_id, "speed": speed, "replayed": replayed, "message": message }),
                );
                finish(false, replayed, aggregates, Some(message))
            }
        }
    }

    async fn pass(
        &self,
        read_model: &dyn MaintainedReadModel,
        rebuild_id: Uuid,
        speed: Speed,
        pace: Duration,
    ) -> Result<(), String> {
        let aggregate_type = read_model.aggregate_type();
        let activity = self.activity.clone();
        let counter = self.replayed.clone();
        let last_progress = Mutex::new(Instant::now() - Duration::from_secs(1));
        // The library counts per replay; `replayed` here counts across both passes.
        let on_event: Box<ReplayProgressObserver> = Box::new(move |progress: &ReplayProgress<'_>| {
            let count = counter.fetch_add(1, Ordering::SeqCst) + 1;
            // Slow mode: a message per event; fast mode: at most ~20 per second (the final
            // `rebuild.finished` carries the exact total).
            let emit = speed == Speed::Slow
                || last_progress
                    .lock()
                    .map(|mut last| {
                        if last.elapsed() >= Duration::from_millis(50) {
                            *last = Instant::now();
                            true
                        } else {
                            false
                        }
                    })
                    .unwrap_or(true);
            if emit {
                activity.publish(
                    "rebuild.progress",
                    json!({ "rebuild_id": rebuild_id, "replayed": count, "aggregate_type": aggregate_type,
                            "aggregate_id": progress.aggregate_id, "sequence": progress.sequence, "event_type": progress.event_type }),
                );
            }
        });
        read_model.rebuild(pace, on_event).await.map_err(|e| e.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::{StorageError, ports::LedgerTotals, projections::maintenance::Verification};
    use async_trait::async_trait;

    /// A read model whose history is `events` (aggregate id, sequence, event type) entries.
    struct FakeReadModel {
        name: &'static str,
        events: Vec<(&'static str, usize, &'static str)>,
        wipes: AtomicU64,
        broken: bool,
    }

    impl FakeReadModel {
        fn new(name: &'static str, events: Vec<(&'static str, usize, &'static str)>) -> Arc<Self> {
            Arc::new(Self {
                name,
                events,
                wipes: AtomicU64::new(0),
                broken: false,
            })
        }
    }

    #[async_trait]
    impl MaintainedReadModel for FakeReadModel {
        fn name(&self) -> &'static str {
            self.name
        }

        fn aggregate_type(&self) -> &'static str {
            "Fake"
        }

        async fn wipe(&self) -> Result<(), StorageError> {
            self.wipes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn rebuild(&self, _: Duration, on_event: Box<ReplayProgressObserver>) -> Result<(), StorageError> {
            if self.broken {
                return Err(StorageError("Fake replay failed: boom".into()));
            }
            for (replayed, (id, sequence, event_type)) in (1..).zip(&self.events) {
                on_event(&ReplayProgress {
                    aggregate_id: id,
                    sequence: *sequence,
                    event_type: (*event_type).to_owned(),
                    replayed,
                });
            }
            Ok(())
        }

        async fn verify(&self, _: Uuid) -> Result<Verification, StorageError> {
            unreachable!("not used by rebuild")
        }
    }

    struct FixedLedger;

    #[async_trait]
    impl EventLedgerStats for FixedLedger {
        async fn totals(&self) -> Result<LedgerTotals, StorageError> {
            Ok(LedgerTotals {
                events: 3,
                aggregates: 2,
            })
        }
    }

    fn service(read_models: Vec<Arc<dyn MaintainedReadModel>>) -> (Arc<RebuildService>, Arc<BoxOfficeLock>, Arc<ActivityHub>) {
        let lock = Arc::new(BoxOfficeLock::new());
        let activity = Arc::new(ActivityHub::new(64, 64));
        let service = RebuildService::new(
            lock.clone(),
            read_models,
            Arc::new(FixedLedger),
            activity.clone(),
            Duration::ZERO,
        );
        (Arc::new(service), lock, activity)
    }

    async fn wait_until_idle(service: &RebuildService) -> serde_json::Value {
        for _ in 0..200 {
            let status = service.status_json();
            if status["state"] == "idle" && !status["last_result"].is_null() {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("the rebuild did not finish")
    }

    #[tokio::test]
    async fn status_is_idle_and_a_rebuild_is_refused_while_the_box_office_is_locked() {
        let (service, lock, _) = service(vec![]);
        assert_eq!(service.status_json()["state"], "idle");
        assert!(!service.is_locked());
        let guard = lock.try_acquire().unwrap();
        assert_eq!(service.start(Speed::Fast), Err(AlreadyRunning));
        drop(guard);
        assert!(!service.is_locked());
    }

    #[tokio::test]
    async fn a_rebuild_wipes_replays_every_read_model_in_order_and_releases_the_lock() {
        let halls = FakeReadModel::new(
            "concert_halls",
            vec![("c-1", 1, "ConcertAnnounced"), ("c-1", 2, "SeatBooked")],
        );
        let wallets = FakeReadModel::new("fan_wallets", vec![("fox", 1, "TicketRecorded")]);
        let (service, lock, activity) = service(vec![halls.clone(), wallets.clone()]);
        service.start(Speed::Slow).unwrap();
        let status = wait_until_idle(&service).await;
        assert_eq!(status["last_result"]["ok"], true);
        assert_eq!(status["last_result"]["replayed"], 3);
        assert_eq!(status["last_result"]["aggregates"], 2);
        assert!(!lock.is_locked());
        assert_eq!(
            (halls.wipes.load(Ordering::SeqCst), wallets.wipes.load(Ordering::SeqCst)),
            (1, 1)
        );
        let kinds: Vec<String> = activity.subscribe().0.iter().map(|e| e.kind.clone()).collect();
        assert_eq!(
            kinds,
            [
                "rebuild.started",
                "rebuild.progress",
                "rebuild.progress",
                "rebuild.progress",
                "rebuild.finished"
            ]
        );
    }

    #[tokio::test]
    async fn a_rebuild_wipes_nothing_until_the_commands_admitted_before_it_have_finished() {
        let halls = FakeReadModel::new("concert_halls", vec![("c-1", 1, "ConcertAnnounced")]);
        let (service, lock, _) = service(vec![halls.clone()]);
        let command = lock.admit().unwrap();
        service.start(Speed::Fast).unwrap();
        assert!(
            lock.admit().is_none(),
            "no command is admitted once the rebuild holds the lock"
        );
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(halls.wipes.load(Ordering::SeqCst), 0, "the admitted command is still running");
        assert_eq!(service.status_json()["state"], "running");
        drop(command);
        let status = wait_until_idle(&service).await;
        assert_eq!(status["last_result"]["ok"], true);
        assert_eq!(halls.wipes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_failed_pass_stops_the_rebuild_and_is_reported() {
        let broken = Arc::new(FakeReadModel {
            name: "concert_halls",
            events: vec![],
            wipes: AtomicU64::new(0),
            broken: true,
        });
        let wallets = FakeReadModel::new("fan_wallets", vec![("fox", 1, "TicketRecorded")]);
        let (service, lock, activity) = service(vec![broken, wallets]);
        service.start(Speed::Fast).unwrap();
        let status = wait_until_idle(&service).await;
        assert_eq!(status["last_result"]["ok"], false);
        assert_eq!(status["last_result"]["error"], "Fake replay failed: boom");
        assert_eq!(status["last_result"]["replayed"], 0, "the second pass never ran");
        assert!(!lock.is_locked());
        assert_eq!(activity.subscribe().0.last().unwrap().kind, "rebuild.failed");
    }
}
