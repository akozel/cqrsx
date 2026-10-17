//! Encore! — wipe and rebuild the read models from the ledger.
//!
//! Why a lock instead of a live rebuild: simpler and deterministic for a demo. UI commands get 423
//! while a rebuild runs; the outbox workers keep running (the write side never depends on the
//! read side) and chain tasks that hit the lock simply retry a moment later.
//! Replay is safe because it only calls `View::update`: no commands, no tasks, no e-mails.
use crate::{
    application::{
        activity::ActivityHub,
        lock::{BoxOfficeLock, LockGuard},
        projections::progress::ProgressHandler,
    },
    domain::{concert::Concert, fan::Fan},
    infrastructure::mongo::read_side::{CONCERT_HALLS, FAN_WALLETS, ReadSide},
};
use chrono::{SecondsFormat, Utc};
use cqrsx::{
    cqrs::{
        aggregate::Aggregate,
        event_handler::{EventHandler, replay::EventReplay},
        projection::ViewProjection,
    },
    postgres::event_repository::PostgresEventRepository,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::PgPool;
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WipeError {
    #[error("a rebuild is in progress")]
    Locked,
    #[error("read model storage unavailable: {0}")]
    Storage(String),
}

pub struct RebuildService {
    lock: Arc<BoxOfficeLock>,
    replayed: Arc<AtomicU64>,
    status: Mutex<Status>,
    pool: PgPool,
    read: Arc<ReadSide>,
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
        pool: PgPool,
        read: Arc<ReadSide>,
        activity: Arc<ActivityHub>,
        slow_delay: Duration,
    ) -> Self {
        Self {
            lock,
            replayed: Arc::new(AtomicU64::new(0)),
            status: Mutex::new(Status::default()),
            pool,
            read,
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

    /// 🧨 Drop both read-model collections (the inbox is not a read model and stays).
    pub async fn wipe(&self) -> Result<Vec<&'static str>, WipeError> {
        if self.is_locked() {
            return Err(WipeError::Locked);
        }
        let storage = |e: cqrsx::cqrs::store::PersistenceError| WipeError::Storage(e.to_string());
        self.read.halls.wipe().await.map_err(storage)?;
        self.read.wallets.wipe().await.map_err(storage)?;
        let collections = vec![CONCERT_HALLS, FAN_WALLETS];
        self.activity
            .publish("projections.wiped", json!({ "collections": collections }));
        Ok(collections)
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
        // Progress figures only: a failed count is reported as 0, the replay itself still decides.
        let total: i64 = sqlx::query_scalar("SELECT count(*) FROM events")
            .fetch_one(&self.pool)
            .await
            .unwrap_or(0);
        let aggregates: i64 = sqlx::query_scalar("SELECT count(DISTINCT (aggregate_type, aggregate_id)) FROM events")
            .fetch_one(&self.pool)
            .await
            .unwrap_or(0);
        let aggregates = aggregates.max(0) as u64;
        if let Ok(mut status) = self.status.lock()
            && let Some(running) = status.running.as_mut()
        {
            running.total = total.max(0) as u64;
        }
        self.activity.publish(
            "rebuild.started",
            json!({ "rebuild_id": rebuild_id, "speed": speed, "total": total, "aggregates": aggregates }),
        );
        // Always from scratch: never trust what a previous (maybe failed) run left behind.
        if let Err(error) = self.read.halls.wipe().await.and(self.read.wallets.wipe().await) {
            let message = format!("wipe failed: {error}");
            self.activity
                .publish("rebuild.failed", json!({ "rebuild_id": rebuild_id, "message": message }));
            return finish(false, 0, 0, Some(message));
        }
        let delay = (speed == Speed::Slow).then_some(self.slow_delay);
        // Two passes, one per aggregate type: `stream_all_events::<A>` filters by aggregate_type.
        // Within a pass events arrive ordered by `sequence` across aggregates ("seats light up in
        // every hall at once"); per aggregate the order is preserved, which is all LWW needs.
        let result = match self
            .pass::<Concert, _>(rebuild_id, speed, ViewProjection::new(self.read.halls.clone()), delay)
            .await
        {
            Ok(()) => {
                self.pass::<Fan, _>(rebuild_id, speed, ViewProjection::new(self.read.wallets.clone()), delay)
                    .await
            }
            Err(error) => Err(error),
        };
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

    async fn pass<A, H>(&self, rebuild_id: Uuid, speed: Speed, handler: H, delay: Option<Duration>) -> Result<(), String>
    where
        A: Aggregate + 'static,
        H: EventHandler<A> + 'static,
    {
        let activity = self.activity.clone();
        let counter = self.replayed.clone();
        let last_progress = Mutex::new(Instant::now() - Duration::from_secs(1));
        let on_event = Box::new(move |aggregate_id: &str, sequence: usize, event_type: &str| {
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
                    json!({ "rebuild_id": rebuild_id, "replayed": count, "aggregate_type": A::TYPE, "aggregate_id": aggregate_id,
                            "sequence": sequence, "event_type": event_type }),
                );
            }
        });
        // Repository and handler are created per rebuild: `EventReplay::new` takes them by value.
        let replay = EventReplay::new(
            PostgresEventRepository::new(self.pool.clone()),
            ProgressHandler::new(handler, delay, on_event),
        );
        replay
            .replay_all()
            .await
            .map_err(|e| format!("{} replay failed: {e}", A::TYPE))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing;

    #[tokio::test]
    async fn status_is_idle_and_wipe_is_refused_while_the_box_office_is_locked() {
        let lock = Arc::new(BoxOfficeLock::new());
        let service = Arc::new(RebuildService::new(
            lock.clone(),
            testing::lazy_pool(),
            Arc::new(testing::lazy_read_side().await),
            Arc::new(ActivityHub::new(8, 8)),
            Duration::ZERO,
        ));
        assert_eq!(service.status_json()["state"], "idle");
        assert!(!service.is_locked());
        let guard = lock.try_acquire().unwrap();
        assert_eq!(service.wipe().await, Err(WipeError::Locked));
        assert_eq!(service.start(Speed::Fast), Err(AlreadyRunning));
        drop(guard);
        assert!(!service.is_locked());
    }
}
