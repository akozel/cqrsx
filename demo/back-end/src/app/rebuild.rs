//! Encore! — wipe and rebuild the read models from the ledger.
//!
//! Why a lock instead of a live rebuild: simpler and deterministic for a demo. UI commands get 423
//! while a rebuild runs; the outbox workers keep running (the write side never depends on the
//! read side) and chain tasks that hit the lock simply retry a second later.
//! Replay is safe because it only calls `View::update`: no commands, no tasks, no e-mails.
use crate::{
    app::{
        activity::ActivityHub,
        read_side::{CONCERT_HALLS, FAN_WALLETS, ReadSide},
    },
    domain::{concert::Concert, fan::Fan},
    projections::progress::ProgressHandler,
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
        atomic::{AtomicBool, AtomicU64, Ordering},
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

struct Deps {
    pool: PgPool,
    read: Arc<ReadSide>,
    activity: Arc<ActivityHub>,
    slow_delay: Duration,
}

pub struct RebuildService {
    locked: AtomicBool,
    replayed: Arc<AtomicU64>,
    status: Mutex<Status>,
    deps: Option<Deps>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartError {
    AlreadyRunning,
    NotConfigured,
}

/// RAII guard: the lock is released on every exit path, including a panic in the spawned task.
struct LockGuard(Arc<RebuildService>);

impl Drop for LockGuard {
    fn drop(&mut self) {
        self.0.locked.store(false, Ordering::SeqCst);
        if let Ok(mut status) = self.0.status.lock() {
            status.running = None;
        }
    }
}

impl RebuildService {
    /// Only the lock, no storage (unit tests).
    pub fn lock_only() -> Self {
        Self {
            locked: AtomicBool::new(false),
            replayed: Arc::new(AtomicU64::new(0)),
            status: Mutex::new(Status::default()),
            deps: None,
        }
    }

    pub fn new(pool: PgPool, read: Arc<ReadSide>, activity: Arc<ActivityHub>, slow_delay: Duration) -> Self {
        Self {
            deps: Some(Deps {
                pool,
                read,
                activity,
                slow_delay,
            }),
            ..Self::lock_only()
        }
    }

    pub fn is_locked(&self) -> bool {
        self.locked.load(Ordering::SeqCst)
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
    pub async fn wipe(&self) -> Result<Vec<&'static str>, String> {
        if self.is_locked() {
            return Err("rebuild_in_progress".into());
        }
        let deps = self.deps.as_ref().ok_or("read side not configured")?;
        deps.read.halls.wipe().await.map_err(|e| e.to_string())?;
        deps.read.wallets.wipe().await.map_err(|e| e.to_string())?;
        let collections = vec![CONCERT_HALLS, FAN_WALLETS];
        deps.activity
            .publish("projections.wiped", json!({ "collections": collections }));
        Ok(collections)
    }

    /// ⏪ Start a rebuild in the background; returns at once with its id.
    pub fn start(self: &Arc<Self>, speed: Speed) -> Result<Uuid, StartError> {
        if self.deps.is_none() {
            return Err(StartError::NotConfigured);
        }
        if self.locked.swap(true, Ordering::SeqCst) {
            return Err(StartError::AlreadyRunning);
        }
        let guard = LockGuard(self.clone());
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
            let _guard = guard;
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
        let Some(deps) = self.deps.as_ref() else {
            return finish(false, 0, 0, Some("read side not configured".into()));
        };
        let total: i64 = sqlx::query_scalar("SELECT count(*) FROM events")
            .fetch_one(&deps.pool)
            .await
            .unwrap_or(0);
        let aggregates: i64 = sqlx::query_scalar("SELECT count(DISTINCT (aggregate_type, aggregate_id)) FROM events")
            .fetch_one(&deps.pool)
            .await
            .unwrap_or(0);
        let aggregates = aggregates.max(0) as u64;
        if let Ok(mut status) = self.status.lock()
            && let Some(running) = status.running.as_mut()
        {
            running.total = total.max(0) as u64;
        }
        deps.activity.publish(
            "rebuild.started",
            json!({ "rebuild_id": rebuild_id, "speed": speed, "total": total, "aggregates": aggregates }),
        );
        // Always from scratch: never trust what a previous (maybe failed) run left behind.
        if let Err(error) = deps.read.halls.wipe().await.and(deps.read.wallets.wipe().await) {
            let message = format!("wipe failed: {error}");
            deps.activity
                .publish("rebuild.failed", json!({ "rebuild_id": rebuild_id, "message": message }));
            return finish(false, 0, 0, Some(message));
        }
        let delay = (speed == Speed::Slow).then_some(deps.slow_delay);
        // Two passes, one per aggregate type: `stream_all_events::<A>` filters by aggregate_type.
        // Within a pass events arrive ordered by `sequence` across aggregates ("seats light up in
        // every hall at once"); per aggregate the order is preserved, which is all LWW needs.
        let result = match self
            .pass::<Concert, _>(deps, rebuild_id, speed, ViewProjection::new(deps.read.halls.clone()), delay)
            .await
        {
            Ok(()) => {
                self.pass::<Fan, _>(deps, rebuild_id, speed, ViewProjection::new(deps.read.wallets.clone()), delay)
                    .await
            }
            Err(error) => Err(error),
        };
        let replayed = self.replayed.load(Ordering::SeqCst);
        match result {
            Ok(()) => {
                let result = finish(true, replayed, aggregates, None);
                deps.activity.publish(
                    "rebuild.finished",
                    json!({ "rebuild_id": rebuild_id, "speed": speed, "replayed": replayed, "aggregates": aggregates,
                            "duration_ms": result.duration_ms }),
                );
                result
            }
            Err(message) => {
                deps.activity.publish(
                    "rebuild.failed",
                    json!({ "rebuild_id": rebuild_id, "speed": speed, "replayed": replayed, "message": message }),
                );
                finish(false, replayed, aggregates, Some(message))
            }
        }
    }

    async fn pass<A, H>(
        &self,
        deps: &Deps,
        rebuild_id: Uuid,
        speed: Speed,
        handler: H,
        delay: Option<Duration>,
    ) -> Result<(), String>
    where
        A: Aggregate + 'static,
        H: EventHandler<A> + 'static,
    {
        let activity = deps.activity.clone();
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
            PostgresEventRepository::new(deps.pool.clone()),
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

    #[tokio::test]
    async fn lock_only_service_reports_idle_and_refuses_to_start() {
        let service = Arc::new(RebuildService::lock_only());
        assert!(!service.is_locked());
        assert_eq!(service.status_json()["state"], "idle");
        assert_eq!(service.start(Speed::Fast), Err(StartError::NotConfigured));
        assert_eq!(service.wipe().await, Err("read side not configured".to_owned()));
        assert!(!service.is_locked());
    }

    #[test]
    fn guard_releases_the_lock() {
        let service = Arc::new(RebuildService::lock_only());
        service.locked.store(true, Ordering::SeqCst);
        drop(LockGuard(service.clone()));
        assert!(!service.is_locked());
    }
}
