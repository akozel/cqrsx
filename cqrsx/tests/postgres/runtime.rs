use std::sync::Mutex;

use super::*;
use cqrsx::{
    task::policy::Policies,
    worker::{dispatcher::TaskDispatcherError, queue::storage_config::TaskStorageConfig, runtime::config::TaskRuntimeConfig},
};

struct DummyHandler(Arc<Mutex<Vec<(Uuid, String)>>>);

impl TaskAdapter for DummyHandler {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![format()]
    }

    async fn invoke(&self, ctx: &TaskContext) -> TaskResult {
        let delivery = ctx.attempt();
        let Ok(payload) = serde_json::from_value::<DummyTaskV1>(delivery.payload().clone()) else {
            return TaskResult::fail("invalid dummy task payload");
        };
        self.0.lock().unwrap().push((delivery.task_id(), payload.value));
        TaskResult::Done
    }
}

#[tokio::test]
async fn two_live_workers_complete_every_supported_task_and_quarantine_bad_payloads() {
    let db = Database::new().await;
    let first = NewTask::try_new(DummyTaskV1 { value: "first".into() }).unwrap();
    let second = NewTask::try_new(DummyTaskV1 { value: "second".into() }).unwrap();
    let bad = NewTask::from_payload(format(), json!({})).unwrap();
    enqueue(&db.pool, vec![first.clone(), second.clone(), bad.clone()]).await;
    let handled = Arc::new(Mutex::new(Vec::new()));
    let dispatcher = || TaskDispatcher::try_new(vec![DummyHandler(handled.clone())]).unwrap();
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let config = TaskWorkerPoolConfig::try_new(
        2,
        2,
        Duration::from_millis(10),
        TaskWorkerPoolConfig::default().handler_timeout(),
        TaskWorkerPoolConfig::default().lease_duration(),
    )
    .unwrap();
    let config = TaskRuntimeConfig::try_new(TaskStorageConfig::default(), config).unwrap();
    // A startup failure must leave existing durable work unclaimed: a dispatcher that cannot be built never runs.
    let failed = TaskDispatcher::<DummyHandler>::try_new(vec![]);
    assert!(matches!(failed, Err(TaskDispatcherError::Empty)));
    let left = task_runtime(&db.pool, Arc::new(dispatcher()), config.clone(), Policies::default());
    let right = task_runtime(&db.pool, Arc::new(dispatcher()), config, Policies::default());
    for task in [&first, &second, &bad] {
        assert_worker_state(&db.pool, task.id(), "pending", 0, None).await;
    }
    assert!(handled.lock().unwrap().is_empty());
    let left = tokio::spawn(left.run(shutdown.clone()));
    let right = tokio::spawn(right.run(shutdown));
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM transactional_outbox WHERE finished_at IS NOT NULL")
                .fetch_one(&db.pool)
                .await
                .unwrap();
            if count == 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    stop.cancel();
    left.await.unwrap().unwrap();
    right.await.unwrap().unwrap();

    let handled = handled.lock().unwrap().clone();
    assert_eq!(handled.len(), 2);
    for (task, expected) in [(first, "first"), (second, "second")] {
        assert!(handled.contains(&(task.id(), expected.to_owned())));
        assert_eq!(row(&db.pool, task.id()).await.get::<String, _>("status"), "completed");
    }
    assert_eq!(row(&db.pool, bad.id()).await.get::<String, _>("status"), "failed");
    db.close().await;
}

#[tokio::test]
async fn one_worker_pool_routes_two_registered_task_types() {
    struct AuditHandler(Arc<AtomicUsize>);

    impl TaskAdapter for AuditHandler {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![TaskFormat::try_new("audit", 1).unwrap()]
        }

        async fn invoke(&self, _: &TaskContext) -> TaskResult {
            self.0.fetch_add(1, Ordering::SeqCst);
            TaskResult::Done
        }
    }

    let db = Database::new().await;
    let dummy = new_task();
    let audit = NewTask::from_payload(TaskFormat::try_new("audit", 1).unwrap(), json!({"kind":"runtime"})).unwrap();
    let unknown = NewTask::from_payload(TaskFormat::try_new("future", 1).unwrap(), json!({"value":"runtime"})).unwrap();
    enqueue(&db.pool, vec![dummy.clone(), audit.clone(), unknown.clone()]).await;

    enum WorkerAdapter {
        Dummy(DummyHandler),
        Audit(AuditHandler),
    }
    impl TaskAdapter for WorkerAdapter {
        fn formats(&self) -> Vec<TaskFormat> {
            match self {
                Self::Dummy(adapter) => adapter.formats(),
                Self::Audit(adapter) => adapter.formats(),
            }
        }
        async fn invoke(&self, ctx: &TaskContext) -> TaskResult {
            match self {
                Self::Dummy(adapter) => adapter.invoke(ctx).await,
                Self::Audit(adapter) => adapter.invoke(ctx).await,
            }
        }
    }
    let handled = Arc::new(Mutex::new(Vec::new()));
    let audited = Arc::new(AtomicUsize::new(0));
    let dispatcher = TaskDispatcher::try_new(vec![
        WorkerAdapter::Dummy(DummyHandler(handled.clone())),
        WorkerAdapter::Audit(AuditHandler(audited.clone())),
    ])
    .unwrap();
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let prepared = task_runtime(
        &db.pool,
        Arc::new(dispatcher),
        TaskRuntimeConfig::try_new(
            TaskStorageConfig::default(),
            TaskWorkerPoolConfig::try_new(
                2,
                2,
                Duration::from_millis(10),
                TaskWorkerPoolConfig::default().handler_timeout(),
                TaskWorkerPoolConfig::default().lease_duration(),
            )
            .unwrap(),
        )
        .unwrap(),
        Policies::default(),
    );
    for task in [&dummy, &audit, &unknown] {
        assert_worker_state(&db.pool, task.id(), "pending", 0, None).await;
    }
    assert!(handled.lock().unwrap().is_empty());
    assert_eq!(audited.load(Ordering::SeqCst), 0);
    let worker = tokio::spawn(prepared.run(shutdown));
    tokio::time::timeout(Duration::from_secs(10), async {
        while row(&db.pool, dummy.id()).await.get::<String, _>("status") != "completed"
            || row(&db.pool, audit.id()).await.get::<String, _>("status") != "completed"
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    stop.cancel();
    worker.await.unwrap().unwrap();
    assert_eq!(handled.lock().unwrap().len(), 1);
    assert_eq!(audited.load(Ordering::SeqCst), 1);
    assert_eq!(row(&db.pool, unknown.id()).await.get::<String, _>("status"), "pending");
    assert_eq!(row(&db.pool, unknown.id()).await.get::<i32, _>("attempts"), 0);
    db.close().await;
}

async fn terminal_history(pool: &PgPool, ids: &[Uuid]) -> Value {
    sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM transactional_outbox t WHERE id=ANY($1)")
        .bind(ids)
        .fetch_one(pool)
        .await
        .unwrap()
}
#[tokio::test]
async fn old_terminal_tasks_are_retained_while_old_pending_and_processing_tasks_finish() {
    let db = Database::new().await;
    let completed = new_task();
    let failed = new_task();
    let pending = new_task();
    let processing = new_task();
    enqueue(
        &db.pool,
        vec![completed.clone(), failed.clone(), pending.clone(), processing.clone()],
    )
    .await;
    sqlx::query("UPDATE transactional_outbox SET created_at=now()-interval '2 years',available_at=now()-interval '1 year'")
        .execute(&db.pool)
        .await
        .unwrap();
    for (id, status) in [(completed.id(), "completed"), (failed.id(), "failed")] {
        sqlx::query("UPDATE transactional_outbox SET status=$2,attempts=1,finished_at=now()-interval '1 year',last_error=CASE WHEN $2='failed' THEN 'historic failure' ELSE NULL END WHERE id=$1").bind(id).bind(status).execute(&db.pool).await.unwrap();
    }
    sqlx::query("UPDATE transactional_outbox SET status='processing',attempts=1,locked_by='old worker',lock_token=$2,locked_until=now()-interval '1 year' WHERE id=$1").bind(processing.id()).bind(Uuid::new_v4()).execute(&db.pool).await.unwrap();
    let terminal = [completed.id(), failed.id()];
    let before = terminal_history(&db.pool, &terminal).await;
    let queue = PostgresTaskQueue::new(db.pool.clone());
    let owned = queue
        .claim(&format(), "retention probe", 1, Duration::from_secs(60))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(owned.attempt().task_id(), pending.id());
    assert_eq!(terminal_history(&db.pool, &terminal).await, before);
    assert!(
        queue
            .settle(
                &owned,
                SettlementRequest::Retry {
                    delay: Duration::ZERO,
                    reason: "retention probe".into()
                }
            )
            .await
            .unwrap()
            .is_applied()
    );
    let recovered = queue.recover_expired(10).await.unwrap();
    assert_eq!(recovered.retry_scheduled_count(), 1);
    assert!(recovered.failures().is_empty());
    assert_eq!(terminal_history(&db.pool, &terminal).await, before);
    let handled = Arc::new(AtomicUsize::new(0));
    let dispatcher = TaskDispatcher::try_new(vec![CountingAdapter(handled.clone())]).unwrap();
    let workers = TaskWorkerPoolConfig::try_new(
        1,
        1,
        Duration::from_millis(10),
        Duration::from_secs(15),
        Duration::from_secs(60),
    )
    .unwrap();
    let config = TaskRuntimeConfig::try_new(TaskStorageConfig::default(), workers).unwrap();
    let prepared = task_runtime(&db.pool, Arc::new(dispatcher), config, Policies::default());
    let shutdown = CancellationToken::new();
    let runtime = tokio::spawn(prepared.run(shutdown.clone()));
    let progress = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let count: i64 =
                sqlx::query_scalar("SELECT count(*) FROM transactional_outbox WHERE id=ANY($1) AND status='completed'")
                    .bind([pending.id(), processing.id()].as_slice())
                    .fetch_one(&db.pool)
                    .await
                    .unwrap();
            if count == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), runtime)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    progress.unwrap();
    assert_eq!(handled.load(Ordering::SeqCst), 2);
    assert_eq!(terminal_history(&db.pool, &terminal).await, before);
    for task in [pending, processing] {
        let saved = row(&db.pool, task.id()).await;
        assert_eq!(saved.get::<String, _>("status"), "completed");
        assert_eq!(saved.get::<i32, _>("attempts"), 2);
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM transactional_outbox")
            .fetch_one(&db.pool)
            .await
            .unwrap(),
        4
    );
    db.close().await;
}
