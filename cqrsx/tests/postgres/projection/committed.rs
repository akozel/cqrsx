use crate::support::{Database, new_task};
use cqrsx::{
    cqrs::{
        CqrsFramework,
        aggregate::Aggregate,
        command::{CommandGatewayError, FailedHandler, executor::CqrsCommandExecutor},
        event::{DomainEvent, EventEnvelope},
        event_handler::{EventHandler, delivery::DeliveryConfig},
        event_sink::EventSink,
        projection::{
            View, ViewProjection,
            repository::{ViewContext, ViewRepository},
        },
        store::{
            PersistenceError, persisted::PersistedEventStore, repository::PersistedEventRepository,
            serialized_event::SerializedEvent,
        },
    },
    postgres::{outbox::PostgresEventRepositoryWithOutbox, view_repository::PostgresViewRepository},
};
use serde::{Deserialize, Serialize};
use std::{
    convert::Infallible,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct State(Vec<String>);
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Recorded(String);
impl DomainEvent for Recorded {
    fn event_type(&self) -> String {
        "recorded".into()
    }
    fn event_version(&self) -> String {
        "1".into()
    }
}
// Services deliberately do not implement Clone.
struct Services(Arc<AtomicUsize>);
impl Aggregate for State {
    const TYPE: &'static str = "projection_command";
    type Command = String;
    type Event = Recorded;
    type Error = Infallible;
    type Services = Services;
    async fn handle(&mut self, command: String, services: &Services, sink: &EventSink<Self>) -> Result<(), Infallible> {
        services.0.fetch_add(1, Ordering::SeqCst);
        sink.write(Recorded(command), self).await;
        Ok(())
    }
    fn apply(&mut self, event: Recorded) {
        self.0.push(event.0);
    }
}
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
struct Projection(Vec<String>);
impl View<State> for Projection {
    fn update(&mut self, event: &EventEnvelope<State>) {
        self.0.push(event.payload.0.clone());
    }
}
struct WrittenButFailed(PostgresViewRepository<Projection, State>);
impl ViewRepository<Projection, State> for WrittenButFailed {
    async fn load(&self, id: &str) -> Result<Option<Projection>, PersistenceError> {
        self.0.load(id).await
    }
    async fn load_with_context(&self, id: &str) -> Result<Option<(Projection, ViewContext)>, PersistenceError> {
        self.0.load_with_context(id).await
    }
    async fn update_view(&self, view: Projection, context: ViewContext) -> Result<(), PersistenceError> {
        self.0.update_view(view, context).await?;
        Err(PersistenceError::ConnectionError(
            std::io::Error::other("view reply lost after actual commit").into(),
        ))
    }
}
struct Conflict(Arc<AtomicUsize>);
impl ViewRepository<Projection, State> for Conflict {
    async fn load(&self, _: &str) -> Result<Option<Projection>, PersistenceError> {
        Ok(None)
    }
    async fn load_with_context(&self, _: &str) -> Result<Option<(Projection, ViewContext)>, PersistenceError> {
        Ok(None)
    }
    async fn update_view(&self, _: Projection, _: ViewContext) -> Result<(), PersistenceError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(PersistenceError::OptimisticLockError)
    }
}
struct Capture(Arc<Mutex<Vec<SerializedEvent>>>);
#[async_trait::async_trait]
impl EventHandler<State> for Capture {
    async fn handle(&self, _: &str, events: &[EventEnvelope<State>]) -> Result<(), PersistenceError> {
        for event in events {
            self.0.lock().unwrap().push(SerializedEvent::try_from(event)?);
        }
        Ok(())
    }
}
/// The handlers in registration order: a view whose write succeeds but whose reply is lost, a capture,
/// a view that always conflicts, and a second capture.
fn handlers(
    pool: sqlx::PgPool,
    attempts: Arc<AtomicUsize>,
    reported: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<SerializedEvent>>>,
) -> Vec<Arc<dyn EventHandler<State>>> {
    let mut written =
        ViewProjection::new(Arc::new(WrittenButFailed(PostgresViewRepository::new("command_view", pool)))).with_name("written");
    written.set_error_observer(Box::new(move |error| {
        assert!(matches!(error, PersistenceError::ConnectionError(_)));
        reported.fetch_add(1, Ordering::SeqCst);
    }));
    let conflict = ViewProjection::new(Arc::new(Conflict(attempts))).with_max_attempts(NonZeroUsize::new(3).unwrap());
    vec![
        Arc::new(written),
        Arc::new(Capture(seen.clone())),
        Arc::new(conflict),
        Arc::new(Capture(seen)),
    ]
}
#[tokio::test]
async fn projection_failures_preserve_one_command_commit_and_outbox_and_invoke_other_handlers() {
    let db = Database::new().await;
    sqlx::query("CREATE TABLE command_view (view_id text PRIMARY KEY, version bigint NOT NULL, payload json NOT NULL)")
        .execute(&db.pool)
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let attempts = Arc::new(AtomicUsize::new(0));
    let reported = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let first_task = new_task();
    let second_task = new_task();
    let executor = CqrsCommandExecutor::new(
        {
            let pool = db.pool.clone();
            let calls = calls.clone();
            move |tasks| {
                let repository = PostgresEventRepositoryWithOutbox::new(pool.clone()).with_outbox(tasks);
                CqrsFramework::new(
                    PersistedEventStore::new_snapshot_store(repository, 1),
                    Services(calls.clone()),
                )
            }
        },
        Default::default(),
        cqrsx::cqrs::command::locks::LockLimits::default(),
    );
    let executor = handlers(db.pool.clone(), attempts.clone(), reported.clone(), seen.clone())
        .into_iter()
        .fold(executor, |executor, handler| {
            executor.with_event_handler(handler, DeliveryConfig::default())
        });
    for (id, task) in [("first", &first_task), ("second", &second_task)] {
        assert_eq!(
            executor.execute(id, "accepted".into(), vec![task.clone()]).await,
            Err(CommandGatewayError::CommittedWithEventHandlerFailure {
                last_sequence: 1,
                handlers: vec![
                    FailedHandler {
                        index: 0,
                        name: "written".into()
                    },
                    FailedHandler {
                        index: 2,
                        name: std::any::type_name::<Projection>().into()
                    },
                ]
            })
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(attempts.load(Ordering::SeqCst), 6);
    assert_eq!(reported.load(Ordering::SeqCst), 2);
    let repo = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    for (id, task) in [("first", first_task), ("second", second_task)] {
        let events = repo.get_events::<State>(id).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, 1);
        let snapshot = repo.get_snapshot::<State>(id).await.unwrap().unwrap();
        assert_eq!(snapshot.aggregate, serde_json::json!(["accepted"]));
        assert_eq!(snapshot.last_sequence, 1);
        assert_eq!(snapshot.snapshot_version, 1);
        let ids: Vec<uuid::Uuid> =
            sqlx::query_scalar("SELECT id FROM transactional_outbox WHERE aggregate_type=$1 AND aggregate_id=$2")
                .bind(State::TYPE)
                .bind(id)
                .fetch_all(&db.pool)
                .await
                .unwrap();
        assert_eq!(ids, [task.id()]);
        let view = PostgresViewRepository::<Projection, State>::new("command_view", db.pool.clone())
            .load_with_context(id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(view.0, Projection(vec!["accepted".into()]));
        assert_eq!(view.1.version, 1);
        let observed: Vec<_> = seen
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.aggregate_id == id)
            .cloned()
            .collect();
        assert_eq!(observed, [events[0].clone(), events[0].clone()]);
    }
    db.close().await;
}
