//! Command services: the demo's use of the cqrsx command port.
//!
//! One long-lived `CqrsCommandExecutor` per aggregate. Every call builds a fresh `CqrsFramework`
//! (via the factory closure) that binds the immutable outbox task list to one PostgreSQL
//! repository; conflict retries reuse that framework. Metadata (`command_id`, `correlation_id`,
//! `causation_id`, `recorded_at`, `source`) travels through `execute_with_metadata` (decision
//! D-04) and ends up in the `events.metadata` column of every committed event.
use crate::{
    application::{activity::ActivityHub, lock::BoxOfficeLock, projections::notifying::NotifyingHandler, tasks},
    domain::{
        ConcertId, FanId,
        concert::{Concert, ConcertCommand, ConcertError, ConcertServices},
        fan::{Fan, FanCommand, FanError},
        shared::venue::VENUE,
    },
};
use chrono::{SecondsFormat, Utc};
use cqrsx::{
    cqrs::{
        CqrsFramework,
        aggregate::Aggregate,
        command::{CommandGatewayError, executor::CqrsCommandExecutor, retry::AggregateConflictRetryPolicy},
        event_handler::EventHandler,
        projection::{View, ViewProjection, repository::ViewRepository},
        store::{EventStore, persisted::PersistedEventStore},
    },
    postgres::{event_repository::PostgresEventRepository, outbox::PostgresEventRepositoryWithOutbox},
    task::NewTask,
};
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::PgPool;
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

/// Who issued the command and how it relates to earlier work. For UI commands the three ids are
/// equal; internal commands inherit the task's `correlation_id` and point `causation_id` at the
/// task. `recorded_at` has a fixed width so the sidecar can sort it as text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandMetadata {
    pub command_id: Uuid,
    pub correlation_id: Uuid,
    pub causation_id: Uuid,
    pub recorded_at: String,
    pub source: String,
}

impl CommandMetadata {
    fn now() -> String {
        Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true)
    }

    pub fn with_source(source: impl Into<String>) -> Self {
        let command_id = Uuid::now_v7();
        Self {
            command_id,
            correlation_id: command_id,
            causation_id: command_id,
            recorded_at: Self::now(),
            source: source.into(),
        }
    }

    pub fn ui() -> Self {
        Self::with_source("ui")
    }

    /// Metadata of an internal command issued by an outbox task handler.
    pub fn from_task(correlation_id: Uuid, task_id: Uuid, task_type: &str) -> Self {
        Self {
            command_id: Uuid::now_v7(),
            correlation_id,
            causation_id: task_id,
            recorded_at: Self::now(),
            source: format!("task:{task_type}"),
        }
    }

    pub fn to_map(&self) -> HashMap<String, String> {
        HashMap::from([
            ("command_id".to_owned(), self.command_id.to_string()),
            ("correlation_id".to_owned(), self.correlation_id.to_string()),
            ("causation_id".to_owned(), self.causation_id.to_string()),
            ("recorded_at".to_owned(), self.recorded_at.clone()),
            ("source".to_owned(), self.source.clone()),
        ])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Warning {
    pub code: &'static str,
    pub projection: String,
}

/// Outcome of a command after the gateway error has been interpreted (§6.6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandOutcome<E> {
    Accepted {
        warnings: Vec<Warning>,
    },
    Rejected(E),
    Conflict,
    Unavailable,
    /// The box-office lock is held: closed for a moment while the read models are rebuilt.
    Locked,
}

impl<E> CommandOutcome<E> {
    pub fn is_accepted(&self) -> bool {
        matches!(self, CommandOutcome::Accepted { .. })
    }
}

/// Maps the library outcome to the demo outcome; projection names resolve the handler indices
/// reported by `CommittedWithEventHandlerFailure` (registration order).
pub fn map_gateway_result<E>(result: Result<(), CommandGatewayError<E>>, projections: &[&'static str]) -> CommandOutcome<E> {
    match result {
        Ok(()) => CommandOutcome::Accepted { warnings: vec![] },
        Err(CommandGatewayError::Domain(error)) => CommandOutcome::Rejected(error),
        Err(CommandGatewayError::Conflict) => CommandOutcome::Conflict,
        Err(CommandGatewayError::Unavailable) => CommandOutcome::Unavailable,
        Err(CommandGatewayError::CommittedWithEventHandlerFailure { handler_indices, .. }) => CommandOutcome::Accepted {
            warnings: handler_indices
                .into_iter()
                .map(|index| Warning {
                    code: "projection_failed",
                    projection: projections.get(index).copied().unwrap_or("unknown").to_owned(),
                })
                .collect(),
        },
    }
}

type Store<A> = PersistedEventStore<PostgresEventRepositoryWithOutbox, A>;
type BuildFramework<A> = Box<dyn Fn(Vec<NewTask>) -> CqrsFramework<A, Store<A>> + Send + Sync>;
pub type HandlerFactory<A> = Arc<dyn Fn() -> Box<dyn EventHandler<A>> + Send + Sync>;

/// An event handler under the name the activity log and the API report it by. Handlers are built
/// per framework (one per command); they are cheap — an `Arc` to the view repository. The
/// registration order is the handler index in `CommittedWithEventHandlerFailure`, and the name
/// list the services use is derived from this vector, so the two cannot drift apart.
pub struct NamedHandler<A> {
    pub name: &'static str,
    pub build: HandlerFactory<A>,
}

impl<A: Aggregate + 'static> NamedHandler<A> {
    /// A `ViewProjection` over `repository`, wrapped so the activity log sees
    /// `projection.updated` / `projection.failed`.
    pub fn projection<R, V>(name: &'static str, repository: Arc<R>, activity: Arc<ActivityHub>) -> Self
    where
        R: ViewRepository<V, A> + 'static,
        V: View<A> + 'static,
    {
        Self {
            name,
            build: Arc::new(move || {
                Box::new(NotifyingHandler::new(
                    ViewProjection::new(repository.clone()),
                    name,
                    activity.clone(),
                ))
            }),
        }
    }
}

/// Shared wiring for both aggregates.
pub struct Executors {
    pub pool: PgPool,
    pub snapshot_every: usize,
    pub retry: AggregateConflictRetryPolicy,
}

impl Executors {
    /// The executor plus the projection names in registration order.
    fn build<A>(
        &self,
        handlers: Vec<NamedHandler<A>>,
        services: A::Services,
    ) -> (CqrsCommandExecutor<A, BuildFramework<A>>, Vec<&'static str>)
    where
        A: Aggregate + Clone + 'static,
        A::Services: Clone,
    {
        let (names, factories): (Vec<_>, Vec<_>) = handlers.into_iter().map(|h| (h.name, h.build)).unzip();
        let pool = self.pool.clone();
        let snapshot_every = self.snapshot_every;
        let build: BuildFramework<A> = Box::new(move |tasks| {
            let repository = PostgresEventRepositoryWithOutbox::new(pool.clone()).with_outbox(tasks);
            CqrsFramework::new(
                PersistedEventStore::new_snapshot_store(repository, snapshot_every),
                factories.iter().map(|build| build()).collect(),
                services.clone(),
            )
        });
        (CqrsCommandExecutor::new(build, self.retry), names)
    }
}

/// What the HTTP layer and task handlers get back: the interpreted outcome plus the formats of the
/// tasks that were bound to the command (published only if it was accepted).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Executed<E> {
    pub outcome: CommandOutcome<E>,
    pub tasks: Vec<String>,
}

impl<E> Executed<E> {
    fn locked() -> Self {
        Self {
            outcome: CommandOutcome::Locked,
            tasks: vec![],
        }
    }
}

pub struct ConcertCommands {
    executor: CqrsCommandExecutor<Concert, BuildFramework<Concert>>,
    /// Read-only access to committed concert state, used to resolve the booking a cancellation
    /// refers to before the command runs (tasks must exist before execution).
    reader: PersistedEventStore<PostgresEventRepository, Concert>,
    projections: Vec<&'static str>,
    activity: Arc<ActivityHub>,
    lock: Arc<BoxOfficeLock>,
    /// Attempt budget of the chain tasks this service publishes (`encore_record_*`).
    task_max_attempts: i32,
}

impl ConcertCommands {
    pub fn new(
        executors: &Executors,
        handlers: Vec<NamedHandler<Concert>>,
        services: ConcertServices,
        activity: Arc<ActivityHub>,
        lock: Arc<BoxOfficeLock>,
        task_max_attempts: i32,
    ) -> Self {
        let (executor, projections) = executors.build(handlers, services);
        Self {
            executor,
            reader: PersistedEventStore::new_snapshot_store(
                PostgresEventRepository::new(executors.pool.clone()),
                executors.snapshot_every,
            ),
            projections,
            activity,
            lock,
            task_max_attempts,
        }
    }

    /// Tasks that must be published atomically with the event of an accepted command. They are
    /// built *before* the command runs, from command data and the venue (the price is static).
    /// An invalid or unbooked seat yields no task: the aggregate rejects such commands before
    /// anything is written, so no task can ever be published without its event.
    async fn tasks_for(
        &self,
        concert_id: &ConcertId,
        command: &ConcertCommand,
        metadata: &CommandMetadata,
    ) -> Result<Vec<NewTask>, String> {
        let task = match command {
            ConcertCommand::AnnounceConcert { .. } => return Ok(vec![]),
            ConcertCommand::BookSeat {
                seat,
                fan_id,
                fan_name,
                booking_id,
            } => {
                let Some(info) = VENUE.seat(seat) else { return Ok(vec![]) };
                tasks::new_task(
                    tasks::RecordTicketV1 {
                        booking_id: *booking_id,
                        concert_id: concert_id.to_string(),
                        seat: info.id,
                        zone: info.zone,
                        price: info.price,
                        fan_id: fan_id.to_string(),
                        fan_name: fan_name.clone(),
                    },
                    metadata,
                    concert_id.as_str(),
                    fan_id.as_str(),
                    self.task_max_attempts,
                )
            }
            ConcertCommand::CancelBooking {
                seat,
                fan_id,
                cancellation_id,
            } => {
                let Some(info) = VENUE.seat(seat) else { return Ok(vec![]) };
                // The booking identity and the refund ("what was paid back then") live in the
                // concert's history, so read the committed state first. If the booking changes
                // between this read and the commit, the command is retried on fresh state by the
                // conflict policy and the aggregate still decides; the task only carries what the
                // Fan aggregate needs to find the ticket.
                let context = self
                    .reader
                    .load_aggregate(concert_id.as_str())
                    .await
                    .map_err(|e| e.to_string())?;
                let Some(booking) = context.aggregate.bookings().get(&info.id).filter(|b| &b.fan_id == fan_id) else {
                    return Ok(vec![]);
                };
                tasks::new_task(
                    tasks::RecordCancellationV1 {
                        booking_id: booking.booking_id,
                        cancellation_id: *cancellation_id,
                        concert_id: concert_id.to_string(),
                        seat: info.id,
                        fan_id: fan_id.to_string(),
                        refund: booking.price,
                    },
                    metadata,
                    concert_id.as_str(),
                    fan_id.as_str(),
                    self.task_max_attempts,
                )
            }
        };
        task.map(|t| vec![t]).map_err(|e| e.to_string())
    }

    pub async fn execute(
        &self,
        concert_id: &ConcertId,
        command: ConcertCommand,
        metadata: CommandMetadata,
    ) -> Executed<ConcertError> {
        if self.lock.is_locked() {
            return Executed::locked();
        }
        let tasks = match self.tasks_for(concert_id, &command, &metadata).await {
            Ok(tasks) => tasks,
            Err(error) => {
                tracing::error!("cannot build outbox tasks: {error}");
                return Executed {
                    outcome: CommandOutcome::Unavailable,
                    tasks: vec![],
                };
            }
        };
        let task_formats: Vec<String> = tasks.iter().map(|t| t.format().to_string()).collect();
        let command_type = command.name();
        let fan_id = command.fan_id().cloned();
        let result = self
            .executor
            .execute_with_metadata(concert_id.as_str(), command, metadata.to_map(), tasks)
            .await;
        let outcome = map_gateway_result(result, &self.projections);
        publish_outcome(
            &self.activity,
            &OutcomeContext {
                aggregate_type: Concert::TYPE,
                aggregate_id: concert_id.as_str(),
                command_type,
                fan_id: fan_id.as_ref().map(FanId::as_str),
                metadata: &metadata,
                task_formats: &task_formats,
            },
            &outcome,
            |e| (e.code(), e.params()),
        );
        Executed {
            outcome,
            tasks: task_formats,
        }
    }
}

pub struct FanCommands {
    executor: CqrsCommandExecutor<Fan, BuildFramework<Fan>>,
    projections: Vec<&'static str>,
    activity: Arc<ActivityHub>,
    lock: Arc<BoxOfficeLock>,
}

impl FanCommands {
    pub fn new(
        executors: &Executors,
        handlers: Vec<NamedHandler<Fan>>,
        activity: Arc<ActivityHub>,
        lock: Arc<BoxOfficeLock>,
    ) -> Self {
        let (executor, projections) = executors.build(handlers, ());
        Self {
            executor,
            projections,
            activity,
            lock,
        }
    }

    pub fn lock(&self) -> &Arc<BoxOfficeLock> {
        &self.lock
    }

    /// Internal commands come from task handlers, which also build the follow-up tasks (from the
    /// parent task's payload). During a rebuild the outcome is `Locked`, which the task handler
    /// turns into a short retry: the task is durable, so waiting costs nothing, and the wallet
    /// projection is not written while its collection is being rebuilt.
    pub async fn execute(
        &self,
        fan_id: &FanId,
        command: FanCommand,
        metadata: CommandMetadata,
        tasks: Vec<NewTask>,
    ) -> Executed<FanError> {
        if self.lock.is_locked() {
            return Executed::locked();
        }
        let task_formats: Vec<String> = tasks.iter().map(|t| t.format().to_string()).collect();
        let command_type = command.name();
        let result = self
            .executor
            .execute_with_metadata(fan_id.as_str(), command, metadata.to_map(), tasks)
            .await;
        let outcome = map_gateway_result(result, &self.projections);
        publish_outcome(
            &self.activity,
            &OutcomeContext {
                aggregate_type: Fan::TYPE,
                aggregate_id: fan_id.as_str(),
                command_type,
                fan_id: Some(fan_id.as_str()),
                metadata: &metadata,
                task_formats: &task_formats,
            },
            &outcome,
            |e| (e.code(), e.params()),
        );
        Executed {
            outcome,
            tasks: task_formats,
        }
    }
}

/// What every `command.*` activity entry says about the command, whatever its outcome.
struct OutcomeContext<'a> {
    aggregate_type: &'a str,
    aggregate_id: &'a str,
    command_type: &'a str,
    fan_id: Option<&'a str>,
    metadata: &'a CommandMetadata,
    task_formats: &'a [String],
}

fn publish_outcome<E>(
    activity: &ActivityHub,
    context: &OutcomeContext<'_>,
    outcome: &CommandOutcome<E>,
    describe: impl Fn(&E) -> (&'static str, Value),
) {
    let base = json!({
        "aggregate_type": context.aggregate_type,
        "aggregate_id": context.aggregate_id,
        "command_id": context.metadata.command_id,
        "correlation_id": context.metadata.correlation_id,
        "source": context.metadata.source,
        "command_type": context.command_type,
        "fan_id": context.fan_id,
    });
    let merge = |extra: Value| {
        let mut map = base.as_object().cloned().unwrap_or_default();
        if let Value::Object(extra) = extra {
            map.extend(extra);
        }
        Value::Object(map)
    };
    match outcome {
        CommandOutcome::Accepted { warnings } => {
            activity.publish(
                "command.accepted",
                merge(json!({ "tasks": context.task_formats, "warnings": warnings })),
            );
            for warning in warnings {
                activity.publish(
                    "projection.failed",
                    json!({ "projection": warning.projection, "view_id": context.aggregate_id, "message": "event handler failed after commit" }),
                );
            }
        }
        CommandOutcome::Rejected(error) => {
            let (code, params) = describe(error);
            activity.publish("command.rejected", merge(json!({ "code": code, "params": params })));
        }
        CommandOutcome::Conflict => {
            activity.publish(
                "command.failed",
                merge(json!({ "outcome": "conflict", "message": "concurrent change; retries exhausted" })),
            );
        }
        CommandOutcome::Unavailable => {
            activity.publish(
                "command.failed",
                merge(json!({ "outcome": "unavailable", "message": "storage unavailable" })),
            );
        }
        CommandOutcome::Locked => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ui_metadata_uses_one_id_for_all_three_roles_and_fixed_width_time() {
        let metadata = CommandMetadata::ui();
        assert_eq!(metadata.command_id, metadata.correlation_id);
        assert_eq!(metadata.command_id, metadata.causation_id);
        assert_eq!(metadata.source, "ui");
        assert_eq!(metadata.recorded_at.len(), "2026-10-17T09:12:03.123456Z".len());
        let map = metadata.to_map();
        assert_eq!(map["source"], "ui");
        assert_eq!(map["recorded_at"], metadata.recorded_at);
    }

    #[test]
    fn task_metadata_inherits_correlation_and_points_causation_at_the_task() {
        let correlation = Uuid::now_v7();
        let task = Uuid::new_v4();
        let metadata = CommandMetadata::from_task(correlation, task, "encore_record_ticket");
        assert_eq!(metadata.correlation_id, correlation);
        assert_eq!(metadata.causation_id, task);
        assert_ne!(metadata.command_id, correlation);
        assert_eq!(metadata.source, "task:encore_record_ticket");
    }

    #[test]
    fn gateway_results_map_to_outcomes_and_projection_names() {
        let names = ["concert_halls"];
        assert_eq!(
            map_gateway_result::<ConcertError>(Ok(()), &names),
            CommandOutcome::Accepted { warnings: vec![] }
        );
        assert_eq!(
            map_gateway_result(Err(CommandGatewayError::Domain(ConcertError::SoldOut)), &names),
            CommandOutcome::Rejected(ConcertError::SoldOut)
        );
        assert_eq!(
            map_gateway_result::<ConcertError>(Err(CommandGatewayError::Conflict), &names),
            CommandOutcome::Conflict
        );
        assert_eq!(
            map_gateway_result::<ConcertError>(Err(CommandGatewayError::Unavailable), &names),
            CommandOutcome::Unavailable
        );
        assert_eq!(
            map_gateway_result::<ConcertError>(
                Err(CommandGatewayError::CommittedWithEventHandlerFailure {
                    last_sequence: 7,
                    handler_indices: vec![0, 5]
                }),
                &names
            ),
            CommandOutcome::Accepted {
                warnings: vec![
                    Warning {
                        code: "projection_failed",
                        projection: "concert_halls".into()
                    },
                    Warning {
                        code: "projection_failed",
                        projection: "unknown".into()
                    },
                ]
            }
        );
    }

    #[tokio::test]
    async fn a_locked_box_office_refuses_commands_before_touching_storage() {
        let lock = Arc::new(BoxOfficeLock::new());
        let executors = Executors {
            pool: crate::testing::lazy_pool(),
            snapshot_every: 10,
            retry: AggregateConflictRetryPolicy::default(),
        };
        let activity = Arc::new(ActivityHub::new(8, 8));
        let concerts = ConcertCommands::new(
            &executors,
            vec![],
            ConcertServices::default(),
            activity.clone(),
            lock.clone(),
            6,
        );
        let fans = FanCommands::new(&executors, vec![], activity.clone(), lock.clone());
        let _guard = lock.try_acquire().unwrap();
        let concert_id = ConcertId::parse("c-1").unwrap();
        let fan_id = FanId::parse("fox").unwrap();
        let executed = concerts
            .execute(
                &concert_id,
                ConcertCommand::CancelBooking {
                    seat: "B4".into(),
                    fan_id: fan_id.clone(),
                    cancellation_id: Uuid::now_v7(),
                },
                CommandMetadata::ui(),
            )
            .await;
        assert_eq!(executed, Executed::locked());
        let executed = fans
            .execute(
                &fan_id,
                FanCommand::RecordCancellation {
                    booking_id: Uuid::now_v7(),
                    cancellation_id: Uuid::now_v7(),
                    refund: 80,
                },
                CommandMetadata::ui(),
                vec![],
            )
            .await;
        assert_eq!(executed, Executed::locked());
        assert!(activity.subscribe().0.is_empty(), "a locked outcome is not logged");
    }
}
