use super::{CommandExecutor, CommandGatewayError, retry::retry_on_aggregate_conflict};
use crate::{
    cqrs::{
        AggregateError, CqrsFramework, aggregate::Aggregate, command::retry::AggregateConflictRetryPolicy, store::EventStore,
    },
    task::NewTask,
};
use std::{error::Error, marker::PhantomData};
/// Executes commands for one aggregate type through frameworks supplied by the
/// composition root or a runtime adapter factory.
///
/// The framework is built exactly once per [`execute`](Self::execute) call.
/// Every optimistic-conflict attempt reloads aggregate state through that same
/// framework and repeats clones of the original command. The immutable outbox
/// task list is handed to the factory once and therefore cannot be regenerated
/// or changed between attempts.
pub struct CqrsCommandExecutor<A, BuildFramework> {
    build_framework: BuildFramework,
    retry_policy: AggregateConflictRetryPolicy,
    aggregate: PhantomData<fn() -> A>,
}

impl<A, BuildFramework> CqrsCommandExecutor<A, BuildFramework> {
    pub fn new(build_framework: BuildFramework, retry_policy: AggregateConflictRetryPolicy) -> Self {
        Self {
            build_framework,
            retry_policy,
            aggregate: PhantomData,
        }
    }
}

impl<A, BuildFramework> CqrsCommandExecutor<A, BuildFramework>
where
    A: Aggregate + Clone,
    A::Command: Clone + Send,
{
    pub async fn execute<ES>(
        &self,
        aggregate_id: &str,
        command: A::Command,
        tasks: Vec<NewTask>,
    ) -> Result<(), CommandGatewayError<A::Error>>
    where
        BuildFramework: Fn(Vec<NewTask>) -> CqrsFramework<A, ES> + Send + Sync,
        ES: EventStore<A> + 'static,
        ES::AC: Send,
    {
        let framework = (self.build_framework)(tasks);
        let framework_ref = &framework;
        retry_on_aggregate_conflict(self.retry_policy, move || {
            framework_ref.execute(aggregate_id, command.clone())
        })
        .await
        .inspect_err(|error| log_aggregate_error(A::TYPE, error))
        .map_err(map_aggregate_error)
    }
}

fn log_aggregate_error<D: Error>(aggregate_type: &str, error: &AggregateError<D>) {
    let (category, source) = match error {
        AggregateError::DatabaseConnectionError(source) => ("connection", source),
        AggregateError::DeserializationError(source) => ("deserialization", source),
        AggregateError::UnexpectedError(source) => ("unexpected", source),
        AggregateError::CommittedWithEventHandlerFailure(failure) => {
            for error in &failure.errors {
                crate::diagnostics::log_command_failure(aggregate_type, "event_handler", &error.error);
            }
            return;
        }
        AggregateError::Domain(_) | AggregateError::AggregateConflict => return,
    };
    crate::diagnostics::log_command_failure(aggregate_type, category, source.as_ref());
}

impl<A, BuildFramework, ES> CommandExecutor<A> for CqrsCommandExecutor<A, BuildFramework>
where
    A: Aggregate + Clone + 'static,
    A::Command: Clone + Send + 'static,
    A::Error: Send + 'static,
    BuildFramework: Fn(Vec<NewTask>) -> CqrsFramework<A, ES> + Send + Sync,
    ES: EventStore<A> + 'static,
    ES::AC: Send,
{
    async fn execute(
        &self,
        aggregate_id: &str,
        command: A::Command,
        tasks: Vec<NewTask>,
    ) -> Result<(), CommandGatewayError<A::Error>> {
        CqrsCommandExecutor::execute(self, aggregate_id, command, tasks).await
    }
}

pub(super) fn map_aggregate_error<D: Error>(error: AggregateError<D>) -> CommandGatewayError<D> {
    match error {
        AggregateError::Domain(error) => CommandGatewayError::Domain(error),
        AggregateError::AggregateConflict => CommandGatewayError::Conflict,
        AggregateError::CommittedWithEventHandlerFailure(failure) => CommandGatewayError::CommittedWithEventHandlerFailure {
            last_sequence: failure.last_sequence,
            handler_indices: failure.errors.into_iter().map(|e| e.handler_index).collect(),
        },
        AggregateError::DatabaseConnectionError(_)
        | AggregateError::DeserializationError(_)
        | AggregateError::UnexpectedError(_) => CommandGatewayError::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        io,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use crate::cqrs::event::DomainEvent;
    use crate::cqrs::event::EventEnvelope;
    use crate::cqrs::event_sink::EventSink;
    use crate::cqrs::store::AggregateContext;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::cqrs::AggregateError;
    use crate::cqrs::CqrsFramework;
    use crate::cqrs::aggregate::Aggregate;
    use crate::cqrs::store::EventStore;
    use crate::task::{NewTask, format::TaskPayload};
    use std::time::Duration;
    use thiserror::Error;

    #[derive(Clone, Debug, Error, Eq, PartialEq)]
    #[error("rejected")]
    struct DummyError;

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum DummyCommand {
        Record(String),
        Reject,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    enum DummyEvent {
        Recorded(String),
    }

    impl DomainEvent for DummyEvent {
        fn event_type(&self) -> String {
            "recorded".to_owned()
        }

        fn event_version(&self) -> String {
            "1".to_owned()
        }
    }

    #[derive(Clone, Default, Serialize, Deserialize)]
    struct DummyAggregate;

    impl Aggregate for DummyAggregate {
        const TYPE: &'static str = "dummy";
        type Command = DummyCommand;
        type Event = DummyEvent;
        type Error = DummyError;
        type Services = Arc<Mutex<Vec<DummyCommand>>>;

        async fn handle(
            &mut self,
            command: Self::Command,
            seen: &Self::Services,
            sink: &EventSink<Self>,
        ) -> Result<(), Self::Error> {
            seen.lock().unwrap().push(command.clone());
            match command {
                DummyCommand::Record(value) => {
                    sink.write(DummyEvent::Recorded(value), self).await;
                    Ok(())
                }
                DummyCommand::Reject => Err(DummyError),
            }
        }

        fn apply(&mut self, _: Self::Event) {}
    }

    struct DummyContext(DummyAggregate);

    impl AggregateContext<DummyAggregate> for DummyContext {
        fn aggregate(&mut self) -> &mut DummyAggregate {
            &mut self.0
        }
    }

    #[derive(Clone)]
    struct DummyStore {
        commits: Arc<AtomicUsize>,
        conflicts: usize,
        load_error: Option<fn() -> AggregateError<DummyError>>,
    }

    impl EventStore<DummyAggregate> for DummyStore {
        type AC = DummyContext;

        async fn load_events(&self, _: &str) -> Result<Vec<EventEnvelope<DummyAggregate>>, AggregateError<DummyError>> {
            Ok(Vec::new())
        }

        async fn load_aggregate(&self, _: &str) -> Result<Self::AC, AggregateError<DummyError>> {
            if let Some(error) = self.load_error {
                return Err(error());
            }
            Ok(DummyContext(DummyAggregate))
        }

        async fn commit(
            &self,
            _: Vec<DummyEvent>,
            _: Self::AC,
            _: HashMap<String, String>,
        ) -> Result<Vec<EventEnvelope<DummyAggregate>>, AggregateError<DummyError>> {
            if self.commits.fetch_add(1, Ordering::SeqCst) < self.conflicts {
                Err(AggregateError::AggregateConflict)
            } else {
                Ok(Vec::new())
            }
        }
    }

    #[derive(Serialize)]
    struct DummyTask {
        value: &'static str,
    }

    impl TaskPayload for DummyTask {
        const TASK_TYPE: &'static str = "dummy";
        const TASK_VERSION: i32 = 1;
    }

    fn no_delay_policy(max_attempts: usize) -> AggregateConflictRetryPolicy {
        AggregateConflictRetryPolicy::new(max_attempts, Duration::ZERO, Duration::ZERO, 0.0).unwrap()
    }

    #[derive(Clone, Default, Serialize, Deserialize)]
    struct SendOnlyAggregate;

    impl Aggregate for SendOnlyAggregate {
        const TYPE: &'static str = "send_only";
        type Command = std::cell::Cell<u32>;
        type Event = DummyEvent;
        type Error = DummyError;
        type Services = ();

        async fn handle(
            &mut self,
            command: Self::Command,
            _: &(),
            sink: &crate::cqrs::event_sink::EventSink<Self>,
        ) -> Result<(), DummyError> {
            sink.write(DummyEvent::Recorded(command.get().to_string()), self).await;
            Ok(())
        }

        fn apply(&mut self, _: DummyEvent) {}
    }

    #[tokio::test]
    async fn executor_accepts_send_commands_without_requiring_sync() {
        let executor = Arc::new(CqrsCommandExecutor::new(
            |_| {
                CqrsFramework::new(
                    crate::cqrs::testing::mem_store::MemStore::<SendOnlyAggregate>::default(),
                    vec![],
                    (),
                )
            },
            no_delay_policy(1),
        ));
        let result = tokio::spawn(async move {
            CommandExecutor::execute(executor.as_ref(), "send-only", std::cell::Cell::new(7), vec![]).await
        })
        .await
        .unwrap();
        assert_eq!(result, Ok(()));
    }

    struct BorrowingExecutor;

    impl CommandExecutor<SendOnlyAggregate> for BorrowingExecutor {
        async fn execute(
            &self,
            aggregate_id: &str,
            command: std::cell::Cell<u32>,
            tasks: Vec<NewTask>,
        ) -> Result<(), CommandGatewayError<DummyError>> {
            let address = aggregate_id.as_ptr() as usize;
            command.set(command.get() + 1);
            tokio::task::yield_now().await;
            assert_eq!(aggregate_id, "borrowed-id");
            assert_eq!(aggregate_id.as_ptr() as usize, address);
            assert_eq!(command.get(), 8);
            assert!(tasks.is_empty());
            Ok(())
        }
    }

    #[tokio::test]
    async fn native_command_contract_keeps_a_borrowed_id_and_send_only_command_across_await() {
        fn require_send<F: std::future::Future + Send>(_: &F) {}
        let executor = BorrowingExecutor;
        let aggregate_id = String::from("borrowed-id");
        let future = CommandExecutor::execute(&executor, &aggregate_id, std::cell::Cell::new(7), vec![]);
        require_send(&future);
        future.await.unwrap();
        assert_eq!(aggregate_id, "borrowed-id");
    }

    #[tokio::test]
    async fn executor_builds_once_and_retries_the_unchanged_command_and_tasks() {
        let builds = Arc::new(AtomicUsize::new(0));
        let commits = Arc::new(AtomicUsize::new(0));
        let commands = Arc::new(Mutex::new(Vec::new()));
        let captured_tasks = Arc::new(Mutex::new(Vec::new()));
        let executor = Arc::new(CqrsCommandExecutor::<DummyAggregate, _>::new(
            {
                let builds = builds.clone();
                let commits = commits.clone();
                let commands = commands.clone();
                let captured_tasks = captured_tasks.clone();
                move |tasks| {
                    builds.fetch_add(1, Ordering::SeqCst);
                    captured_tasks.lock().unwrap().push(tasks);
                    CqrsFramework::new(
                        DummyStore {
                            commits: commits.clone(),
                            conflicts: 2,
                            load_error: None,
                        },
                        vec![],
                        commands.clone(),
                    )
                }
            },
            no_delay_policy(3),
        ));
        let task = NewTask::new(DummyTask { value: "same" }).unwrap();

        CommandExecutor::execute(
            executor.as_ref(),
            "aggregate",
            DummyCommand::Record("same".to_owned()),
            vec![task.clone()],
        )
        .await
        .unwrap();

        assert_eq!(builds.load(Ordering::SeqCst), 1);
        assert_eq!(commits.load(Ordering::SeqCst), 3);
        assert_eq!(
            *commands.lock().unwrap(),
            vec![
                DummyCommand::Record("same".to_owned()),
                DummyCommand::Record("same".to_owned()),
                DummyCommand::Record("same".to_owned()),
            ]
        );
        assert_eq!(*captured_tasks.lock().unwrap(), vec![vec![task]]);
    }

    #[tokio::test]
    async fn executor_maps_domain_conflict_and_technical_errors_without_wrong_retries() {
        let domain_executor = Arc::new(CqrsCommandExecutor::<DummyAggregate, _>::new(
            |_| {
                CqrsFramework::new(
                    DummyStore {
                        commits: Arc::default(),
                        conflicts: 0,
                        load_error: None,
                    },
                    vec![],
                    Arc::default(),
                )
            },
            no_delay_policy(3),
        ));
        assert_eq!(
            CommandExecutor::execute(domain_executor.as_ref(), "aggregate", DummyCommand::Reject, vec![]).await,
            Err(CommandGatewayError::Domain(DummyError))
        );

        let commits = Arc::new(AtomicUsize::new(0));
        let conflict_executor = Arc::new(CqrsCommandExecutor::<DummyAggregate, _>::new(
            {
                let commits = commits.clone();
                move |_| {
                    CqrsFramework::new(
                        DummyStore {
                            commits: commits.clone(),
                            conflicts: usize::MAX,
                            load_error: None,
                        },
                        vec![],
                        Arc::default(),
                    )
                }
            },
            no_delay_policy(3),
        ));
        assert_eq!(
            CommandExecutor::execute(
                conflict_executor.as_ref(),
                "aggregate",
                DummyCommand::Record("value".to_owned()),
                vec![]
            )
            .await,
            Err(CommandGatewayError::Conflict)
        );
        assert_eq!(commits.load(Ordering::SeqCst), 3);

        for error in [
            || AggregateError::DatabaseConnectionError(Box::new(io::Error::other("connection"))),
            || AggregateError::DeserializationError(Box::new(io::Error::other("json"))),
            || AggregateError::UnexpectedError(Box::new(io::Error::other("unexpected"))),
        ] {
            let executor = Arc::new(CqrsCommandExecutor::<DummyAggregate, _>::new(
                move |_| {
                    CqrsFramework::new(
                        DummyStore {
                            commits: Arc::default(),
                            conflicts: 0,
                            load_error: Some(error),
                        },
                        vec![],
                        Arc::default(),
                    )
                },
                no_delay_policy(3),
            ));
            assert_eq!(
                CommandExecutor::execute(
                    executor.as_ref(),
                    "aggregate",
                    DummyCommand::Record("value".to_owned()),
                    vec![]
                )
                .await,
                Err(CommandGatewayError::Unavailable)
            );
        }
    }

    #[tokio::test]
    async fn executor_logs_technical_failure_once_and_suppresses_domain_and_conflict_failures() {
        let (_, log) = crate::diagnostics::test_support::capture(async {
            for error in [
                || AggregateError::DatabaseConnectionError(Box::new(io::Error::other("private_connection"))),
                || AggregateError::DeserializationError(Box::new(io::Error::other("private_json"))),
                || AggregateError::UnexpectedError(Box::new(io::Error::other("private_unexpected"))),
            ] {
                let executor = CqrsCommandExecutor::<DummyAggregate, _>::new(
                    move |_| {
                        CqrsFramework::new(
                            DummyStore {
                                commits: Arc::default(),
                                conflicts: 0,
                                load_error: Some(error),
                            },
                            vec![],
                            Arc::default(),
                        )
                    },
                    no_delay_policy(3),
                );
                assert_eq!(
                    executor
                        .execute(
                            "private_id@example.com",
                            DummyCommand::Record("private_command".into()),
                            vec![]
                        )
                        .await,
                    Err(CommandGatewayError::Unavailable)
                );
            }
            let commits = Arc::new(AtomicUsize::new(0));
            let count = commits.clone();
            let executor = CqrsCommandExecutor::<DummyAggregate, _>::new(
                move |_| {
                    CqrsFramework::new(
                        DummyStore {
                            commits: count.clone(),
                            conflicts: usize::MAX,
                            load_error: None,
                        },
                        vec![],
                        Arc::default(),
                    )
                },
                no_delay_policy(3),
            );
            assert_eq!(
                executor.execute("private_id", DummyCommand::Reject, vec![]).await,
                Err(CommandGatewayError::Domain(DummyError))
            );
            assert_eq!(
                executor
                    .execute("private_id", DummyCommand::Record("private_command".into()), vec![])
                    .await,
                Err(CommandGatewayError::Conflict)
            );
            assert_eq!(commits.load(Ordering::SeqCst), 3);
        })
        .await;
        assert_eq!(log.matches("command execution failed").count(), 3);
        assert!(log.contains("aggregate_type=\"dummy\""));
        for category in ["connection", "deserialization", "unexpected"] {
            assert!(log.contains(category));
        }
        assert!(!log.contains("private_"));
    }

    #[test]
    fn maps_every_framework_error_category() {
        assert_eq!(
            CommandGatewayError::<DummyError>::CommittedWithEventHandlerFailure {
                last_sequence: 42,
                handler_indices: vec![2]
            }
            .to_string(),
            "command committed through sequence 42; event handlers failed"
        );
        assert_eq!(
            map_aggregate_error(AggregateError::Domain(DummyError)),
            CommandGatewayError::Domain(DummyError)
        );
        assert_eq!(
            map_aggregate_error::<DummyError>(AggregateError::AggregateConflict),
            CommandGatewayError::Conflict
        );
        assert_eq!(
            map_aggregate_error::<DummyError>(AggregateError::CommittedWithEventHandlerFailure(
                crate::cqrs::CommittedWithEventHandlerFailure {
                    last_sequence: 42,
                    errors: vec![crate::cqrs::EventHandlerFailure {
                        handler_index: 2,
                        error: crate::cqrs::store::PersistenceError::OptimisticLockError
                    }]
                }
            )),
            CommandGatewayError::CommittedWithEventHandlerFailure {
                last_sequence: 42,
                handler_indices: vec![2]
            }
        );
        for error in [
            AggregateError::DatabaseConnectionError(Box::new(io::Error::other("db"))),
            AggregateError::DeserializationError(Box::new(io::Error::other("json"))),
            AggregateError::UnexpectedError(Box::new(io::Error::other("unexpected"))),
        ] {
            assert_eq!(map_aggregate_error::<DummyError>(error), CommandGatewayError::Unavailable);
        }
    }
}
