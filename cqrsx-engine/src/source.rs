//! A command from an entry point of the application ([`Source`]) and what became of it ([`CommandResult`]).
use crate::Cqrs;
use crate::diagnostics;
use cqrsx::{
    command::{CommandError, CommandExecutor, CommitReport, metadata::CommandMetadata},
    domain::aggregate::Aggregate,
};
use std::fmt;
use uuid::Uuid;

/// What became of a command: accepted with the tasks committed with its event, rejected by its aggregate, a conflict
/// after every retry, or unavailable (not run, or its outcome unknown). The same answer as the command executors'.
pub type CommandResult<A> = Result<CommitReport<A>, CommandError<<A as Aggregate>::Rejection>>;

/// An entry point of the application, e.g. the HTTP API (`cqrs.source("ui")`): the source of the commands it issues.
///
/// The id of a command that starts a chain is the argument of [`execute_with_id`](Source::execute_with_id);
/// [`execute`](Source::execute) generates one. A command never tells the engine its own id: a field of it that is a
/// `Uuid` is data.
pub struct Source<'a> {
    cqrs: &'a Cqrs,
    source: String,
}

// By hand: the engine is not printed with every entry point.
impl fmt::Debug for Source<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Source").field("source", &self.source).finish_non_exhaustive()
    }
}

impl Source<'_> {
    /// Runs `command` on the aggregate `id` with the executor of `A`. The command starts a chain from this source
    /// (`CommandMetadata::root`): its id, its correlation id and its cause are one new time-ordered id, and every
    /// event it commits stores them with the source and when it was issued. Without an executor of `A` the command does
    /// not run: unavailable, the wiring fault is logged. The executor runs the command without an allocation of the
    /// engine's own.
    pub async fn execute<A>(&self, id: &A::Id, command: A::Command) -> CommandResult<A>
    where
        A: Aggregate + Clone + 'static,
        A::Command: Clone + 'static,
        A::Rejection: Send + 'static,
    {
        self.run(None, id, command).await
    }

    /// [`execute`](Self::execute), with `command_id` as the id of the command, its correlation id and its cause: the id
    /// an entry point answers with before it knows the outcome (an HTTP response of every outcome names its command).
    pub async fn execute_with_id<A>(&self, command_id: Uuid, id: &A::Id, command: A::Command) -> CommandResult<A>
    where
        A: Aggregate + Clone + 'static,
        A::Command: Clone + 'static,
        A::Rejection: Send + 'static,
    {
        self.run(Some(command_id), id, command).await
    }

    async fn run<A>(&self, given: Option<Uuid>, id: &A::Id, command: A::Command) -> CommandResult<A>
    where
        A: Aggregate + Clone + 'static,
        A::Command: Clone + 'static,
        A::Rejection: Send + 'static,
    {
        let mut metadata = CommandMetadata::root(self.source.clone());
        if let Some(command_id) = given {
            let causation = &mut metadata.causation;
            (causation.command_id, causation.correlation_id, causation.causation_id) = (command_id, command_id, command_id);
        }
        let Some(executor) = self.cqrs.executor_of::<A>() else {
            diagnostics::log_unregistered_aggregate(A::TYPE);
            return Err(CommandError::Unavailable);
        };
        executor.execute_with_metadata(id, command, metadata.to_map()).await
    }
}

impl Cqrs {
    /// The entry point `source` of the application: its commands start their chains from it.
    pub fn source(&self, source: impl Into<String>) -> Source<'_> {
        Source {
            cqrs: self,
            source: source.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::test_support::*;
    use cqrsx::{
        command::{
            CommandError,
            causation::{CAUSATION_ID, COMMAND_ID, CORRELATION_ID, SOURCE},
            metadata::RECORDED_AT,
        },
        journal::repository::EventRepository,
    };
    use std::collections::BTreeSet;
    use uuid::Uuid;

    #[tokio::test]
    async fn a_command_gets_the_given_id_or_a_fresh_one_and_the_metadata_of_its_source() {
        let (cqrs, _) = engine().await;
        let given = Uuid::now_v7();
        cqrs.source("ui")
            .execute_with_id::<Show>(given, &"s-1".into(), book("A1", "fox"))
            .await
            .unwrap();
        cqrs.source("kiosk")
            .execute::<Show>(&"s-1".into(), book("A2", "panda"))
            .await
            .unwrap();
        cqrs.source("kiosk")
            .execute::<Show>(&"s-1".into(), book("A3", "wolf"))
            .await
            .unwrap();
        let events = cqrs.journal().unwrap().events::<Show>("s-1").await.unwrap();
        let metadata: Vec<&serde_json::Value> = events.iter().map(|event| &event.metadata).collect();
        let keys: BTreeSet<&str> = metadata[0].as_object().unwrap().keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            BTreeSet::from([COMMAND_ID, CORRELATION_ID, CAUSATION_ID, SOURCE, RECORDED_AT])
        );
        let id = given.to_string();
        assert_eq!(
            [
                &metadata[0][COMMAND_ID],
                &metadata[0][CORRELATION_ID],
                &metadata[0][CAUSATION_ID],
                &metadata[0][SOURCE]
            ],
            [&id, &id, &id, "ui"],
            "the given id in every role"
        );
        let fresh: Vec<Uuid> = metadata[1..]
            .iter()
            .map(|metadata| Uuid::parse_str(metadata[COMMAND_ID].as_str().unwrap()).unwrap())
            .collect();
        let pairs = fresh.iter().zip(&metadata[1..]);
        for (fresh, metadata) in pairs {
            assert_eq!(fresh.get_version_num(), 7, "a new time-ordered id");
            assert_eq!(
                [&metadata[CORRELATION_ID], &metadata[CAUSATION_ID], &metadata[SOURCE]],
                [&fresh.to_string(), &fresh.to_string(), "kiosk"]
            );
        }
        assert!(
            fresh[0] != fresh[1] && !fresh.contains(&given),
            "every command without an id gets its own"
        );
        for metadata in metadata {
            assert_eq!(
                metadata[RECORDED_AT].as_str().unwrap().len(),
                "2026-10-17T09:12:03.123456Z".len(),
                "fixed width"
            );
        }
    }

    #[tokio::test]
    async fn execute_answers_a_rejection_and_an_unregistered_aggregate_is_unavailable() {
        let (cqrs, _) = engine().await;
        let ui = cqrs.source("ui");
        ui.execute::<Show>(&"s-1".into(), book("A1", "fox")).await.unwrap();
        assert_eq!(
            ui.execute::<Show>(&"s-1".into(), book("A1", "panda")).await.unwrap_err(),
            CommandError::Rejected(SeatTaken)
        );
        let shows = memory_builder().module(shows).build().await.unwrap();
        let (answer, log) = crate::test_support::capture(
            shows
                .source("ui")
                .execute::<Wallet>(&"fox".into(), Record { seat: "A1".into() }),
        )
        .await;
        assert_eq!(answer.unwrap_err(), CommandError::Unavailable);
        assert!(log.contains("no module registers the aggregate type of the command"), "{log}");
        assert!(
            log.contains("wallet") && !log.contains("fox"),
            "the type, never the id: {log}"
        );
    }
}
