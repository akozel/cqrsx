//! A command from an entry point of the application ([`Source`]) and what became of it ([`Outcome`]).
use crate::{
    cqrs::{
        CommitReport,
        aggregate::Aggregate,
        command::{CommandGatewayError, CommandInfo, metadata::CommandMetadata},
    },
    diagnostics,
    engine::Cqrs,
};

/// What became of a command: accepted with the tasks committed with its event, rejected by its aggregate, a conflict
/// after every retry, or unavailable (not run, or its outcome unknown). The same answer as the command executors'.
pub type Outcome<A> = Result<CommitReport<A>, CommandGatewayError<<A as Aggregate>::Error>>;

/// An entry point of the application, e.g. the HTTP API (`cqrs.from("ui")`): the source of the commands it issues.
pub struct Source<'a> {
    cqrs: &'a Cqrs,
    source: String,
}

impl Source<'_> {
    /// Runs `command` on the aggregate `id` with the executor of `A`. The command starts a chain from this source
    /// (`CommandMetadata::root`): its id, its correlation id and its cause are the id the command carries
    /// (`#[command_id]`), else a new time-ordered id, and every event it commits stores them with the source and when
    /// it was issued. Without an executor of `A` the command does not run: unavailable, the wiring fault is logged.
    pub async fn execute<A>(&self, id: &A::Id, command: A::Command) -> Outcome<A>
    where
        A: Aggregate + 'static,
        A::Command: CommandInfo,
    {
        let mut metadata = CommandMetadata::root(self.source.clone());
        if let Some(command_id) = command.command_id() {
            let causation = &mut metadata.causation;
            (causation.command_id, causation.correlation_id, causation.causation_id) = (command_id, command_id, command_id);
        }
        let Some(executor) = self.cqrs.executors.get::<A>() else {
            diagnostics::log_unregistered_aggregate(A::TYPE);
            return Err(CommandGatewayError::Unavailable);
        };
        executor.run(id, command, metadata.to_map()).await
    }
}

impl Cqrs {
    /// The entry point `source` of the application: its commands start their chains from it.
    pub fn from(&self, source: impl Into<String>) -> Source<'_> {
        Source {
            cqrs: self,
            source: source.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        cqrs::{
            command::{
                CommandGatewayError,
                causation::{CAUSATION_ID, COMMAND_ID, CORRELATION_ID, SOURCE},
                metadata::RECORDED_AT,
            },
            store::repository::PersistedEventRepository,
        },
        engine::{Cqrs, test_support::*},
    };
    use std::collections::BTreeSet;
    use uuid::Uuid;

    #[tokio::test]
    async fn execute_stores_the_metadata_of_its_source_and_the_id_its_command_carries() {
        let (cqrs, _) = engine();
        let booking_id = Uuid::now_v7();
        let carried = Book {
            booking_id: Some(booking_id),
            ..book("A1", "fox")
        };
        cqrs.from("ui").execute::<Show>(&"s-1".into(), carried).await.unwrap();
        cqrs.from("kiosk")
            .execute::<Show>(&"s-1".into(), book("A2", "panda"))
            .await
            .unwrap();
        let events = cqrs.journal().get_events::<Show>("s-1").await.unwrap();
        let metadata: Vec<&serde_json::Value> = events.iter().map(|event| &event.metadata).collect();
        let keys: BTreeSet<&str> = metadata[0].as_object().unwrap().keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            BTreeSet::from([COMMAND_ID, CORRELATION_ID, CAUSATION_ID, SOURCE, RECORDED_AT])
        );
        let id = booking_id.to_string();
        assert_eq!(
            [
                &metadata[0][COMMAND_ID],
                &metadata[0][CORRELATION_ID],
                &metadata[0][CAUSATION_ID],
                &metadata[0][SOURCE]
            ],
            [&id, &id, &id, "ui"],
            "the command's own id in every role"
        );
        let fresh = Uuid::parse_str(metadata[1][COMMAND_ID].as_str().unwrap()).unwrap();
        assert_eq!(fresh.get_version_num(), 7, "a new time-ordered id");
        assert_eq!(
            [&metadata[1][CORRELATION_ID], &metadata[1][CAUSATION_ID], &metadata[1][SOURCE]],
            [&fresh.to_string(), &fresh.to_string(), "kiosk"]
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
        let (cqrs, _) = engine();
        let ui = cqrs.from("ui");
        ui.execute::<Show>(&"s-1".into(), book("A1", "fox")).await.unwrap();
        assert_eq!(
            ui.execute::<Show>(&"s-1".into(), book("A1", "panda")).await.unwrap_err(),
            CommandGatewayError::Domain(SeatTaken)
        );
        let shows = Cqrs::in_memory().module(shows).build().unwrap();
        let (answer, log) = crate::diagnostics::test_support::capture(
            shows
                .from("ui")
                .execute::<Wallet>(&"fox".into(), Record { seat: "A1".into() }),
        )
        .await;
        assert_eq!(answer.unwrap_err(), CommandGatewayError::Unavailable);
        assert!(log.contains("no module registers the aggregate type of the command"), "{log}");
        assert!(
            log.contains("wallet") && !log.contains("fox"),
            "the type, never the id: {log}"
        );
    }
}
