//! The engine's reports as activity log entries: what the command executors did (`command.*`, one observer for every
//! aggregate) and what the feed and the X-ray show of the read side (`projection.*`, an observer per read model). The
//! modules write nothing for the feed: an entry names a command by what its derives say of it (`CommandInfo`: its
//! type's name and its `#[label]` fields) and a rejection by its code and params (`Rejection`). An executor calls its
//! observer on the caller's task and a processor on its own, so every method only publishes.
use crate::shared::{application::activity::ActivityHub, domain::FanLedgerId};
use cqrsx::{
    command::{
        causation::{COMMAND_ID, CORRELATION_ID, SOURCE},
        observer::{self, AnyCommandReport, AnyOutcome, CommandObserver, CommandReport},
    },
    domain::{aggregate::Aggregate, command::CommandInfo, rejection::Rejection},
    projection::processor::observer::{Applied, Failed, LeaseEvent, ProjectionObserver, RebuildReason, Rebuilt},
};
use serde_json::{Value, json};
use std::sync::Arc;

/// The observer of every command executor of this process: one entry per command, once its lock is released.
pub struct CommandActivityObserver {
    activity: Arc<ActivityHub>,
}

impl CommandActivityObserver {
    pub fn new(activity: Arc<ActivityHub>) -> Self {
        Self { activity }
    }
}

impl observer::AnyCommandObserver for CommandActivityObserver {
    /// `command.accepted` with the formats of the tasks committed with the event, `command.rejected` with the code and
    /// the params of the rejection, `command.failed` with a conflict or an unavailable store. Every entry names the
    /// aggregate, the ids and the source of the command's metadata, the command's type and its fan: the command's
    /// `fan_id` label, else the fan of the aggregate's id when it is a fan's ledger, else nobody.
    fn command(&self, report: &AnyCommandReport<'_>) {
        let fan_id = report
            .command
            .labels()
            .into_iter()
            .find_map(|label| (label.name == "fan_id").then_some(label.value))
            .or_else(|| {
                let ledger = report.id.downcast_ref::<FanLedgerId>();
                ledger.map(|ledger| ledger.fan().as_str().to_owned())
            });
        let base = json!({
            "aggregate_type": report.aggregate_type,
            "aggregate_id": report.aggregate_id,
            "command_id": report.metadata.get(COMMAND_ID),
            "correlation_id": report.metadata.get(CORRELATION_ID),
            "source": report.metadata.get(SOURCE),
            "command_type": report.command.name(),
            "fan_id": fan_id,
        });
        let merge = |extra: Value| {
            let mut map = base.as_object().cloned().unwrap_or_default();
            if let Value::Object(extra) = extra {
                map.extend(extra);
            }
            Value::Object(map)
        };
        match report.outcome {
            AnyOutcome::Accepted { tasks } => {
                let formats: Vec<String> = tasks.iter().map(|task| task.format().to_string()).collect();
                self.activity.publish("command.accepted", merge(json!({ "tasks": formats })));
            }
            AnyOutcome::Rejected(error) => {
                self.activity.publish(
                    "command.rejected",
                    merge(json!({ "code": error.code(), "params": error.params() })),
                );
            }
            AnyOutcome::Conflict => {
                self.activity.publish(
                    "command.failed",
                    merge(json!({ "outcome": "conflict", "message": "concurrent change; retries exhausted" })),
                );
            }
            AnyOutcome::Unavailable => {
                self.activity.publish(
                    "command.failed",
                    merge(json!({ "outcome": "unavailable", "message": "not run: storage unavailable or aggregate busy" })),
                );
            }
        }
    }
}

/// The same observer as the observer of one executor of its own (the tests of the modules' flows).
impl<A> CommandObserver<A> for CommandActivityObserver
where
    A: Aggregate,
    A::Command: CommandInfo,
    A::Error: Rejection,
{
    fn command(&self, report: &CommandReport<'_, A>) {
        observer::AnyCommandObserver::command(self, &AnyCommandReport::of(report));
    }
}

/// The observer of one read model's processor on this node.
pub struct ActivityObserver {
    projection: &'static str,
    owner: String,
    activity: Arc<ActivityHub>,
}

impl ActivityObserver {
    pub fn new(projection: &'static str, owner: impl Into<String>, activity: Arc<ActivityHub>) -> Self {
        Self {
            projection,
            owner: owner.into(),
            activity,
        }
    }
}

impl ProjectionObserver for ActivityObserver {
    /// One document took the events of a live batch (or skipped them as already applied).
    fn applied(&self, report: &Applied<'_>) {
        self.activity.publish(
            "projection.updated",
            json!({
                "projection": self.projection,
                "view_id": report.aggregate_id,
                "sequence": report.last_sequence,
                "events": report.events,
            }),
        );
    }

    /// A round failed: the processor stops before it, pauses and tries again; the status shows it stalled.
    fn failed(&self, report: &Failed<'_>) {
        self.activity.publish(
            "projection.failed",
            json!({
                "projection": self.projection,
                "view_id": report.aggregate_id,
                "handler": report.handler,
                "category": report.category.as_str(),
                "failures": report.failures,
                "message": format!("{}: {}", report.handler, report.category),
            }),
        );
    }

    /// A document was replaced by the fold of its history: a requested rebuild or a healed gap.
    fn rebuilt(&self, report: &Rebuilt<'_>) {
        let reason = match report.reason {
            RebuildReason::Requested => "requested",
            RebuildReason::Healed => "healed",
        };
        self.activity.publish(
            "projection.rebuilt",
            json!({
                "projection": self.projection,
                "view_id": report.aggregate_id,
                "events": report.events,
                "reason": reason,
            }),
        );
    }

    /// This node took, lost or gave back the projection's checkpoint.
    fn lease(&self, event: LeaseEvent) {
        let lease = match event {
            LeaseEvent::Acquired => "acquired",
            LeaseEvent::Lost => "lost",
            LeaseEvent::Released => "released",
        };
        self.activity.publish(
            "projection.lease",
            json!({ "projection": self.projection, "lease": lease, "owner": self.owner }),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::domain::ConcertId;
    use cqrsx::command::observer::Outcome;
    use cqrsx::{
        domain::{command::Decide, event::DomainEvent},
        journal::feed::Position,
        projection::processor::observer::FailureCategory,
        task::{NewTask, format::TaskFormat},
    };
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    fn entries(activity: &ActivityHub) -> Vec<(String, Value)> {
        activity
            .subscribe()
            .0
            .iter()
            .map(|entry| (entry.kind.clone(), Value::Object(entry.fields.clone())))
            .collect()
    }

    #[test]
    fn every_report_becomes_one_entry_with_the_projection_and_this_node() {
        let activity = Arc::new(ActivityHub::new(16, 16));
        let observer = ActivityObserver::new("concert_halls", "node-a", activity.clone());
        observer.applied(&Applied {
            aggregate_id: "c-1",
            last_sequence: 7,
            events: 2,
            position: Position::new(9),
        });
        observer.failed(&Failed {
            aggregate_id: Some("c-1"),
            handler: "concert_halls",
            category: FailureCategory::Connection,
            position: Position::new(9),
            failures: 3,
        });
        observer.failed(&Failed {
            aggregate_id: None,
            handler: "journal",
            category: FailureCategory::Storage,
            position: Position::START,
            failures: 1,
        });
        for reason in [RebuildReason::Requested, RebuildReason::Healed] {
            observer.rebuilt(&Rebuilt {
                aggregate_id: "c-2",
                events: 41,
                reason,
            });
        }
        for event in [LeaseEvent::Acquired, LeaseEvent::Lost, LeaseEvent::Released] {
            observer.lease(event);
        }
        let entries = entries(&activity);
        let kinds: Vec<&str> = entries.iter().map(|(kind, _)| kind.as_str()).collect();
        assert_eq!(
            kinds,
            [
                "projection.updated",
                "projection.failed",
                "projection.failed",
                "projection.rebuilt",
                "projection.rebuilt",
                "projection.lease",
                "projection.lease",
                "projection.lease"
            ]
        );
        assert_eq!(
            entries[0].1,
            json!({ "projection": "concert_halls", "view_id": "c-1", "sequence": 7, "events": 2 })
        );
        assert_eq!(
            entries[1].1,
            json!({ "projection": "concert_halls", "view_id": "c-1", "handler": "concert_halls", "category": "connection",
                    "failures": 3, "message": "concert_halls: connection" })
        );
        assert_eq!(entries[2].1["view_id"], Value::Null, "a failure without a key");
        assert_eq!(
            [entries[3].1["reason"].as_str(), entries[4].1["reason"].as_str()],
            [Some("requested"), Some("healed")]
        );
        assert_eq!(
            entries[5..]
                .iter()
                .map(|(_, fields)| fields["lease"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["acquired", "lost", "released"]
        );
        assert!(entries[5..].iter().all(|(_, fields)| fields["owner"] == "node-a"));
    }

    /// A hall of the tests: its id is a concert's; a booking names its fan by its label, an announcement nobody.
    #[derive(Default, Serialize, Deserialize)]
    struct Hall;

    /// A ledger of the tests: one fan's, by its id; its command names no fan.
    #[derive(Default, Serialize, Deserialize)]
    struct Ledger;

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Booked;

    impl DomainEvent for Booked {
        fn event_type(&self) -> &'static str {
            "booked"
        }

        fn event_version(&self) -> &'static str {
            "1"
        }
    }

    #[derive(Debug, thiserror::Error, cqrsx::Rejection)]
    enum Refused {
        #[error("seat {seat} is taken")]
        SeatTaken { seat: String },
    }

    #[derive(cqrsx::Command)]
    struct Book {
        seat: String,
        #[label]
        fan_id: String,
    }

    #[derive(cqrsx::Command)]
    struct Announce {
        artist: String,
    }

    #[derive(cqrsx::Commands)]
    enum HallCommand {
        Book(Book),
        Announce(Announce),
    }

    #[derive(cqrsx::Command)]
    struct Record {
        seat: String,
    }

    impl Decide<Hall> for Book {
        fn decide(self, _: &Hall, _: &()) -> Result<Booked, Refused> {
            Err(Refused::SeatTaken { seat: self.seat })
        }
    }

    impl Decide<Hall> for Announce {
        fn decide(self, _: &Hall, _: &()) -> Result<Booked, Refused> {
            let _ = self.artist;
            Ok(Booked)
        }
    }

    impl Decide<Ledger> for Record {
        fn decide(self, _: &Ledger, _: &()) -> Result<Booked, Refused> {
            let _ = self.seat;
            Ok(Booked)
        }
    }

    impl Aggregate for Hall {
        const TYPE: &'static str = "Hall";
        type Id = ConcertId;
        type Command = HallCommand;
        type Event = Booked;
        type Error = Refused;
        type Rules = ();

        fn apply(&mut self, _: Booked) {}
    }

    impl Aggregate for Ledger {
        const TYPE: &'static str = "Ledger";
        type Id = FanLedgerId;
        type Command = Record;
        type Event = Booked;
        type Error = Refused;
        type Rules = ();

        fn apply(&mut self, _: Booked) {}
    }

    fn hall() -> ConcertId {
        ConcertId::parse("c-1").unwrap()
    }

    fn announce() -> HallCommand {
        HallCommand::Announce(Announce { artist: "A".into() })
    }

    fn book(fan_id: &str) -> HallCommand {
        HallCommand::Book(Book {
            seat: "B4".into(),
            fan_id: fan_id.into(),
        })
    }

    /// The metadata of a UI command: one id in all three roles.
    fn ui() -> HashMap<String, String> {
        HashMap::from([
            (COMMAND_ID.to_owned(), "id-1".to_owned()),
            (CORRELATION_ID.to_owned(), "id-1".to_owned()),
            ("causation_id".to_owned(), "id-1".to_owned()),
            (SOURCE.to_owned(), "ui".to_owned()),
            ("recorded_at".to_owned(), "2026-10-27T09:12:03.123456Z".to_owned()),
        ])
    }

    #[test]
    fn an_accepted_command_is_published_once_with_its_tasks_and_no_read_model_news() {
        let activity = Arc::new(ActivityHub::new(8, 8));
        let observer = CommandActivityObserver::new(activity.clone());
        observer.command(&CommandReport::<Hall> {
            aggregate_id: &hall(),
            command: &announce(),
            metadata: &ui(),
            outcome: Outcome::Accepted { tasks: &[] },
        });
        let (entries, _) = activity.subscribe();
        assert_eq!(entries.len(), 1, "{entries:?}");
        assert_eq!(entries[0].kind, "command.accepted");
        assert_eq!(entries[0].fields["tasks"], json!([]));
        assert!(
            !entries[0].fields.contains_key("warnings"),
            "the read models follow from the journal"
        );
    }

    #[test]
    fn an_accepted_command_reports_the_tasks_its_commit_published() {
        let activity = Arc::new(ActivityHub::new(8, 8));
        let task = NewTask::from_payload(TaskFormat::try_new("encore_record_ticket", 1).unwrap(), json!({})).unwrap();
        CommandActivityObserver::new(activity.clone()).command(&CommandReport::<Hall> {
            aggregate_id: &hall(),
            command: &announce(),
            metadata: &ui(),
            outcome: Outcome::Accepted { tasks: &[task] },
        });
        let (entries, _) = activity.subscribe();
        assert_eq!(entries[0].fields["tasks"], json!(["encore_record_ticket v1"]));
    }

    #[test]
    fn every_outcome_becomes_one_command_entry_with_the_command_and_its_metadata() {
        let activity = Arc::new(ActivityHub::new(8, 8));
        let observer = CommandActivityObserver::new(activity.clone());
        let task = NewTask::from_payload(TaskFormat::try_new("encore_record_ticket", 1).unwrap(), json!({})).unwrap();
        let refused = Refused::SeatTaken { seat: "B4".into() };
        let metadata = ui();
        for outcome in [
            Outcome::Accepted { tasks: &[task] },
            Outcome::Rejected(&refused),
            Outcome::Conflict,
            Outcome::Unavailable,
        ] {
            observer.command(&CommandReport::<Hall> {
                aggregate_id: &hall(),
                command: &book("fox"),
                metadata: &metadata,
                outcome,
            });
        }
        let command = json!({ "aggregate_type": "Hall", "aggregate_id": "c-1", "command_id": "id-1",
                               "correlation_id": "id-1", "source": "ui", "command_type": "Book", "fan_id": "fox" });
        let with = |fields: Value| {
            let mut entry = command.clone();
            entry.as_object_mut().unwrap().extend(fields.as_object().unwrap().clone());
            entry
        };
        assert_eq!(
            entries(&activity),
            [
                (
                    "command.accepted".to_owned(),
                    with(json!({ "tasks": ["encore_record_ticket v1"] }))
                ),
                (
                    "command.rejected".to_owned(),
                    with(json!({ "code": "seat_taken", "params": { "seat": "B4" } }))
                ),
                (
                    "command.failed".to_owned(),
                    with(json!({ "outcome": "conflict", "message": "concurrent change; retries exhausted" }))
                ),
                (
                    "command.failed".to_owned(),
                    with(json!({ "outcome": "unavailable", "message": "not run: storage unavailable or aggregate busy" }))
                ),
            ]
        );
    }

    #[test]
    fn the_fan_of_a_command_entry_is_its_label_else_the_fan_of_its_ledger_else_nobody() {
        let activity = Arc::new(ActivityHub::new(8, 8));
        let observer = CommandActivityObserver::new(activity.clone());
        let metadata = HashMap::new();
        for command in [book("fox"), announce()] {
            observer.command(&CommandReport::<Hall> {
                aggregate_id: &hall(),
                command: &command,
                metadata: &metadata,
                outcome: Outcome::Conflict,
            });
        }
        let ledger = FanLedgerId::parse("panda:c-1").unwrap();
        observer.command(&CommandReport::<Ledger> {
            aggregate_id: &ledger,
            command: &Record { seat: "B4".into() },
            metadata: &metadata,
            outcome: Outcome::Conflict,
        });
        let entries = entries(&activity);
        let named: Vec<(&Value, &Value, &Value)> = entries
            .iter()
            .map(|(_, fields)| (&fields["command_type"], &fields["aggregate_id"], &fields["fan_id"]))
            .collect();
        assert_eq!(
            named,
            [
                (&json!("Book"), &json!("c-1"), &json!("fox")),
                (&json!("Announce"), &json!("c-1"), &Value::Null),
                (&json!("Record"), &json!("panda:c-1"), &json!("panda")),
            ]
        );
        assert_eq!(
            (&entries[2].1["command_id"], &entries[2].1["source"]),
            (&Value::Null, &Value::Null),
            "no metadata, no ids"
        );
    }
}
