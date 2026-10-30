//! Reactions end to end in memory: a process across two aggregates written as their reactions, with the policy of
//! every task type, the order and the ids of several tasks of one format, a delayed task, labels kept in the
//! metadata, and a redelivered step that publishes nothing twice.
use cqrsx::{
    Task,
    cqrs::{
        aggregate::Aggregate,
        command::{CommandExecutor, CommandGatewayError, causation::Causation, decide::Decide},
        event::DomainEvent,
        reactions::{Decided, Reactions, ReactionsPlanner, Then},
        store::persisted::PersistedEventStore,
        testing::{command_executor::TestCommandExecutor, mem_event_repository::MemEventRepository},
    },
    task::{
        DEFAULT_MAX_ATTEMPTS, NewTask, TaskSchedule,
        attempt::TaskAttempt,
        format::TaskFormat,
        policy::{Policies, PolicyName, Retry, TaskPolicy},
    },
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{collections::HashMap, time::Duration};

/// The box office of one show: a seat goes to one fan.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Show {
    seats: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum ShowEvent {
    Booked { seat: String, fan: String },
}

impl DomainEvent for ShowEvent {
    fn event_type(&self) -> String {
        "booked".into()
    }
    fn event_version(&self) -> String {
        "1".into()
    }
}

#[derive(Clone, Debug)]
struct Book {
    seat: String,
    fan: String,
}

#[derive(Debug, PartialEq, thiserror::Error)]
#[error("seat taken")]
struct SeatTaken;

impl Aggregate for Show {
    const TYPE: &'static str = "show";
    type Id = String;
    type Command = Book;
    type Event = ShowEvent;
    type Error = SeatTaken;
    type Rules = ();

    fn apply(&mut self, ShowEvent::Booked { seat, fan }: ShowEvent) {
        self.seats.insert(seat, fan);
    }
}

impl Decide<Show> for Book {
    fn decide(self, show: &Show, _: &()) -> Result<ShowEvent, SeatTaken> {
        let Book { seat, fan } = self;
        if show.seats.contains_key(&seat) {
            return Err(SeatTaken);
        }
        Ok(ShowEvent::Booked { seat, fan })
    }
}

/// A fan's wallet: every ticket once, so a redelivered step is refused.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Wallet {
    tickets: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum WalletEvent {
    Recorded { ticket: String },
}

impl DomainEvent for WalletEvent {
    fn event_type(&self) -> String {
        "recorded".into()
    }
    fn event_version(&self) -> String {
        "1".into()
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
#[error("already recorded")]
struct AlreadyRecorded;

impl Aggregate for Wallet {
    const TYPE: &'static str = "wallet";
    type Id = String;
    type Command = String;
    type Event = WalletEvent;
    type Error = AlreadyRecorded;
    type Rules = ();

    fn apply(&mut self, WalletEvent::Recorded { ticket }: WalletEvent) {
        self.tickets.push(ticket);
    }
}

impl Decide<Wallet> for String {
    fn decide(self, wallet: &Wallet, _: &()) -> Result<WalletEvent, AlreadyRecorded> {
        if wallet.tickets.contains(&self) {
            return Err(AlreadyRecorded);
        }
        Ok(WalletEvent::Recorded { ticket: self })
    }
}

/// Step one of the delivery: a booked seat goes into the fan's wallet.
#[derive(Debug, Serialize, Deserialize, Task)]
#[task(name = "record_in_wallet", version = 1)]
struct RecordInWallet {
    fan: String,
    ticket: String,
}

/// Step two: the ticket is mailed, to whom the label says.
#[derive(Debug, Serialize, Deserialize, Task)]
#[task(name = "mail_ticket", version = 1)]
struct MailTicket {
    #[label]
    to: String,
    ticket: String,
}

/// An hour later the fan is reminded; no policy names it.
#[derive(Debug, Serialize, Deserialize, Task)]
#[task(name = "remind", version = 1)]
struct Remind {
    ticket: String,
}

/// The box office's part of the delivery: the booked seat goes into the fan's wallet.
impl Reactions for Show {
    fn react(decided: &Decided<'_, Self>, then: &mut Then) {
        match decided.event {
            ShowEvent::Booked { seat, fan } => then.task(RecordInWallet {
                fan: fan.clone(),
                ticket: format!("{}/{seat}", decided.id),
            }),
        }
    }
}

/// The wallet's part: the ticket is mailed to the fan and to the box office, and the fan reminded an hour later.
impl Reactions for Wallet {
    fn react(decided: &Decided<'_, Self>, then: &mut Then) {
        match decided.event {
            WalletEvent::Recorded { ticket } => {
                then.task(MailTicket {
                    to: decided.id.clone(),
                    ticket: ticket.clone(),
                });
                then.task(MailTicket {
                    to: "box-office".into(),
                    ticket: ticket.clone(),
                });
                then.task_after(Duration::from_secs(3600), Remind { ticket: ticket.clone() });
            }
        }
    }
}

const STEP: PolicyName = PolicyName::new("step").unwrap();
const MAIL: PolicyName = PolicyName::new("mail").unwrap();

/// Steps wait a second and get six attempts, mails back off and get three; reminders keep the engine's defaults.
fn policies() -> Policies {
    let step = TaskPolicy::new(Retry::fixed(Duration::from_secs(1)), 6).unwrap();
    let mail = TaskPolicy::new(Retry::backoff(Duration::from_secs(1), Duration::from_secs(8)), 3).unwrap();
    Policies::default()
        .with_policy(STEP, step)
        .unwrap()
        .with_policy(MAIL, mail)
        .unwrap()
        .with_task::<RecordInWallet>(STEP)
        .unwrap()
        .with_task::<MailTicket>(MAIL)
        .unwrap()
}

/// What the task runtime would hand the wallet step: a claim of `task`.
fn claim(task: &NewTask, number: i32) -> TaskAttempt {
    TaskAttempt::new(
        task.id(),
        task.format().clone(),
        task.payload().clone(),
        task.metadata().clone(),
        number,
    )
    .unwrap()
}

/// The wallet step: the task's payload becomes the wallet's command, which continues the task's chain; its
/// causation and the tasks the wallet's reactions planned.
async fn record_in_wallet(
    wallets: &TestCommandExecutor<Wallet, PersistedEventStore<MemEventRepository, Wallet>>,
    attempt: &TaskAttempt,
) -> Result<(Causation, Vec<NewTask>), CommandGatewayError<AlreadyRecorded>> {
    let step: RecordInWallet = serde_json::from_value(attempt.payload().clone()).unwrap();
    let causation = Causation::from_task(attempt);
    let report = wallets
        .execute_with_metadata(&step.fan, step.ticket, causation.event_metadata())
        .await?;
    Ok((causation, report.tasks))
}

#[tokio::test]
async fn reactions_plan_across_two_aggregates_with_the_policy_of_each_task_type() {
    let journal = MemEventRepository::default();
    let shows = TestCommandExecutor::<Show, _>::over_journal(journal.clone(), ())
        .with_task_planner(ReactionsPlanner::<Show>::new(policies()));
    let wallets = TestCommandExecutor::<Wallet, _>::over_journal(journal.clone(), ())
        .with_task_planner(ReactionsPlanner::<Wallet>::new(policies()));

    let root = Causation::root("ui");
    let book = Book {
        seat: "A1".into(),
        fan: "fox".into(),
    };
    let booking = shows
        .execute_with_metadata(&"gig-1".into(), book, root.event_metadata())
        .await
        .unwrap();
    let [step] = &booking.tasks[..] else {
        panic!("one step per booking: {:?}", booking.tasks);
    };
    assert_eq!(step.id(), root.task_id(Show::TYPE, "gig-1", step.format(), 0));
    assert_eq!(step.payload(), &json!({ "fan": "fox", "ticket": "gig-1/A1" }));
    assert_eq!((step.max_attempts(), step.schedule()), (6, &TaskSchedule::Now));

    let delivery = claim(step, 1);
    let (chain, follow_ups) = record_in_wallet(&wallets, &delivery).await.unwrap();
    let mail = TaskFormat::of::<MailTicket>().unwrap();
    let remind = TaskFormat::of::<Remind>().unwrap();
    assert_eq!(
        follow_ups.iter().map(NewTask::id).collect::<Vec<_>>(),
        [
            chain.task_id(Wallet::TYPE, "fox", &mail, 0),
            chain.task_id(Wallet::TYPE, "fox", &mail, 1),
            chain.task_id(Wallet::TYPE, "fox", &remind, 0),
        ],
        "the tasks in the order of the reaction's calls, numbered per format"
    );
    assert_eq!(
        follow_ups
            .iter()
            .map(|task| (task.payload().clone(), task.metadata()["to"].clone()))
            .collect::<Vec<_>>(),
        [
            (json!({ "ticket": "gig-1/A1" }), json!("fox")),
            (json!({ "ticket": "gig-1/A1" }), json!("box-office")),
            (json!({ "ticket": "gig-1/A1" }), json!(null)),
        ],
        "the label stays in the metadata, next to the causation"
    );
    assert_eq!(
        follow_ups
            .iter()
            .map(|task| (task.max_attempts(), task.schedule().clone()))
            .collect::<Vec<_>>(),
        [
            (3, TaskSchedule::Now),
            (3, TaskSchedule::Now),
            (DEFAULT_MAX_ATTEMPTS, TaskSchedule::After(Duration::from_secs(3600)))
        ],
        "the budget of each type's policy; a type without one keeps the engine's"
    );
    for task in &follow_ups {
        assert_eq!(
            task.metadata()["correlation_id"],
            json!(root.correlation_id),
            "the chain keeps its correlation"
        );
        assert_eq!(task.metadata()["causation_id"], json!(step.id()), "caused by the step");
    }

    // The runtime delivers the step again (a lease expired): the wallet refuses the repeat and nothing more is
    // published.
    assert_eq!(
        record_in_wallet(&wallets, &claim(step, 2)).await.map(|(_, tasks)| tasks),
        Err(CommandGatewayError::Domain(AlreadyRecorded))
    );
    assert_eq!((journal.outbox::<Show>().len(), journal.outbox::<Wallet>().len()), (1, 3));
    assert_eq!(
        (shows.take_published_tasks().len(), wallets.take_published_tasks().len()),
        (1, 3)
    );
}
