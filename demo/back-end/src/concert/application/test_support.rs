//! A box office over a test event store, as the composition runs it (the reactions' planner and the activity feed's
//! observer), for the tests of its flow; the facts and the command its reactions are tested with.
use crate::{
    concert::domain::{
        Concert, ConcertCommand, ConcertError, ConcertEvent, ConcertRules,
        commands::{AnnounceConcert, BookSeat, CancelBooking},
        events::{BookingCancelled, SeatBooked},
    },
    shared::{
        application::{
            activity::{ActivityEntry, ActivityHub},
            metadata::CommandMetadata,
        },
        domain::{
            ConcertId, FanId,
            venue::{SeatId, Zone},
        },
    },
};
use chrono::NaiveDate;
use cqrsx::{
    cqrs::{
        AggregateError, CommitReport,
        command::{CommandExecutor, CommandGatewayError},
        event::EventEnvelope,
        reactions::ReactionsPlanner,
        store::EventStore,
        testing::{
            command::TestCommandExecutor,
            mem_store::{MemStore, MemStoreAggregateContext},
        },
    },
    task::NewTask,
};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
};
use tokio::sync::Semaphore;
use uuid::Uuid;

/// What the box office answers a command: what it committed, or why not.
pub type Answer = Result<CommitReport<Concert>, CommandGatewayError<ConcertError>>;

pub struct Harness<S: EventStore<Concert>> {
    /// The box office's executor: its store, its reactions' planner, the activity feed's observer and the record of
    /// the tasks it published.
    pub executor: TestCommandExecutor<Concert, S>,
    pub activity: Arc<ActivityHub>,
}

pub fn harness<S>(store: S) -> Harness<S>
where
    S: EventStore<Concert> + 'static,
    S::AC: Send,
{
    let activity = Arc::new(ActivityHub::new(64, 64));
    let executor = TestCommandExecutor::new(store, ConcertRules::default())
        .with_task_planner(ReactionsPlanner::<Concert>::new(crate::testing::policies()))
        .with_observer(crate::testing::command_feed(&activity));
    Harness { executor, activity }
}

impl<S> Harness<S>
where
    S: EventStore<Concert> + 'static,
    S::AC: Send,
{
    /// Runs `command` on the concert under `metadata`, as the HTTP handler does.
    pub async fn execute(&self, concert_id: &ConcertId, command: ConcertCommand, metadata: CommandMetadata) -> Answer {
        self.executor
            .execute_with_metadata(concert_id, command, metadata.to_map())
            .await
    }

    pub fn activity(&self) -> Vec<Arc<ActivityEntry>> {
        self.activity.subscribe().0
    }
}

/// The formats of the tasks an accepted command committed, none for a refused one: what the HTTP answer lists.
pub fn committed(answer: &Answer) -> Vec<String> {
    match answer {
        Ok(report) => report.tasks.iter().map(|task| task.format().to_string()).collect(),
        Err(_) => Vec::new(),
    }
}

/// An in-memory store that counts the aggregate loads and, once its gate is closed, makes every load wait
/// for a permit: a command holds the concert's lock while its load waits.
#[derive(Clone, Default)]
pub struct ProbeStore {
    inner: MemStore<Concert>,
    loads: Arc<AtomicUsize>,
    gate: Arc<Mutex<Option<Arc<Semaphore>>>>,
}

impl ProbeStore {
    pub fn loads(&self) -> usize {
        self.loads.load(Ordering::SeqCst)
    }

    /// From now on every load waits for a permit of the returned gate.
    pub fn close_gate(&self) -> Arc<Semaphore> {
        let gate = Arc::new(Semaphore::new(0));
        *self.gate.lock().unwrap() = Some(gate.clone());
        gate
    }
}

impl EventStore<Concert> for ProbeStore {
    type AC = MemStoreAggregateContext<Concert>;

    async fn load_events(&self, id: &str) -> Result<Vec<EventEnvelope<Concert>>, AggregateError<ConcertError>> {
        self.inner.load_events(id).await
    }

    async fn load_aggregate(&self, id: &str) -> Result<Self::AC, AggregateError<ConcertError>> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        let gate = self.gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.acquire().await.unwrap().forget();
        }
        self.inner.load_aggregate(id).await
    }

    async fn commit(
        &self,
        events: Vec<ConcertEvent>,
        context: Self::AC,
        metadata: HashMap<String, String>,
        tasks: &[NewTask],
    ) -> Result<Vec<EventEnvelope<Concert>>, AggregateError<ConcertError>> {
        self.inner.commit(events, context, metadata, tasks).await
    }
}

/// Polls `future` once: its output if it finished, `None` if it waits.
pub async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Option<F::Output> {
    std::future::poll_fn(|cx| {
        Poll::Ready(match future.as_mut().poll(cx) {
            Poll::Ready(output) => Some(output),
            Poll::Pending => None,
        })
    })
    .await
}

pub fn concert_id() -> ConcertId {
    ConcertId::parse("radiohead-2026-11-21").unwrap()
}

pub fn fan(id: &str) -> FanId {
    FanId::parse(id).unwrap()
}

pub fn announce() -> ConcertCommand {
    ConcertCommand::AnnounceConcert(AnnounceConcert {
        artist: "Radiohead".into(),
        city: "Tbilisi".into(),
        date: NaiveDate::from_ymd_opt(2026, 11, 21).unwrap(),
    })
}

pub fn book(seat: &str, fan_id: &str, booking_id: Uuid) -> ConcertCommand {
    ConcertCommand::BookSeat(BookSeat {
        seat: seat.into(),
        fan_id: fan(fan_id),
        fan_name: fan_id.to_uppercase(),
        booking_id,
    })
}

pub fn cancel(seat: &str, fan_id: &str, cancellation_id: Uuid) -> ConcertCommand {
    ConcertCommand::CancelBooking(CancelBooking {
        seat: seat.into(),
        fan_id: fan(fan_id),
        cancellation_id,
    })
}

/// Fox's VIP seat A1 at 120, booked as `booking_id`.
pub fn booked(booking_id: Uuid) -> ConcertEvent {
    ConcertEvent::SeatBooked(SeatBooked {
        seat: SeatId::parse("A1").unwrap(),
        zone: Zone::Vip,
        price: 120,
        fan_id: FanId::parse("fox").unwrap(),
        fan_name: "Fox".into(),
        booking_id,
    })
}

/// The cancellation of Fox's A1, refunding the 120 paid.
pub fn cancelled_booking(booking_id: Uuid, cancellation_id: Uuid) -> ConcertEvent {
    ConcertEvent::BookingCancelled(BookingCancelled {
        seat: SeatId::parse("A1").unwrap(),
        fan_id: FanId::parse("fox").unwrap(),
        booking_id,
        cancellation_id,
        refund: 120,
    })
}

/// Some box-office command: the reactions never read it.
pub fn concert_command() -> ConcertCommand {
    ConcertCommand::BookSeat(BookSeat {
        seat: "A1".into(),
        fan_id: FanId::parse("fox").unwrap(),
        fan_name: "Fox".into(),
        booking_id: Uuid::nil(),
    })
}
