//! A box office over a test event store, planning its tasks as in production, for the use-case tests.
use crate::{
    application::{
        activity::{ActivityEntry, ActivityHub},
        command_service::CommandService,
        concert::ConcertApp,
        workflows::concert_tasks,
    },
    domain::{
        ConcertId, FanId,
        concert::{
            Concert, ConcertError, ConcertEvent, ConcertServices,
            commands::{AnnounceConcert, BookSeat, CancelBooking},
        },
    },
};
use chrono::NaiveDate;
use cqrsx::{
    cqrs::{
        AggregateError,
        event::EventEnvelope,
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

pub struct Harness<S: EventStore<Concert>> {
    pub app: ConcertApp<TestCommandExecutor<Concert, S>>,
    /// A clone of the executor the app runs its commands through: shares the store and the task records.
    pub executor: TestCommandExecutor<Concert, S>,
    pub activity: Arc<ActivityHub>,
}

pub fn harness<S>(store: S) -> Harness<S>
where
    S: EventStore<Concert> + 'static,
    S::AC: Send,
{
    let executor = TestCommandExecutor::new(store, ConcertServices::default()).with_task_planner(concert_tasks(6));
    let activity = Arc::new(ActivityHub::new(64, 64));
    let service = CommandService::new(executor.clone(), activity.clone());
    Harness {
        app: ConcertApp::new(service),
        executor,
        activity,
    }
}

impl<S: EventStore<Concert>> Harness<S> {
    pub fn activity(&self) -> Vec<Arc<ActivityEntry>> {
        self.activity.subscribe().0
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

pub fn announce() -> AnnounceConcert {
    AnnounceConcert {
        artist: "Radiohead".into(),
        city: "Tbilisi".into(),
        date: NaiveDate::from_ymd_opt(2026, 11, 21).unwrap(),
    }
}

pub fn book(seat: &str, fan_id: &str, booking_id: Uuid) -> BookSeat {
    BookSeat {
        seat: seat.into(),
        fan_id: fan(fan_id),
        fan_name: fan_id.to_uppercase(),
        booking_id,
    }
}

pub fn cancel(seat: &str, fan_id: &str, cancellation_id: Uuid) -> CancelBooking {
    CancelBooking {
        seat: seat.into(),
        fan_id: fan(fan_id),
        cancellation_id,
    }
}
