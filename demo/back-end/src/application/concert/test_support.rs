//! A box office over a test event store, for the use-case tests.
use crate::{
    application::{
        activity::{ActivityEntry, ActivityHub},
        command_service::CommandService,
        concert::ConcertApp,
        lock::BoxOfficeLock,
    },
    domain::{
        ConcertId, FanId,
        concert::{
            Concert, ConcertServices,
            commands::{AnnounceConcert, BookSeat, CancelBooking},
        },
    },
};
use chrono::NaiveDate;
use cqrsx::cqrs::{store::EventStore, testing::command::TestCommandExecutor};
use std::sync::Arc;
use uuid::Uuid;

pub struct Harness<S: EventStore<Concert>> {
    pub app: ConcertApp<TestCommandExecutor<Concert, S>, S>,
    /// A clone of the executor the app runs its commands through: shares the store and the task records.
    pub executor: TestCommandExecutor<Concert, S>,
    pub activity: Arc<ActivityHub>,
    pub lock: Arc<BoxOfficeLock>,
}

pub fn harness<S>(store: S) -> Harness<S>
where
    S: EventStore<Concert> + Clone + 'static,
    S::AC: Send,
{
    let executor = TestCommandExecutor::new(store.clone(), ConcertServices::default());
    let activity = Arc::new(ActivityHub::new(64, 64));
    let lock = Arc::new(BoxOfficeLock::new());
    let service = CommandService::new(executor.clone(), activity.clone(), lock.clone());
    Harness {
        app: ConcertApp::new(service, store, 6),
        executor,
        activity,
        lock,
    }
}

impl<S: EventStore<Concert>> Harness<S> {
    pub fn activity(&self) -> Vec<Arc<ActivityEntry>> {
        self.activity.subscribe().0
    }
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
