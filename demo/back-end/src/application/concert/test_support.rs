//! A box office over in-memory storage, for the use-case tests.
use crate::{
    application::{
        activity::{ActivityEntry, ActivityHub},
        command_service::CommandService,
        concert::ConcertApp,
        lock::BoxOfficeLock,
        test_support::{BoundTasks, TestExecutor, executor},
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
use cqrsx::cqrs::store::EventStore;
use std::sync::Arc;
use uuid::Uuid;

pub struct Harness<S: EventStore<Concert>> {
    pub app: ConcertApp<TestExecutor<Concert, S>, S>,
    pub bound: BoundTasks,
    pub activity: Arc<ActivityHub>,
    pub lock: Arc<BoxOfficeLock>,
}

pub fn harness<S>(store: S) -> Harness<S>
where
    S: EventStore<Concert> + Clone + 'static,
    S::AC: Send,
{
    let bound = BoundTasks::default();
    let activity = Arc::new(ActivityHub::new(64, 64));
    let lock = Arc::new(BoxOfficeLock::new());
    let service = CommandService::new(
        executor(store.clone(), ConcertServices::default(), bound.clone()),
        activity.clone(),
        lock.clone(),
    );
    Harness {
        app: ConcertApp::new(service, store, 6),
        bound,
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
