//! ⏳ Time machine: the state of one concert after event `n`, computed from its events alone: the framework's history of
//! the concert reads no snapshot — the point is "this state is stored nowhere".
use crate::{
    concert::domain::{Concert, ConcertEvent},
    shared::{application::StorageError, domain::ConcertId},
};
use cqrsx::domain::{
    aggregate::Aggregate,
    event::{EventEnvelope, Events},
};
use cqrsx_framework::Cqrs;
use std::collections::HashMap;

/// One event of a concert's history: its place in it, what it was and when it was recorded.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordedEvent {
    pub sequence: usize,
    /// The name of the event's kind, as the journal stores it.
    pub event_type: &'static str,
    pub payload: ConcertEvent,
    /// What the framework recorded with the event: the command, the correlation, the source, the instant.
    pub metadata: HashMap<String, String>,
}

impl RecordedEvent {
    fn of(envelope: EventEnvelope<Concert>) -> Self {
        Self {
            sequence: envelope.sequence,
            event_type: envelope.payload.event_type(),
            payload: envelope.payload,
            metadata: envelope.metadata,
        }
    }

    /// The instant the framework recorded the event at, as it wrote it.
    pub fn recorded_at(&self) -> Option<&str> {
        self.metadata.get("recorded_at").map(String::as_str)
    }
}

/// A concert as it was after one of its events.
#[derive(Debug, Clone, PartialEq)]
pub struct ConcertAt {
    pub concert_id: ConcertId,
    /// The state, folded from the events up to `event` alone.
    pub concert: Concert,
    /// The last event the state contains.
    pub event: RecordedEvent,
    /// The sequence of the concert's latest event.
    pub last_sequence: usize,
}

async fn events(cqrs: &Cqrs, concert_id: &ConcertId) -> Result<Vec<EventEnvelope<Concert>>, StorageError> {
    cqrs.history::<Concert>(concert_id).await.map_err(StorageError::new)
}

/// Every event of the concert, in order.
pub async fn timeline(cqrs: &Cqrs, concert_id: &ConcertId) -> Result<Vec<RecordedEvent>, StorageError> {
    Ok(events(cqrs, concert_id).await?.into_iter().map(RecordedEvent::of).collect())
}

/// `None` when the concert has fewer than `sequence` events (or none at all).
pub async fn state_at(cqrs: &Cqrs, concert_id: &ConcertId, sequence: usize) -> Result<Option<ConcertAt>, StorageError> {
    let events = events(cqrs, concert_id).await?;
    let last_sequence = events.last().map(|e| e.sequence).unwrap_or(0);
    if sequence == 0 || sequence > last_sequence {
        return Ok(None);
    }
    let mut concert = Concert::default();
    let mut at = None;
    for event in events {
        if event.sequence > sequence {
            break;
        }
        concert.apply(event.payload.clone());
        at = Some(event);
    }
    Ok(at.map(|event| ConcertAt {
        concert_id: concert_id.clone(),
        concert,
        event: RecordedEvent::of(event),
        last_sequence,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        concert::domain::{
            ConcertCommand,
            commands::{AnnounceConcert, BookSeat},
        },
        shared::domain::FanId,
        testing::test_app,
    };
    use chrono::NaiveDate;
    use uuid::Uuid;

    /// A concert with three events: the announcement and two bookings.
    async fn booked_concert() -> (Cqrs, ConcertId) {
        let (cqrs, _) = test_app().await;
        let concert_id = ConcertId::parse("c-1").unwrap();
        let ui = cqrs.source("ui");
        let announce = AnnounceConcert {
            artist: "A".into(),
            city: "C".into(),
            date: NaiveDate::from_ymd_opt(2026, 11, 21).unwrap(),
        };
        ui.execute::<Concert>(&concert_id, ConcertCommand::AnnounceConcert(announce))
            .await
            .unwrap();
        for (seat, fan) in [("A1", "fox"), ("A2", "panda")] {
            let book = BookSeat {
                seat: seat.into(),
                fan_id: FanId::parse(fan).unwrap(),
                fan_name: fan.into(),
                booking_id: Uuid::now_v7(),
            };
            ui.execute::<Concert>(&concert_id, ConcertCommand::BookSeat(book))
                .await
                .unwrap();
        }
        (cqrs, concert_id)
    }

    #[tokio::test]
    async fn the_timeline_lists_every_event_with_its_place_and_kind() {
        let (cqrs, concert_id) = booked_concert().await;
        let timeline = timeline(&cqrs, &concert_id).await.unwrap();
        let rows: Vec<(usize, &str)> = timeline.iter().map(|event| (event.sequence, event.event_type)).collect();
        assert_eq!(rows, [(1, "ConcertAnnounced"), (2, "SeatBooked"), (3, "SeatBooked")]);
        assert!(matches!(timeline[0].payload, ConcertEvent::ConcertAnnounced(_)));
        assert!(timeline.iter().all(|event| event.metadata.contains_key("command_id")));
        assert_eq!(
            timeline[1].recorded_at(),
            timeline[1].metadata.get("recorded_at").map(String::as_str)
        );
        let unknown = ConcertId::parse("c-9").unwrap();
        assert!(super::timeline(&cqrs, &unknown).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn the_state_after_an_event_holds_the_events_up_to_it_and_no_more() {
        let (cqrs, concert_id) = booked_concert().await;
        let at = state_at(&cqrs, &concert_id, 2).await.unwrap().unwrap();
        assert_eq!((at.concert_id.as_str(), at.event.sequence, at.last_sequence), ("c-1", 2, 3));
        assert_eq!(at.concert.bookings().len(), 1);
        assert_eq!(at.event.event_type, "SeatBooked");
        let latest = state_at(&cqrs, &concert_id, 3).await.unwrap().unwrap();
        assert_eq!(latest.concert.bookings().len(), 2);
        let announced = state_at(&cqrs, &concert_id, 1).await.unwrap().unwrap();
        assert!(announced.concert.bookings().is_empty() && announced.concert.info().is_some());
    }

    #[tokio::test]
    async fn no_state_exists_before_the_first_event_after_the_last_or_for_an_unknown_concert() {
        let (cqrs, concert_id) = booked_concert().await;
        for sequence in [0, 4] {
            assert_eq!(state_at(&cqrs, &concert_id, sequence).await.unwrap(), None, "{sequence}");
        }
        let unknown = ConcertId::parse("c-9").unwrap();
        assert_eq!(state_at(&cqrs, &unknown, 1).await.unwrap(), None);
    }
}
