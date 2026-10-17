//! ⏳ Time machine: the state of one concert after event `n`, computed from its events alone.
//! Snapshots are deliberately not used — the point is "this state is stored nowhere".
use crate::{
    application::StorageError,
    domain::{
        ConcertId,
        concert::{Concert, ConcertEvent},
    },
};
use cqrsx::{
    cqrs::{
        aggregate::Aggregate,
        event::{DomainEvent, EventEnvelope},
        store::{EventStore, persisted::PersistedEventStore},
    },
    postgres::event_repository::PostgresEventRepository,
};
use serde_json::{Value, json};
use sqlx::PgPool;

pub struct TimeTravel {
    store: PersistedEventStore<PostgresEventRepository, Concert>,
}

/// One row of the timeline under the slider.
fn summary(event: &ConcertEvent) -> String {
    match event {
        ConcertEvent::ConcertAnnounced(e) => format!("{} · {}", e.artist, e.city),
        ConcertEvent::SeatBooked(e) => format!("{} → {}", e.seat, e.fan_name),
        ConcertEvent::BookingCancelled(e) => format!("{} ✕ (refund {} €)", e.seat, e.refund),
    }
}

impl TimeTravel {
    pub fn new(pool: PgPool) -> Self {
        Self {
            store: PersistedEventStore::new_event_store(PostgresEventRepository::new(pool)),
        }
    }

    async fn events(&self, concert_id: &ConcertId) -> Result<Vec<EventEnvelope<Concert>>, StorageError> {
        self.store.load_events(concert_id.as_str()).await.map_err(StorageError::new)
    }

    pub async fn timeline(&self, concert_id: &ConcertId) -> Result<Vec<Value>, StorageError> {
        Ok(self
            .events(concert_id)
            .await?
            .iter()
            .map(|e| {
                json!({
                    "sequence": e.sequence,
                    "event_type": e.payload.event_type(),
                    "recorded_at": e.metadata.get("recorded_at"),
                    "summary": summary(&e.payload),
                })
            })
            .collect())
    }

    /// `None` when the concert has fewer than `sequence` events (or none at all).
    pub async fn state_at(&self, concert_id: &ConcertId, sequence: usize) -> Result<Option<Value>, StorageError> {
        let events = self.events(concert_id).await?;
        let last_sequence = events.last().map(|e| e.sequence).unwrap_or(0);
        if sequence == 0 || sequence > last_sequence {
            return Ok(None);
        }
        let mut concert = Concert::default();
        let mut at = None;
        for event in &events {
            if event.sequence > sequence {
                break;
            }
            concert.apply(event.payload.clone());
            at = Some(event);
        }
        let Some(event) = at else { return Ok(None) };
        Ok(Some(json!({
            "concert_id": concert_id,
            "sequence": sequence,
            "last_sequence": last_sequence,
            "state": {
                "info": concert.info(),
                "bookings": concert.bookings(),
                "sold": concert.bookings().len(),
                "free_seats": concert.free_seats(),
                "sold_out": concert.is_sold_out(),
                "revenue": concert.revenue(),
            },
            "event": {
                "sequence": event.sequence,
                "event_type": event.payload.event_type(),
                "payload": event.payload,
                "metadata": event.metadata,
                "summary": summary(&event.payload),
            },
        })))
    }
}
