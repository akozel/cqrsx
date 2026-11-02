//! The one-line text of an event the time machine shows: a row under the slider and the header of a past state.
use crate::concert::domain::ConcertEvent;

/// What the event says in a line: who or what it concerns.
pub fn summary(event: &ConcertEvent) -> String {
    match event {
        ConcertEvent::ConcertAnnounced(e) => format!("{} · {}", e.artist, e.city),
        ConcertEvent::SeatBooked(e) => format!("{} → {}", e.seat, e.fan_name),
        ConcertEvent::BookingCancelled(e) => format!("{} ✕ (refund {} €)", e.seat, e.refund),
    }
}
