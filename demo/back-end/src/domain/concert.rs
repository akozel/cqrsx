//! `Concert` aggregate: one concert = one event stream; the source of truth for seats and for
//! every business rule R1–R8. Rules are checked synchronously here, which is what makes a rejected
//! click visible as "Δ events = 0".
pub mod command;
pub mod error;
pub mod event;
#[cfg(test)]
mod tests;

use crate::domain::{
    FanId,
    venue::{SeatId, VENUE},
};
use chrono::NaiveDate;
use cqrsx::cqrs::{aggregate::Aggregate, event_sink::EventSink};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

pub use command::ConcertCommand;
pub use error::ConcertError;
pub use event::ConcertEvent;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConcertInfo {
    pub artist: String,
    pub city: String,
    pub date: NaiveDate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Booking {
    pub fan_id: FanId,
    pub fan_name: String,
    pub booking_id: Uuid,
    pub price: u32,
}

/// Configuration the rules depend on; history does not (replay calls `apply`, never `handle`).
#[derive(Debug, Clone)]
pub struct ConcertServices {
    pub ticket_limit: usize,
}

impl Default for ConcertServices {
    fn default() -> Self {
        Self { ticket_limit: 4 }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct Concert {
    /// `None` ⇒ the concert has not been announced (rule R1).
    info: Option<ConcertInfo>,
    /// Only occupied seats; a cancelled booking is removed.
    bookings: BTreeMap<SeatId, Booking>,
}

impl Concert {
    pub fn info(&self) -> Option<&ConcertInfo> {
        self.info.as_ref()
    }

    pub fn bookings(&self) -> &BTreeMap<SeatId, Booking> {
        &self.bookings
    }

    pub fn free_seats(&self) -> u32 {
        VENUE.capacity().saturating_sub(self.bookings.len() as u32)
    }

    /// Derived, not an event (decision D-03).
    pub fn is_sold_out(&self) -> bool {
        self.free_seats() == 0
    }

    pub fn tickets_of(&self, fan_id: &FanId) -> usize {
        self.bookings.values().filter(|b| &b.fan_id == fan_id).count()
    }

    pub fn revenue(&self) -> u32 {
        self.bookings.values().map(|b| b.price).sum()
    }
}

impl Aggregate for Concert {
    const TYPE: &'static str = "Concert";
    type Command = ConcertCommand;
    type Event = ConcertEvent;
    type Error = ConcertError;
    type Services = ConcertServices;

    // Rules are checked in the documented order R1 → R8, from "does the concert exist" to the
    // most specific ownership check, so the first failing rule is the most fundamental one.
    // Every accepted command writes exactly ONE event (decision D-02): the outbox repository
    // commits one event plus its tasks atomically and rejects multi-event batches.
    async fn handle(
        &mut self,
        command: ConcertCommand,
        services: &ConcertServices,
        sink: &EventSink<Self>,
    ) -> Result<(), ConcertError> {
        match command {
            ConcertCommand::AnnounceConcert { artist, city, date } => {
                if self.info.is_some() {
                    return Err(ConcertError::AlreadyAnnounced); // R2
                }
                sink.write(
                    ConcertEvent::ConcertAnnounced {
                        artist,
                        city,
                        date,
                        capacity: VENUE.capacity(),
                    },
                    self,
                )
                .await;
            }
            ConcertCommand::BookSeat {
                seat,
                fan_id,
                fan_name,
                booking_id,
            } => {
                if self.info.is_none() {
                    return Err(ConcertError::ConcertNotFound); // R1
                }
                let seat = VENUE.seat(&seat).ok_or(ConcertError::UnknownSeat { seat })?; // R3
                if self.is_sold_out() {
                    return Err(ConcertError::SoldOut); // R4
                }
                if let Some(booking) = self.bookings.get(&seat.id) {
                    return Err(ConcertError::SeatTaken {
                        seat: seat.id,
                        by: booking.fan_name.clone(),
                    }); // R5
                }
                if self.tickets_of(&fan_id) >= services.ticket_limit {
                    return Err(ConcertError::TicketLimit {
                        limit: services.ticket_limit,
                    }); // R6
                }
                // Sold-out is derived state, not a second event (D-02/D-03).
                sink.write(
                    ConcertEvent::SeatBooked {
                        seat: seat.id,
                        zone: seat.zone,
                        price: seat.price,
                        fan_id,
                        fan_name,
                        booking_id,
                    },
                    self,
                )
                .await;
            }
            ConcertCommand::CancelBooking {
                seat,
                fan_id,
                cancellation_id,
            } => {
                if self.info.is_none() {
                    return Err(ConcertError::ConcertNotFound); // R1
                }
                let seat = VENUE.seat(&seat).ok_or(ConcertError::UnknownSeat { seat })?; // R3
                let booking = self
                    .bookings
                    .get(&seat.id)
                    .cloned()
                    .ok_or_else(|| ConcertError::SeatNotBooked { seat: seat.id.clone() })?; // R7
                if booking.fan_id != fan_id {
                    return Err(ConcertError::NotYourTicket); // R8
                }
                // The refund is the price that was actually paid (a fact from history), not a
                // formula over today's price list.
                sink.write(
                    ConcertEvent::BookingCancelled {
                        seat: seat.id,
                        fan_id,
                        booking_id: booking.booking_id,
                        cancellation_id,
                        refund: booking.price,
                    },
                    self,
                )
                .await;
            }
        }
        Ok(())
    }

    fn apply(&mut self, event: ConcertEvent) {
        match event {
            ConcertEvent::ConcertAnnounced { artist, city, date, .. } => self.info = Some(ConcertInfo { artist, city, date }),
            ConcertEvent::SeatBooked {
                seat,
                fan_id,
                fan_name,
                booking_id,
                price,
                ..
            } => {
                self.bookings.insert(
                    seat,
                    Booking {
                        fan_id,
                        fan_name,
                        booking_id,
                        price,
                    },
                );
            }
            ConcertEvent::BookingCancelled { seat, .. } => {
                self.bookings.remove(&seat);
            }
        }
    }
}
