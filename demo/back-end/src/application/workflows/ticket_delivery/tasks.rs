//! Task contracts of ticket delivery and how [`TicketDelivery`](super::TicketDelivery) builds them from
//! the facts it follows. Names are `[a-z0-9_]` only (decision D-05): the library's diagnostics print
//! other names as `unknown`.
use crate::{
    application::tasks::new_task,
    domain::{
        ConcertId, FanId, FanLedgerId,
        concert::events::SeatBooked,
        fan::events::TicketRecorded,
        shared::venue::{SeatId, Zone},
    },
};
use cqrsx::task::{NewTask, TaskValidationError, format::TaskPayload};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// `Concert::SeatBooked` → `Fan.RecordTicket`: step one of ticket delivery
/// ([`record_ticket`](super::record_ticket)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordTicketV1 {
    pub booking_id: Uuid,
    pub concert_id: ConcertId,
    pub seat: SeatId,
    pub zone: Zone,
    pub price: u32,
    pub fan_id: FanId,
    pub fan_name: String,
}

impl TaskPayload for RecordTicketV1 {
    const TASK_TYPE: &'static str = "encore_record_ticket";
    const TASK_VERSION: i32 = 1;
}

/// `Fan::TicketRecorded` → e-mail "🎫 Your e-ticket" (step [`send_e_ticket`](super::send_e_ticket)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SendETicketV1 {
    pub booking_id: Uuid,
    pub concert_id: ConcertId,
    pub seat: SeatId,
    pub fan_id: FanId,
    pub fan_name: String,
    pub price: u32,
}

impl TaskPayload for SendETicketV1 {
    const TASK_TYPE: &'static str = "encore_send_e_ticket";
    const TASK_VERSION: i32 = 1;
}

/// The booking as the fan's ledger will record it: everything comes from the fact, the price paid included.
pub fn record_ticket(concert_id: &ConcertId, booked: &SeatBooked, max_attempts: i32) -> Result<NewTask, TaskValidationError> {
    let payload = RecordTicketV1 {
        booking_id: booked.booking_id,
        concert_id: concert_id.clone(),
        seat: booked.seat.clone(),
        zone: booked.zone,
        price: booked.price,
        fan_id: booked.fan_id.clone(),
        fan_name: booked.fan_name.clone(),
    };
    new_task(payload, concert_id, &booked.fan_id, max_attempts)
}

/// The e-ticket of a ticket the ledger recorded; the fan is the ledger's.
pub fn send_e_ticket(ledger: &FanLedgerId, recorded: &TicketRecorded, max_attempts: i32) -> Result<NewTask, TaskValidationError> {
    let fan_id = ledger.fan();
    let payload = SendETicketV1 {
        booking_id: recorded.booking_id,
        concert_id: recorded.concert_id.clone(),
        seat: recorded.seat.clone(),
        fan_id: fan_id.clone(),
        fan_name: recorded.fan_name.clone(),
        price: recorded.price,
    };
    new_task(payload, &recorded.concert_id, &fan_id, max_attempts)
}
