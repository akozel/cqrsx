//! Ticket delivery: `Concert::SeatBooked` → task `encore_record_ticket` →
//! [`record_ticket`] (`Fan.RecordTicket` → `Fan::TicketRecorded`) → task `encore_send_e_ticket` →
//! [`send_e_ticket`] (e-mail "🎫 Your e-ticket").
//!
//! The first task is published by the [`book_seat`](crate::application::concert::book_seat) use case.
pub mod record_ticket;
pub mod send_e_ticket;
pub mod tasks;
