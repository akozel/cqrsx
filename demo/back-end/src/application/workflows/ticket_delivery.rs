//! Ticket delivery: `Concert::SeatBooked` → task `encore_record_ticket` → `Fan.RecordTicket` →
//! `Fan::TicketRecorded` → task `encore_send_e_ticket` → e-mail "🎫 Your e-ticket".
pub mod record_ticket;
pub mod send_e_ticket;
