//! Refund: `Concert::BookingCancelled` → task `encore_record_cancellation` → `Fan.RecordCancellation`
//! → `Fan::TicketCancelled` → task `encore_issue_refund` → e-mail "💸 Refund".
pub mod issue_refund;
pub mod record_cancellation;
