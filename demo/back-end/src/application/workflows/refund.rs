//! Refund: `Concert::BookingCancelled` → task `encore_record_cancellation` →
//! [`record_cancellation`] (`Fan.RecordCancellation` → `Fan::TicketCancelled`) → task
//! `encore_issue_refund` → [`issue_refund`] (e-mail "💸 Refund").
//!
//! The first task is published by the [`cancel_booking`](crate::application::concert::cancel_booking)
//! use case.
pub mod issue_refund;
pub mod record_cancellation;
pub mod tasks;
