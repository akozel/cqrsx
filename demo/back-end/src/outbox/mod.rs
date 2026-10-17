//! Outbox task handlers: the two "postmen" of the aggregate chain (`record_*` → commands to the
//! `Fan` aggregate) and the two mail handlers over the flaky mail server. The 20 % chaos lives only
//! in `chaos` / `flaky_mail` and is marked `// DEMO ONLY`.
pub mod adapter;
pub mod chain;
pub mod chaos;
pub mod dispatcher;
pub mod flaky_mail;
pub mod issue_refund;
pub mod record_cancellation;
pub mod record_ticket;
pub mod send_e_ticket;
