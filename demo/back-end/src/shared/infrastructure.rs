//! Technical adapters: the HTTP client of the external mail service, called from the task handlers. The journal, the
//! outbox and the read models are the engine's (PostgreSQL and MongoDB through cqrsx). The demo's provider (a moody one
//! that refuses 20 % of the deliveries) is hosted by the sidecar; this crate only knows the contract — `POST /mail`,
//! `200` delivered, `503` try again later — and would work unchanged against a real provider.
pub mod http_mailer;
