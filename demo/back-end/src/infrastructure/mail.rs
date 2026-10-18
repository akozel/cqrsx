//! The external mail service as the box office sees it: an HTTP API called from the task handlers.
//! The demo's provider (a moody one that refuses 20 % of the deliveries) is hosted by the sidecar;
//! this crate only knows the contract — `POST /mail`, `200` delivered, `503` try again later — and
//! would work unchanged against a real provider.
pub mod http_mailer;
