//! Technical adapters: PostgreSQL (the committed history) and the HTTP client of the external mail service, called
//! from the task handlers; the read models in MongoDB are cqrsx's (`MongoViewRepository`), the composition picks them. The
//! demo's provider (a moody one that refuses 20 % of the deliveries) is hosted by the sidecar; this crate only knows
//! the contract — `POST /mail`, `200` delivered, `503` try again later — and would work unchanged against a real
//! provider.
pub mod http_mailer;
pub mod postgres;
