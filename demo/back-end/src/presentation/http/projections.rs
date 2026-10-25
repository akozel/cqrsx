//! `/api/projections`: the read side — ⏪ Encore! (rebuild), status, 🔍 verify. Dropping the read models (🧨) is
//! a demo operation and lives in the sidecar; the processors rebuild them by themselves.
pub mod rebuild;
pub mod status;
pub mod verify;
