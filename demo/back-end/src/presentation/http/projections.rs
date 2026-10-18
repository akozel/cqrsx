//! `/api/projections`: operations on the read models — ⏪ Encore! (rebuild), status, 🔍 verify.
//! Dropping them without a rebuild (🧨) is a demo operation and lives in the sidecar.
pub mod rebuild;
pub mod status;
pub mod verify;
