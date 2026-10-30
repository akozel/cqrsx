//! `Probe` aggregate: one probe = one event stream. It refuses nothing: every ping is accepted and asks for the tasks
//! its rules say, so the stand measures the path of a command, not a rule.
pub mod aggregate;
pub mod commands;
pub mod error;
pub mod events;
pub mod id;

pub use aggregate::{Probe, ProbeRules};
pub use commands::Ping;
pub use error::ProbeError;
pub use events::{Pinged, ProbeEvent};
pub use id::{InvalidProbeId, ProbeId};
