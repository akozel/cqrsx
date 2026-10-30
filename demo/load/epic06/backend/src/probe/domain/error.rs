use cqrsx::Rejection;
use thiserror::Error;

/// A probe refuses nothing: no value of this type exists, so no ping is ever answered `422`.
#[derive(Debug, Clone, PartialEq, Eq, Error, Rejection)]
pub enum ProbeError {}
