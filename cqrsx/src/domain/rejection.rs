//! How a business refusal is reported outside the aggregate.
//!
//! An aggregate refuses a command with its `Error`; the API answers with it and an activity feed records it. Both
//! need the same two things of the refusal: a stable code (a key the UI texts are looked up by) and the values those
//! texts interpolate. `#[derive(Rejection)]` on the error enum writes them from the variants.

pub use cqrsx_macros::Rejection;

/// A business refusal as the API and the activity feed report it.
///
/// The code is a contract (UI translations and log readers key on it), so it never changes for a rule; the params
/// are the values the texts interpolate. `#[derive(Rejection)]` on an enum implements both: the code of a
/// variant is its name in snake case, or what `#[rejection(code = "…")]` gives; its params are its named fields as a
/// JSON object, `{}` for a unit variant. The fields are serialized with their `Serialize` implementations.
///
/// ```
/// use cqrsx::domain::rejection::Rejection;
/// use serde_json::json;
///
/// #[derive(Debug, thiserror::Error, Rejection)]
/// pub enum ConcertError {
///     #[error("this concert has not been announced")]
///     ConcertNotFound,
///     #[error("seat {seat} is already taken by {by}")]
///     SeatTaken { seat: String, by: String },
///     #[error("at most {limit} tickets per fan")]
///     #[rejection(code = "anti_scalper")]
///     TicketLimit { limit: usize },
/// }
///
/// let taken = ConcertError::SeatTaken { seat: "B4".into(), by: "Fox".into() };
/// assert_eq!(taken.code(), "seat_taken");
/// assert_eq!(taken.params(), json!({ "seat": "B4", "by": "Fox" }));
/// assert_eq!((ConcertError::ConcertNotFound.code(), ConcertError::ConcertNotFound.params()), ("concert_not_found", json!({})));
/// let limit = ConcertError::TicketLimit { limit: 4 };
/// assert_eq!((limit.code(), limit.params()), ("anti_scalper", json!({ "limit": 4 })));
/// ```
///
/// The params are named, so a tuple variant does not compile:
///
/// ```compile_fail
/// # use cqrsx::domain::rejection::Rejection;
/// #[derive(Debug, thiserror::Error, Rejection)]
/// pub enum ConcertError {
///     #[error("this concert has not been announced")]
///     ConcertNotFound,
///     #[error("seat {0} is already taken by {1}")]
///     SeatTaken(String, String),
/// }
/// ```
pub trait Rejection {
    /// The stable key of the refusal: what the API answers as `code` and the UI translates.
    fn code(&self) -> &'static str;
    /// The values the texts of the refusal interpolate, as a JSON object.
    fn params(&self) -> serde_json::Value;
}
