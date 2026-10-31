//! What is left of the CQRS composition until the next move: the contract of a committed-event reaction
//! ([`event_handler`]), the projections and the read models they feed ([`projection`]), and the public downstream
//! testing toolkit ([`testing`]).
pub mod event_handler;
pub mod projection;
pub mod testing;
