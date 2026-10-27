//! The box office: the `Concert` aggregate and its layers. The root registers what the box office plugs into the
//! engine: the aggregate with its rules (`ENCORE_TICKET_LIMIT`, 4 unless set) and the concert halls.
pub mod application;
pub mod domain;
pub mod presentation;

use crate::concert::{
    application::views::concert_hall::ConcertHallView,
    domain::{Concert, ConcertRules},
};
use cqrsx::engine::{
    module::Module,
    setup::{Setup, SetupError},
};

pub fn module(setup: &Setup) -> Result<Module, SetupError> {
    Ok(Module::new("concert")
        .aggregate::<Concert>(ConcertRules {
            ticket_limit: setup.env("TICKET_LIMIT", 4)?,
        })
        .view::<ConcertHallView, Concert>())
}
