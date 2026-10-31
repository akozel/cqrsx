//! The box office: the `Concert` aggregate and its layers. The root registers what the box office plugs into the
//! engine: the aggregate with its rules (`ENCORE_TICKET_LIMIT`, 4 unless set, positive), the ticket limit the UI shows,
//! the concert halls and the HTTP routes of the box office.
pub mod application;
pub mod domain;
pub mod presentation;

use crate::{
    concert::{
        application::views::concert_hall::ConcertHallView,
        domain::{Concert, ConcertRules},
    },
    shared::application::TicketLimit,
};
use cqrsx_engine::{
    module::Module,
    setup::{Setup, SetupError},
};
use std::sync::Arc;

pub fn module(setup: &Setup) -> Result<Module, SetupError> {
    let ticket_limit: usize = setup.env("TICKET_LIMIT", 4)?;
    if ticket_limit == 0 {
        return Err(setup.invalid("TICKET_LIMIT", "must be positive"));
    }
    Ok(Module::new("concert")
        .aggregate::<Concert>(ConcertRules { ticket_limit })
        .provide(Arc::new(TicketLimit(ticket_limit)))
        .view::<ConcertHallView, Concert>()
        .routes(presentation::routes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqrsx_engine::Cqrs;

    async fn limit(value: Option<&'static str>) -> Result<TicketLimit, String> {
        let cqrs = Cqrs::from_lookup("ENCORE", move |name| {
            (name == "ENCORE_TICKET_LIMIT").then(|| value.map(str::to_owned)).flatten()
        })
        .module(module)
        .build()
        .await
        .map_err(|error| error.to_string())?;
        Ok(*cqrs.dependency::<TicketLimit>().unwrap())
    }

    #[tokio::test]
    async fn the_ticket_limit_comes_from_its_variable_and_must_be_positive() {
        assert_eq!(limit(None).await, Ok(TicketLimit(4)));
        assert_eq!(limit(Some(" 7 ")).await, Ok(TicketLimit(7)));
        assert_eq!(
            limit(Some("0")).await,
            Err("ENCORE_TICKET_LIMIT: must be positive".to_owned())
        );
        assert_eq!(
            limit(Some("four")).await,
            Err(r#"ENCORE_TICKET_LIMIT: cannot parse "four": invalid digit found in string"#.to_owned())
        );
    }
}
