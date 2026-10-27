//! The box office's command pipeline (`ConcertApp`) and how its rejections are reported; one file per use case
//! next to it. The tasks that follow its decisions are planned by its [reactions](super::reactions).
use crate::{
    concert::domain::{Concert, ConcertCommand, ConcertError},
    shared::{
        application::command_service::{CommandLabel, CommandMetadata, CommandService, Executed, Rejection},
        domain::{ConcertId, FanId},
    },
};
use cqrsx::cqrs::command::CommandExecutor;
use serde_json::{Value, json};

/// The box office: the command pipeline of `Concert`. Its executor's planner plans the tasks that follow
/// every decision, so a use case passes the command only.
pub struct ConcertApp<E> {
    service: CommandService<Concert, E>,
}

impl<E: CommandExecutor<Concert>> ConcertApp<E> {
    pub fn new(service: CommandService<Concert, E>) -> Self {
        Self { service }
    }

    pub(super) async fn execute(
        &self,
        concert_id: &ConcertId,
        command: ConcertCommand,
        metadata: CommandMetadata,
    ) -> Executed<ConcertError> {
        let fan_id = command.fan_id().cloned();
        let label = CommandLabel {
            aggregate_id: concert_id.as_str(),
            command_type: command.name(),
            fan_id: fan_id.as_ref().map(FanId::as_str),
        };
        self.service.run(label, command, metadata).await
    }
}

/// Rules R1–R8 as the API reports them (`{ code, params }`); texts live in the front-end i18n.
impl Rejection for ConcertError {
    fn code(&self) -> &'static str {
        ConcertError::code(self)
    }

    fn params(&self) -> Value {
        match self {
            ConcertError::UnknownSeat { seat } => json!({ "seat": seat }),
            ConcertError::SeatTaken { seat, by } => json!({ "seat": seat, "by": by }),
            ConcertError::TicketLimit { limit } => json!({ "limit": limit }),
            ConcertError::SeatNotBooked { seat } => json!({ "seat": seat }),
            ConcertError::ConcertNotFound
            | ConcertError::AlreadyAnnounced
            | ConcertError::SoldOut
            | ConcertError::NotYourTicket => json!({}),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::domain::venue::SeatId;

    #[test]
    fn rejections_carry_what_the_ui_texts_interpolate() {
        let seat = SeatId::parse("B4").unwrap();
        let taken = ConcertError::SeatTaken { seat, by: "Fox".into() };
        assert_eq!(Rejection::code(&taken), "seat_taken");
        assert_eq!(taken.params(), json!({ "seat": "B4", "by": "Fox" }));
        assert_eq!(ConcertError::TicketLimit { limit: 4 }.params(), json!({ "limit": 4 }));
        assert_eq!(
            ConcertError::UnknownSeat { seat: "Z9".into() }.params(),
            json!({ "seat": "Z9" })
        );
        assert_eq!(ConcertError::NotYourTicket.params(), json!({}));
    }
}
