//! The box office (`Concert` aggregate): one file per use case, the task contracts those use cases
//! publish and the read model.
pub mod announce_concert;
pub mod book_seat;
pub mod cancel_booking;
pub mod tasks;
#[cfg(test)]
mod test_support;
pub mod views;

use crate::{
    application::command_service::{CommandLabel, CommandMetadata, CommandService, Executed, Rejection},
    domain::{
        ConcertId, FanId,
        concert::{Concert, ConcertCommand, ConcertError},
    },
};
use cqrsx::{
    cqrs::{command::CommandExecutor, store::EventStore},
    task::NewTask,
};
use serde_json::{Value, json};

/// The box office: the command pipeline of `Concert` plus read access to its committed history.
pub struct ConcertApp<E, S> {
    service: CommandService<Concert, E>,
    /// Committed concert state, read before `cancel_booking` builds its task (tasks are bound to
    /// the command before it runs).
    history: S,
    /// Attempt budget of the chain tasks the use cases publish (`encore_record_*`).
    chain_max_attempts: i32,
}

impl<E, S> ConcertApp<E, S>
where
    E: CommandExecutor<Concert>,
    S: EventStore<Concert>,
{
    pub fn new(service: CommandService<Concert, E>, history: S, chain_max_attempts: i32) -> Self {
        Self {
            service,
            history,
            chain_max_attempts,
        }
    }

    async fn execute(
        &self,
        concert_id: &ConcertId,
        command: ConcertCommand,
        metadata: CommandMetadata,
        tasks: Vec<NewTask>,
    ) -> Executed<ConcertError> {
        let fan_id = command.fan_id().cloned();
        let label = CommandLabel {
            aggregate_id: concert_id.as_str(),
            command_type: command.name(),
            fan_id: fan_id.as_ref().map(FanId::as_str),
        };
        self.service.run(label, command, metadata, tasks).await
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
    use crate::domain::shared::venue::SeatId;

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
