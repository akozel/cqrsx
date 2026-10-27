//! The box office's command pipeline (`ConcertApp`); one file per use case next to it. The tasks that follow its
//! decisions are planned by its [reactions](super::reactions).
use crate::{
    concert::domain::{Concert, ConcertCommand, ConcertError},
    shared::{
        application::command_service::{CommandLabel, CommandMetadata, CommandService, Executed},
        domain::{ConcertId, FanId},
    },
};
use cqrsx::cqrs::command::CommandExecutor;

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
