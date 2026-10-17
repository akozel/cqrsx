//! One adapter type for the dispatcher: `TaskDispatcher<A>` takes a single `A` and `TaskAdapter`
//! is not object-safe, so the four `JsonTaskAdapter`s are wrapped in an enum that delegates
//! `formats()` / `invoke()`. Adding a task means adding a variant here and registering it in
//! `dispatcher`.
use crate::{
    application::{
        concert::tasks::{RecordCancellationV1, RecordTicketV1},
        ports::Mailer,
        workflows::{
            refund::{issue_refund::IssueRefundHandler, record_cancellation::RecordCancellationHandler, tasks::IssueRefundV1},
            ticket_delivery::{record_ticket::RecordTicketHandler, send_e_ticket::SendETicketHandler, tasks::SendETicketV1},
        },
    },
    domain::fan::Fan,
};
use cqrsx::{
    cqrs::command::CommandExecutor,
    task::{
        adapter::{TaskAdapter, json::JsonTaskAdapter},
        attempt::TaskAttempt,
        format::TaskFormat,
        handler::TaskHandlerError,
    },
};

/// `E` executes the `Fan` commands of the ledger steps, `M` sends the e-mails.
pub enum EncoreTaskAdapter<E, M> {
    RecordTicket(JsonTaskAdapter<RecordTicketV1, RecordTicketHandler<E>>),
    RecordCancellation(JsonTaskAdapter<RecordCancellationV1, RecordCancellationHandler<E>>),
    SendETicket(JsonTaskAdapter<SendETicketV1, SendETicketHandler<M>>),
    IssueRefund(JsonTaskAdapter<IssueRefundV1, IssueRefundHandler<M>>),
}

impl<E, M> TaskAdapter for EncoreTaskAdapter<E, M>
where
    E: CommandExecutor<Fan> + 'static,
    M: Mailer + 'static,
{
    fn formats(&self) -> Vec<TaskFormat> {
        match self {
            Self::RecordTicket(a) => a.formats(),
            Self::RecordCancellation(a) => a.formats(),
            Self::SendETicket(a) => a.formats(),
            Self::IssueRefund(a) => a.formats(),
        }
    }

    async fn invoke(&mut self, attempt: &TaskAttempt) -> Result<(), TaskHandlerError> {
        match self {
            Self::RecordTicket(a) => a.invoke(attempt).await,
            Self::RecordCancellation(a) => a.invoke(attempt).await,
            Self::SendETicket(a) => a.invoke(attempt).await,
            Self::IssueRefund(a) => a.invoke(attempt).await,
        }
    }
}
