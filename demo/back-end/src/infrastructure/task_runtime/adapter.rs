//! One adapter type for the dispatcher: `TaskDispatcher<A>` takes a single `A`, so the four
//! `JsonTaskAdapter`s are wrapped in an enum that delegates `formats()` / `invoke()`.
use crate::{
    application::{
        tasks::{IssueRefundV1, RecordCancellationV1, RecordTicketV1, SendETicketV1},
        workflows::{
            refund::{issue_refund::IssueRefundHandler, record_cancellation::RecordCancellationHandler},
            ticket_delivery::{record_ticket::RecordTicketHandler, send_e_ticket::SendETicketHandler},
        },
    },
    infrastructure::mail::{chaos::ChaosPolicy, flaky_mail_server::Inbox},
};
use cqrsx::task::{
    adapter::{TaskAdapter, json::JsonTaskAdapter},
    attempt::TaskAttempt,
    format::TaskFormat,
    handler::TaskHandlerError,
};

pub enum EncoreTaskAdapter<I: Inbox, C: ChaosPolicy> {
    RecordTicket(JsonTaskAdapter<RecordTicketV1, RecordTicketHandler>),
    RecordCancellation(JsonTaskAdapter<RecordCancellationV1, RecordCancellationHandler>),
    SendETicket(JsonTaskAdapter<SendETicketV1, SendETicketHandler<I, C>>),
    IssueRefund(JsonTaskAdapter<IssueRefundV1, IssueRefundHandler<I, C>>),
}

impl<I: Inbox + 'static, C: ChaosPolicy + 'static> TaskAdapter for EncoreTaskAdapter<I, C> {
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
