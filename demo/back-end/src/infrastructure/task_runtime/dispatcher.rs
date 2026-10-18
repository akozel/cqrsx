//! `TaskDispatcherFactory`: a fresh dispatcher (fresh handlers) for every worker and every worker
//! restart. The ordered format list is identical for every instance; the mailers share one HTTP
//! client, since a connection pool is meant to be shared.
use crate::{
    application::{
        activity::ActivityHub,
        fan::FanApp,
        workflows::{
            refund::{issue_refund::IssueRefundHandler, record_cancellation::RecordCancellationHandler},
            ticket_delivery::{record_ticket::RecordTicketHandler, send_e_ticket::SendETicketHandler},
        },
    },
    domain::fan::Fan,
    infrastructure::mail::http_mailer::{HttpMailer, MailClientSettings},
};
use cqrsx::postgres::command::PostgresCommandExecutor;
use cqrsx::task::{
    adapter::boxed::BoxedTaskAdapter, dispatcher::TaskDispatcher, runtime::dispatcher_factory::TaskDispatcherFactory,
};
use std::sync::Arc;

pub struct DispatcherDeps {
    pub fans: Arc<FanApp<PostgresCommandExecutor<Fan>>>,
    pub activity: Arc<ActivityHub>,
    /// Shared by every mailer: one connection pool to the external mail service.
    pub http: reqwest::Client,
    pub mail: MailClientSettings,
    /// Attempt budget of the e-mail tasks the ledger steps publish.
    pub task_max_attempts: i32,
}

/// Adding a task type means adding one registration line here.
pub fn dispatcher_factory(deps: DispatcherDeps) -> TaskDispatcherFactory<BoxedTaskAdapter> {
    Arc::new(move || {
        let mailer = || HttpMailer::new(deps.http.clone(), deps.mail.clone(), deps.activity.clone());
        TaskDispatcher::try_new(vec![
            BoxedTaskAdapter::json(RecordTicketHandler {
                fans: deps.fans.clone(),
                follow_up_max_attempts: deps.task_max_attempts,
            })?,
            BoxedTaskAdapter::json(RecordCancellationHandler {
                fans: deps.fans.clone(),
                follow_up_max_attempts: deps.task_max_attempts,
            })?,
            BoxedTaskAdapter::json(SendETicketHandler { mailer: mailer() })?,
            BoxedTaskAdapter::json(IssueRefundHandler { mailer: mailer() })?,
        ])
    })
}
