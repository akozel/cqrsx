//! The outbox task handlers: one registration line per task type. A fresh dispatcher (fresh handlers)
//! is built for every worker and every worker restart; the mailers share one HTTP client to the
//! external mail service, since a connection pool is meant to be shared.
use super::{FanApp, WiringError};
use crate::{
    application::{
        activity::ActivityHub,
        workflows::{
            refund::{issue_refund::IssueRefundHandler, record_cancellation::RecordCancellationHandler},
            ticket_delivery::{record_ticket::RecordTicketHandler, send_e_ticket::SendETicketHandler},
        },
    },
    infrastructure::mail::http_mailer::{HttpMailer, MailClientSettings},
    process::Process,
};
use cqrsx::task::{
    adapter::boxed::BoxedTaskAdapter, dispatcher::TaskDispatcher, runtime::dispatcher_factory::TaskDispatcherFactory,
};
use std::sync::Arc;

/// The chain steps between the two aggregates and the e-mails they end in.
pub fn dispatcher(
    process: &Process,
    fans: Arc<FanApp>,
    activity: Arc<ActivityHub>,
) -> Result<TaskDispatcherFactory<BoxedTaskAdapter>, WiringError> {
    let config = &process.config;
    let mail = MailClientSettings {
        base_url: config.mail_service_url.clone(),
        timeout: config.mail_request_timeout(),
        retry_base: config.task_retry_base,
        retry_max: config.task_retry_max,
    };
    let http = HttpMailer::client(&mail)?;
    // Attempt budget of the e-mail tasks the ledger steps publish.
    let follow_up_max_attempts = config.task_max_attempts;
    Ok(Arc::new(move || {
        let mailer = || HttpMailer::new(http.clone(), mail.clone(), activity.clone());
        TaskDispatcher::try_new(vec![
            BoxedTaskAdapter::json(RecordTicketHandler {
                fans: fans.clone(),
                follow_up_max_attempts,
            })?,
            BoxedTaskAdapter::json(RecordCancellationHandler {
                fans: fans.clone(),
                follow_up_max_attempts,
            })?,
            BoxedTaskAdapter::json(SendETicketHandler { mailer: mailer() })?,
            BoxedTaskAdapter::json(IssueRefundHandler { mailer: mailer() })?,
        ])
    }))
}
