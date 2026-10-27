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
    config::Config,
    infrastructure::mail::http_mailer::{HttpMailer, MailClientSettings},
    process::Process,
};
use cqrsx::task::{
    adapter::boxed::BoxedTaskAdapter, dispatcher::TaskDispatcher, runtime::dispatcher_factory::TaskDispatcherFactory,
};
use std::sync::Arc;

/// The client of the external mail service: its address, a request timeout that ends before the handler's, and the
/// backoff an e-mail task asks for after a refused delivery.
pub fn mail_settings(config: &Config) -> MailClientSettings {
    MailClientSettings {
        base_url: config.mail_service_url.clone(),
        timeout: config.mail_request_timeout(),
        retry_base: config.task_retry_base,
        retry_max: config.task_retry_max,
    }
}

/// The chain steps between the two aggregates and the e-mails they end in.
pub fn dispatcher(
    process: &Process,
    fans: Arc<FanApp>,
    activity: Arc<ActivityHub>,
) -> Result<TaskDispatcherFactory<BoxedTaskAdapter>, WiringError> {
    let mail = mail_settings(&process.config);
    let http = HttpMailer::client(&mail)?;
    Ok(Arc::new(move || {
        let mailer = || HttpMailer::new(http.clone(), mail.clone(), activity.clone());
        TaskDispatcher::try_new(vec![
            BoxedTaskAdapter::json(RecordTicketHandler { fans: fans.clone() })?,
            BoxedTaskAdapter::json(RecordCancellationHandler { fans: fans.clone() })?,
            BoxedTaskAdapter::json(SendETicketHandler { mailer: mailer() })?,
            BoxedTaskAdapter::json(IssueRefundHandler { mailer: mailer() })?,
        ])
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn the_mail_settings_come_from_the_configuration() {
        let mut config = crate::testing::config();
        config.mail_service_url = "http://mail:8025".into();
        config.task_retry_base = Duration::from_millis(300);
        config.task_retry_max = Duration::from_secs(5);
        let settings = mail_settings(&config);
        assert_eq!(settings.base_url, "http://mail:8025");
        assert_eq!(settings.timeout, config.mail_request_timeout());
        assert_eq!(
            (settings.retry_base, settings.retry_max),
            (Duration::from_millis(300), Duration::from_secs(5))
        );
    }
}
