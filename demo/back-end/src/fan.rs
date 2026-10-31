//! A fan's ticket ledger: the `Fan` aggregate and its layers. The root registers what the ledger plugs into the
//! engine: the aggregate, its tasks with the policies of their types (a step of the chain waits a second, an e-mail
//! backs off from `ENCORE_TASK_RETRY_BASE_MS` to `ENCORE_TASK_RETRY_MAX_MS`; both get `ENCORE_TASK_MAX_ATTEMPTS`
//! attempts, positive), the mail service its e-mails go through (`ENCORE_MAIL_SERVICE_URL`), the fan wallets and the
//! HTTP route of the wallet.
pub mod application;
pub mod domain;
pub mod presentation;

use crate::{
    fan::{
        application::{
            issue_refund::issue_refund, record_cancellation::record_cancellation, record_ticket::record_ticket,
            send_e_ticket::send_e_ticket, views::fan_wallet::FanWalletView,
        },
        domain::Fan,
    },
    shared::{
        application::{
            mail::Mailer,
            policies::{CHAIN_RETRY_AFTER, MAIL, STEP},
        },
        infrastructure::http_mailer::{HttpMailer, MailClientSettings},
    },
};
use cqrsx::{
    engine::{
        module::Module,
        settings::duration_text,
        setup::{Setup, SetupError},
    },
    task::policy::Retry,
};
use std::{num::NonZeroU16, sync::Arc};

/// The attempt budget of both policies without `ENCORE_TASK_MAX_ATTEMPTS`.
const DEFAULT_TASK_MAX_ATTEMPTS: NonZeroU16 = NonZeroU16::new(6).unwrap();

pub fn module(setup: &Setup) -> Result<Module, SetupError> {
    let attempts: NonZeroU16 = setup.env("TASK_MAX_ATTEMPTS", DEFAULT_TASK_MAX_ATTEMPTS)?;
    let (base, max) = (
        setup.millis("TASK_RETRY_BASE_MS", 1000)?,
        setup.millis("TASK_RETRY_MAX_MS", 8000)?,
    );
    if base > max {
        let message = format!("must not exceed {}", setup.variable("TASK_RETRY_MAX_MS"));
        return Err(setup.invalid("TASK_RETRY_BASE_MS", message));
    }
    let mail = Retry::backoff(base, max);
    Ok(Module::new("fan")
        .aggregate::<Fan>(())
        .provide(mailer(setup)?)
        .policy(STEP, Retry::fixed(CHAIN_RETRY_AFTER), attempts)
        .policy(MAIL, mail, attempts)
        .task(record_ticket)
        .policy(STEP)
        .task(record_cancellation)
        .policy(STEP)
        .task(send_e_ticket)
        .policy(MAIL)
        .task(issue_refund)
        .policy(MAIL)
        .view::<FanWalletView, Fan>()
        .routes(presentation::routes()))
}

/// The client of the external mail service: its address (`ENCORE_MAIL_SERVICE_URL`, without a trailing slash) and a
/// request timeout that ends a second before the handler's, never below a second (`mail.request_timeout` in the
/// engine's description).
pub fn mail_settings(setup: &Setup) -> Result<MailClientSettings, SetupError> {
    let base_url: String = setup.env("MAIL_SERVICE_URL", "http://127.0.0.1:8090".to_owned())?;
    let timeout = MailClientSettings::request_timeout(setup.engine().handler_timeout());
    setup.describe("mail.request_timeout", duration_text(timeout));
    Ok(MailClientSettings {
        base_url: base_url.trim_end_matches('/').to_owned(),
        timeout,
    })
}

/// The mail service of the ledger's e-mails, one for every worker, with one HTTP client: a connection pool is meant to
/// be shared.
pub fn mailer(setup: &Setup) -> Result<Arc<Mailer>, SetupError> {
    let settings = mail_settings(setup)?;
    let client = HttpMailer::client(&settings).map_err(SetupError::failed)?;
    Ok(Arc::new(HttpMailer::new(client, settings)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqrsx::{
        command::locks::LockLimits,
        engine::settings::Settings,
        worker::runtime::{config::TaskRuntimeConfig, worker_pool_config::TaskWorkerPoolConfig},
    };
    use std::time::Duration;

    /// The mail settings of the demo's engine: its defaults, or its task runtime with a handler that may run a second
    /// (and a lock that waits less).
    fn mail_request_timeout(one_second_handler: bool) -> Duration {
        let mut settings = crate::testing::settings();
        if one_second_handler {
            let (demo, second) = (settings.task_runtime(), Duration::from_secs(1));
            let worker = demo.worker_pool();
            let worker = TaskWorkerPoolConfig::try_new(
                worker.concurrency(),
                worker.buffer_capacity(),
                worker.poll_interval(),
                second,
                worker.lease_duration(),
            )
            .unwrap();
            let runtime = TaskRuntimeConfig::try_new(*demo.storage(), worker).expect("a one-second handler timeout validates");
            let waits_less = LockLimits::try_new(Duration::from_millis(500), 128).unwrap();
            settings = Settings::try_new(settings.snapshot_every(), waits_less, settings.processor(), runtime).unwrap();
        }
        mail_settings(&Setup::new("ENCORE", |_| None, settings)).unwrap().timeout
    }

    #[test]
    fn the_mail_request_gives_up_a_second_before_the_handler_timeout_but_never_below_a_second() {
        assert_eq!(mail_request_timeout(false), Duration::from_secs(4));
        assert_eq!(mail_request_timeout(true), Duration::from_secs(1));
    }
}
