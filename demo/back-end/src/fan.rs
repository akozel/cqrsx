//! A fan's ticket ledger: the `Fan` aggregate and its layers. The root registers what the ledger plugs into the
//! engine: the aggregate, its tasks with the policies of their types (a step of the chain waits a second, an e-mail
//! backs off from `ENCORE_TASK_RETRY_BASE_MS` to `ENCORE_TASK_RETRY_MAX_MS`; both get `ENCORE_TASK_MAX_ATTEMPTS`
//! attempts), the mail service its e-mails go through and the fan wallets.
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
        setup::{Setup, SetupError},
    },
    task::policy::Retry,
};
use std::sync::Arc;

pub fn module(setup: &Setup) -> Result<Module, SetupError> {
    let attempts = setup.env("TASK_MAX_ATTEMPTS", 6)?;
    let mail = Retry::backoff(
        setup.millis("TASK_RETRY_BASE_MS", 1000)?,
        setup.millis("TASK_RETRY_MAX_MS", 8000)?,
    );
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
        .view::<FanWalletView, Fan>())
}

/// The client of the external mail service: its address (`ENCORE_MAIL_SERVICE_URL`) and a request timeout that ends
/// before the handler's.
pub fn mail_settings(setup: &Setup) -> Result<MailClientSettings, SetupError> {
    let base_url: String = setup.env("MAIL_SERVICE_URL", "http://127.0.0.1:8090".to_owned())?;
    Ok(MailClientSettings {
        base_url: base_url.trim_end_matches('/').to_owned(),
        timeout: MailClientSettings::request_timeout(setup.engine().handler_timeout),
    })
}

/// The mail service of the ledger's e-mails, one for every worker, with one HTTP client: a connection pool is meant to
/// be shared.
pub fn mailer(setup: &Setup) -> Result<Arc<Mailer>, SetupError> {
    let settings = mail_settings(setup)?;
    let client = HttpMailer::client(&settings).map_err(SetupError::failed)?;
    Ok(Arc::new(HttpMailer::new(client, settings)))
}
