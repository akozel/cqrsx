//! `TaskDispatcherFactory`: a fresh dispatcher (fresh handlers, own RNG) for every worker and every
//! worker restart. The factory gets no worker index, so deterministic seeds come from
//! `ENCORE_CHAOS_SEED + counter`. The ordered format list is identical for every instance.
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
    infrastructure::{
        mail::{
            chaos::Random,
            flaky_mail_server::{FlakyMailServer, MailSettings},
        },
        mongo::inbox::MongoInbox,
    },
};
use cqrsx::postgres::command::PostgresCommandExecutor;
use cqrsx::task::{
    adapter::boxed::BoxedTaskAdapter, dispatcher::TaskDispatcher, runtime::dispatcher_factory::TaskDispatcherFactory,
};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

pub struct DispatcherDeps {
    pub fans: Arc<FanApp<PostgresCommandExecutor<Fan>>>,
    pub inbox: Arc<MongoInbox>,
    pub activity: Arc<ActivityHub>,
    pub mail: MailSettings,
    pub failure_rate: f64,
    pub chaos_seed: Option<u64>,
    /// Attempt budget of the e-mail tasks the ledger steps publish.
    pub task_max_attempts: i32,
}

/// Adding a task type means adding one registration line here.
pub fn dispatcher_factory(deps: DispatcherDeps) -> TaskDispatcherFactory<BoxedTaskAdapter> {
    let counter = AtomicU64::new(0);
    Arc::new(move || {
        let instance = counter.fetch_add(1, Ordering::SeqCst);
        let seed = deps.chaos_seed.map(|s| s.wrapping_add(instance));
        let mail_server = || {
            FlakyMailServer::new(
                deps.inbox.clone(),
                Random::new(deps.failure_rate, seed),
                deps.mail.clone(),
                deps.activity.clone(),
                seed.unwrap_or(instance).wrapping_mul(7919),
            )
        };
        TaskDispatcher::try_new(vec![
            BoxedTaskAdapter::json(RecordTicketHandler {
                fans: deps.fans.clone(),
                follow_up_max_attempts: deps.task_max_attempts,
            })?,
            BoxedTaskAdapter::json(RecordCancellationHandler {
                fans: deps.fans.clone(),
                follow_up_max_attempts: deps.task_max_attempts,
            })?,
            BoxedTaskAdapter::json(SendETicketHandler { mailer: mail_server() })?,
            BoxedTaskAdapter::json(IssueRefundHandler { mailer: mail_server() })?,
        ])
    })
}
