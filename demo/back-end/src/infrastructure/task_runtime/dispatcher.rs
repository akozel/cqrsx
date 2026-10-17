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
        postgres::PostgresExecutor,
        task_runtime::adapter::EncoreTaskAdapter,
    },
};
use cqrsx::task::{
    adapter::json::JsonTaskAdapter,
    dispatcher::{TaskDispatcher, TaskDispatcherError},
    runtime::dispatcher_factory::TaskDispatcherFactory,
};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// The adapter the runtime dispatches to in production.
pub type EncoreAdapter = EncoreTaskAdapter<PostgresExecutor<Fan>, FlakyMailServer<MongoInbox, Random>>;

pub struct DispatcherDeps {
    pub fans: Arc<FanApp<PostgresExecutor<Fan>>>,
    pub inbox: Arc<MongoInbox>,
    pub activity: Arc<ActivityHub>,
    pub mail: MailSettings,
    pub failure_rate: f64,
    pub chaos_seed: Option<u64>,
    /// Attempt budget of the e-mail tasks the ledger steps publish.
    pub task_max_attempts: i32,
}

pub fn dispatcher_factory(deps: DispatcherDeps) -> TaskDispatcherFactory<EncoreAdapter> {
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
        let invalid = TaskDispatcherError::InvalidFormat;
        let adapters = vec![
            EncoreTaskAdapter::RecordTicket(
                JsonTaskAdapter::new(RecordTicketHandler {
                    fans: deps.fans.clone(),
                    follow_up_max_attempts: deps.task_max_attempts,
                })
                .map_err(invalid)?,
            ),
            EncoreTaskAdapter::RecordCancellation(
                JsonTaskAdapter::new(RecordCancellationHandler {
                    fans: deps.fans.clone(),
                    follow_up_max_attempts: deps.task_max_attempts,
                })
                .map_err(invalid)?,
            ),
            EncoreTaskAdapter::SendETicket(JsonTaskAdapter::new(SendETicketHandler { mailer: mail_server() }).map_err(invalid)?),
            EncoreTaskAdapter::IssueRefund(JsonTaskAdapter::new(IssueRefundHandler { mailer: mail_server() }).map_err(invalid)?),
        ];
        TaskDispatcher::try_new(adapters)
    })
}
