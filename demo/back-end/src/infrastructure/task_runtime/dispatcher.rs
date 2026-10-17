//! `TaskDispatcherFactory`: a fresh dispatcher (fresh handlers, own RNG) for every worker and every
//! worker restart. The factory gets no worker index, so deterministic seeds come from
//! `ENCORE_CHAOS_SEED + counter`. The ordered format list is identical for every instance.
use crate::{
    application::{
        activity::ActivityHub,
        command_service::FanCommands,
        workflows::{
            refund::{issue_refund::IssueRefundHandler, record_cancellation::RecordCancellationHandler},
            ticket_delivery::{record_ticket::RecordTicketHandler, send_e_ticket::SendETicketHandler},
        },
    },
    infrastructure::{
        mail::{
            chaos::Random,
            flaky_mail_server::{FlakyMailServer, MailSettings, MongoInbox},
        },
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

pub struct DispatcherDeps {
    pub fan_commands: Arc<FanCommands>,
    pub inbox: Arc<MongoInbox>,
    pub activity: Arc<ActivityHub>,
    pub mail: MailSettings,
    pub failure_rate: f64,
    pub chaos_seed: Option<u64>,
    pub task_max_attempts: i32,
}

pub fn dispatcher_factory(deps: DispatcherDeps) -> TaskDispatcherFactory<EncoreTaskAdapter<MongoInbox, Random>> {
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
                    fan_commands: deps.fan_commands.clone(),
                    task_max_attempts: deps.task_max_attempts,
                })
                .map_err(invalid)?,
            ),
            EncoreTaskAdapter::RecordCancellation(
                JsonTaskAdapter::new(RecordCancellationHandler {
                    fan_commands: deps.fan_commands.clone(),
                    task_max_attempts: deps.task_max_attempts,
                })
                .map_err(invalid)?,
            ),
            EncoreTaskAdapter::SendETicket(JsonTaskAdapter::new(SendETicketHandler { mail: mail_server() }).map_err(invalid)?),
            EncoreTaskAdapter::IssueRefund(JsonTaskAdapter::new(IssueRefundHandler { mail: mail_server() }).map_err(invalid)?),
        ];
        TaskDispatcher::try_new(adapters)
    })
}
