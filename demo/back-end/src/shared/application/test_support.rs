//! Test tools the flows of several modules share: what a planner (the reactions' planner of an aggregate) plans for a
//! decision built by hand, and a mail service that records the letters.
use crate::shared::application::{
    activity::ActivityHub,
    mail::{Mail, MailService},
};
use async_trait::async_trait;
use cqrsx::{
    command::planner::{Decision, TaskPlanError, TaskPlanner},
    domain::aggregate::Aggregate,
    task::{NewTask, context::TaskContext, result::TaskResult},
};
use std::{collections::HashMap, sync::Mutex};

/// A mail service that records every letter, with the id of the task that sent it, and answers `answer`.
pub struct RecordingMailer {
    pub sent: Mutex<Vec<(uuid::Uuid, Mail)>>,
    pub answer: TaskResult,
}

impl RecordingMailer {
    /// A mail service that delivers everything.
    pub fn delivering() -> Self {
        Self {
            sent: Mutex::new(Vec::new()),
            answer: TaskResult::Done,
        }
    }

    /// The letters sent so far.
    pub fn sent(&self) -> Vec<(uuid::Uuid, Mail)> {
        self.sent.lock().unwrap().clone()
    }

    /// The kinds of the letters sent so far, in order.
    pub fn kinds(&self) -> Vec<&'static str> {
        self.sent().iter().map(|(_, mail)| mail.kind).collect()
    }

    /// The letters a provider keeps: one per idempotency key (`Mail::id`), in the order of the first send.
    pub fn letters(&self) -> Vec<Mail> {
        let mut letters: Vec<Mail> = Vec::new();
        for (_, mail) in self.sent() {
            if !letters.iter().any(|letter| letter.id == mail.id) {
                letters.push(mail);
            }
        }
        letters
    }
}

#[async_trait]
impl MailService for RecordingMailer {
    async fn send(&self, ctx: &TaskContext, _: &ActivityHub, mail: Mail) -> TaskResult {
        self.sent.lock().unwrap().push((ctx.task_id(), mail));
        self.answer.clone()
    }
}

/// What `planner` plans for `events` decided on `aggregate_id`, the decision built by hand (the reactions
/// read neither the state nor the command).
pub fn plan<A: Aggregate, P: TaskPlanner<A>>(
    planner: &P,
    aggregate_id: &A::Id,
    command: &A::Command,
    events: &[A::Event],
) -> Result<Vec<NewTask>, TaskPlanError> {
    let state = A::default();
    planner.plan(&Decision {
        aggregate_id,
        before: &state,
        after: &state,
        command,
        events,
        metadata: &HashMap::new(),
        causation: None,
    })
}

/// The formats of the planned tasks, in plan order.
pub fn formats(tasks: &[cqrsx::task::NewTask]) -> Vec<String> {
    tasks.iter().map(|task| task.format().to_string()).collect()
}
