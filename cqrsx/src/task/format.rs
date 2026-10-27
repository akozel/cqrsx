//! Versioned wire DTO contract and checked routing key.
use serde::Serialize;
use std::fmt;
use thiserror::Error;
/// Versioned wire DTO. Retain supported versions while queued work exists.
///
/// The type and the version are stored with every queued task and route it to its handler, so they never
/// change for a payload's shape; `#[derive(cqrsx::Task)]` writes them from `#[task(name = "…", version = N)]`.
///
/// ```
/// use cqrsx::{Task, task::{NewTask, format::{TaskFormat, TaskPayload}}};
/// use serde::Serialize;
///
/// #[derive(Serialize, Task)]
/// #[task(name = "send_e_ticket", version = 2)]
/// struct SendETicket {
///     seat: String,
/// }
///
/// assert_eq!((SendETicket::TASK_TYPE, SendETicket::TASK_VERSION), ("send_e_ticket", 2));
/// assert_eq!(TaskFormat::of::<SendETicket>().unwrap().to_string(), "send_e_ticket v2");
/// let task = NewTask::new(SendETicket { seat: "B4".into() }).unwrap();
/// assert_eq!(task.payload()["seat"], "B4");
/// ```
///
/// A version is positive, as [`TaskFormat::new`] requires; the derive refuses another at compile time:
///
/// ```compile_fail
/// # use cqrsx::Task;
/// # use serde::Serialize;
/// #[derive(Serialize, Task)]
/// #[task(name = "send_e_ticket", version = 0)]
/// struct SendETicket {
///     seat: String,
/// }
/// ```
///
/// A field marked `#[label]` is kept in the task's metadata, under its name, instead of its body: what is said of the
/// task besides its business data (a tenant, say), never a copy of the body. [`NewTask::new`](crate::task::NewTask::new)
/// moves it there, and the JSON adapter puts it back before it decodes the payload, so the handler gets the whole
/// value:
///
/// ```
/// use cqrsx::{Task, task::{NewTask, format::TaskPayload}};
/// use serde::Serialize;
/// use serde_json::json;
///
/// #[derive(Serialize, Task)]
/// #[task(name = "issue_refund", version = 1)]
/// struct IssueRefund {
///     amount: u32,
///     #[label]
///     tenant: String,
/// }
///
/// assert_eq!(IssueRefund::LABELS, ["tenant"]);
/// let task = NewTask::new(IssueRefund { amount: 80, tenant: "acme".into() }).unwrap();
/// assert_eq!(task.payload(), &json!({ "amount": 80 }));
/// assert_eq!(task.metadata(), &json!({ "tenant": "acme" }));
/// ```
///
/// A label moves under its field's name, so a serde attribute that renames or drops the key does not compile:
///
/// ```compile_fail
/// # use cqrsx::Task;
/// # use serde::Serialize;
/// #[derive(Serialize, Task)]
/// #[task(name = "issue_refund", version = 1)]
/// struct IssueRefund {
///     amount: u32,
///     #[label]
///     #[serde(rename = "tenant_id")]
///     tenant: String,
/// }
/// ```
pub trait TaskPayload: Serialize {
    const TASK_TYPE: &'static str;
    const TASK_VERSION: i32;
    /// The fields kept in the task's metadata instead of its body, by name (`#[label]`); none by default.
    const LABELS: &'static [&'static str] = &[];
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum TaskFormatError {
    #[error("task type must not be empty")]
    EmptyType,
    #[error("task version must be positive")]
    InvalidVersion,
}

/// One durable type/version pair; also the dispatcher's routing key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TaskFormat {
    task_type: String,
    task_version: i32,
}
impl TaskFormat {
    pub fn new(task_type: impl Into<String>, task_version: i32) -> Result<Self, TaskFormatError> {
        let task_type = task_type.into();
        if task_type.is_empty() {
            return Err(TaskFormatError::EmptyType);
        }
        if task_version <= 0 {
            return Err(TaskFormatError::InvalidVersion);
        }
        Ok(Self { task_type, task_version })
    }
    pub fn of<T: TaskPayload>() -> Result<Self, TaskFormatError> {
        Self::new(T::TASK_TYPE, T::TASK_VERSION)
    }
    pub fn task_type(&self) -> &str {
        &self.task_type
    }
    pub fn task_version(&self) -> i32 {
        self.task_version
    }
}
impl fmt::Display for TaskFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} v{}", self.task_type, self.task_version)
    }
}
