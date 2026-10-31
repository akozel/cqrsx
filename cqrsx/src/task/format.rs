//! Versioned wire DTO contract and checked routing key.
use crate::task::Task;
use std::fmt;
use thiserror::Error;
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum TaskFormatError {
    #[error("task type must not be empty")]
    EmptyType,
    #[error("task version must be positive")]
    InvalidVersion,
}

/// One durable type/version pair; also the dispatcher's routing key. The queue stores it with every task, an
/// adapter names the formats it handles, a worker claims the tasks of one format at a time. Built from a payload type
/// ([`of`](Self::of)) or read back from storage ([`try_new`](Self::try_new)); displayed as `<type> v<version>`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TaskFormat {
    task_type: String,
    task_version: i32,
}
impl TaskFormat {
    /// A format read at run time (from a row of the queue, say): the type must not be empty, the version must be
    /// positive.
    pub fn try_new(task_type: impl Into<String>, task_version: i32) -> Result<Self, TaskFormatError> {
        let task_type = task_type.into();
        if task_type.is_empty() {
            return Err(TaskFormatError::EmptyType);
        }
        if task_version <= 0 {
            return Err(TaskFormatError::InvalidVersion);
        }
        Ok(Self { task_type, task_version })
    }

    /// The format of `T`. Its constants obey the rules of [`try_new`](Self::try_new), checked when the compiler
    /// instantiates this function for `T`: a payload type with an empty `TASK_TYPE` or a `TASK_VERSION` below 1 fails
    /// `cargo build` (a check made after monomorphization, which `cargo check` may not report), whether its
    /// `Task` is derived or written by hand.
    pub fn of<T: Task>() -> Self {
        const {
            assert!(!T::TASK_TYPE.is_empty(), "the task type must not be empty");
            assert!(T::TASK_VERSION > 0, "the task version must be positive");
        }
        Self {
            task_type: T::TASK_TYPE.to_owned(),
            task_version: T::TASK_VERSION,
        }
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
