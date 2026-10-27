//! Public testing tools of the task side: a task queue in memory over the outbox of a journal in memory
//! ([`mem_task_queue::MemTaskQueue`]), next to the journal, the checkpoints and the views of `cqrs::testing`.
pub mod mem_task_queue;
