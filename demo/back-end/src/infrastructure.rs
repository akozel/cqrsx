//! Technical adapters: MongoDB (read models, the mail inbox), the simulated mail service and the
//! wiring of the cqrsx task runtime.
pub mod mail;
pub mod mongo;
pub mod task_runtime;
