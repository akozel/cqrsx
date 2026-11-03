//! The PostgreSQL stores against a real server: the event repository, the outbox, the task queue, the pools and, with
//! `projections-postgres`, the checkpoints and the view repository. Every test starts its own container.
#![cfg(feature = "event-store-postgres")]
// Test code: a panic is the failed check (rule 8 of .claude/rules/rust-style.md); clippy.toml allows it only inside
// `#[test]` functions, not in the helpers of this crate.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::indexing_slicing
)]
use cqrsx::journal::store::EventSourcedStore;
use cqrsx_fixtures::postgres::Postgres;
use std::collections::HashMap;

use std::{
    collections::HashSet,
    num::NonZeroU16,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use chrono::{DateTime, Utc};
use cqrsx::domain::aggregate::Aggregate;
use cqrsx::domain::command::Decide;
use cqrsx::domain::event::Events;
use cqrsx::journal::PersistenceError;
use cqrsx::journal::repository::EventRepository;
use cqrsx::journal::serialized_event::{EventFormat, SerializedEvent};
use cqrsx::worker::runtime::worker_pool_config::TaskWorkerPoolConfig;
use cqrsx::{
    task::{NewTask, Task, TaskSchedule, format::TaskFormat},
    worker::{
        TaskResult,
        adapter::TaskAdapter,
        context::TaskContext,
        dispatcher::TaskDispatcher,
        executor::{TaskExecutor, default_executor::DefaultTaskExecutor},
        queue::{
            TaskProcessingError, TaskQueue, lease::LeasedTask, settlement::SettlementRequest, storage_config::TaskStorageConfig,
        },
    },
};
#[cfg(feature = "projections-postgres")]
use cqrsx_engine::postgres::PROJECTION_CHECKPOINTS_SCHEMA;
use cqrsx_engine::postgres::{
    EVENT_STORE_SCHEMA, REQUEUE_FAILED_OUTBOX_TASK, TRANSACTIONAL_OUTBOX_SCHEMA,
    outbox::PostgresEventRepositoryWithOutbox,
    pool::{ConnectionPingPolicy, DEFAULT_IDLE_PING_AFTER, StatementTimeout, connect_pool, connect_pool_with},
    task_queue::PostgresTaskQueue,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use cqrsx::{
    domain::event::EventEnvelope,
    journal::{AggregateStore, snapshot::SerializedSnapshot},
};
use cqrsx_engine::postgres::event_repository::PostgresEventRepository;
use support::ledger::*;
use support::repository::{finish_writers, wait_for_writers};
use support::*;
use tokio::{sync::Barrier, task::JoinSet};
#[path = "postgres/command.rs"]
mod command;
#[path = "postgres/compatibility.rs"]
mod compatibility;
#[path = "postgres/event_repository.rs"]
mod event_repository;
#[path = "postgres/outbox.rs"]
mod outbox;
#[path = "postgres/planning.rs"]
mod planning;
#[path = "postgres/pool.rs"]
mod pool;
#[cfg(feature = "projections-postgres")]
#[path = "postgres/projection.rs"]
mod projection;
#[path = "postgres/runtime.rs"]
mod runtime;
#[cfg(feature = "projections-postgres")]
#[path = "postgres/schema.rs"]
mod schema;
#[path = "postgres/support.rs"]
mod support;
#[path = "postgres/task_queue.rs"]
mod task_queue;
