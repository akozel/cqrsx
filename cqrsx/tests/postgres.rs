#![cfg(feature = "postgres")]
// Test code: a panic is the failed check (rule 8 of .claude/rules/rust-style.md); clippy.toml allows it only inside
// `#[test]` functions, not in the helpers of this crate.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::indexing_slicing
)]
use cqrsx::journal::persisted::PersistedEventStore;
use cqrsx::task::attempt::TaskAttempt;
use cqrsx::worker::queue::lease::TaskLease;
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
use cqrsx::domain::event::DomainEvent;
use cqrsx::journal::PersistenceError;
use cqrsx::journal::repository::PersistedEventRepository;
use cqrsx::journal::serialized_event::{EventFormat, SerializedEvent};
use cqrsx::worker::runtime::worker_pool_config::TaskWorkerPoolConfig;
use cqrsx::{
    postgres::{
        EVENT_STORE_SCHEMA, PROJECTION_CHECKPOINTS_SCHEMA, REQUEUE_FAILED_OUTBOX_TASK, TRANSACTIONAL_OUTBOX_SCHEMA,
        outbox::PostgresEventRepositoryWithOutbox,
        pool::{ConnectionPingPolicy, DEFAULT_IDLE_PING_AFTER, StatementTimeout, connect_pool, connect_pool_with},
        task_queue::PostgresTaskQueue,
    },
    task::{
        NewTask, TaskSchedule,
        format::{TaskFormat, TaskPayload},
    },
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
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use cqrsx::postgres::event_repository::PostgresEventRepository;
use cqrsx::{
    command::framework::CqrsFramework,
    domain::event::EventEnvelope,
    journal::{
        EventStore,
        snapshot::{SerializedSnapshot, SnapshotUpdate},
    },
};
use support::ledger::*;
use support::repository::{finish_writers, wait_for_writers};
use support::*;
use tokio::{sync::Barrier, task::JoinSet};
#[path = "postgres/command.rs"]
mod command;
#[path = "postgres/compatibility.rs"]
mod compatibility;
#[path = "postgres/event_store.rs"]
mod event_store;
#[path = "postgres/outbox.rs"]
mod outbox;
#[path = "postgres/planning.rs"]
mod planning;
#[path = "postgres/pool.rs"]
mod pool;
#[path = "support/port_lease.rs"]
mod port_lease;
#[path = "support/postgres.rs"]
mod postgres_container;
#[path = "postgres/projection.rs"]
mod projection;
#[path = "postgres/runtime.rs"]
mod runtime;
#[path = "postgres/schema.rs"]
mod schema;
#[path = "postgres/support.rs"]
mod support;
#[path = "postgres/task_queue.rs"]
mod task_queue;
