#![cfg(feature = "postgres")]
use cqrsx::cqrs::store::persisted::PersistedEventStore;
use cqrsx::task::{attempt::TaskAttempt, queue::lease::TaskLease};
use std::collections::HashMap;

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use chrono::{DateTime, Utc};
use cqrsx::cqrs::aggregate::Aggregate;
use cqrsx::cqrs::event::DomainEvent;
use cqrsx::cqrs::event_sink::EventSink;
use cqrsx::cqrs::store::PersistenceError;
use cqrsx::cqrs::store::repository::PersistedEventRepository;
use cqrsx::cqrs::store::serialized_event::SerializedEvent;
#[cfg(feature = "worker")]
use cqrsx::task::runtime::{dispatcher_factory::TaskDispatcherFactory, worker_pool_config::TaskWorkerPoolConfig};
use cqrsx::{
    postgres::{
        EVENT_STORE_SCHEMA, REQUEUE_FAILED_OUTBOX_TASK, TRANSACTIONAL_OUTBOX_SCHEMA, outbox::PostgresEventRepositoryWithOutbox,
        task_queue::PostgresTaskQueue,
    },
    task::{
        NewTask, TaskSchedule,
        adapter::TaskAdapter,
        dispatcher::TaskDispatcher,
        executor::{TaskExecutor, default_executor::DefaultTaskExecutor},
        format::{TaskFormat, TaskPayload},
        handler::TaskHandlerError,
        queue::{
            TaskProcessingError, TaskQueue, lease::LeasedTask, settlement::SettlementRequest, storage_config::TaskStorageConfig,
        },
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
#[cfg(feature = "worker")]
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use cqrsx::cqrs::{
    CqrsFramework,
    event::EventEnvelope,
    store::{
        EventStore,
        snapshot::{SerializedSnapshot, SnapshotUpdate},
    },
};
use cqrsx::postgres::event_repository::PostgresEventRepository;
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
#[path = "postgres/pool.rs"]
mod pool;
#[path = "postgres/projection.rs"]
mod projection;
#[cfg(feature = "worker")]
#[path = "postgres/runtime.rs"]
mod runtime;
#[path = "postgres/schema.rs"]
mod schema;
#[path = "postgres/support.rs"]
mod support;
#[path = "postgres/task_queue.rs"]
mod task_queue;
