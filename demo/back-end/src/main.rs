//! Encore back-end: composition root (see PLAN.md §6.3).
#![allow(clippy::result_large_err)]
use app::{
    activity::ActivityHub,
    command_service::{ConcertCommands, Executors, FanCommands, HandlerFactory},
    read_side::{CONCERT_HALLS, FAN_WALLETS, ReadSide},
    rebuild::RebuildService,
    time_travel::TimeTravel,
    verify::VerifyService,
};
use config::Config;
use cqrsx::cqrs::{event_handler::EventHandler, projection::ViewProjection};
use cqrsx::{
    cqrs::command::retry::AggregateConflictRetryPolicy,
    host::{lifecycle, signal::shutdown_signal},
    logging::LoggingConfig,
    postgres::{
        command::PostgresCommandConfig, pool::connect_pool, task_queue::PostgresTaskQueueConfig,
        task_runtime::prepare_task_runtime,
    },
};
use domain::concert::ConcertServices;
use domain::{concert::Concert, fan::Fan};
use encore_backend::{app, config, domain, http, outbox, projections};
use outbox::{
    dispatcher::{DispatcherDeps, dispatcher_factory},
    flaky_mail::{MailSettings, MongoInbox},
};
use projections::notifying::NotifyingHandler;
use sqlx::postgres::PgConnectOptions;
use std::{future::IntoFuture, sync::Arc};
use tokio_util::sync::CancellationToken;

fn fail(message: impl std::fmt::Display) -> ! {
    tracing::error!("{message}");
    eprintln!("encore-backend: {message}");
    std::process::exit(2)
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    if let Err(error) = LoggingConfig::from_env()
        .map_err(|e| e.to_string())
        .and_then(|c| c.try_init().map_err(|e| e.to_string()))
    {
        eprintln!("encore-backend: logging setup failed: {error}");
        std::process::exit(2);
    }
    let config = Arc::new(Config::from_env().unwrap_or_else(|e| fail(format!("invalid configuration: {e}"))));

    // PostgreSQL: event store + outbox. Readiness is the job of scripts/wait-db.sh, not ours.
    let options = config
        .database_url
        .parse::<PgConnectOptions>()
        .unwrap_or_else(|e| fail(format!("ENCORE_DATABASE_URL: {e}")))
        .application_name("encore-backend");
    let pool = connect_pool(options)
        .await
        .unwrap_or_else(|e| fail(format!("cannot connect to PostgreSQL: {e}")));

    // MongoDB: read models (`encore_read`) and the simulated external mail service (`mail_service`).
    // Short server-selection timeout: when MongoDB is down a command must still answer quickly
    // (200 + `projection_failed` warning), not hang for the driver's default 30 s.
    let mut mongo_options = mongodb::options::ClientOptions::parse(&config.mongodb_uri)
        .await
        .unwrap_or_else(|e| fail(format!("ENCORE_MONGODB_URI: {e}")));
    mongo_options.server_selection_timeout = Some(std::time::Duration::from_secs(3));
    mongo_options.connect_timeout = Some(std::time::Duration::from_secs(3));
    mongo_options.app_name = Some("encore-backend".into());
    let mongo = mongodb::Client::with_options(mongo_options).unwrap_or_else(|e| fail(format!("MongoDB client: {e}")));
    let mail_db = mongo.database(&config.mail_db);
    let inbox = Arc::new(MongoInbox::new(mail_db.collection("inbox")));
    let read_db = mongo.database(&config.read_db);
    let read = Arc::new(ReadSide::new(&read_db, &mail_db));

    let activity = Arc::new(ActivityHub::new(1024, 100));
    let rebuild = Arc::new(RebuildService::new(
        pool.clone(),
        read.clone(),
        activity.clone(),
        config.replay_slow_delay,
    ));
    let time_travel = Arc::new(TimeTravel::new(pool.clone()));
    let verify = Arc::new(VerifyService::new(pool.clone(), read_db, read.clone()));
    let command_config = PostgresCommandConfig::new(config.snapshot_every, AggregateConflictRetryPolicy::default())
        .unwrap_or_else(|e| fail(format!("snapshot configuration: {e}")));
    let executors = Executors {
        pool: pool.clone(),
        snapshot_every: command_config.snapshot_interval(),
        retry: command_config.conflict_retry(),
    };
    // Event-handler factories: called for every framework (one per command). Handlers are cheap —
    // they hold an Arc to the view repository — and are wrapped so the activity log sees
    // `projection.updated` / `projection.failed`. Registration order = index in
    // `CommittedWithEventHandlerFailure`, hence the parallel name lists.
    let concert_handlers: HandlerFactory<Concert> = {
        let halls = read.halls.clone();
        let activity = activity.clone();
        Arc::new(move || {
            vec![Box::new(NotifyingHandler::new(
                ViewProjection::new(halls.clone()),
                CONCERT_HALLS,
                activity.clone(),
            )) as Box<dyn EventHandler<Concert>>]
        })
    };
    let fan_handlers: HandlerFactory<Fan> = {
        let wallets = read.wallets.clone();
        let activity = activity.clone();
        Arc::new(move || {
            vec![Box::new(NotifyingHandler::new(
                ViewProjection::new(wallets.clone()),
                FAN_WALLETS,
                activity.clone(),
            )) as Box<dyn EventHandler<Fan>>]
        })
    };
    let concert_commands = Arc::new(ConcertCommands::new(
        &executors,
        concert_handlers,
        vec![CONCERT_HALLS],
        ConcertServices {
            ticket_limit: config.ticket_limit,
        },
        activity.clone(),
        rebuild.clone(),
        config.chain_max_attempts,
    ));
    let fan_commands = Arc::new(FanCommands::new(
        &executors,
        fan_handlers,
        vec![FAN_WALLETS],
        activity.clone(),
        rebuild.clone(),
    ));

    if config.seed {
        app::seed::run(&concert_commands).await;
    }

    let shutdown = CancellationToken::new();
    let listener = tokio::net::TcpListener::bind(config.http_addr)
        .await
        .unwrap_or_else(|e| fail(format!("cannot bind {}: {e}", config.http_addr)));
    tracing::info!("encore-backend listening on http://{}", config.http_addr);

    // Outbox task runtime: prepared here (no background work yet), started by `lifecycle::run`.
    let factory = dispatcher_factory(DispatcherDeps {
        fan_commands: fan_commands.clone(),
        inbox,
        activity: activity.clone(),
        mail: MailSettings {
            latency: config.task_latency,
            retry_base: config.task_retry_base,
            retry_max: config.task_retry_max,
        },
        failure_rate: config.task_failure_rate,
        chaos_seed: config.chaos_seed,
        task_max_attempts: config.task_max_attempts,
    });
    let queue_config = PostgresTaskQueueConfig::from_lookup(|name| std::env::var(name).ok())
        .unwrap_or_else(|e| fail(format!("task queue configuration: {e}")));
    let runtime = prepare_task_runtime(pool.clone(), factory, queue_config, config.task_runtime.clone())
        .unwrap_or_else(|e| fail(format!("task runtime setup: {e}")));
    let task_runtime = runtime.run(shutdown.clone());

    let state = http::AppState {
        config: config.clone(),
        concert_commands,
        fan_commands,
        activity,
        rebuild,
        read,
        time_travel,
        verify,
        shutdown: shutdown.clone(),
    };
    // axum 0.8: `with_graceful_shutdown` returns an `IntoFuture`, not a `Future`.
    let inbound = axum::serve(listener, http::router(state))
        .with_graceful_shutdown(shutdown.clone().cancelled_owned())
        .into_future();
    if let Err(error) = lifecycle::run(inbound, task_runtime, shutdown.clone(), shutdown_signal()).await {
        error.log("encore-backend stopped with an error");
    }
    pool.close().await;
    tracing::info!("encore-backend stopped");
}
