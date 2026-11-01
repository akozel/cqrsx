//! Composition root: what the sidecar serves, built from a [`Process`]:
//!
//! - the X-ray: one poller over PostgreSQL and MongoDB broadcasting to the SSE clients, and the
//!   `/inspect` routes (`http`);
//! - DEMO ONLY: the demo lifecycle: seed at start-up, wipe and reset (`demo`);
//! - DEMO ONLY: the external mail service the back-end's e-mail tasks call, `POST /mail` (`mail`).
//!
//! [`wire`] starts the poller and the seeder as background tasks, so it runs inside the runtime.
use crate::{
    demo::DemoAdmin,
    http,
    mail::{self, MailService, MailSettings, inbox::MongoInbox},
    mongo::{MongoInspector, WHITELIST},
    postgres::{self, PgInspector},
    process::Process,
    state::{SourceError, State},
};
use axum::Router;
use chrono::{SecondsFormat, Utc};
use sqlx::postgres::PgConnectOptions;
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinHandle};
use tokio_util::sync::CancellationToken;

/// What the process serves, and the background work it stops.
pub struct App {
    pub router: Router,
    /// The X-ray poller; ends with the stop signal.
    pub poller: JoinHandle<()>,
    /// DEMO ONLY: the start-up seed when enabled; aborted when the process stops.
    pub seeder: Option<JoinHandle<()>>,
}

#[derive(Debug, thiserror::Error)]
pub enum WiringError {
    #[error("SIDECAR_ADMIN_DATABASE_URL: {0}")]
    AdminDatabaseUrl(sqlx::Error),
}

pub fn wire(process: &Process) -> Result<App, WiringError> {
    let config = &process.config;

    // DEMO ONLY: the lifecycle of the demo data (seed, wipe, reset); see demo.rs.
    let admin_options = config
        .admin_database_url
        .parse::<PgConnectOptions>()
        .map_err(WiringError::AdminDatabaseUrl)?
        .application_name("encore-sidecar-demo");
    let demo = DemoAdmin::new(
        admin_options,
        process.mongo.client(),
        config.read_db.clone(),
        config.mail_db.clone(),
        config.backend_url.clone(),
    );
    let seeder = config.seed.then(|| {
        let demo = demo.clone();
        tokio::spawn(async move { demo.seed_with_retries(90, Duration::from_secs(2)).await })
    });

    // DEMO ONLY: the external mail service the back-end's e-mail tasks call; see mail.rs.
    let mail = Arc::new(MailService::new(
        MongoInbox::new(process.mongo.client().database(&config.mail_db).collection("inbox")),
        MailSettings {
            failure_rate: config.mail_failure_rate,
            latency: config.mail_latency,
            seed: config.mail_chaos_seed,
        },
    ));

    // The X-ray: one shared poller for all clients, broadcasting only when the content hash changes.
    let (sender, receiver) = watch::channel(Arc::new(State::default()));
    let poller = spawn_poller(
        process.postgres.clone(),
        process.mongo.clone(),
        config.poll_interval,
        process.shutdown.clone(),
        sender,
    );

    let router = http::router(http::AppState {
        postgres: process.postgres.clone(),
        mongo: process.mongo.clone(),
        state: receiver,
        shutdown: process.shutdown.clone(),
        demo,
        reset_enabled: config.reset_enabled,
    })
    .merge(mail::router(mail));
    Ok(App { router, poller, seeder })
}

fn spawn_poller(
    postgres: PgInspector,
    mongo: MongoInspector,
    interval: Duration,
    shutdown: CancellationToken,
    sender: watch::Sender<Arc<State>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let limits = http::default_limits();
        let mut last_hash = String::new();
        loop {
            let state = poll(&postgres, &mongo, &limits).await;
            let hash = state.content_hash();
            if hash != last_hash {
                last_hash = hash;
                let _ = sender.send(Arc::new(state));
            }
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = shutdown.cancelled() => break,
            }
        }
    })
}

/// One poll: everything from PostgreSQL in one snapshot, then the whitelisted collections.
/// An unavailable database becomes an entry in `errors`; the process never dies because of it.
async fn poll(postgres: &PgInspector, mongo: &MongoInspector, limits: &postgres::Limits) -> State {
    let mut state = State {
        polled_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        ..State::default()
    };
    match postgres.read_consistent(limits).await {
        Ok(pg) => state = state.with_postgres(pg),
        Err(error) => state.errors.push(SourceError {
            source: "postgres",
            message: error.to_string(),
        }),
    }
    let mut documents = serde_json::Map::new();
    for (database, collection) in WHITELIST {
        match mongo.collection(database, collection, 100).await {
            Ok(view) => {
                documents.insert(collection.to_owned(), serde_json::json!(view.total));
                state.mongo.insert(collection.to_owned(), view);
            }
            Err(error) => {
                state.errors.push(SourceError {
                    source: "mongodb",
                    message: error.to_string(),
                });
                break;
            }
        }
    }
    if let serde_json::Value::Object(stats) = &mut state.stats {
        stats.insert("documents".into(), serde_json::Value::Object(documents));
    } else {
        state.stats = serde_json::json!({ "documents": documents });
    }
    state
}
