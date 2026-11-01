//! The modules of `modules.rs` on the engine over PostgreSQL 18.6, with their views in MongoDB 8.2 or in PostgreSQL:
//! a command, the task its reaction published, the command of the task's chain, the view; the pools the engine opens.
use crate::{database::Database, modules::*};
use cqrsx::{command::causation::CORRELATION_ID, journal::feed::Position};
use cqrsx_engine::{Cqrs, build::BuildError, postgres::Postgres, settings::Defaults, views::JournalTotals};
use cqrsx_fixtures::mongo::Mongo;
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

/// The connections' name of these engines.
const SHOP: Defaults = Defaults::new("shop-test", &[]);

/// The URL of the fixture's database (its password is the fixture's).
fn url(database: &Database) -> String {
    let options = database.pool.connect_options();
    format!(
        "postgresql://{}:cqrsx_test@{}:{}/{}?sslmode=disable",
        options.get_username(),
        options.get_host(),
        options.get_port(),
        options.get_database().unwrap_or_default()
    )
}

/// The variables of an engine over the fixture's database, the views in the database `read_db`.
fn variables(url: String, read_db: String) -> impl Fn(&str) -> Option<String> + Send + Sync + 'static {
    move |name| match name {
        "SHOP_DATABASE_URL" => Some(url.clone()),
        "SHOP_READ_DB" => Some(read_db.clone()),
        "SHOP_ATTEMPTS" => Some("4".to_owned()),
        _ => None,
    }
}

/// Books A1 for fox, runs the step and the e-mail, lets the halls catch up: what both stores of the views show alike.
async fn book_and_follow_the_chain(cqrs: &Cqrs, outbox: &Outbox) {
    let booking_id = Uuid::now_v7();
    let booked = cqrs
        .source("ui")
        .execute::<Show>(
            &"s-1".into(),
            BookSeat {
                seat: "A1".into(),
                fan: "fox".into(),
                booking_id,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        booked.tasks[0].max_attempts().get(),
        4,
        "the policy's budget from SHOP_ATTEMPTS"
    );
    assert_eq!(
        cqrs.run_tasks().await,
        2,
        "the step on the queue in PostgreSQL, then its e-mail"
    );
    assert_eq!(*outbox.0.lock().unwrap(), ["fox:A1"]);
    let recorded = cqrs.history::<Wallet>(&"fox".into()).await.unwrap();
    assert_eq!(recorded[0].metadata[CORRELATION_ID], booking_id.to_string(), "one chain");
    assert_eq!(cqrs.run_tasks().await, 0, "nothing more is due");
    cqrs.catch_up().await;
    let hall = cqrs.view::<Hall>().get(&"s-1").await.unwrap().unwrap();
    assert_eq!(hall.view.seats, BTreeMap::from([("A1".to_owned(), "fox".to_owned())]));
    let views = cqrs.views();
    assert_eq!(
        (
            views.lag("halls").await.unwrap(),
            views.verify("halls").await.unwrap().differences
        ),
        (0, vec![])
    );
    assert_eq!(views.count_after("show", Position::START, 10).await.unwrap(), 1);
    assert_eq!(
        views.totals().await.unwrap(),
        JournalTotals {
            events: 1,
            aggregates: 1
        },
        "the journal of the views' aggregate type"
    );
    let rows = views.status().await.unwrap();
    let owners: Vec<(&str, Option<&str>)> = rows
        .iter()
        .map(|row| (row.projection.as_str(), row.owner.as_deref()))
        .collect();
    assert_eq!(owners, [("halls", Some("node-a"))]);
}

fn engine(url: String, read_db: String, outbox: &Arc<Outbox>) -> cqrsx_engine::build::Builder {
    let mail: Arc<Mailer> = outbox.clone();
    Cqrs::from_lookup("SHOP", move |name| match name {
        "SHOP_NODE_NAME" => Some("node-a".to_owned()),
        other => variables(url.clone(), read_db.clone())(other),
    })
    .defaults(SHOP)
    .event_store(Postgres)
    .module(box_office)
    .module(wallets)
    .provide(mail)
}

/// G-09, G-11, G-27: the journal, the outbox and the queue in PostgreSQL, the halls and their checkpoint in MongoDB.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_modules_run_on_postgres_with_their_views_in_mongodb() {
    let (database, mongo) = (Database::new().await, Mongo::start().await);
    let outbox = Arc::new(Outbox::default());
    let read_db = mongo.database.name().to_owned();
    let cqrs = engine(url(&database), read_db, &outbox)
        .projections(cqrsx_engine::mongodb::Mongo)
        .mongodb_client(mongo.database.client().clone())
        .build()
        .await
        .unwrap();
    book_and_follow_the_chain(&cqrs, &outbox).await;
    let stored = mongo
        .database
        .collection::<mongodb::bson::Document>("halls")
        .count_documents(mongodb::bson::doc! {})
        .await
        .unwrap();
    assert_eq!(stored, 1, "the hall is a document of the read database");
    drop(cqrs);
    mongo.close().await;
    database.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_modules_run_on_postgres_with_their_views_in_postgres() {
    let database = Database::new().await;
    sqlx::raw_sql(
        "CREATE TABLE halls (view_id TEXT COLLATE \"C\" PRIMARY KEY, revision BIGINT CHECK (revision >= 0), \
         last_sequence BIGINT NOT NULL DEFAULT 0, payload JSON NOT NULL)",
    )
    .execute(&database.pool)
    .await
    .unwrap();
    let outbox = Arc::new(Outbox::default());
    let cqrs = engine(url(&database), "unused".into(), &outbox)
        .projections(Postgres)
        .build()
        .await
        .unwrap();
    book_and_follow_the_chain(&cqrs, &outbox).await;
    let stored: i64 = sqlx::query_scalar("SELECT count(*) FROM projection_checkpoints WHERE projection = 'halls'")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(stored, 1, "the checkpoint is a row of the database");
    drop(cqrs);
    database.close().await;
}

/// G-29: the engine opens its three pools at once, one connection each, under the connections' name; a URL or a URI it
/// cannot read names its variable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_engine_opens_its_pools_as_the_plan_says() {
    let database = Database::new().await;
    let outbox = Arc::new(Outbox::default());
    let cqrs = engine(url(&database), "unused".into(), &outbox).build().await.unwrap();
    let opened: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE application_name = 'shop-test'")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(
        opened, 3,
        "the main, the projection and the history pool, one connection each"
    );
    drop(cqrs);
    let lazy = Cqrs::from_lookup("SHOP", {
        let url = url(&database);
        move |name| match name {
            "SHOP_DATABASE_URL" => Some(url.clone()),
            "SHOP_MONGODB_URI" => Some("mongodb://127.0.0.1:1".to_owned()),
            _ => None,
        }
    })
    .event_store(Postgres)
    .projections(cqrsx_engine::mongodb::Mongo)
    .module(box_office)
    .build()
    .await;
    assert!(lazy.is_ok(), "the client of MongoDB connects when it is first used");
    for (name, value, refused) in [
        (
            "SHOP_DATABASE_URL",
            "postgresql://shop@localhost:port/shop",
            "SHOP_DATABASE_URL: ",
        ),
        ("SHOP_MONGODB_URI", "nonsense://", "SHOP_MONGODB_URI: "),
    ] {
        let url = url(&database);
        let error = Cqrs::from_lookup("SHOP", move |asked| match asked {
            asked if asked == name => Some(value.to_owned()),
            "SHOP_DATABASE_URL" => Some(url.clone()),
            _ => None,
        })
        .event_store(Postgres)
        .projections(cqrsx_engine::mongodb::Mongo)
        .build()
        .await
        .err()
        .unwrap();
        assert!(error.to_string().starts_with(refused), "{error}");
        assert!(matches!(error, BuildError::DatabaseUrl { .. } | BuildError::MongoUri { .. }));
    }
    database.close().await;
}

/// G-30 over PostgreSQL: the task runtime of `serve` runs on the queue of the main pool, and on the signal the server,
/// the runtime and the processors stop before the pools close.
#[cfg(feature = "http")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_runs_the_tasks_on_postgres_until_the_signal() {
    let database = Database::new().await;
    let outbox = Arc::new(Outbox::default());
    let cqrs = engine(url(&database), "unused".into(), &outbox).build().await.unwrap();
    cqrs.source("ui")
        .execute::<Show>(
            &"s-1".into(),
            BookSeat {
                seat: "A1".into(),
                fan: "fox".into(),
                booking_id: Uuid::now_v7(),
            },
        )
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let served = tokio::spawn(cqrs.serve(listener, async move {
        let _ = stopped.await;
        Ok(())
    }));
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while outbox.0.lock().unwrap().is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the runtime of the process ran the step and its e-mail");
    stop.send(()).unwrap();
    served.await.unwrap().unwrap();
    let completed: i64 = sqlx::query_scalar("SELECT count(*) FROM transactional_outbox WHERE status = 'completed'")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(completed, 2, "the step and the e-mail, settled in PostgreSQL");
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE application_name = 'shop-test'")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(left, 0, "the pools of the engine are closed");
    database.close().await;
}
