use super::*;
use cqrsx_postgres::pool::DEFAULT_MAX_CONNECTIONS;

#[tokio::test]
async fn connection_helper_preserves_ten_connection_limit_and_returns_authentication_error() {
    let db = Database::new().await;
    let options = db.pool.connect_options().as_ref().clone();
    let pool = connect_pool(options.clone(), ConnectionPingPolicy::Default, StatementTimeout::Default)
        .await
        .unwrap();
    assert_eq!(pool.options().get_max_connections(), 10);
    assert!(pool.options().get_test_before_acquire());
    assert_eq!(DEFAULT_MAX_CONNECTIONS.get(), 10);
    assert_eq!(sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(&pool).await.unwrap(), 1);
    pool.close().await;

    // The general variant takes any SQLx pool options, here a smaller ceiling and the idle policy.
    let sized = connect_pool_with(
        options.clone(),
        PgPoolOptions::new().max_connections(3),
        ConnectionPingPolicy::Idle(DEFAULT_IDLE_PING_AFTER),
        StatementTimeout::Default,
    )
    .await
    .unwrap();
    assert_eq!(sized.options().get_max_connections(), 3);
    assert!(!sized.options().get_test_before_acquire());
    assert_eq!(sqlx::query_scalar::<_, i32>("SELECT 2").fetch_one(&sized).await.unwrap(), 2);
    sized.close().await;

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        connect_pool(
            options.password("private_incorrect_password"),
            ConnectionPingPolicy::Default,
            StatementTimeout::Default,
        ),
    )
    .await
    .unwrap();
    let error = result.unwrap_err();
    assert!(matches!(error, sqlx::Error::Database(_)));
    assert_eq!(error.as_database_error().unwrap().code().as_deref(), Some("28P01"));
    db.close().await;
}

async fn backend_pid(pool: &PgPool) -> Result<i32, sqlx::Error> {
    sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(pool).await
}

/// SQLx returns a connection to the pool in a spawned task (after its own ping); wait for it.
async fn wait_idle(pool: &PgPool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while pool.num_idle() < 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("bounded return to the pool");
}

/// Kills a backend and waits until PostgreSQL no longer lists it, so the client's next use of that
/// connection meets a closed session rather than a race with the termination.
async fn terminate_and_wait(admin: &PgPool, pid: i32) {
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
            .bind(pid)
            .fetch_one(admin)
            .await
            .unwrap()
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        while sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pg_stat_activity WHERE pid = $1")
            .bind(pid)
            .fetch_one(admin)
            .await
            .unwrap()
            > 0
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("bounded backend termination");
}

#[tokio::test]
async fn connection_idle_longer_than_the_threshold_is_pinged_and_replaced_when_its_backend_died() {
    let db = Database::new().await;
    let options = db.pool.connect_options().as_ref().clone();
    let threshold = DEFAULT_IDLE_PING_AFTER;
    let pool = connect_pool_with(
        options,
        PgPoolOptions::new().max_connections(1),
        ConnectionPingPolicy::Idle(threshold),
        StatementTimeout::Default,
    )
    .await
    .unwrap();
    assert!(!pool.options().get_test_before_acquire());
    let first = backend_pid(&pool).await.unwrap();
    wait_idle(&pool).await;
    terminate_and_wait(&db.pool, first).await;
    tokio::time::sleep(threshold + Duration::from_millis(100)).await;
    // The hand-out pings, the ping fails, SQLx drops the connection and opens a new one: the caller
    // never sees the dead one.
    let second = tokio::time::timeout(Duration::from_secs(5), backend_pid(&pool))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(first, second);
    pool.close().await;
    db.close().await;
}

#[tokio::test]
async fn recently_used_connection_is_not_pinged_so_its_dead_backend_fails_the_first_query_only() {
    let db = Database::new().await;
    let options = db.pool.connect_options().as_ref().clone();
    // A threshold far above the time the test needs, so the hand-out below certainly skips the ping.
    let threshold = Duration::from_secs(60);
    let pool = connect_pool_with(
        options,
        PgPoolOptions::new().max_connections(1),
        ConnectionPingPolicy::Idle(threshold),
        StatementTimeout::Default,
    )
    .await
    .unwrap();
    let first = backend_pid(&pool).await.unwrap();
    wait_idle(&pool).await;
    let released = std::time::Instant::now();
    terminate_and_wait(&db.pool, first).await;
    // Handed out without a ping, so the dead session surfaces on the query: the documented risk.
    let error = tokio::time::timeout(Duration::from_secs(5), backend_pid(&pool))
        .await
        .unwrap()
        .unwrap_err();
    assert!(
        released.elapsed() < threshold,
        "the premise: the connection was idle for less than the threshold"
    );
    assert!(
        matches!(&error, sqlx::Error::Io(_)) || error.as_database_error().and_then(|e| e.code()).as_deref() == Some("57P01"),
        "a dead connection, not another failure: {error:?}"
    );
    // SQLx drops the broken connection when it comes back; the next query opens a new one.
    let second = tokio::time::timeout(Duration::from_secs(5), backend_pid(&pool))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(first, second);
    pool.close().await;
    db.close().await;
}

#[tokio::test]
async fn default_policy_pings_every_hand_out_so_a_recently_dead_backend_is_replaced_unseen() {
    let db = Database::new().await;
    let options = db.pool.connect_options().as_ref().clone();
    let pool = connect_pool_with(
        options,
        PgPoolOptions::new().max_connections(1),
        ConnectionPingPolicy::Default,
        StatementTimeout::Default,
    )
    .await
    .unwrap();
    let first = backend_pid(&pool).await.unwrap();
    wait_idle(&pool).await;
    terminate_and_wait(&db.pool, first).await;
    // No idle time to speak of, yet SQLx's own ping finds the dead session before the query does.
    let second = tokio::time::timeout(Duration::from_secs(5), backend_pid(&pool))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(first, second);
    pool.close().await;
    db.close().await;
}

#[tokio::test]
async fn every_session_carries_the_pools_statement_timeout() {
    let db = Database::new().await;
    let options = db.pool.connect_options().as_ref().clone();
    for (timeout, shown) in [
        (StatementTimeout::Default, "4s"),
        (StatementTimeout::Custom(Duration::from_millis(125)), "125ms"),
        (StatementTimeout::Unlimited, "0"),
    ] {
        let pool = connect_pool_with(
            options.clone(),
            PgPoolOptions::new().max_connections(2),
            ConnectionPingPolicy::Default,
            timeout,
        )
        .await
        .unwrap();
        // Both connections of the pool, not just the first one handed out.
        let (mut first, mut second) = (pool.acquire().await.unwrap(), pool.acquire().await.unwrap());
        for connection in [&mut first, &mut second] {
            let setting: String = sqlx::query_scalar("SHOW statement_timeout")
                .fetch_one(&mut **connection)
                .await
                .unwrap();
            assert_eq!(setting, shown, "{timeout:?}");
        }
        drop((first, second));
        pool.close().await;
    }
    // The cap is enforced: a statement longer than it fails, the next one runs.
    let capped = connect_pool_with(
        options.clone(),
        PgPoolOptions::new().max_connections(1),
        ConnectionPingPolicy::Default,
        StatementTimeout::Custom(Duration::from_millis(100)),
    )
    .await
    .unwrap();
    let error = sqlx::query("SELECT pg_sleep(1)").execute(&capped).await.unwrap_err();
    assert_eq!(error.as_database_error().unwrap().code().as_deref(), Some("57014"));
    assert_eq!(sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(&capped).await.unwrap(), 1);
    capped.close().await;
    // A cap PostgreSQL cannot hold is refused before connecting.
    let error = connect_pool(
        options,
        ConnectionPingPolicy::Default,
        StatementTimeout::Custom(Duration::ZERO),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, sqlx::Error::Configuration(_)));
    db.close().await;
}

struct SupervisedPoolChild {
    child: std::process::Child,
    lines: std::sync::mpsc::Receiver<String>,
    reader: Option<std::thread::JoinHandle<()>>,
}
impl SupervisedPoolChild {
    fn marker(&self, marker: &str) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let line = self
                .lines
                .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
                .expect("bounded pool child readiness");
            if line.contains(marker) {
                return;
            }
        }
    }
}
impl Drop for SupervisedPoolChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(reader) = self.reader.take() {
            reader.join().unwrap();
        }
    }
}
#[tokio::test]
async fn held_connection_shutdown_child() {
    let Ok(url) = std::env::var("CQRSX_POOL_SHUTDOWN_DATABASE") else {
        return;
    };
    let options = url
        .parse::<sqlx::postgres::PgConnectOptions>()
        .unwrap()
        .application_name("cqrs-shutdown-held");
    let pool = PgPoolOptions::new().max_connections(1).connect_with(options).await.unwrap();
    let held = pool.acquire().await.unwrap();
    let (send, stop) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).unwrap();
        assert_eq!(line.trim(), "stop");
        let _ = send.send(());
    });
    println!("READY");
    stop.await.unwrap();
    println!("CLOSING");
    pool.close().await;
    drop(held);
    println!("CLOSED");
}
#[tokio::test]
async fn external_supervisor_forces_and_reaps_pool_close_with_a_held_connection() {
    use std::io::{BufRead, Write};
    let db = Database::new().await;
    let options = db.pool.connect_options();
    let url = format!(
        "postgresql://cqrsx_test:cqrsx_test@{}:{}/cqrsx_test",
        options.get_host(),
        options.get_port()
    );
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "pool::held_connection_shutdown_child", "--nocapture"])
        .env("CQRSX_POOL_SHUTDOWN_DATABASE", url)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (send, lines) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in std::io::BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            if send.send(line).is_err() {
                break;
            }
        }
    });
    let mut supervised = SupervisedPoolChild {
        child,
        lines,
        reader: Some(reader),
    };
    supervised.marker("READY");
    let held: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE application_name='cqrs-shutdown-held'")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(held, 1);
    supervised.child.stdin.as_mut().unwrap().write_all(b"stop\n").unwrap();
    supervised.child.stdin.as_mut().unwrap().flush().unwrap();
    supervised.marker("CLOSING");
    let deadline = std::time::Instant::now() + Duration::from_millis(200);
    while std::time::Instant::now() < deadline {
        assert!(supervised.child.try_wait().unwrap().is_none());
        std::thread::sleep(Duration::from_millis(10));
    }
    supervised.child.kill().unwrap();
    assert!(!supervised.child.wait().unwrap().success());
    drop(supervised);
    db.close().await;
}
