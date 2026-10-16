use super::*;
use cqrsx::postgres::pool::connect_pool;

#[tokio::test]
async fn connection_helper_preserves_ten_connection_limit_and_returns_authentication_error() {
    let db = Database::new().await;
    let options = db.pool.connect_options().as_ref().clone();
    let pool = connect_pool(options.clone()).await.unwrap();
    assert_eq!(pool.options().get_max_connections(), 10);
    assert_eq!(sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(&pool).await.unwrap(), 1);
    pool.close().await;

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        connect_pool(options.password("private_incorrect_password")),
    )
    .await
    .unwrap();
    let error = result.unwrap_err();
    assert!(matches!(error, sqlx::Error::Database(_)));
    assert_eq!(error.as_database_error().unwrap().code().as_deref(), Some("28P01"));
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
    let Ok(url) = std::env::var("CQRS_POOL_SHUTDOWN_DATABASE") else {
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
        "postgresql://outbox_test:outbox_test@{}:{}/outbox_test",
        options.get_host(),
        options.get_port()
    );
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "pool::held_connection_shutdown_child", "--nocapture"])
        .env("CQRS_POOL_SHUTDOWN_DATABASE", url)
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
