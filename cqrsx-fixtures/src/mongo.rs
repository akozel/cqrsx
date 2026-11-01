//! A MongoDB 8.2 container, one per test, on a leased host port: a standalone server (no replica set, so no
//! transactions), with a fresh database for the test.
use crate::port_lease::PortLease;
use mongodb::{Client, Database, bson::doc, options::ClientOptions};
use std::time::Duration;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

/// A MongoDB container of one test, with a database of its own.
pub struct Mongo {
    /// A fresh database of the server.
    pub database: Database,
    container: ContainerAsync<GenericImage>,
    _port_lease: PortLease,
}

impl std::fmt::Debug for Mongo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mongo").finish_non_exhaustive()
    }
}

impl Mongo {
    /// Starts the container and waits until the server answers commands.
    pub async fn start() -> Self {
        let port_lease = PortLease::acquire();
        let container = tokio::time::timeout(
            Duration::from_secs(60),
            GenericImage::new("docker.io/library/mongo", "8.2")
                .with_exposed_port(27017.tcp())
                .with_wait_for(WaitFor::Nothing)
                .with_mapped_port(port_lease.port(), 27017.tcp())
                .start(),
        )
        .await
        .expect("bounded MongoDB container startup")
        .unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(27017.tcp()).await.unwrap();
        let mut options = ClientOptions::parse(format!("mongodb://{host}:{port}/?directConnection=true"))
            .await
            .unwrap();
        options.server_selection_timeout = Some(Duration::from_secs(2));
        options.app_name = Some("cqrsx-tests".into());
        let client = Client::with_options(options).unwrap();
        // The port opens before the server answers commands. This is startup readiness, not a retry.
        tokio::time::timeout(Duration::from_secs(60), async {
            while client.database("admin").run_command(doc! { "ping": 1 }).await.is_err() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("bounded MongoDB readiness");
        Self {
            database: client.database(&format!("test_{}", uuid::Uuid::now_v7().simple())),
            container,
            _port_lease: port_lease,
        }
    }

    /// Stops the container.
    pub async fn close(self) {
        self.container.stop().await.unwrap();
    }
}
