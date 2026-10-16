use std::{
    fs::{File, OpenOptions, TryLockError},
    net::{Ipv4Addr, TcpListener},
    path::Path,
};

const FIRST_PORT: u16 = 20_000;
const PORT_COUNT: u16 = 10_000;

// Shared by the runtime and service integration suites through #[path]. This
// stays test-owned; no fixture API or dependency is added to either library.
pub(super) struct PortLease {
    port: u16,
    _lock: File,
}

impl PortLease {
    pub(super) fn acquire() -> Self {
        let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
        let root = manifest
            .ancestors()
            .find(|path| path.join(".git").exists())
            .unwrap_or_else(|| manifest.parent().unwrap());
        let directory = std::env::var_os("CQRS_TEST_PORT_LEASE_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| root.join("target/stage4/postgres-ports"));
        std::fs::create_dir_all(&directory).expect("PostgreSQL fixture port lease directory");
        // Avoid the default client ephemeral range and coordinate parallel test
        // processes without serializing container startup or database work.
        let initial_port = (uuid::Uuid::new_v4().as_u128() % u128::from(PORT_COUNT)) as u16;
        Self::acquire_from(
            &directory,
            (0..PORT_COUNT).map(|offset| FIRST_PORT + (initial_port + offset) % PORT_COUNT),
        )
        .expect("free leased PostgreSQL fixture port outside default ephemeral range")
    }

    fn acquire_from(directory: &Path, ports: impl IntoIterator<Item = u16>) -> Option<Self> {
        ports.into_iter().find_map(|port| {
            let lock = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(directory.join(format!("{port}.lock")))
                .expect("PostgreSQL fixture port lease file");
            match lock.try_lock() {
                Ok(()) => {}
                Err(TryLockError::WouldBlock) => return None,
                Err(error) => panic!("cannot lease PostgreSQL test port: {error}"),
            }
            let reservation = TcpListener::bind((Ipv4Addr::UNSPECIFIED, port)).ok()?;
            drop(reservation);
            Some(Self { port, _lock: lock })
        })
    }

    pub(super) fn port(&self) -> u16 {
        self.port
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn directory() -> std::path::PathBuf {
        let directory = std::env::temp_dir().join(format!("cqrs-port-lease-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        directory
    }

    #[test]
    fn leased_ports_are_unavailable_until_the_owner_drops() {
        let directory = directory();
        let lease = PortLease::acquire_from(&directory, FIRST_PORT..FIRST_PORT + PORT_COUNT).unwrap();
        let port = lease.port();
        assert!(PortLease::acquire_from(&directory, [port]).is_none());
        let competing_lock = OpenOptions::new()
            .read(true)
            .write(true)
            .open(directory.join(format!("{port}.lock")))
            .unwrap();
        assert!(matches!(competing_lock.try_lock(), Err(TryLockError::WouldBlock)));
        drop(lease);
        competing_lock.try_lock().unwrap();
        drop(competing_lock);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn listening_ports_are_skipped_without_starting_a_container() {
        let directory = directory();
        let listener = (FIRST_PORT..FIRST_PORT + PORT_COUNT)
            .find_map(|port| TcpListener::bind((Ipv4Addr::UNSPECIFIED, port)).ok())
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        assert!(PortLease::acquire_from(&directory, [port]).is_none());
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .open(directory.join(format!("{port}.lock")))
            .unwrap();
        lock.try_lock().unwrap();
        drop(lock);
        drop(listener);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn concurrent_allocations_hold_distinct_ports() {
        let directory = directory();
        let leases = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..16)
                .map(|_| scope.spawn(|| PortLease::acquire_from(&directory, FIRST_PORT..FIRST_PORT + PORT_COUNT).unwrap()))
                .collect();
            handles.into_iter().map(|handle| handle.join().unwrap()).collect::<Vec<_>>()
        });
        let ports: std::collections::HashSet<_> = leases.iter().map(PortLease::port).collect();
        assert_eq!(ports.len(), 16);
        drop(leases);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
