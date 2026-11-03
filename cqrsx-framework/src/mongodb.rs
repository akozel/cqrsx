//! The views of a framework in MongoDB (feature `projections-mongodb`): [`MongoDb`] keeps the views and their
//! checkpoints in one database (`Builder::projections`); the journal stays in PostgreSQL. Its settings are
//! [`MongoDbSettings`]; the client connects lazily, when a view or a checkpoint is first read or written.
//!
//! The module also holds the stores: read-model documents ([`view_repository::MongoDbViewRepository`]) and the
//! checkpoints of their projections ([`checkpoint_store::MongoDbCheckpointStore`]), next to each other in the read
//! database.
//!
//! MongoDB is only the read side here: the journal, the outbox and the task queue stay in PostgreSQL, and a
//! checkpoint keeps a position of that journal. How the two databases are reset is in the README of `cqrsx-framework`
//! («Resetting the databases»). Nothing needs to be applied: a collection appears with its first document.
//!
//! Every write is one statement on one document; leases compare against the server clock (`$$NOW`, MongoDB
//! 4.2 or later). The stores do not connect: the application builds the `mongodb::Client` with its own options and
//! hands over a `mongodb::Database`.
pub mod checkpoint_store;
mod storage_failure;
pub mod view_repository;

use crate::{
    build::BuildError,
    settings::{Settings, SettingsError, VariableReader, duration_text},
};
use ::mongodb::{
    Client,
    error::{Error, ErrorKind, WriteFailure},
    options::ClientOptions,
};
use cqrsx::journal::PersistenceError;
use std::{collections::BTreeMap, num::NonZeroU32, time::Duration};

/// The views and their checkpoints in MongoDB (`Builder::projections`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MongoDb;

/// The server the framework connects to.
pub const DEFAULT_MONGODB_URI: &str = "mongodb://localhost:27017";
/// Connections of the client: the driver's own default.
pub const DEFAULT_MONGODB_POOL_SIZE: NonZeroU32 = NonZeroU32::new(10).unwrap();
/// The database of the views and their checkpoints.
pub const DEFAULT_MONGODB_DATABASE: &str = "read";
/// How long the client looks for a server, and waits for a connection: when MongoDB is down the read side fails fast
/// and retries (the processors back off, a status answers with the rows it saw last) instead of hanging for the
/// driver's 30 s. The commands never touch MongoDB.
pub const MONGODB_SERVER_SELECTION_TIMEOUT: Duration = Duration::from_secs(3);
pub const MONGODB_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

const MONGODB_URI: &str = "MONGODB_URI";
const MONGODB_POOL_SIZE: &str = "MONGODB_POOL_SIZE";
const MONGODB_DATABASE: &str = "MONGODB_DATABASE";

/// The variables of the client, by their names after the prefix.
pub(crate) const VARIABLES: [&str; 3] = [MONGODB_URI, MONGODB_POOL_SIZE, MONGODB_DATABASE];

/// The server's code for a duplicate key.
const DUPLICATE_KEY: i32 = 11000;

/// A driver error as a persistence error: a server out of reach is a connection error, anything else is
/// unexpected.
fn persistence_error(error: Error) -> PersistenceError {
    match error.kind.as_ref() {
        ErrorKind::Io(_) | ErrorKind::ServerSelection { .. } | ErrorKind::ConnectionPoolCleared { .. } => {
            PersistenceError::Connection(Box::new(error))
        }
        _ => PersistenceError::Unexpected(Box::new(error)),
    }
}

/// The code the server answered with, when it answered.
pub(crate) fn server_code(error: &Error) -> Option<i32> {
    match error.kind.as_ref() {
        ErrorKind::Command(command) => Some(command.code),
        ErrorKind::Write(WriteFailure::WriteError(write)) => Some(write.code),
        ErrorKind::Write(WriteFailure::WriteConcernError(concern)) => Some(concern.code),
        _ => None,
    }
}

fn is_duplicate_key(error: &Error) -> bool {
    server_code(error) == Some(DUPLICATE_KEY)
}

#[cfg(test)]
pub(crate) mod test_support {
    //! Driver errors built without a server, and a database no server answers for.
    use ::mongodb::{
        Client, Database,
        bson::{self, doc},
        error::{CommandError, Error, ErrorKind, WriteConcernError, WriteError, WriteFailure},
        options::ClientOptions,
    };
    use cqrsx::domain::{aggregate::Aggregate, command::Decide, event::Events};
    use serde::{Deserialize, Serialize};

    /// An aggregate for the views of the unit tests: it only has to exist.
    #[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
    pub(crate) struct Ledger {
        balance: i64,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub(crate) struct Delta(i32);

    impl Events for Delta {
        fn event_type(&self) -> &'static str {
            "delta"
        }
        fn event_version(&self) -> &'static str {
            "1"
        }
    }

    #[derive(Debug, PartialEq, thiserror::Error)]
    #[error("rejected")]
    pub(crate) struct Rejected;

    impl Aggregate for Ledger {
        const TYPE: &'static str = "ledger";
        type Id = String;
        type Command = ();
        type Event = Delta;
        type Rejection = Rejected;
        type Rules = ();
        fn apply(&mut self, event: Delta) {
            self.balance += i64::from(event.0);
        }
    }

    impl Decide<Ledger> for () {
        fn decide(self, _: &Ledger, _: &()) -> Result<Delta, Rejected> {
            Ok(Delta(0))
        }
    }

    /// A database on a port nobody listens on: every operation fails after a short server selection.
    pub(crate) async fn unreachable() -> Database {
        let options = ClientOptions::parse("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=100&connectTimeoutMS=100")
            .await
            .unwrap();
        Client::with_options(options).unwrap().database("unused")
    }

    pub(crate) fn io() -> Error {
        Error::from(std::io::Error::other("private_io"))
    }

    pub(crate) fn command(code: i32) -> Error {
        let command: CommandError = bson::from_document(doc! { "code": code, "errmsg": "private_command" }).unwrap();
        Error::from(ErrorKind::Command(command))
    }

    pub(crate) fn write(code: i32) -> Error {
        let write: WriteError = bson::from_document(doc! { "code": code, "errmsg": "private_write" }).unwrap();
        Error::from(ErrorKind::Write(WriteFailure::WriteError(write)))
    }

    pub(crate) fn write_concern(code: i32) -> Error {
        let concern: WriteConcernError = bson::from_document(doc! { "code": code, "errmsg": "private_concern" }).unwrap();
        Error::from(ErrorKind::Write(WriteFailure::WriteConcernError(concern)))
    }

    pub(crate) fn bson() -> Error {
        Error::from(bson::from_document::<i32>(doc! {}).unwrap_err())
    }

    pub(crate) fn custom() -> Error {
        Error::custom("private_custom")
    }
}

/// The client of the settings: their URI with the plan on top. It connects when it is first used.
pub(crate) async fn client(settings: &Settings) -> Result<Client, BuildError> {
    let mongodb = settings.mongodb();
    let mut options = ClientOptions::parse(mongodb.uri())
        .await
        .map_err(|source| BuildError::MongoDbUri {
            variable: settings.variable(MONGODB_URI),
            source,
        })?;
    mongodb.client_plan(settings.application_name()).apply(&mut options);
    Client::with_options(options).map_err(BuildError::MongoDbClient)
}

/// The client of a framework with its views in MongoDB and their database.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MongoDbSettings {
    uri: String,
    pool_size: NonZeroU32,
    database: String,
}

impl Default for MongoDbSettings {
    fn default() -> Self {
        Self::new(DEFAULT_MONGODB_URI, DEFAULT_MONGODB_POOL_SIZE, DEFAULT_MONGODB_DATABASE)
    }
}

impl MongoDbSettings {
    /// The server `uri`, the connections of the client and the database of the views.
    pub fn new(uri: impl Into<String>, pool_size: NonZeroU32, database: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            pool_size,
            database: database.into(),
        }
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub fn pool_size(&self) -> NonZeroU32 {
        self.pool_size
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    /// The client as it is built, under the connections' name `application_name`: what the framework sets whatever the URI says.
    pub fn client_plan(&self, application_name: &str) -> ClientPlan {
        ClientPlan {
            max_pool_size: self.pool_size,
            server_selection_timeout: MONGODB_SERVER_SELECTION_TIMEOUT,
            connect_timeout: MONGODB_CONNECT_TIMEOUT,
            application_name: application_name.to_owned(),
        }
    }

    pub(crate) fn read(read: &VariableReader) -> Result<Self, SettingsError> {
        Ok(Self {
            uri: read.text(MONGODB_URI).unwrap_or_else(|| DEFAULT_MONGODB_URI.to_owned()),
            pool_size: read.value(MONGODB_POOL_SIZE, DEFAULT_MONGODB_POOL_SIZE)?,
            database: read
                .text(MONGODB_DATABASE)
                .unwrap_or_else(|| DEFAULT_MONGODB_DATABASE.to_owned()),
        })
    }

    /// The variables of these settings and the plan of the client, in the order described.
    pub(crate) fn entries(&self, name: &dyn Fn(&str) -> String, application_name: &str) -> Vec<(String, String)> {
        let mut entries = vec![
            (name(MONGODB_URI), self.uri.clone()),
            (name(MONGODB_POOL_SIZE), self.pool_size.to_string()),
            (name(MONGODB_DATABASE), self.database.clone()),
        ];
        entries.extend(self.client_plan(application_name).entries());
        entries
    }
}

/// How the framework connects to MongoDB, on top of what its URI says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientPlan {
    pub max_pool_size: NonZeroU32,
    pub server_selection_timeout: Duration,
    pub connect_timeout: Duration,
    pub application_name: String,
}

impl ClientPlan {
    /// Sets the plan's options, whatever the URI said about them.
    pub fn apply(&self, options: &mut ClientOptions) {
        options.server_selection_timeout = Some(self.server_selection_timeout);
        options.connect_timeout = Some(self.connect_timeout);
        options.app_name = Some(self.application_name.clone());
        options.max_pool_size = Some(self.max_pool_size.get());
    }

    /// The client as `mongodb.<option>`, by key; the application's name under the option's name in MongoDB's URI
    /// (`appName`).
    pub fn description(&self) -> BTreeMap<String, String> {
        self.entries().into_iter().collect()
    }

    /// What [`description`](Self::description) holds, in the order described.
    pub(crate) fn entries(&self) -> Vec<(String, String)> {
        vec![
            ("mongodb.application_name".to_owned(), self.application_name.clone()),
            ("mongodb.connect_timeout".to_owned(), duration_text(self.connect_timeout)),
            ("mongodb.max_pool_size".to_owned(), self.max_pool_size.to_string()),
            (
                "mongodb.server_selection_timeout".to_owned(),
                duration_text(self.server_selection_timeout),
            ),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::settings::{Defaults, Settings};

    fn pool_size(value: Option<&str>) -> Result<NonZeroU32, SettingsError> {
        let lookup = |name: &str| {
            (name == "ENCORE_MONGODB_POOL_SIZE")
                .then(|| value.map(str::to_owned))
                .flatten()
        };
        Settings::from_lookup("ENCORE", lookup, &Defaults::LIBRARY).map(|settings| settings.mongodb().pool_size())
    }

    #[test]
    fn the_mongodb_pool_takes_its_default_and_rejects_zero() {
        assert_eq!(pool_size(None).unwrap(), DEFAULT_MONGODB_POOL_SIZE);
        assert_eq!(pool_size(Some("64")).unwrap().get(), 64);
        let error = pool_size(Some("0")).unwrap_err();
        assert!(error.to_string().contains("ENCORE_MONGODB_POOL_SIZE"), "{error}");
    }

    #[tokio::test]
    async fn the_mongo_plan_overrides_what_the_uri_says() {
        let config = MongoDbSettings::new("mongodb://localhost:27018", NonZeroU32::new(12).unwrap(), "encore_read");
        let plan = config.client_plan("encore-backend");
        let mut options = ClientOptions::parse(
            "mongodb://localhost:27018/?maxPoolSize=50&appName=other&connectTimeoutMS=100&serverSelectionTimeoutMS=100",
        )
        .await
        .unwrap();
        plan.apply(&mut options);
        assert_eq!(
            (
                options.max_pool_size,
                options.server_selection_timeout,
                options.connect_timeout,
                options.app_name.as_deref()
            ),
            (
                Some(12),
                Some(Duration::from_secs(3)),
                Some(Duration::from_secs(3)),
                Some("encore-backend")
            )
        );
        assert_eq!(
            plan.description(),
            BTreeMap::from(
                [
                    ("mongodb.application_name", "encore-backend"),
                    ("mongodb.connect_timeout", "3 s"),
                    ("mongodb.max_pool_size", "12"),
                    ("mongodb.server_selection_timeout", "3 s"),
                ]
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
            )
        );
        assert_eq!(
            (config.uri(), config.database()),
            ("mongodb://localhost:27018", "encore_read")
        );
    }
}

#[cfg(test)]
mod store_tests {
    use super::{test_support::*, *};

    #[test]
    fn a_server_out_of_reach_is_a_connection_error_and_everything_else_is_unexpected() {
        assert!(matches!(persistence_error(io()), PersistenceError::Connection(source) if source.is::<Error>()));
        for error in [command(2), write(121), write_concern(64), bson(), custom()] {
            assert!(matches!(persistence_error(error), PersistenceError::Unexpected(source) if source.is::<Error>()));
        }
    }

    #[test]
    fn server_codes_come_from_commands_and_writes_and_mark_duplicate_keys() {
        assert_eq!(
            [command(11000), write(121), write_concern(64), io(), custom()].map(|error| server_code(&error)),
            [Some(11000), Some(121), Some(64), None, None]
        );
        assert!(is_duplicate_key(&write(DUPLICATE_KEY)));
        assert!(is_duplicate_key(&command(DUPLICATE_KEY)));
        assert!(!is_duplicate_key(&write(121)));
        assert!(!is_duplicate_key(&io()));
    }
}
