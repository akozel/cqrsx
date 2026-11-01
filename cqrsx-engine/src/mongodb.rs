//! The views of an engine in MongoDB (feature `mongodb`): [`MongoDb`] keeps the views and their checkpoints in one
//! database (`Builder::projections`); the journal stays in PostgreSQL. Its settings are [`MongoSettings`]; the client
//! connects lazily, when a view or a checkpoint is first read or written.
use crate::settings::{SettingsError, VariableReader, duration_text};
#[cfg(feature = "postgres")]
use crate::{build::BuildError, settings::Settings};
#[cfg(feature = "postgres")]
use mongodb::Client;
use mongodb::options::ClientOptions;
use std::{collections::BTreeMap, num::NonZeroU32, time::Duration};

/// The views and their checkpoints in MongoDB (`Builder::projections`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MongoDb;

/// The server the engine connects to.
pub const DEFAULT_MONGODB_URI: &str = "mongodb://localhost:27017";
/// Connections of the client: the driver's own default.
pub const DEFAULT_MONGODB_POOL_SIZE: NonZeroU32 = NonZeroU32::new(10).unwrap();
/// The database of the views and their checkpoints.
pub const DEFAULT_READ_DB: &str = "read";
/// How long the client looks for a server, and waits for a connection: when MongoDB is down the read side fails fast
/// and retries (the processors back off, a status answers with the rows it saw last) instead of hanging for the
/// driver's 30 s. The commands never touch MongoDB.
pub const MONGODB_SERVER_SELECTION_TIMEOUT: Duration = Duration::from_secs(3);
pub const MONGODB_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

const MONGODB_URI: &str = "MONGODB_URI";
const MONGODB_POOL_SIZE: &str = "MONGODB_POOL_SIZE";
const READ_DB: &str = "READ_DB";

/// The variables of the client, by their names after the prefix.
pub(crate) const VARIABLES: [&str; 3] = [MONGODB_URI, MONGODB_POOL_SIZE, READ_DB];

/// The client of the settings: their URI with the plan on top. It connects when it is first used.
#[cfg(feature = "postgres")]
pub(crate) async fn client(settings: &Settings) -> Result<Client, BuildError> {
    let mongodb = settings.mongodb();
    let mut options = ClientOptions::parse(mongodb.uri())
        .await
        .map_err(|source| BuildError::MongoUri {
            variable: settings.variable(MONGODB_URI),
            source,
        })?;
    mongodb.mongo_plan(settings.application_name()).apply(&mut options);
    Client::with_options(options).map_err(BuildError::MongoClient)
}

/// The client of an engine with its views in MongoDB and their database.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MongoSettings {
    uri: String,
    pool_size: NonZeroU32,
    read_db: String,
}

impl Default for MongoSettings {
    fn default() -> Self {
        Self::new(DEFAULT_MONGODB_URI, DEFAULT_MONGODB_POOL_SIZE, DEFAULT_READ_DB)
    }
}

impl MongoSettings {
    /// The server `uri`, the connections of the client and the database of the views.
    pub fn new(uri: impl Into<String>, pool_size: NonZeroU32, read_db: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            pool_size,
            read_db: read_db.into(),
        }
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub fn pool_size(&self) -> NonZeroU32 {
        self.pool_size
    }

    pub fn read_db(&self) -> &str {
        &self.read_db
    }

    /// The client as it is built, under the connections' name `application_name`: what the engine sets whatever the URI says.
    pub fn mongo_plan(&self, application_name: &str) -> MongoPlan {
        MongoPlan {
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
            read_db: read.text(READ_DB).unwrap_or_else(|| DEFAULT_READ_DB.to_owned()),
        })
    }

    /// The variables of these settings and the plan of the client, in the order described.
    pub(crate) fn entries(&self, name: &dyn Fn(&str) -> String, application_name: &str) -> Vec<(String, String)> {
        let mut entries = vec![
            (name(MONGODB_URI), self.uri.clone()),
            (name(MONGODB_POOL_SIZE), self.pool_size.to_string()),
            (name(READ_DB), self.read_db.clone()),
        ];
        entries.extend(self.mongo_plan(application_name).entries());
        entries
    }
}

/// How the engine connects to MongoDB, on top of what its URI says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MongoPlan {
    pub max_pool_size: NonZeroU32,
    pub server_selection_timeout: Duration,
    pub connect_timeout: Duration,
    pub application_name: String,
}

impl MongoPlan {
    /// Sets the plan's options, whatever the URI said about them.
    pub fn apply(&self, options: &mut ClientOptions) {
        options.server_selection_timeout = Some(self.server_selection_timeout);
        options.connect_timeout = Some(self.connect_timeout);
        options.app_name = Some(self.application_name.clone());
        options.max_pool_size = Some(self.max_pool_size.get());
    }

    /// The client as `mongodb.<option>`, by key; the application's name under the option's name in MongoDB's URI
    /// (`appName`).
    pub fn describe(&self) -> BTreeMap<String, String> {
        self.entries().into_iter().collect()
    }

    /// What [`describe`](Self::describe) holds, in the order described.
    pub(crate) fn entries(&self) -> Vec<(String, String)> {
        vec![
            ("mongodb.app_name".to_owned(), self.application_name.clone()),
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
        let config = MongoSettings::new("mongodb://localhost:27018", NonZeroU32::new(12).unwrap(), "encore_read");
        let plan = config.mongo_plan("encore-backend");
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
            plan.describe(),
            BTreeMap::from(
                [
                    ("mongodb.app_name", "encore-backend"),
                    ("mongodb.connect_timeout", "3 s"),
                    ("mongodb.max_pool_size", "12"),
                    ("mongodb.server_selection_timeout", "3 s"),
                ]
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
            )
        );
        assert_eq!((config.uri(), config.read_db()), ("mongodb://localhost:27018", "encore_read"));
    }
}
