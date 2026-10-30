//! The MongoDB read side: read-model documents ([`view_repository::MongoViewRepository`]) and the checkpoints
//! of their projections ([`checkpoint_store::MongoCheckpointStore`]), next to each other in the read
//! database.
//!
//! MongoDB is only the read side here: the journal, the outbox and the task queue stay in PostgreSQL. A
//! checkpoint keeps a position of the PostgreSQL journal, so the two databases are reset together: a journal
//! recreated under old checkpoints would start below their positions, and its events would never reach the
//! views. Dropping the read side alone is safe: a processor registers its checkpoint again and rebuilds every
//! document from the start of the journal.
//!
//! Every write is one statement on one document; leases compare against the server clock (`$$NOW`, MongoDB
//! 4.2 or later). Nothing here connects: the application builds the `mongodb::Client` with its own options and
//! hands over a `mongodb::Database`.
pub mod checkpoint_store;
pub mod view_repository;

use crate::cqrs::store::PersistenceError;
use ::mongodb::error::{Error, ErrorKind, WriteFailure};

/// The server's code for a duplicate key.
const DUPLICATE_KEY: i32 = 11000;

/// A driver error as a persistence error: a server out of reach is a connection error, anything else is
/// unexpected.
fn persistence(error: Error) -> PersistenceError {
    match error.kind.as_ref() {
        ErrorKind::Io(_) | ErrorKind::ServerSelection { .. } | ErrorKind::ConnectionPoolCleared { .. } => {
            PersistenceError::ConnectionError(Box::new(error))
        }
        _ => PersistenceError::UnknownError(Box::new(error)),
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

#[cfg(test)]
mod tests {
    use super::{test_support::*, *};

    #[test]
    fn a_server_out_of_reach_is_a_connection_error_and_everything_else_is_unexpected() {
        assert!(matches!(persistence(io()), PersistenceError::ConnectionError(source) if source.is::<Error>()));
        for error in [command(2), write(121), write_concern(64), bson(), custom()] {
            assert!(matches!(persistence(error), PersistenceError::UnknownError(source) if source.is::<Error>()));
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
