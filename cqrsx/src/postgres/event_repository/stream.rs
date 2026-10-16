//! Forward database failures as terminal stream items and release abandoned reads.
use crate::{
    cqrs::store::{PersistenceError, stream::ReplayStream},
    postgres::error::PostgresRepositoryError,
};
use futures::TryStreamExt;
use sqlx::PgPool;

pub(super) fn spawn_stream(
    query: String,
    aggregate_type: String,
    aggregate_id: Option<String>,
    pool: PgPool,
    capacity: usize,
) -> ReplayStream {
    let (mut feed, stream) = ReplayStream::new(capacity);
    tokio::spawn(async move {
        let mut connection = tokio::select! {
            biased;
            _ = feed.closed() => return,
            result = pool.acquire() => match result {
                Ok(connection) => connection,
                Err(error) => {
                    let _ = feed.push(Err(PostgresRepositoryError::from(error).into())).await;
                    return;
                }
            }
        };
        let mut query = sqlx::query(&query).bind(&aggregate_type);
        if let Some(id) = &aggregate_id {
            query = query.bind(id);
        }
        let mut rows = query.fetch(&mut *connection);
        loop {
            let next = tokio::select! {
                biased;
                _ = feed.closed() => break,
                result = rows.try_next() => result,
            };
            let event = match next {
                Ok(Some(row)) => super::read::decode_event_row(&row).map_err(PersistenceError::from),
                Ok(None) => return,
                Err(error) => Err(PostgresRepositoryError::from(error).into()),
            };
            if event.is_err() {
                drop(rows);
                connection.close_on_drop();
                let _ = feed.push(event).await;
                return;
            }
            if feed.push(event).await.is_err() {
                break;
            }
        }
        // Abandoned reads may have unread protocol data. Never return that
        // connection to the pool for an unrelated operation to drain.
        drop(rows);
        connection.close_on_drop();
    });
    stream
}
