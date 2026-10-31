//! `GET /api/activity` — Server-Sent Events. First a `hello` with the recent backlog, then live
//! `activity` events. The stream ends when the engine stops so Ctrl+C never waits for open tabs.
use crate::{
    operations::presentation::config::config_json,
    shared::{
        application::activity::ActivityHub,
        presentation::{dependency, error::ApiError},
    },
};
use axum::{
    extract::State,
    response::sse::{Event, KeepAlive, Sse},
};
use cqrsx_engine::Cqrs;
use futures::{StreamExt, stream};
use serde_json::json;
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio_stream::wrappers::{BroadcastStream, errors::BroadcastStreamRecvError};

pub async fn handle(
    State(cqrs): State<Arc<Cqrs>>,
) -> Result<Sse<impl futures::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let (backlog, receiver) = dependency::<ActivityHub>(&cqrs)?.subscribe();
    let hello = json!({ "config": config_json(&cqrs)?, "backlog": backlog });
    let first = stream::once(async move { Ok(Event::default().event("hello").data(hello.to_string())) });
    let live = BroadcastStream::new(receiver).filter_map(|item| async move {
        match item {
            Ok(entry) => Some(Ok(Event::default()
                .event("activity")
                .id(entry.id.to_string())
                .data(serde_json::to_string(&*entry).unwrap_or_else(|_| "{}".to_owned())))),
            // A slow client skipped entries; the next ones still arrive.
            Err(BroadcastStreamRecvError::Lagged(_)) => None,
        }
    });
    let stopping = cqrs.stopping();
    Ok(Sse::new(first.chain(live).take_until(stopping.cancelled_owned()))
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)).text("keep-alive")))
}
