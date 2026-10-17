//! `GET /api/activity` — Server-Sent Events. First a `hello` with the recent backlog, then live
//! `activity` events. The stream ends on shutdown so Ctrl+C never waits for open tabs.
use crate::{bootstrap::AppState, presentation::http::config::config_json};
use axum::{
    extract::State,
    response::sse::{Event, KeepAlive, Sse},
};
use futures::{StreamExt, stream};
use serde_json::json;
use std::{convert::Infallible, time::Duration};
use tokio_stream::wrappers::{BroadcastStream, errors::BroadcastStreamRecvError};

pub async fn handle(State(state): State<AppState>) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let (backlog, receiver) = state.activity.subscribe();
    let hello = json!({ "config": config_json(&state.config), "backlog": backlog });
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
    let shutdown = state.shutdown.clone();
    Sse::new(first.chain(live).take_until(shutdown.cancelled_owned()))
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)).text("keep-alive"))
}
