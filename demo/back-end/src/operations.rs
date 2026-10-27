//! Operations over the whole box office, without an aggregate: health, settings, the activity log and the read side.
//! The root registers the activity log with the engine: the hub every entry goes to (the mail service's attempts take
//! it as a dependency), the observer of every command and the observer of every projection processor.
pub mod application;
pub mod presentation;

use crate::{
    operations::application::activity_observer::{ActivityObserver, CommandActivityObserver},
    shared::application::activity::ActivityHub,
};
use cqrsx::engine::{
    module::Module,
    setup::{Setup, SetupError},
};
use std::sync::Arc;

/// Broadcast capacity of the activity log: a slow SSE client lags (and skips) past this many entries, it never blocks
/// a publisher.
pub const ACTIVITY_CHANNEL_CAPACITY: usize = 1024;
/// How many recent entries a new SSE subscriber receives in its `hello`.
pub const ACTIVITY_BACKLOG: usize = 100;

pub fn module(_: &Setup) -> Result<Module, SetupError> {
    let activity = Arc::new(ActivityHub::new(ACTIVITY_CHANNEL_CAPACITY, ACTIVITY_BACKLOG));
    let projections = activity.clone();
    Ok(Module::new("operations")
        .provide(activity.clone())
        .observe_commands(Arc::new(CommandActivityObserver::new(activity)))
        .observe_projections(move |projection, owner| Arc::new(ActivityObserver::new(projection, owner, projections.clone()))))
}
