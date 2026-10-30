//! Operations over the whole box office, without an aggregate: health, settings, the activity log and the read side.
//! The root registers with the engine the activity log (the hub every entry goes to, which the mail service's attempts
//! take as a dependency, the observer of every command and the observer of every projection processor), the pace of
//! Encore! (`ENCORE_REPLAY_SLOW_DELAY_MS` at most `ENCORE_REPLAY_MAX_PACE_MS`), what this node sees of Encore! and the
//! service routes.
pub mod application;
pub mod presentation;

use crate::{
    operations::application::{
        activity_observer::{ActivityObserver, CommandActivityObserver},
        rebuild::{DEFAULT_MAX_PACE, DEFAULT_SLOW_PACE, RebuildState, Replay},
    },
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

pub fn module(setup: &Setup) -> Result<Module, SetupError> {
    let replay = replay(setup)?;
    let activity = Arc::new(ActivityHub::new(ACTIVITY_CHANNEL_CAPACITY, ACTIVITY_BACKLOG));
    let projections = activity.clone();
    Ok(Module::new("operations")
        .provide(activity.clone())
        .provide(Arc::new(replay))
        .provide(Arc::new(RebuildState::default()))
        .observe_commands(Arc::new(CommandActivityObserver::new(activity)))
        .observe_projections(move |processor| {
            Arc::new(ActivityObserver::new(
                processor.projection,
                processor.node,
                projections.clone(),
            ))
        })
        .routes(presentation::routes()))
}

/// Encore! paces between a slow-motion delay the UI asks for and a maximum nobody may exceed.
fn replay(setup: &Setup) -> Result<Replay, SetupError> {
    let slow_delay = setup.millis("REPLAY_SLOW_DELAY_MS", DEFAULT_SLOW_PACE.as_millis() as u64)?;
    let max_pace = setup.millis("REPLAY_MAX_PACE_MS", DEFAULT_MAX_PACE.as_millis() as u64)?;
    if slow_delay > max_pace {
        let message = format!("must not exceed {}", setup.variable("REPLAY_MAX_PACE_MS"));
        return Err(setup.invalid("REPLAY_SLOW_DELAY_MS", message));
    }
    Ok(Replay { slow_delay, max_pace })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqrsx::engine::settings::Settings;
    use std::{collections::HashMap, time::Duration};

    const SLOW_DELAY: &str = "ENCORE_REPLAY_SLOW_DELAY_MS";
    const MAX_PACE: &str = "ENCORE_REPLAY_MAX_PACE_MS";

    fn settings(pairs: &[(&'static str, &str)]) -> Result<Replay, SetupError> {
        let pairs: HashMap<_, _> = pairs.iter().map(|(k, v)| (*k, v.to_string())).collect();
        replay(&Setup::new(
            "ENCORE",
            move |name| pairs.get(name).cloned(),
            Settings::default(),
        ))
    }

    /// The part of the configuration's read side the `operations` module reads (the lag cap and the status timeout are
    /// the engine's settings).
    #[test]
    fn the_replay_pace_defaults_and_names_the_variable_it_rejects() {
        assert_eq!(
            settings(&[]).unwrap(),
            Replay {
                slow_delay: DEFAULT_SLOW_PACE,
                max_pace: DEFAULT_MAX_PACE,
            }
        );
        assert_eq!(settings(&[(SLOW_DELAY, "0")]).unwrap().slow_delay, Duration::ZERO);
        for (pairs, name) in [
            (vec![(SLOW_DELAY, "x")], SLOW_DELAY),
            (vec![(SLOW_DELAY, "500"), (MAX_PACE, "100")], SLOW_DELAY),
            (vec![(MAX_PACE, "x")], MAX_PACE),
        ] {
            let error = settings(&pairs).unwrap_err().to_string();
            assert!(error.starts_with(&format!("{name}: ")), "{pairs:?}: {error}");
        }
        assert_eq!(
            settings(&[(SLOW_DELAY, "500"), (MAX_PACE, "100")]).unwrap_err().to_string(),
            "ENCORE_REPLAY_SLOW_DELAY_MS: must not exceed ENCORE_REPLAY_MAX_PACE_MS"
        );
    }
}
