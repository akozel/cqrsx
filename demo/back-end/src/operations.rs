//! Operations over the whole box office, without an aggregate: health, settings, the activity log and the read side.
//! The root registers with the engine the activity log (the hub every entry goes to, which the mail service's attempts
//! take as a dependency, the observer of every command and the observer of every projection processor), the pace of
//! Encore! (`ENCORE_REBUILD_SLOW_PACE_MS` at most `ENCORE_REBUILD_MAX_PACE_MS`), what this node sees of Encore! and the
//! service routes.
pub mod application;
pub mod presentation;

use crate::{
    operations::application::{
        activity_observer::{ActivityObserver, CommandActivityObserver},
        rebuild::{DEFAULT_MAX_PACE, DEFAULT_SLOW_PACE, ReadSideState, RebuildPace},
    },
    shared::application::activity::ActivityHub,
};
use cqrsx_engine::{
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
    let rebuild_pace = rebuild_pace(setup)?;
    let activity = Arc::new(ActivityHub::new(ACTIVITY_CHANNEL_CAPACITY, ACTIVITY_BACKLOG));
    let projections = activity.clone();
    Ok(Module::new("operations")
        .provide(activity.clone())
        .provide(Arc::new(rebuild_pace))
        .provide(Arc::new(ReadSideState::default()))
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
fn rebuild_pace(setup: &Setup) -> Result<RebuildPace, SetupError> {
    let slow_pace = setup.millis("REBUILD_SLOW_PACE_MS", DEFAULT_SLOW_PACE.as_millis() as u64)?;
    let max_pace = setup.millis("REBUILD_MAX_PACE_MS", DEFAULT_MAX_PACE.as_millis() as u64)?;
    if slow_pace > max_pace {
        let message = format!("must not exceed {}", setup.variable("REBUILD_MAX_PACE_MS"));
        return Err(setup.invalid("REBUILD_SLOW_PACE_MS", message));
    }
    Ok(RebuildPace { slow_pace, max_pace })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqrsx_engine::settings::Settings;
    use std::{collections::HashMap, time::Duration};

    const SLOW_PACE: &str = "ENCORE_REBUILD_SLOW_PACE_MS";
    const MAX_PACE: &str = "ENCORE_REBUILD_MAX_PACE_MS";

    fn settings(pairs: &[(&'static str, &str)]) -> Result<RebuildPace, SetupError> {
        let pairs: HashMap<_, _> = pairs.iter().map(|(k, v)| (*k, v.to_string())).collect();
        rebuild_pace(&Setup::new(
            "ENCORE",
            move |name| pairs.get(name).cloned(),
            Settings::default(),
        ))
    }

    /// The part of the configuration's read side the `operations` module reads (the lag cap and the status timeout are
    /// the engine's settings).
    #[test]
    fn the_rebuild_pace_defaults_and_names_the_variable_it_rejects() {
        assert_eq!(
            settings(&[]).unwrap(),
            RebuildPace {
                slow_pace: DEFAULT_SLOW_PACE,
                max_pace: DEFAULT_MAX_PACE,
            }
        );
        assert_eq!(settings(&[(SLOW_PACE, "0")]).unwrap().slow_pace, Duration::ZERO);
        for (pairs, name) in [
            (vec![(SLOW_PACE, "x")], SLOW_PACE),
            (vec![(SLOW_PACE, "500"), (MAX_PACE, "100")], SLOW_PACE),
            (vec![(MAX_PACE, "x")], MAX_PACE),
        ] {
            let error = settings(&pairs).unwrap_err().to_string();
            assert!(error.starts_with(&format!("{name}: ")), "{pairs:?}: {error}");
        }
        assert_eq!(
            settings(&[(SLOW_PACE, "500"), (MAX_PACE, "100")]).unwrap_err().to_string(),
            "ENCORE_REBUILD_SLOW_PACE_MS: must not exceed ENCORE_REBUILD_MAX_PACE_MS"
        );
    }
}
