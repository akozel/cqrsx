//! Test fixtures: the box office on the engine in memory ([`test_app`], [`app`]), as `main.rs` assembles it but with
//! its stores in memory, and what its tests read from it.
use crate::{
    DEMO_DEFAULTS,
    operations::application::activity_observer::CommandActivityObserver,
    shared::application::{activity::ActivityHub, mail::Mailer, test_support::RecordingMailer},
};
use cqrsx::{
    domain::aggregate::Aggregate,
    engine::{Cqrs, build::Builder, settings::Settings},
    task::policy::Policies,
    view::{View, repository::StoredView},
};
use std::{collections::HashMap, future::Future, sync::Arc};

/// The modules of the box office as `main.rs` registers them, over the variables `pairs` (full names) and the demo's
/// defaults, in memory.
pub fn app(pairs: &[(&str, &str)]) -> Builder {
    let pairs: HashMap<String, String> = pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    Cqrs::from_lookup("ENCORE", move |name| pairs.get(name).cloned())
        .defaults(DEMO_DEFAULTS)
        .module(crate::concert::module)
        .module(crate::fan::module)
        .module(crate::operations::module)
}

/// The box office on the engine in memory: the modules `concert`, `fan` and `operations` as the process registers
/// them, with a mail service that records the letters instead of the module's (a test's `provide` wins).
pub async fn test_app() -> (Cqrs, Arc<RecordingMailer>) {
    let mail = Arc::new(RecordingMailer::delivering());
    let mailer: Arc<Mailer> = mail.clone();
    let cqrs = app(&[]).provide(mailer).build().await.expect("the modules assemble");
    (cqrs, mail)
}

/// The activity feed's observer of the command executors, publishing to `activity`: what the composition registers
/// with every executor, for the executors of the modules' tests.
pub fn command_feed(activity: &Arc<ActivityHub>) -> Arc<CommandActivityObserver> {
    Arc::new(CommandActivityObserver::new(activity.clone()))
}

/// The engine's settings of the process without variables: the library's with the demo's defaults.
pub fn settings() -> Settings {
    Settings::from_lookup("ENCORE", |_| None, &DEMO_DEFAULTS).expect("the demo's defaults validate")
}

/// The policies of the task types of the process without variables: six attempts, a step waits 1 s, an e-mail 1 s to
/// 8 s.
pub fn policies() -> Policies {
    ready(app(&[]).build()).expect("the modules assemble").policies().clone()
}

/// The output of `future`, which must not wait: an engine in memory builds without one.
pub fn ready<F: Future>(future: F) -> F::Output {
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    match std::pin::pin!(future).poll(&mut context) {
        std::task::Poll::Ready(output) => output,
        std::task::Poll::Pending => panic!("the future waits"),
    }
}

/// Writes `documents` into the repository of the view `V` of the engine in memory, as a processor would have.
pub fn put<V: View<A> + 'static, A: Aggregate + 'static>(cqrs: &Cqrs, documents: impl IntoIterator<Item = StoredView<V>>) {
    let repository = cqrs.documents::<V, A>().expect("a view of the engine in memory");
    for document in documents {
        repository
            .put(&document.id, document.version, document.last_sequence, &document.view)
            .unwrap();
    }
}
