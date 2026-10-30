//! The load stand's synthetic module (this crate only, never in the demo's build): the `Probe` aggregate, one
//! command and one event, with as many views of that event and tasks per event as the stand asks for. The box office
//! keeps one view per aggregate and at most one task per event, so what grows with them is measured here (backlog
//! epic 06, ticket 01). The root reads `ENCORE_LOAD_VIEWS` (the views that count the pings, 0–4) and
//! `ENCORE_LOAD_TASKS` (the tasks every ping plans, 0–4), both 0 unless set, and registers the aggregate with its
//! rules, that many views, the task type when a ping plans any, and the route `POST /api/probe/{probe_id}`.
pub mod application;
pub mod domain;
pub mod presentation;

use crate::probe::{
    application::{echo::echo, views::PingCount},
    domain::{Probe, ProbeRules},
};
use cqrsx::engine::{
    module::Module,
    setup::{Setup, SetupError},
};

/// The most views of a ping (`ENCORE_LOAD_VIEWS`).
pub const MAX_VIEWS: usize = 4;
/// The most tasks a ping plans (`ENCORE_LOAD_TASKS`).
pub const MAX_TASKS: u8 = 4;

/// The views in the order the module takes them: `ENCORE_LOAD_VIEWS=2` registers the first two.
const VIEWS: [fn(Module) -> Module; MAX_VIEWS] = [
    Module::view::<PingCount<1>, Probe>,
    Module::view::<PingCount<2>, Probe>,
    Module::view::<PingCount<3>, Probe>,
    Module::view::<PingCount<4>, Probe>,
];

/// The probe's registrations over its two variables; a value above its maximum, or one that does not parse, names
/// its variable.
pub fn module(setup: &Setup) -> Result<Module, SetupError> {
    let views: usize = setup.env("LOAD_VIEWS", 0)?;
    if views > MAX_VIEWS {
        return Err(setup.invalid("LOAD_VIEWS", format!("must be at most {MAX_VIEWS}")));
    }
    let tasks: u8 = setup.env("LOAD_TASKS", 0)?;
    if tasks > MAX_TASKS {
        return Err(setup.invalid("LOAD_TASKS", format!("must be at most {MAX_TASKS}")));
    }
    let probe = Module::new("probe")
        .aggregate::<Probe>(ProbeRules { tasks })
        .routes(presentation::routes());
    let registered = VIEWS.into_iter().take(views);
    let probe = registered.fold(probe, |probe, view| view(probe));
    // Without tasks the type stays unregistered: the task runtime claims every registered type on every poll.
    Ok(if tasks == 0 {
        probe
    } else {
        probe.task(echo).without_policy()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::probe::{
        application::echo::ProbeEchoV1,
        domain::{Ping, ProbeId},
    };
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use cqrsx::{
        engine::Cqrs,
        task::{format::TaskFormat, testing::mem_task_queue::TaskStatus},
    };
    use serde_json::{Value, json};
    use std::sync::Arc;
    use tower::ServiceExt;

    /// The probe module on the engine in memory over `ENCORE_LOAD_VIEWS` and `ENCORE_LOAD_TASKS`.
    async fn engine(views: Option<&'static str>, tasks: Option<&'static str>) -> Result<Cqrs, String> {
        Cqrs::from_lookup("ENCORE", move |name| match name {
            "ENCORE_LOAD_VIEWS" => views.map(str::to_owned),
            "ENCORE_LOAD_TASKS" => tasks.map(str::to_owned),
            _ => None,
        })
        .module(module)
        .build()
        .await
        .map_err(|error| error.to_string())
    }

    #[tokio::test]
    async fn without_its_variables_a_probe_has_no_view_and_plans_no_task() {
        let cqrs = engine(None, None).await.unwrap();
        assert!(cqrs.dispatcher().is_none(), "no task type");
        let pinged = cqrs
            .from("load")
            .execute::<Probe>(&"p-1".parse().unwrap(), Ping)
            .await
            .unwrap();
        assert!(pinged.tasks.is_empty());
        assert_eq!(cqrs.run_tasks().await, 0);
        assert!(cqrs.views().keys().is_empty(), "no view");
        assert_eq!(cqrs.describe().get("ENCORE_LOAD_VIEWS").map(String::as_str), Some("0"));
        assert_eq!(cqrs.describe().get("ENCORE_LOAD_TASKS").map(String::as_str), Some("0"));
    }

    /// The whole path the stand drives: a ping commits its event with its tasks, every view folds the event, every task
    /// runs once and keeps the probe in its label.
    #[tokio::test]
    async fn a_ping_is_folded_by_every_view_and_its_tasks_run() {
        let cqrs = engine(Some("4"), Some("4")).await.unwrap();
        let probe: ProbeId = "k6-run-p7".parse().unwrap();
        let load = cqrs.from("load");
        let pinged = load.execute::<Probe>(&probe, Ping).await.unwrap();
        load.execute::<Probe>(&probe, Ping).await.unwrap();
        let formats: Vec<String> = pinged.tasks.iter().map(|task| task.format().to_string()).collect();
        assert_eq!(formats, ["encore_probe_echo v1"; 4]);
        assert_eq!(pinged.tasks[3].payload(), &json!({ "slot": 3 }));
        assert_eq!(pinged.tasks[3].metadata()["probe_id"], "k6-run-p7", "the label");

        assert_eq!(cqrs.run_tasks().await, 8);
        let tasks = cqrs.outbox().tasks();
        assert!(tasks.iter().all(|task| task.status == TaskStatus::Completed));
        assert_eq!(tasks[0].entry.task.format(), &TaskFormat::of::<ProbeEchoV1>().unwrap());

        cqrs.catch_up().await;
        let counted = [
            cqrs.view::<PingCount<1>>().get(&probe).await.unwrap().unwrap().view.pings,
            cqrs.view::<PingCount<2>>().get(&probe).await.unwrap().unwrap().view.pings,
            cqrs.view::<PingCount<3>>().get(&probe).await.unwrap().unwrap().view.pings,
            cqrs.view::<PingCount<4>>().get(&probe).await.unwrap().unwrap().view.pings,
        ];
        assert_eq!(counted, [2; 4]);
        let names: Vec<String> = cqrs.views().keys().iter().map(|key| key.projection().to_owned()).collect();
        assert_eq!(names, ["probe_pings_1", "probe_pings_2", "probe_pings_3", "probe_pings_4"]);
    }

    /// The route through the engine's router: a ping with two tasks per ping, and an id the path refuses.
    #[tokio::test]
    async fn the_route_hands_a_ping_to_the_engine_and_refuses_a_bad_id_before_it() {
        let cqrs = Arc::new(engine(None, Some("2")).await.unwrap());
        let ping = |probe_id: &str| {
            let request = Request::post(format!("/api/probe/{probe_id}")).body(Body::empty()).unwrap();
            cqrs.router().oneshot(request)
        };
        let accepted = ping("k6-p1").await.unwrap();
        assert_eq!(accepted.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(accepted.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes).unwrap(),
            json!({ "outcome": "accepted", "probe_id": "k6-p1", "tasks": 2 })
        );
        assert_eq!(ping("K6!").await.unwrap().status(), StatusCode::BAD_REQUEST);
        assert_eq!(cqrs.run_tasks().await, 2);
    }

    #[tokio::test]
    async fn the_variables_take_the_first_views_and_at_most_four_of_each() {
        let cqrs = engine(Some("2"), Some("1")).await.unwrap();
        let names: Vec<String> = cqrs.views().keys().iter().map(|key| key.projection().to_owned()).collect();
        assert_eq!(names, ["probe_pings_1", "probe_pings_2"]);
        assert_eq!(
            engine(Some("5"), None).await.err(),
            Some("ENCORE_LOAD_VIEWS: must be at most 4".to_owned())
        );
        assert_eq!(
            engine(None, Some("5")).await.err(),
            Some("ENCORE_LOAD_TASKS: must be at most 4".to_owned())
        );
        assert!(engine(Some("many"), None).await.is_err());
    }
}
