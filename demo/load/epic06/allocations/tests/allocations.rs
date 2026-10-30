//! Allocations of the typical scenario on the engine in memory, counted by the global allocator: a command and its one
//! event, the views that fold that event, the tasks its reaction plans. Backlog epic 06, ticket 01: the numbers before
//! the fixes of ticket 04 and after them, exact where a load stand sees only noise.
//!
//! `#[ignore]`: it measures, it checks nothing of the engine. Run it from the repository root with
//! `cargo test --release --locked --manifest-path demo/load/epic06/Cargo.toml -p epic06-allocations -- --ignored --nocapture`.
//!
//! - One aggregate type with one command and one event; K views of that event (K ∈ {1, 4}); a reaction that plans M
//!   tasks per event (M ∈ {0, 1, 4}), of a type with a `#[label]` field or without one.
//! - Every configuration on an engine of its own (`Cqrs::in_memory()`), on a `current_thread` runtime with a paused
//!   clock: no other thread allocates, and the processors' timers advance only when the runtime is idle, so two runs
//!   count the same.
//! - A warm-up of the whole path first, then the measured phases, each counted on its own: the commands (`execute`),
//!   the tasks (`run_tasks`, after every round of [`ROUND`] commands) and, once at the end, the views (`catch_up`).
//!   Commands and tasks run unconstrained by Tokio's budget, so the processors woken by the commits never run inside
//!   them; the test checks that the views are still [`COMMANDS`] events behind before they catch up.
//! - The counter is `stats_alloc` over the system allocator: a counting allocator of our own would need `unsafe`, which
//!   the code guidelines rule out (rule 32). Allocations are allocations and reallocations; bytes are the bytes
//!   requested, a reallocation's growth included.
//!
//! What the stores in memory add: a page of the journal is read as a copy of every event of the aggregate type
//! (warm-up included), and a task is claimed after a scan of the pending ones. Both are the same before and after a
//! fix of the engine; the warm-up leaves the journal's and the outbox's vectors with room for the measured events. The
//! table also shows a view's cost without the journal's reads: the two pages every processor reads to catch up (the
//! measured events, then none) are read once more after the measurement, and their allocations subtracted.
// Test code: a panic is the failed check (rule 8 of .claude/rules/rust-style.md); clippy.toml allows it only inside
// `#[test]` functions, not in the helpers of this crate.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::indexing_slicing
)]
use cqrsx::{
    Command, Event, Events, Rejection, Task,
    cqrs::{
        aggregate::Aggregate,
        command::decide::Decide,
        event::EventEnvelope,
        projection::View,
        reactions::{Decided, Reactions, Then},
        store::repository::PersistedEventRepository,
    },
    engine::{Cqrs, module::Module},
    task::result::TaskResult,
    task_handler,
};
use serde::{Deserialize, Serialize};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, Stats, StatsAlloc};
use std::{alloc::System, fmt::Write as _, future::Future};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// The aggregates the commands go to, in turn: each gets a snapshot every tenth event and replays up to nine.
const KEYS: usize = 10;
/// Commands of the warm-up. With the measured ones the journal grows from 1048 to 2048 events and the outbox from
/// 1048·M to 2048·M tasks: their vectors grow before the measurement, never inside it.
const WARM_UP: usize = 1048;
/// Measured commands; also one page of a view's processor (1000 events by default).
const COMMANDS: usize = 1000;
/// Commands between two runs of the tasks: the in-memory queue scans the pending tasks for every claim, so they are
/// kept few, as a worker's buffer keeps them.
const ROUND: usize = 10;

/// The aggregate: it counts its events.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Meter {
    ticks: u64,
}

/// The one event of the one command.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Event)]
#[event(name = "ticked", version = "1")]
struct Ticked {
    at: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Events)]
enum MeterEvent {
    Ticked(Ticked),
}

/// The tasks a tick plans: the command carries them, so the event is the same in every configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Plan {
    /// `Echo` tasks, without a label.
    Plain(u8),
    /// `LabeledEcho` tasks: the key goes into the task's metadata.
    Labeled(u8),
}

#[derive(Clone, Debug, Command)]
struct Tick {
    plan: Plan,
}

/// A meter refuses nothing.
#[derive(Debug, thiserror::Error, Rejection)]
enum MeterError {}

impl Aggregate for Meter {
    const TYPE: &'static str = "meter";
    type Id = String;
    type Command = Tick;
    type Event = MeterEvent;
    type Error = MeterError;
    type Rules = ();

    fn apply(&mut self, MeterEvent::Ticked(ticked): MeterEvent) {
        self.ticks = ticked.at;
    }
}

impl Decide<Meter> for Tick {
    fn decide(self, meter: &Meter, _: &()) -> Result<MeterEvent, MeterError> {
        Ok(MeterEvent::Ticked(Ticked { at: meter.ticks + 1 }))
    }
}

impl Reactions for Meter {
    fn react(decided: &Decided<'_, Self>, then: &mut Then) {
        match decided.event {
            MeterEvent::Ticked(_) => match decided.command.plan {
                Plan::Plain(tasks) => {
                    for slot in 0..tasks {
                        then.task(Echo { slot });
                    }
                }
                Plan::Labeled(tasks) => {
                    for slot in 0..tasks {
                        then.task(LabeledEcho {
                            key: decided.id.clone(),
                            slot,
                        });
                    }
                }
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Task)]
#[task(name = "echo", version = 1)]
struct Echo {
    slot: u8,
}

#[derive(Debug, Serialize, Deserialize, Task)]
#[task(name = "labeled_echo", version = 1)]
struct LabeledEcho {
    #[label]
    key: String,
    slot: u8,
}

#[task_handler]
async fn echo(_task: Echo) -> TaskResult {
    TaskResult::done()
}

#[task_handler]
async fn labeled_echo(_task: LabeledEcho) -> TaskResult {
    TaskResult::done()
}

/// A view of the meters, one per `SLOT`: every view reads and folds the events itself.
#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
struct Ticks<const SLOT: u8> {
    ticks: u64,
}

macro_rules! views {
    ($($slot:literal => $name:literal),+) => {
        $(
            impl View<Meter> for Ticks<$slot> {
                const NAME: &'static str = $name;

                fn update(&mut self, event: &EventEnvelope<Meter>) {
                    let MeterEvent::Ticked(ticked) = &event.payload;
                    self.ticks = ticked.at;
                }
            }
        )+
    };
}

views!(1 => "ticks_1", 2 => "ticks_2", 3 => "ticks_3", 4 => "ticks_4");

/// The views in the order a configuration takes them.
const VIEWS: [fn(Module) -> Module; 4] = [
    Module::view::<Ticks<1>, Meter>,
    Module::view::<Ticks<2>, Meter>,
    Module::view::<Ticks<3>, Meter>,
    Module::view::<Ticks<4>, Meter>,
];

/// One measured configuration: K views, M tasks per event, with labels or without.
#[derive(Clone, Copy, Debug)]
struct Configuration {
    views: usize,
    plan: Plan,
}

impl Configuration {
    fn tasks(self) -> usize {
        match self.plan {
            Plan::Plain(tasks) | Plan::Labeled(tasks) => usize::from(tasks),
        }
    }

    fn module(self) -> Module {
        let views = VIEWS.into_iter().take(self.views);
        let module = views.fold(Module::new("meter").aggregate::<Meter>(()), |module, view| view(module));
        match self.plan {
            Plan::Plain(0) | Plan::Labeled(0) => module,
            Plan::Plain(_) => module.task(echo).without_policy(),
            Plan::Labeled(_) => module.task(labeled_echo).without_policy(),
        }
    }
}

/// The allocations of one phase over every round of a configuration.
#[derive(Default)]
struct Phase {
    allocations: usize,
    bytes: usize,
}

impl Phase {
    /// Runs `work` and adds what it allocated.
    async fn count<T>(&mut self, work: impl Future<Output = T>) -> T {
        let region = Region::new(GLOBAL);
        let output = tokio::task::unconstrained(work).await;
        self.add(region.change());
        output
    }

    fn add(&mut self, change: Stats) {
        self.allocations += change.allocations + change.reallocations;
        self.bytes += change.bytes_allocated;
    }

    /// Allocations and bytes of the whole phase, as one table cell.
    fn total(&self) -> String {
        format!("{} / {}", self.allocations, self.bytes)
    }

    /// This phase without `times` times `other`.
    fn without(&self, other: &Phase, times: usize) -> Phase {
        Phase {
            allocations: self.allocations - other.allocations * times,
            bytes: self.bytes - other.bytes * times,
        }
    }

    /// Allocations and bytes per operation, `separator` between them: two table cells (` | `) or one.
    fn per(&self, operations: usize, separator: &str) -> String {
        if operations == 0 {
            return format!("–{separator}–");
        }
        let operations = operations as f64;
        format!(
            "{:.2}{separator}{:.0}",
            self.allocations as f64 / operations,
            self.bytes as f64 / operations
        )
    }
}

/// Two cells of a Markdown table.
const CELLS: &str = " | ";

/// The phases of one configuration, and the reads of the journal in memory inside its views' phase, of one view.
#[derive(Default)]
struct Measured {
    commands: Phase,
    views: Phase,
    tasks: Phase,
    journal_reads: Phase,
}

/// Rounds of [`ROUND`] commands, each followed by the due tasks, over the keys in turn from the first.
async fn rounds(cqrs: &Cqrs, keys: &[String], configuration: Configuration, commands: usize, measured: &mut Measured) {
    let source = cqrs.from("load");
    let expected = ROUND * configuration.tasks();
    let mut cycle = keys.iter().cycle();
    for _ in 0..commands / ROUND {
        for _ in 0..ROUND {
            let key = cycle.next().unwrap();
            let tick = Tick {
                plan: configuration.plan,
            };
            let executed = measured.commands.count(source.execute::<Meter>(key, tick)).await;
            executed.unwrap();
        }
        assert_eq!(measured.tasks.count(cqrs.run_tasks()).await, expected);
    }
}

/// Builds the engine of `configuration`, warms its whole path up and measures it.
async fn measure(configuration: Configuration) -> Measured {
    let cqrs = Cqrs::in_memory()
        .module(move |_| Ok(configuration.module()))
        .build()
        .await
        .unwrap();
    let keys: Vec<String> = (0..KEYS).map(|key| format!("meter-{key}")).collect();
    let mut warm_up = Measured::default();
    rounds(&cqrs, &keys, configuration, WARM_UP - WARM_UP % ROUND, &mut warm_up).await;
    for key in keys.iter().take(WARM_UP % ROUND) {
        let tick = Tick {
            plan: configuration.plan,
        };
        cqrs.from("load").execute::<Meter>(key, tick).await.unwrap();
    }
    cqrs.run_tasks().await;
    cqrs.catch_up().await;

    let mut measured = Measured::default();
    let start = cqrs.journal().head::<Meter>().await.unwrap();
    rounds(&cqrs, &keys, configuration, COMMANDS, &mut measured).await;
    let names: Vec<&str> = VIEWS_NAMES.iter().copied().take(configuration.views).collect();
    for name in &names {
        let lag = cqrs.views().lag(name).await.unwrap();
        assert_eq!(
            lag, COMMANDS as u64,
            "{name}: a processor ran inside the commands or the tasks"
        );
    }
    let region = Region::new(GLOBAL);
    cqrs.catch_up().await;
    measured.views.add(region.change());
    for name in &names {
        assert_eq!(cqrs.views().lag(name).await.unwrap(), 0, "{name} caught up");
    }
    let end = cqrs.journal().head::<Meter>().await.unwrap();
    let region = Region::new(GLOBAL);
    let page = cqrs.journal().feed_after::<Meter>(start, COMMANDS).await.unwrap();
    let none = cqrs.journal().feed_after::<Meter>(end, COMMANDS).await.unwrap();
    measured.journal_reads.add(region.change());
    assert_eq!((page.len(), none.len()), (COMMANDS, 0));
    measured
}

const VIEWS_NAMES: [&str; 4] = [
    <Ticks<1> as View<Meter>>::NAME,
    <Ticks<2> as View<Meter>>::NAME,
    <Ticks<3> as View<Meter>>::NAME,
    <Ticks<4> as View<Meter>>::NAME,
];

#[test]
#[ignore = "a measurement: cargo test --release -p epic06-allocations -- --ignored --nocapture"]
fn allocations_per_command_per_event_and_view_per_task() {
    let mut configurations = Vec::new();
    for views in [1, 4] {
        configurations.push(Configuration {
            views,
            plan: Plan::Plain(0),
        });
        for tasks in [1, 4] {
            configurations.push(Configuration {
                views,
                plan: Plan::Plain(tasks),
            });
            configurations.push(Configuration {
                views,
                plan: Plan::Labeled(tasks),
            });
        }
    }
    let mut totals =
        String::from("| K | M | label | commands | views | journal reads of one view | tasks |\n|---|---|---|---|---|---|---|\n");
    let mut table = String::from(
        "| K | M | label | allocations / command | bytes / command | allocations / event / view | bytes / event / view \
         | without the journal's reads | allocations / task | bytes / task |\n|---|---|---|---|---|---|---|---|---|---|\n",
    );
    for configuration in configurations {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap();
        let measured = runtime.block_on(measure(configuration));
        let label = match configuration.plan {
            Plan::Plain(0) | Plan::Labeled(0) => "–",
            Plan::Plain(_) => "no",
            Plan::Labeled(_) => "yes",
        };
        writeln!(
            table,
            "| {} | {} | {label} | {} | {} | {} | {} |",
            configuration.views,
            configuration.tasks(),
            measured.commands.per(COMMANDS, CELLS),
            measured.views.per(COMMANDS * configuration.views, CELLS),
            measured
                .views
                .without(&measured.journal_reads, configuration.views)
                .per(COMMANDS * configuration.views, " / "),
            measured.tasks.per(COMMANDS * configuration.tasks(), CELLS),
        )
        .unwrap();
        writeln!(
            totals,
            "| {} | {} | {label} | {} | {} | {} | {} |",
            configuration.views,
            configuration.tasks(),
            measured.commands.total(),
            measured.views.total(),
            measured.journal_reads.total(),
            measured.tasks.total(),
        )
        .unwrap();
    }
    println!(
        "\nallocations on the engine in memory: {KEYS} keys, {WARM_UP} commands of warm-up, {COMMANDS} measured, tasks run \
         every {ROUND} commands\n\n{table}\nallocations / bytes in total: two runs print the same\n\n{totals}"
    );
}
