# Load tests (k6) — technical limits of the Encore back-end

A proof-of-concept harness that answers one question per test: where does the stack stop? It
measures the back-end, PostgreSQL (event store + outbox) and MongoDB (read models). The UI, the
sidecar and the business rules are out of scope; the sidecar is replaced by a mail stub.

## Stand

`compose.yaml` builds an isolated stand, project `encore-load`, next to (never inside) the demo's
own stack: separate network, separate volumes, no published ports. Every run starts from empty
databases and destroys them afterwards (`down -v`).

| Service | What | Notes |
|---|---|---|
| `backend` | release build of `demo/back-end/Dockerfile` | `RUST_LOG=warn`, outbox workers and snapshot interval from the run parameters |
| `postgres` | event store + outbox, the library's own schema | same image as the demo |
| `mongo` | read models and the projections' checkpoints | off the command path: the projection processors follow the journal |
| `mail` | nginx stub for `POST /mail` | every e-mail delivered at once: no chaos, no latency |
| `k6` | `grafana/k6`, started per run | scripts mounted read-only, results copied out of the container |

### CPU isolation

The core budget for a 16-core machine is back-end 4, PostgreSQL 4, MongoDB 3, k6 + mail stub 4, one
core left to the host. It follows the measured peaks: the back-end is the tightest component (150 of
200 % on average at the booking plateau on two cores), PostgreSQL uses ~1.4 cores during bursts and up to 4
only while 32 workers drain the outbox, MongoDB 1.0–1.2 cores, and k6 saturates 2–3 cores on the announce
storm. Every run records its budget in `result.json` (`meta.cpu_budget_cores`), so runs with different
budgets stay apart in a comparison.
`run.sh` enforces the budget in one of two ways (`PIN=auto` is the default, `./run.sh --check` tells
which one will be used):

| Mode | Overlay | When | Trade-off |
|---|---|---|---|
| `cpuset` | `compose.pin.yaml` | the `cpuset` cgroup controller is available (docker, or podman with delegation) and the machine has 15 cores or `LOAD_CPUSET_*` is set | best: dedicated cores, no throttling artefacts |
| `quota` | `compose.quota.yaml` | `cpuset` is not delegated (rootless podman by default) | the budget holds, but a quota throttles bursts and makes p99 noisier |
| `0` | none | `PIN=0` | no isolation; numbers depend on what else runs |

Override the pinned cores with `LOAD_CPUSET_BACKEND`, `LOAD_CPUSET_POSTGRES`, `LOAD_CPUSET_MONGO`,
`LOAD_CPUSET_K6` (e.g. `LOAD_CPUSET_BACKEND=0-1 LOAD_CPUSET_POSTGRES=2-5 LOAD_CPUSET_MONGO=6-7
LOAD_CPUSET_K6=8-9` on a 12-core machine), the quotas with
`LOAD_CPUS_*`.

Rootless podman only gets the controllers systemd delegates to the user session, and `cpuset` is
not among the defaults, hence `crun: controller cpuset is not available`. To get pinning, delegate it
once (needs root), then log out and in again:

```bash
sudo mkdir -p /etc/systemd/system/user@.service.d
printf '[Service]\nDelegate=cpu cpuset io memory pids\n' | sudo tee /etc/systemd/system/user@.service.d/delegate.conf
sudo systemctl daemon-reload
# log out and back in, then:
podman info --format '{{.Host.CgroupControllers}}'   # must list cpuset
```

Requirements: docker or podman with compose, and `node` (the result files are assembled by
`tools/collect.mjs`, nothing else is needed on the host). `./run.sh --check` verifies the tooling;
`matrix.sh` runs that check first. The first run builds the back-end image (minutes); later runs
reuse it (`BUILD=1` forces a rebuild).

## Running

```bash
cd demo/load
./run.sh hot-key VUS=20                              # one run, one fresh stand
./run.sh spread-bookings VUS=50 DRAIN=1 WORKERS=8    # burst, then measure the outbox drain
./run.sh spread-bookings VUS=100 WORKERS=8 WARMUP=10s SOAK=4m RATE=1000   # 1000 bookings a second for 4 minutes
./matrix.sh                                          # the whole first-iteration matrix (below)
./matrix.sh PREFIX=c1-                               # the same matrix as a second batch: series c1-01-hot-key, …
./report.sh                                          # one table over results/*/result.json
./smoke.sh                                           # functional check of the fan's ledgers (D-14) on a fresh stand, no k6
./chaos.sh                                           # every chaos scene, one fresh stand each (below)
```

Running the stand from another machine than the one with the container engine (a sandbox over the host's
podman socket): set `LOAD_HOST_DIR` to the host path of this directory so the bind mounts resolve there, and
point `DOCKER_HOST` at the socket. `run.sh --check` reports what it found.

## Report

`matrix.sh` ends with the report; `./report.sh` (or `make load-report` from `demo/`) regenerates it
at any time from whatever is in `results/`:

- a table in the terminal, one line per run;
- **`results/report.html`**: stat tiles, one row of charts per test series (throughput, latency
  percentiles, outcome shares, CPU peaks, outbox drain where measured), the full table and a
  collapsible detail block per run. Self-contained, no network needed: `xdg-open results/report.html`.

Runs are grouped into series by their `NAME` without the `-vus<N>` / `-w<N>` suffix, so the matrix
names (`01-hot-key-vus5` …) line up on one x-axis per series. Every chart has a table view and a
hover tooltip; the page follows the OS light/dark theme.

Two batches of the same matrix (e.g. before and after a change, the second run with `PREFIX=c1-`) are
compared run by run with `node tools/compare.mjs results "" c1-` (`--md` for a Markdown table): req/s,
p50, p99, 409 share, drain figures, back-end CPU and memory, with the relative change.

`run.sh --help` lists every parameter. Each run writes `results/<timestamp>-<NAME>/`:

| File | Content |
|---|---|
| `result.json` | the numbers: rps, p50/p95/p99/max per command, status and outcome counts, CPU and memory per container, drain figures |
| `k6.log` | k6 output including the text summary |
| `stats.tsv` | `docker stats` samples (epoch, container, CPU %, memory), ~every 2–3 s |
| `outbox.tsv` | `DRAIN=1` only: open / completed / failed / total tasks per second |
| `soak.tsv` | `SOAK=…` only: one sample per slice, `epoch  source  key  value` (below) |
| `backend.log` | back-end log of the run |
| `summary.json` | k6's full end-of-test data |

Every test runs a closed loop by default: `VUS` virtual users send requests back to back for 3 s of
warm-up plus 10 s of measured window (`WARMUP`, `DURATION`). Requests are tagged by phase and the
summary reports the measured phase only, so the cold caches of a fresh stand stay out of the numbers.
Ten seconds is enough to find a ceiling; for publishable p99 figures rerun the interesting points
with `DURATION=60s`. `RATE=…` switches to the open model, which a soak run uses (below).

## Constant-rate runs (soak)

A closed loop finds ceilings but hides whether the stand keeps up with a flow: the outbox tasks of a
10-second burst mostly run after the window. `SOAK=<duration>` runs the measured window at a constant
rate instead:

- **Open model.** `RATE=…` starts that many iterations a second (k6 `constant-arrival-rate`), whatever
  the answers, with `VUS` users allocated up front. An iteration that finds every user busy is dropped
  (`dropped_iterations`): the box office did not keep up. Latency then grows instead of the rate falling.
- **Length.** `SOAK` replaces `DURATION`; warm-up plus window is at most `LOAD_SOAK_MAX_S` (300 s, the
  5-minute limit, D-22 in [`../DESIGN.md`](../DESIGN.md)). Minutes of writes are gigabytes: a soak run refuses to start when the
  container storage has less than `LOAD_MIN_FREE_GB` (10) free; `run.sh --check` prints what is free.
- **Slices.** Every `SLICE` (10 s) from k6's start and once more at its end, `run.sh` writes the outbox by
  status to `soak.tsv` and calls `hook_slice` (the `perf/` hooks add table and index sizes, dead rows,
  autovacuum runs, HOT updates, checkpoints, WAL and full-page images, temp bytes). k6 reports requests,
  accepted commands, p50 and p99 per slice of the measured window.
- **`soak` in `result.json`.** `points` (one per slice: open and completed tasks, completed per second, CPU %
  and peak memory per container and peak lag per projection since the slice before, the database's keys),
  `slices` (k6's) and `verdict`.

The verdict: the rate is **sustained** when the box office accepted at least 99 % of it and, by the last slice
under load, neither the open outbox tasks nor the worst projection lag are higher than 60 s after k6 started,
give or take one second of input (`RATE`). `open_growth_per_min` says how fast the queue grows when it is not.
`dropped_iterations` is reported apart: a few dozen of them are stalls of a fraction of a second (every user
waiting at once), not a rate the stand cannot take. The report draws every soak
run over time ("— over time": throughput, latency, open tasks, outbox throughput, projection lag, CPU,
memory and, with the `perf/` hooks, table sizes, dead rows and WAL):

- open tasks that level off after the first minute — the chain keeps up; a line that keeps rising — it
  does not, and its slope is the shortfall in tasks per second;
- throughput below `RATE` or dropped iterations — the box office itself is the limit;
- lag that keeps rising — a projection does not keep up;
- table sizes and dead rows — what minutes of writes leave behind for VACUUM.

The sustained rate is found by halving: 2–3 runs of 2 minutes, raise the rate after a sustained run and
lower it after one that is not; then 5-minute runs at the rate found and 20 % above it.

## Tests

| # | Script | Parameters | What it shows |
|---|---|---|---|
| 1 | `hot-key` | `VUS` 5 / 10 / 20 / 40 | One concert, i.e. one event stream. Each VU owns a seat and alternates book / cancel. Measures the serialisation of a single stream: the commands to one concert wait for its lock in arrival order (503 when the wait is too long), latency, the snapshot every 10th event, and how far the one `concert_halls` document trails the journal. Above 40 VUs seats are shared and 422s are noise. |
| 2a | `announce-storm` | `VUS` 10 / 50 / 100 | A new aggregate per request. The lightest command (no task, no snapshot; the processor inserts one document later): the ceiling of new-stream writes without contention. |
| 2b | `spread-bookings` | `VUS` 10 / 50 / 100 | Full write path with no contention: each VU fills concerts of its own. Every accepted booking publishes an outbox task. Compare with 1 for the cost of contention and with 2a for the cost of tasks and snapshots. |
| 3a | `spread-bookings` + `DRAIN=1` | `WORKERS` 2 / 8 / 32 | The burst of 2b, then `run.sh` samples `transactional_outbox` until no task is open. Reports drain seconds and tasks per second: the throughput of the worker pool (claim → `Fan` command → wallet projection → follow-up task → mail stub). |
| 3b | `spread-bookings` + `DRAIN=1 FAN_MODE=hot` | `WORKERS` 8 | Every booking is for one fan. The fan has a ledger per concert (D-14), so no stream and no wallet document grows with the fan's history: the drain must look like 3a. One stream per fan would make the cost of a task grow with it. |

What the tests do not cover: the read API (`GET /api/concerts`, time machine), rebuild / verify timing, SSE
fan-out. A soak of up to 5 minutes is above, dependency outages are the chaos scenes (below).

## Reading the numbers

- `rps` is requests in the measured window divided by `DURATION`. In a closed loop it is the
  throughput at that concurrency, not an arrival rate.
- Outcomes: `accepted` (200), `conflict` (409, retries exhausted), `rejected` (422 by code),
  `unavailable` (503, storage or a full lock queue). A 200 means the event is in the journal; the read models follow it (D-17).
  `projections` in `result.json` (from `projections.jsonl`, the status once a second) is how far they trailed:
  `lag` p50/p99/max in events over the burst, `caught_up_s` after it, the owners seen and the handovers.
- CPU percentages are `docker stats` figures: 100 % is one core, so the back-end tops out at 200 %
  when pinned to two cores. When the real docker CLI talks to a podman socket (the sandbox case) the
  sampler reads libpod's stats stream instead, once a second; it catches higher peaks than the 2–3 s
  `docker stats` cadence, so compare CPU only between runs sampled the same way. Memory peaks come from
  the cgroup in both cases and compare across batches.
- Memory peak comes from the container's cgroup v2 `memory.peak`; on cgroup v1 the report falls
  back to the maximum sampled by `stats`.
- The outbox's `tasks_per_second` counts tasks completed after the burst ended, over the drain time.
  Accepted bookings create two tasks each (`encore_record_ticket`, then `encore_send_e_ticket`).

## Knobs worth turning

- `WORKERS`: outbox workers. They share the PostgreSQL pool with HTTP and the poller.
- `QUEUE`, `REFILL`, `LEASE`: the claimed tasks waiting for a worker (`OUTBOX_BUFFER_CAPACITY`, default
  `WORKERS`), the percent of it below which the back-end claims again and fills it
  (`OUTBOX_BUFFER_REFILL_PERCENT`, default 25), and the
  task lease (`LEASE` in seconds, default 30; the back-end gets it as `OUTBOX_LEASE_MS`, in milliseconds). The lease must exceed `16 + ceil(QUEUE / WORKERS) * 8` seconds
  with the back-end's handler and storage timeouts, so `run.sh` refuses a `QUEUE` above `WORKERS` without
  `LEASE`. All three are in `meta` (`outbox_queue_capacity`, `outbox_refill_percent`, `outbox_lease_s`).
- `POOL`: the size of that pool (`ENCORE_DATABASE_MAIN_POOL_SIZE`, default 10). The write-path plateaus
  measured on the default stand (~7.3k announces/s, ~4.3k bookings/s) sit on this ceiling: nothing else is
  saturated.
- `MONGO_POOL`: the back-end's MongoDB client pool (`ENCORE_MONGODB_POOL_SIZE`; empty = the back-end's default,
  `meta.mongo_pool` in `result.json`). The read models' writes, the processors' checkpoints and the HTTP reads
  share it.
- `SNAPSHOT_EVERY`: compare 10 with 1000 in test 1 to see what snapshots buy on the command path.
- `LOCK_MAX_WAIT_MS` and `LOCK_MAX_WAITERS`: the back-end runs the commands to one concert one at a time
  under its lock, in arrival order (`ENCORE_COMMAND_LOCK_MAX_WAIT_MS`, `ENCORE_COMMAND_LOCK_MAX_WAITERS`); these
  bound the wait for the lock, past them a command gets 503.
- `PROJECTION_KEY_BUFFER` and `PROJECTION_MAX_BATCH`: a projection processor reads the journal by its checkpoint and
  hands the events to a buffer per document, applied in order and in batches. `PROJECTION_KEY_BUFFER`
  (`ENCORE_PROJECTION_KEY_BUFFER`) bounds that buffer (64 per aggregate), `PROJECTION_MAX_BATCH`
  (`ENCORE_PROJECTION_MAX_BATCH`) the events per read-model update (32; 1 = no batches).
- `VERIFY=1`: after the burst, wait for the chain tasks and for the projections to catch up, then compare every
  document with the history of its aggregate (`verify.json`, `verify` in `result.json`: `ok`, `behind_documents`
  must be 0). Adds a few seconds per run.
- `LOAD_CHAOS=chaos/<scene>.sh`: a fault injected while k6 runs (the chaos scenes, below), its steps in
  `chaos.tsv`.
- `LOAD_OUTBOX_POLL_MS` (compose env): the poller's idle cadence, 300 ms by default.
- PostgreSQL runs with the image defaults (`fsync`, `synchronous_commit` on). That is part of the
  limit being measured; turn it off only as an explicitly labelled experiment.

## Extension points

Measurement tools that live outside this directory plug into a run without copying it:

- `LOAD_COMPOSE_EXTRA=a.yaml:b.yaml` adds compose files after the CPU isolation overlay (e.g. PostgreSQL
  settings). Relative paths inside them resolve against this directory, like those of `compose.yaml`.
- `LOAD_SERVICES_EXTRA="a b"` starts more services (defined by those compose files) with the stand. Each
  must become healthy before k6 starts, and its log is saved next to `backend.log` as `<service>.log`.
  Their order is up to `depends_on` in the compose files.
- `LOAD_HOOKS=hooks.sh` is sourced once the run folder exists. The functions it defines out of
  `hook_before_k6`, `hook_after_k6`, `hook_after_drain` (`DRAIN=1` only), `hook_slice EPOCH` (`SOAK=…`
  only, every slice: append `EPOCH  <source>  <key>  <value>` lines to `soak.tsv`) and `hook_after_collect`
  are called at those points and see `RUN_DIR`, `compose` and `log`. A failing hook is logged, the run goes
  on.

The PostgreSQL measurements use them: [`perf/`](perf/README.md) (statement statistics, work per command, repeats
with medians, two back-end nodes behind nginx).

## Chaos scenes

`chaos.sh` runs the scenes of the read side under load, one fresh stand each, at most 5 minutes a run (D-21,
D-22): each scene injects a fault while k6 runs, then `chaos/check.mjs` holds the result to its criteria —
commands accepted equal events written (or stay within bounds when an answer can be lost), the projections caught
up, verify ok with no document behind. Exit 0 means every run passed.

```bash
./chaos.sh                          # every scene once
./chaos.sh mongo-pause wipe REPEAT=3 VUS=40 DURATION=40s
```

| Scene | Script | Fault |
|---|---|---|
| `mongo-pause` | `chaos/mongo-pause.sh` | MongoDB paused, then resumed |
| `kill-9`, `kill-term` | `chaos/kill-owner.sh` | the node that owns a checkpoint killed (`SIGKILL` or `SIGTERM`); two nodes behind nginx through `perf/run.sh` |
| `postgres-restart` | `chaos/postgres-restart.sh` | PostgreSQL restarted |
| `encore-0`, `encore-80` | `chaos/encore.sh` | Encore! under load, with no pause or 80 ms between events |
| `wipe` | `chaos/wipe.sh` | the read models and their checkpoints dropped under load |

## Layout

```
load/
  compose.yaml        the stand (no ports, mail stub, k6 service)
  compose.pin.yaml    CPU pinning overlay (cpuset)
  compose.quota.yaml  CPU quota overlay, the fallback without cpuset delegation
  mail/nginx.conf     the mail stub
  run.sh              one run: fresh stand → k6 → [drain] → collect → down -v
  matrix.sh           the first-iteration matrix
  report.sh           terminal table + results/report.html
  smoke.sh            functional check of the fan's ledgers: two concerts, one fan, a cancellation, verify
  chaos.sh            every chaos scene on its own stand, then the criteria
  chaos/              one script per scene (encore, kill-owner, mongo-pause, postgres-restart, wipe), check.mjs
  perf/               PostgreSQL measurements: hooks, two-node stand, repeats (perf/README.md)
  tools/collect.mjs   assembles result.json: k6 numbers + meta + container CPU/memory + drain
  tools/report.mjs    the terminal table
  tools/report-html.mjs  the HTML report with charts (inline SVG, no dependencies)
  tools/soak-section.mjs the "over time" section of a soak run, shared with the perf/ report
  tools/compare.mjs   before/after table of two batches (by NAME, e.g. "" vs c1-)
  k6/
    hot-key.js  announce-storm.js  spread-bookings.js
    lib/config.js     parameters, the scenario (constant VUs, or a constant rate), phase and slice tagging
    lib/api.js        the command endpoint and outcome classification
    lib/metrics.js    outcome counters and the sub-metric declarations
    lib/summary.js    handleSummary: text block + result.json
    lib/ids.js        the hall (A1–E8)
  results/            one folder per run (git-ignored)
```
