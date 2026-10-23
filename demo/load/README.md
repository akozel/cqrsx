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
| `mongo` | read models | on the path of every command (synchronous projection) |
| `mail` | nginx stub for `POST /mail` | every e-mail delivered at once: no chaos, no latency |
| `k6` | `grafana/k6`, started per run | scripts mounted read-only, results copied out of the container |

### CPU isolation

The core budget for a 16-core machine is back-end 4, PostgreSQL 4, MongoDB 3, k6 + mail stub 4, one
core left to the host. It was set on 2026-10-18 from the measured peaks: the back-end was the tightest
component (150 of 200 % on average at the booking plateau), PostgreSQL used ~1.4 cores during bursts
and up to 4 only while 32 workers drained the outbox, MongoDB 1.0–1.2 cores, and k6 ran at 98 % of its
2 cores on the announce storm and again at 97 % of 3 cores once the pool was 20. The first batches
(before the `bal-` runs) were measured with 2 / 6 / 2 / 2, the `bal-` and `pool-` runs with 4 / 4 / 3 / 3;
every run records its budget in `result.json` (`meta.cpu_budget_cores`).
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
./matrix.sh                                          # the whole first-iteration matrix (below)
./matrix.sh PREFIX=c1-                               # the same matrix as a second batch: series c1-01-hot-key, …
./report.sh                                          # one table over results/*/result.json
./smoke.sh                                           # functional check of the fan ledgers (D-14) on a fresh stand, no k6
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
| `backend.log` | back-end log of the run |
| `summary.json` | k6's full end-of-test data |

Every test runs a closed loop: `VUS` virtual users send requests back to back for 3 s of warm-up
plus 10 s of measured window (`WARMUP`, `DURATION`). Requests are tagged by phase and the summary
reports the measured phase only, so the cold caches of a fresh stand stay out of the numbers.
Ten seconds is enough to find a ceiling; for publishable p99 figures rerun the interesting points
with `DURATION=60s`.

## Tests

| # | Script | Parameters | What it shows |
|---|---|---|---|
| 1 | `hot-key` | `VUS` 5 / 10 / 20 / 40 | One concert, i.e. one event stream. Each VU owns a seat and alternates book / cancel. Measures the serialisation of a single stream: share of 409 (a version race the back-end could not win in three retries), latency, the snapshot CAS every 10th event and the synchronous CAS on the one `concert_halls` document. Above 40 VUs seats are shared and 422s are noise. |
| 2a | `announce-storm` | `VUS` 10 / 50 / 100 | A new aggregate per request. The lightest command (no task, no snapshot, one Mongo insert): the ceiling of new-stream writes without contention. |
| 2b | `spread-bookings` | `VUS` 10 / 50 / 100 | Full write path with no contention: each VU fills concerts of its own. Every accepted booking publishes an outbox task. Compare with 1 for the cost of contention and with 2a for the cost of tasks and snapshots. |
| 3a | `spread-bookings` + `DRAIN=1` | `WORKERS` 2 / 8 / 32 | The burst of 2b, then `run.sh` samples `transactional_outbox` until no task is open. Reports drain seconds and tasks per second: the throughput of the worker pool (claim → `Fan` command → wallet projection → follow-up task → mail stub). |
| 3b | `spread-bookings` + `DRAIN=1 FAN_MODE=hot` | `WORKERS` 8 | Every booking is for one fan. Before decision D-14 that meant one `Fan` stream and one wallet document growing without bound: the drain decayed 120 → 12 tasks/s and never finished (run `20261018-144205`). Since D-14 the fan has a ledger per concert, so this is the regression check: the drain must look like 3a. |

What the tests deliberately do not cover yet: the read side (`GET /api/concerts`, time machine),
rebuild / verify timing, SSE fan-out, soak, dependency outages. They are the next iteration.

## Reading the numbers

- `rps` is requests in the measured window divided by `DURATION`. In a closed loop it is the
  throughput at that concurrency, not an arrival rate.
- Outcomes: `accepted` (200), `conflict` (409, retries exhausted), `rejected` (422 by code),
  `unavailable` (503, storage), `locked` (423, never expected here), `projection_warning` (200 with
  `projection_failed`: committed, read model not updated).
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

- `WORKERS`: outbox workers and queue capacity together. They share the PostgreSQL pool with HTTP and
  the poller.
- `POOL`: the size of that pool (`ENCORE_DATABASE_POOL_SIZE`, default 10). The 2026-10-18 runs put the
  write-path plateaus (~7.3k announces/s, ~4.3k bookings/s) on this ceiling: nothing else was saturated.
- `SNAPSHOT_EVERY`: compare 10 with 1000 in test 1 to see what snapshots buy on the command path.
- `LOCK_MAX_WAIT_MS` and `LOCK_MAX_WAITERS`: the back-end runs the commands to one concert one at a time
  under its lock, in arrival order; these bound the wait for the lock, past them a command gets 503. Before
  backlog 02/08c an `ORDERING` parameter could also let them race (`concurrent`); `meta.command_ordering` in
  an older `result.json` records it, and `default` there means `concurrent` for images built before 08b.
- `PROJECTION_BUFFER` and `PROJECTION_MAX_BATCH`: a command hands its events to a buffer per concert or fan
  ledger and read model, applied in commit order and in batches after the command released its lock. Before
  backlog 02/08d a `DELIVERY` parameter could also let a command update the read models itself (`inline`);
  `meta.projection_delivery` in an older `result.json` records it. `PROJECTION_BUFFER` bounds that buffer (64 commands per aggregate), `PROJECTION_MAX_BATCH` the commands per
  read-model update (32; 1 = no batches).
- `VERIFY=1`: after the burst, wait for the chain tasks, then compare the read models with a rebuild from
  the journal (`verify.json`, `verify` in `result.json`). Adds a few seconds per run.
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
  `hook_before_k6`, `hook_after_k6`, `hook_after_drain` (`DRAIN=1` only) and `hook_after_collect` are
  called at those points and see `RUN_DIR`, `compose` and `log`. A failing hook is logged, the run goes on.

Backlog epic 02 measures its tickets this way: `docs/backlog/02_postgres_performance/perf/` (statement
statistics, work per command, repeats with medians).

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
  smoke.sh            functional check of the fan ledgers: two concerts, one fan, a cancellation, verify
  tools/collect.mjs   assembles result.json: k6 numbers + meta + container CPU/memory + drain
  tools/report.mjs    the terminal table
  tools/report-html.mjs  the HTML report with charts (inline SVG, no dependencies)
  tools/compare.mjs   before/after table of two batches (by NAME, e.g. "" vs c1-)
  k6/
    hot-key.js  announce-storm.js  spread-bookings.js
    lib/config.js     parameters, the constant-VUs scenario, phase tagging
    lib/api.js        the command endpoint and outcome classification
    lib/metrics.js    outcome counters and the sub-metric declarations
    lib/summary.js    handleSummary: text block + result.json
    lib/ids.js        the hall (A1–E8)
  results/            one folder per run (git-ignored)
```
