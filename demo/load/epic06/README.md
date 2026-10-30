# Замеры эпика 06

Всё для замеров [эпика 06](../../docs/backlog/06_code_guidelines/README.md) (код `cqrsx` по правилам) — в этом
каталоге, отдельно от `cqrsx`, кода demo и постоянного стенда `demo/load` (решение пользователя 2026-10-30). Снаружи на
каталог ничего не ссылается, кроме документов эпика. Задача [08](../../docs/backlog/06_code_guidelines/08_measurement_after.md)
удаляет его целиком (`git rm -r demo/load/epic06`), когда числа «после» записаны.

## Что здесь

```
epic06/
  Cargo.toml, Cargo.lock   свой workspace; lock начат с demo/Cargo.lock — версии те же, что в сборке demo
  backend/                 бэкенд стенда: crate epic06-backend
    src/main.rs            main.rs demo плюс .module(probe::module)
    src/lib.rs, src/probe* синтетический модуль probe и его тесты
  allocations/             подсчёт аллокаций: crate epic06-allocations, тест tests/allocations.rs (#[ignore])
  Dockerfile               образ бэкенда стенда с probe; контекст сборки — корень репозитория
  k6/probe.js              сценарий k6 для probe
  perf/
    build.sh               сборка образа encore-load-backend:<тег>
    series.sh, reap.py     серия вперемешку под сабрипером, каждый прогон не дольше 5 минут
    compose.probe.yaml     надстройка стенда (LOAD_COMPOSE_EXTRA): переменные probe для бэкенда, каталоги для k6
    check.plan             проверочные прогоны задачи 01 (девять конфигураций, REPEAT=1)
    series.plan            серия «до → после» (шесть конфигураций, REPEAT=3)
```

## Модуль `probe`

Свой агрегат `Probe`: одна команда `Ping`, одно событие, маршрут `POST /api/probe/{id}`. Бэкенд — это `main.rs` demo
(модули `concert`, `fan`, `operations` из библиотеки `encore_backend` по пути) и ещё `probe`. Переменные:

| Переменная | Значения | Что делает |
|---|---|---|
| `ENCORE_LOAD_VIEWS` | 0–4, по умолчанию 0 | K витрин `probe_pings_1…K`, каждая считает пинги своего id |
| `ENCORE_LOAD_TASKS` | 0–4, по умолчанию 0 | M пустых задач `encore_probe_echo` на событие, у задачи поле `#[label]` |

Проверки (из корня репозитория; всё собирается офлайн и `--locked`):

```bash
cargo build --locked --manifest-path demo/load/epic06/Cargo.toml --workspace --all-targets
cargo fmt --manifest-path demo/load/epic06/Cargo.toml --all --check
cargo clippy --locked --manifest-path demo/load/epic06/Cargo.toml --workspace --all-targets -- -D warnings
cargo test --locked --manifest-path demo/load/epic06/Cargo.toml --workspace
```

## Подсчёт аллокаций

Тест на движке в памяти (`Cqrs::in_memory()`), `cqrsx` по пути с features по умолчанию — так, как его собирал
`cargo test -p cqrsx`. Числа на одном коммите одинаковы до байта от запуска к запуску:

```bash
cargo test --release --locked --manifest-path demo/load/epic06/Cargo.toml -p epic06-allocations -- --ignored --nocapture
```

Только `-p epic06-allocations`: с `--workspace` cargo объединил бы features `cqrsx` с features бэкенда.

## Стенд

Образы — тем же `encore-load-backend`, что и у постоянного стенда; собираются до серии, во время серии ничего не
собирается и не тестируется. Пока поднят стенд проекта `encore-load`, `build.sh` не собирает.

```bash
S=<рабочий каталог сессии>
SCRATCH=$S demo/load/epic06/perf/build.sh e06-base $S/build.journal a270a06   # движок и demo коммита + probe из дерева
SCRATCH=$S demo/load/epic06/perf/build.sh e06-03 $S/build.journal             # рабочее дерево как есть
SCRATCH=$S SEED=e06c demo/load/epic06/perf/series.sh demo/load/epic06/perf/check.plan 1 $S/series.journal
```

- `build.sh <тег> <журнал> <коммит>` собирает из `git archive <коммит>`, поверх которого положен этот каталог из
  рабочего дерева: движок и demo коммита, `probe` — сегодняшний. Без коммита — из рабочего дерева.
- `series.sh` запускает `perf/run.sh` эпика 02 (профили `PROFILE=app|db`, `pg_stat_statements`, `BACKEND_IMAGE`) и
  добавляет к каждому прогону `compose.probe.yaml`. Строка плана: `<префикс> <тег образа> <сценарий> NAME=<точка>
  КЛЮЧ=ЗНАЧЕНИЕ …`, `NAME` обязателен. `LOAD_HOST_DIR` (путь `demo/load` на хосте) задан в `series.sh`.
- **Сценарий probe** — `../epic06/k6/probe`: `run.sh` стенда не менялся, он ищет `k6/<сценарий>.js` и запускает в
  контейнере `/scripts/<сценарий>.js`, а относительный путь выводит из `k6/` в этот каталог. `compose.probe.yaml`
  монтирует `epic06/k6` в `/epic06/k6` и `k6/` стенда ещё раз в `/k6`, поэтому импорты `../../k6/lib/…` в
  `probe.js` находят библиотеку стенда и в контейнере, и в дереве. Запуск одного прогона без серии — так же, через
  `perf/run.sh` эпика 02 с `LOAD_COMPOSE_EXTRA=<этот каталог>/perf/compose.probe.yaml` и `NAME=…`.
- Результаты — в `demo/load/results/` рядом с прогонами стенда; сравнение «до → после» —
  `node demo/docs/backlog/02_postgres_performance/perf/tools/compare.mjs demo/load/results e06s-base- e06s-head- --md`.
