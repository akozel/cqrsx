# 07. Крейт `cqrsx-engine`: движок, `serve`, `host`, `logging`

| | |
|---|---|
| Статус | сделано 2026-10-31 ([результаты](#результаты)) |
| Ценность | высокая: сборка приложения отделена от компонентов; без этого адаптеры не могут стать крейтами (цикл) |
| Цена | средняя–высокая: новый крейт, `impl Cqrs` по файлам, тесты движка, демо |
| Зависимости | 06 |

## Сейчас

`engine` (около 3 200 строк без тестов, 88 `cfg(feature)`) лежит в ядре и знает о PostgreSQL, MongoDB и HTTP. `host`
(314 строк) и `logging` (64 строки) — отдельные модули и фичи, но их зовёт только `engine::serve`. У движка в памяти
публичные `journal()`, `outbox()`, `checkpoints()` возвращают хранилища памяти и на движке поверх PostgreSQL — пустые.

## Станет

**Крейт `cqrsx-engine`** (зависит от `cqrsx`):

| Сейчас | Станет | Что |
|---|---|---|
| `engine.rs` (`Cqrs`, его поля и публичные методы) | `src/lib.rs` | центральный тип крейта: `cqrsx_engine::Cqrs` |
| `engine.rs` (`install_aggregate`, `executor`, `install_view`, …) | `src/build.rs` | рядом с `Builder` |
| `engine.rs` (`Executor`, адаптер наблюдателя `Commands`) | `src/executor.rs` | |
| `engine/{build,module,setup,settings,outcome,views,memory}.rs` | `src/…` | |
| `engine/{postgres,mongodb}.rs` | `src/…` | фичи `postgres`, `mongodb` |
| `engine/{http,serve}.rs` | `src/…` | фича `http` |
| `host/{lifecycle,signal,error}.rs`, `logging.rs` | `src/serve/…` | закрытые части процесса; публичным остаётся то, что видно из `serve` (`StartupError` и то, что он оборачивает) |
| `engine/test_support.rs` | `src/test_support.rs` | |
| `cqrsx/tests/engine/*`, `tests/logging.rs` | `cqrsx-engine/tests/…` | `log_fallback.rs` — туда, где лежит то, что он проверяет (план сверяет) |

- **Фичи движка:** `postgres = ["cqrsx/postgres", "dep:sqlx"]`, `mongodb = ["postgres", "cqrsx/mongodb",
  "dep:mongodb"]`, `http = ["dep:axum", "dep:tower-http", "dep:tracing-subscriber", "tokio/net", "tokio/signal"]`,
  `default = []`. В ядре остаются только `postgres` и `mongodb` — до задачи 08.
- **Закрытые функции ядра, которыми пользуется движок** (проверено 2026-10-31): в рабочем коде —
  `TaskDispatcher::assemble`, `TaskHandlers::assembled`, `Dependencies::shared`, `Dependencies::merged`,
  `locking::{lock, get_mut}`, `diagnostics::log_unregistered_aggregate`; в тестах движка — `CommandExecutorCore::framework`,
  `CommandExecutorCore::lock_limits`, `CqrsFramework::store` (после 06 — закрытая попытка), `PersistedEventStore::snapshot_interval`.
  Рабочие становятся публичными с документацией (это сборка обработчиков и зависимостей для того, кто собирает
  приложение) или их работа переезжает в движок — выбор по каждой в плане; тестовые — тест переходит на наблюдаемое
  поведение (настройки видны в ответах и логах) или переезжает в ядро. Публичный API ради одного теста не появляется.
- **Помощники:** лог движка `log_unregistered_aggregate` переезжает из `diagnostics`; `log_missing_executor` остаётся в
  ядре — его зовёт `worker/context.rs` (исправлено при утверждении плана 2026-10-31).
  Закрытый `cqrsx/src/locking.rs` становится публичным `cqrsx/src/mutex.rs` (`cqrsx::mutex::{lock, get_mut}`, решение 6
  эпика): документация модуля описывает контракт для пользователя — под мьютексом одна короткая операция над
  контейнером, без пользовательского кода и без `.await`; блокировка, которая охраняет изменение в несколько шагов, сюда
  не идёт. Лог отравления остаётся в закрытом `diagnostics` ядра. Движок берёт мьютексы процессоров и описаний
  `Setup` через него.
- **Хранилища памяти:** `journal()`, `outbox()`, `checkpoints()` отдают хранилище, только если оно и правда в памяти
  (`Option`, как уже у `documents()`); точную форму предлагает план.
- **Рабочая область:** корневой `Cargo.toml` получает `[workspace.dependencies]` для общих версий и `[workspace.lints]`
  с нынешними lints; крейты пишут `lints.workspace = true`.

## Поведение не меняется: чем доказано

Тесты движка переезжают без изменений проверок; эталон настроек (`describe`, `GET /api/config`) и остальные эталоны
демо байт в байт; `serve` демо стартует и останавливается тем же путём (тесты `demo/back-end/tests/http.rs`).
Единственное видимое изменение — `journal()`/`outbox()`/`checkpoints()` на движке поверх PostgreSQL отвечают «нет»
вместо пустого хранилища; демо и тесты на PostgreSQL их не зовут, план это сверяет.

## Демо и документы

`demo/Cargo.toml`: `cqrsx-engine = { path = "../cqrsx-engine", features = ["postgres", "mongodb", "http"] }`, у `cqrsx`
фичи уходят; импорты `cqrsx::engine::…` → `cqrsx_engine::…`; `tests/architecture.rs`: презентации —
`cqrsx_engine::Cqrs`, запреты application — `cqrsx_engine::serve`… вместо `cqrsx::host`, `cqrsx::logging`.
`CLAUDE.md` (крейты, прогоны фич), `components.md`, README.

## Результаты

2026-10-31, на HEAD `cc9e44c` плюс правки задачи.

- **Крейт `cqrsx-engine`:** `Cqrs` (в `lib.rs`), `Builder` и сборка (`build.rs`), `Executor` (`executor.rs`), `Module`,
  `Setup`, `Settings`, запросы витрин, точки входа команд, `serve`; `host` и `logging` — закрытые части `serve`, публичны
  `Builder::serve`, `Cqrs::serve`, `StartupError`, `ReadSideStopped`. Ядро не зависит от движка (`grep cqrsx_engine
  cqrsx/src` пуст). Фичи движка: `postgres`, `mongodb` (включает `postgres`), `http`, по умолчанию — ничего; у ядра
  остались `postgres` и `mongodb` (до задачи 08).
- **Закрытое ядра, которым пользовался движок:** `TaskDispatcher::assemble`, `TaskHandlers::assembled`,
  `Dependencies::{shared, merged}` — публичные с документацией; `cqrsx::mutex::{lock, get_mut}` — публичный модуль с
  контрактом (решение 6); `log_unregistered_aggregate` — в движке; `impl Display for StatementTimeout` — у своего
  типа (правило 15); пределы блокировок — в `Debug` исполнителя, тесты движка читают настройки через `Debug`; четыре
  тестовых доступа ядра удалены. `journal()`, `outbox()`, `checkpoints()` отвечают `Option`.
- **Рабочая область:** `[workspace.dependencies]` и `[workspace.lints]`, `lints.workspace = true`. `unused_mut` без фич
  исправлен (`expect`). README ядра разделён: Quick start (doctest), «Running on PostgreSQL» и «Configuration» — в
  README движка дословно. CI — матрицы фич ядра и движка (с `http`, которого не было), покрытие движка.
- **Правило архитектуры демо** распознаёт все крейты фреймворка (`FRAMEWORK`) — домену `cqrsx_engine` запрещён,
  презентации разрешён только `cqrsx_engine::Cqrs`, слою application запрещены `cqrsx_engine::serve`, `cqrsx_postgres`,
  `cqrsx_mongodb`; закреплено тестом `every_crate_of_the_framework_is_under_the_rules`.
- **Тесты:** 877 (было 875: тест `Display` у `StatementTimeout`, часть `log_fallback.rs` про `HostError` — юнит-тест
  движка в дочернем процессе), демо 175 (+1 тест правила), ignored 0. Покрытие строк: рабочая область 98,23 %, `cqrsx`
  98,05 %, `cqrsx-engine` 98,35 %. Эталоны демо без изменений.
- **Временно, до задачи 08:** тесты движка подключают фикстуры контейнеров ядра через `#[path]`; копии `capture` и
  `isolated` (около 90 строк) лежат в `cqrsx-engine/src/test_support.rs`; `safe_type` (6 строк) продублирован.
