# 07. Крейт `cqrsx-engine`: движок, `serve`, `host`, `logging`

| | |
|---|---|
| Статус | предложено |
| Ценность | высокая: сборка приложения отделена от компонентов; без этого адаптеры не могут стать крейтами (цикл) |
| Цена | средняя–высокая: новый крейт, `impl Cqrs` по файлам, тесты движка, демо |
| Зависимости | 06 |

## Сейчас

`engine` (около 2 600 строк без тестов, 88 `cfg(feature)`) лежит в ядре и знает о PostgreSQL, MongoDB и HTTP. `host`
(152 строки) и `logging` (64 строки) — отдельные модули и фичи, но их зовёт только `engine::serve`. У движка в памяти
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
- **Помощники:** логи движка (`log_unregistered_aggregate`, `log_missing_executor`) переезжают из `diagnostics`.
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
