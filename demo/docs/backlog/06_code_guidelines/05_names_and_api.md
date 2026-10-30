# 05. Имена и типы API

| | |
|---|---|
| Статус | предложено |
| Ценность | средняя: API читается без документации, перестановка аргументов не компилируется, нет устаревшего |
| Цена | высокая: переименования и новые типы в публичном API, правки во всех реализациях и в demo |
| cqrsx | места ниже |
| cqrsx-macros | `src/command.rs` (`labels()` → `Label`) |
| demo | все вызовы переименованного API |
| Зависимости | 04; задача [05/15](../05_engine_api/15_revisit.md) (пересмотр решений по тому же API) |
| Правила | 12–15, 18, 19, 22–25 |

## Сейчас

Аудит, разделы [«API и типы»](../../cqrsx-style-audit.html#api) и [«Имена»](../../cqrsx-style-audit.html#names).
Места — по HEAD `e995a53`.

## Станет

### Имена (правила 22–25)

| Где | Сейчас | Станет |
|---|---|---|
| `LockLimits::new`, `AggregateConflictRetryPolicy::new`, `engine/settings.rs:212`, `engine/postgres.rs:117`, `outbox/write_plan.rs:73`, конфигурации processor (`ProcessorConfig`, `DeliveryLimits`) и задач (`TaskStorageConfig`, `TaskWorkerPoolConfig`, `TaskRuntimeConfig`) | конструкторы с `Result` называются `new` | `try_new`; где проверка сводится к «не ноль» — `NonZero*` и `new` без `Result` |
| `cqrs/store/persisted.rs:235`, `:397` | `new_event_store`, `new_snapshot_store` | `new(repo)`, `with_snapshots(NonZeroUsize)` |
| `cqrs/store/repository.rs:22`, `:28`, `:35` | `get_events`, `get_events_after`, `get_snapshot` | `events`, `events_after`, `snapshot` |
| `cqrs/testing/mem_store.rs:212` | устаревший `get_events` отдаёт `RwLock` | удалить |
| `cqrs/store.rs:25` | ассоциированный тип `EventStore::AC` | `Context` |
| `task.rs:87` | метод builder `scheduled` рядом с `with_metadata` | `with_schedule` |
| `cqrs/testing.rs:150`; `validator.rs:305` | конструктор `TestFramework::with`; `inspect_result(self)` | `new`; `into_result` |
| `engine/mongodb.rs:65`, `:81`, `:86`, `:121` | `app_name` | `application_name`, как в `PoolPlan` |
| `cqrs/testing/command.rs`; `mongodb/projection_checkpoints.rs` | файлы не по главному типу | `command_executor.rs`, `checkpoint_store.rs` |

### Типы (правила 12–15, 19)

| Где | Сейчас | Станет |
|---|---|---|
| `processor/owner.rs:465`, `:498`, `:849` | `spawn_rebuild(key, Duration::ZERO, RebuildReason::Requested, false, Vec::new())`: три сочетания pace, reason, bool, waiters | `enum RebuildJob { Requested, Cursor { pace }, Healed { waiter } }` |
| `cqrs/store/serialized_event.rs:33–41` | `SerializedEvent::new`: 7 позиционных аргументов, 4 `String` подряд | удалить, литерал структуры с именами полей |
| `cqrs/store/upcaster.rs:15`, `:155` | `can_upcast(event_type: &str, event_version: &str)` | `EventFormat { event_type, event_version }` по образцу `TaskFormat` |
| `processor/owner.rs:66–73`, `:261`, `:278–283`; `processor.rs:300`; `lease.rs:111` | `operation`, `category` — `&'static str` из закрытых наборов, рядом | `enum Operation` с `as_str()`; `enum Category` — по решению в плане |
| `projection/verify.rs:89` | `Difference.kind: &'static str` | `enum DifferenceKind` (serde rename, JSON тот же) |
| `cqrs/command.rs:156`; макрос `Commands` | `labels() -> Vec<(&'static str, String)>` | `Label { name, value }` |
| `projection/repository.rs:17–18`, `:44–53`; `engine/views.rs:131` | `load_with_context -> (V, ViewContext)` и `From<(V, ViewContext)>` | `Option<StoredView<V>>` |
| `engine/module.rs:31`, `:45`; `engine/build.rs:309`, `:329–331`; `owner.rs:57`, `:159`, `:585`, `:872` | `Vec<(PolicyName, Retry, i32)>`, псевдоним `TaskRegistration` для кортежа, `(String, Result<..>)` | `PolicyEntry`, `TaskRegistration { adapter, binding }`, `RebuildOutcome { aggregate_id, result }` |
| `describe()` (`engine.rs:176` и настройки); `engine/module.rs:26`, `:131` | `Vec<(String, String)>` по ключу; `observe_projections(Fn(&'static str, &str))` | `BTreeMap<String, String>` (JSON `GET /api/config` тот же); структура с именованными полями для наблюдателя |
| `cqrs/testing/mem_checkpoint_store.rs:49`, `:89–91` | `type RowKey = (String, String)` | ключ `CheckpointKey` (derive `Ord`) |
| `upcaster.rs:43–45`; `engine/postgres.rs:227`; `cqrs.rs:519–526` | `supersedes` повторяет `PartialOrd`; `statement_timeout_text`; ручной `Default` у `CommitReport` | `>`; `impl Display for StatementTimeout`; `#[derive(Default)]` |
| `cqrs/aggregate.rs:226–253`; `mongodb/projection_checkpoints.rs:119`; `engine/settings.rs:517`, `:521` | `AggregateKey` публичный с `impl Into<String>`; внутренние `impl Into<Bson>`, `impl Into<String>` | `pub(crate)`, конкретные типы |
| `task/dispatcher.rs:126`; `task/runtime.rs:63`; `postgres/task_runtime.rs:20` | `impl Into<Arc<TaskAttempt>>`, `impl Into<Arc<TaskDispatcher>>` | `Arc<…>` |
| `cqrs/store.rs:55`; `cqrs.rs:468`, `:473` | `AggregateContext::aggregate(&mut self) -> &mut A`, вызывающие только читают | `aggregate(&self) -> &A` |

### Решить с пользователем в плане

- **`dyn` с одним значением** (правило 18): `processor.rs:143`, `:168`; `rebuild.rs:25`, `:55`; `delivery.rs:109–110`
  — `Arc<dyn RebuildableProjection>`, `dyn EventHandler`, `dyn Rebuilder`. Generic-параметр processor добавит
  параметры типам, а `ProjectionRuntime` и так стирает типы processors. Рекомендация — оставить, если выигрыш не виден в
  замере.

## Этапы

1. Дождаться закрытия задачи 05/15; сверить список с её решениями.
2. План: порядок переименований (сначала трейты хранилища и задач, потом engine), список правок demo.
3. Правки, тесты; `GET /api/config` и тексты SQL — без изменений.
4. `CLAUDE.md`-проверки в cqrsx и demo; коммит.

## Риски

- **Широкий дифф.** Переименования трейтов (`get_*`, `AC`) задевают все реализации в cqrsx и тестах. Только
  переименования — без смены логики в том же коммите.
- **JSON наружу.** `DifferenceKind`, `describe()`, `Label` не должны менять JSON ответов и ленты активности; эталоны
  `demo/back-end/tests/golden/` — проверка.

## Как проверить

- `CLAUDE.md` cqrsx; тесты demo с эталонами `tests/contracts.rs`.

## Критерии приёмки

- [ ] Все строки таблиц сделаны или отклонены пользователем с причиной.
- [ ] Эталоны demo (`effective_config.txt`, лента активности) не изменились.
