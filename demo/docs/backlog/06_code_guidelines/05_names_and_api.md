# 05. Имена и типы API

| | |
|---|---|
| Статус | сделано 2026-10-30, `6b2e865`, `8b0468d` |
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

- [x] Все строки таблиц сделаны или отклонены пользователем с причиной.
- [x] Эталоны demo (`effective_config.txt`, лента активности) не изменились.

## Результаты

Сделано 2026-10-30 на HEAD `5eea784` (задачи 01–03 в коммитах) без коммита. Задача 05/15 ещё ждёт решений
пользователя; по его указанию 05 сделана раньше, пересечения с темами пересмотра — ниже. Задача 04 не сделана: ни одна
её правка (Н6 `engine/views.rs`, Н11 `Arc<dyn Fn>` в `engine/module.rs` и другие) сюда не попала. Тексты SQL,
эталоны `demo/back-end/tests/golden/`, `GET /api/config`, лента активности, JSON verify и тексты ошибок не менялись.

### Имена

| Строка | Итог |
|---|---|
| конструкторы с `Result` | сделано: `try_new` у `LockLimits`, `AggregateConflictRetryPolicy`, `DeliveryLimits`, `ProcessorConfig`, `TaskStorageConfig`, `TaskWorkerPoolConfig`, `TaskRuntimeConfig`, `Settings`, `PostgresSettings`, `WritePlan` (и соседний закрытый `SnapshotWrite`). Ни одна проверка не сводится к «не ноль» целиком (везде есть `Duration` или диапазон), поэтому `new` без `Result` не осталось нигде; `Settings::try_new` принимает интервал снимков как `NonZeroUsize`, как `with_snapshots` и `PostgresCommandConfig::new`, — `try_new` остаётся из-за проверки блокировки против таймаута обработчика |
| `new_event_store`, `new_snapshot_store` | сделано: `PersistedEventStore::new(repo)`, `.with_snapshots(NonZeroUsize)` |
| `get_events`, `get_events_after`, `get_snapshot` | сделано: `events`, `events_after`, `snapshot` |
| `MemStore::get_events` | удалён; его тест переписан: клоны видят одни события через `load_events` |
| `EventStore::AC` | сделано: `Context` |
| `NewTask::scheduled` | сделано: `with_schedule` |
| `TestFramework::with`, `inspect_result` | сделано: `new`, `into_result` |
| `app_name` в `engine/mongodb.rs` | сделано: параметр `mongo_plan`/`describe` и поле `MongoPlan::application_name`. Ключ описания `mongodb.app_name` оставлен: это видимый вывод (эталон `effective_config.txt`) и имя опции MongoDB (`appName`) |
| файлы не по главному типу | сделано: `cqrs/testing/command_executor.rs`, `mongodb/checkpoint_store.rs`; ради симметрии и PostgreSQL-двойник `postgres/projection_checkpoints.rs` → `postgres/checkpoint_store.rs` (в нём `PostgresCheckpointStore`, файл без изменений). Модули: `testing::command_executor`, `mongodb::checkpoint_store`, `postgres::checkpoint_store` |

### Типы

| Строка | Итог |
|---|---|
| `spawn_rebuild(key, pace, reason, bool, waiters)` | сделано: `enum RebuildJob { Requested, Cursor { pace }, Healed { waiter } }`, `spawn_rebuild(key, job)` |
| `SerializedEvent::new` | удалён; все места — литерал структуры с именами полей |
| `can_upcast(&str, &str)` | сделано: `EventFormat<'a> { event_type, event_version }` (заимствует строки события, без копий), `SerializedEvent::format()`, `can_upcast(EventFormat<'_>)` |
| `operation`, `category` | `enum Operation` с `as_str()` сделан (закрытый, имена в логах те же); журнал или хранилище контрольных точек для сбоя без страницы выбирается полным `match` по `Operation`. `Category` введён 2026-10-30 по решению пользователя: публичный `FailureCategory` рядом с `Failed`, вариант на каждое прежнее слово, `as_str()` и `Display` дают те же слова. Тексты в логах, в `last_error` строки контрольной точки и в JSON ленты активности не изменились. Публичный API: `Failed.category` и `CheckpointError::category()` — `FailureCategory` вместо `&'static str` |
| `Difference.kind` | сделано: `enum DifferenceKind { Differs, Unexpected }`, `serde(rename_all = "snake_case")`; JSON тот же (тест в `verify.rs`) |
| `labels() -> Vec<(&str, String)>` | сделано: `Label { name, value }`; derive `Command`/`Commands` порождают его |
| `load_with_context -> (V, ViewContext)` | сделано: `Option<StoredView<V>>`; `From<(V, ViewContext)> for StoredView` удалён; имя метода прежнее |
| кортежи модуля и owner | сделано: `PolicyEntry`, `TaskRegistration { adapter, binding }` с `PolicyBinding { name, bind }`, `RebuildOutcome { aggregate_id, result }` |
| `describe()`, `observe_projections` | сделано: `Cqrs`, `Settings`, `PoolPlan`, `MongoPlan` отдают `BTreeMap<String, String>` (эталон тот же: `render` в `contracts.rs` принимает пары); `observe_projections(Fn(ObservedProcessor<'_>))`, `ObservedProcessor { projection, node }` |
| `RowKey = (String, String)` | сделано: строки `MemCheckpointStore` по `CheckpointKey` (derive `PartialOrd`, `Ord`: порядок прежний — проекция, потом тип агрегата) |
| `supersedes`, `statement_timeout_text`, `Default` у `CommitReport` | сделано: `NumericVersion::supersedes` удалён, сравнение `>`; `impl Display for StatementTimeout` (в `engine/postgres.rs`: текст тот же, `duration_text` живёт в engine); `#[derive(Default)]` |
| `AggregateKey`, `impl Into<Bson>`, `impl Into<String>` | сделано: `AggregateKey` — `pub(crate)`, `new(&'static str, String)`, неиспользуемый геттер `aggregate_type()` удалён; `literal(&str)`; `Read::invalid`/`invalid_named` принимают `&str` |
| `impl Into<Arc<…>>` | сделано: `TaskDispatcher::dispatch(Arc<TaskAttempt>, …)`, `prepare(…, Arc<TaskDispatcher<A>>, …)`, `prepare_task_runtime(…, Arc<TaskDispatcher<A>>, …)` |
| `AggregateContext::aggregate(&mut self)` | сделано: `aggregate(&self) -> &A` |
| `dyn` с одним значением (правило 18) | оставлено по рекомендации: generic-параметр processor добавил бы параметры типам, а `ProjectionRuntime` всё равно стирает их |

### Публичный API: было → стало

- `LockLimits`, `AggregateConflictRetryPolicy`, `DeliveryLimits`, `ProcessorConfig`, `TaskStorageConfig`,
  `TaskWorkerPoolConfig`, `TaskRuntimeConfig`, `PostgresSettings`: `new` → `try_new`; `Settings::new(usize, …)` →
  `Settings::try_new(NonZeroUsize, …)`.
- `PersistedEventStore::new_event_store(repo)` → `new(repo)`; `new_snapshot_store(repo, n)` → `new(repo).with_snapshots(n)`.
- `PersistedEventRepository::{get_events, get_events_after, get_snapshot}` → `{events, events_after, snapshot}`;
  `MemStore::get_events` удалён; `EventStore::AC` → `EventStore::Context`;
  `AggregateContext::aggregate(&mut self) -> &mut A` → `aggregate(&self) -> &A`.
- `NewTask::scheduled` → `with_schedule`; `TestFramework::with` → `new`; `AggregateResultValidator::inspect_result` →
  `into_result`.
- `MongoPlan::app_name` → `application_name` (параметры `mongo_plan` тоже).
- Модули: `cqrs::testing::command` → `command_executor`, `mongodb::projection_checkpoints` → `checkpoint_store`,
  `postgres::projection_checkpoints` → `checkpoint_store`.
- `SerializedEvent::new` удалён; `EventUpcaster::can_upcast(&str, &str)` → `can_upcast(EventFormat<'_>)`, новые
  `EventFormat` и `SerializedEvent::format`; `NumericVersion::supersedes` удалён.
- `Difference::kind: &'static str` → `DifferenceKind`.
- `Failed::category`, `CheckpointError::category()`: `&'static str` → `FailureCategory`, новый `FailureCategory`.
- `TaskFormat::new` → `try_new`, `TaskFormat::of` без `Result`; `NewTask::new` → `try_new`; `TaskAttempt::new` → `try_new`,
  `TaskAttempt::first` → `try_first`; `PolicyName::new` → `try_new`; `JsonTaskAdapter::new`, `BoxedTaskAdapter::json` без
  `Result`; `TaskPolicy::new(Retry, NonZeroU16)` без `Result`; `Module::policy(.., NonZeroU16)`; `DEFAULT_MAX_ATTEMPTS`,
  `Policies::max_attempts`, `NewTask::max_attempts`: `NonZeroU16`; `NewTask::with_max_attempts(NonZeroU16)` без `Result`;
  `TaskContext::new(TaskAttempt, ..)`; удалены `TaskValidationError::{Format, InvalidAttemptLimit}`,
  `PolicyError::{MaxAttempts, Format}`, `TaskDispatcherError::InvalidFormat`.
- Новый `BuildError::DescribedTwice`; `Setup::describe` больше не «последнее значение побеждает».
- `CommandInfo::labels() -> Vec<(&'static str, String)>` → `Vec<Label>`, новый `Label`.
- `ViewRepository::load_with_context -> Option<(V, ViewContext)>` → `Option<StoredView<V>>`; `From<(V, ViewContext)>`
  удалён.
- `Module::observe_projections(Fn(&'static str, &str))` → `Fn(ObservedProcessor<'_>)`, новый `ObservedProcessor`.
- `Cqrs::describe`, `Settings::describe`, `PoolPlan::describe`, `MongoPlan::describe`: `Vec<(String, String)>` →
  `BTreeMap<String, String>`.
- `CheckpointKey`: добавлены `PartialOrd`, `Ord`; `StatementTimeout`: добавлен `Display`.
- `AggregateKey` больше не публичный.
- `TaskDispatcher::dispatch`, `task::runtime::prepare`, `postgres::task_runtime::prepare_task_runtime`:
  `impl Into<Arc<…>>` → `Arc<…>`.

### Пересечения с задачей 05/15

- Темы 2 и 10 (upcasting, версии): `EventFormat` и `can_upcast` — тот же трейт `EventUpcaster`.
- Тема 8 (подписи ленты): `Label { name, value }` — лента ищет `fan_id` через `label.name`.
- Тема 11 (политика у типа задачи): закрытые `TaskRegistration`/`PolicyBinding` — та же привязка, публичный API
  `.task(x).policy(..)` прежний.
- Темы 12 и 13 (запросы витрин, своя реализация под базу): `load_with_context` отдаёт `StoredView`, как три запроса;
  поменялись все три реализации `ViewRepository`.
- Темы 16 и 17 (наблюдатели): аргумент фабрики наблюдателей процессоров — `ObservedProcessor`.
- Темы 20 и 21 (размер движка, `Defaults`): `Settings::try_new(NonZeroUsize, …)`, `describe()` → `BTreeMap`.

Если решения 05/15 поменяют эти API, правки 05 переписываются вместе с ними; ни одно решение 05/15 эта задача не
предрешает.

### Прочее

- **demo:** правки только как у вызывающего: `try_new`, `TestFramework::new`, `with_snapshots`, `Context`,
  `command_executor`, литерал `SerializedEvent`, `DifferenceKind::Differs`, `label.name`/`label.value` в ленте, `StoredView`,
  `ObservedProcessor`, `report.category.as_str()` в ленте, `FailureCategory` в тестах,
  `Arc::new(..)` у `dispatch`/`prepare`. `tests/contracts.rs` правлен в четырёх местах того же рода (`type Context`, путь
  `command_executor`, `Arc::new(dispatcher)`, `FailureCategory::Connection`); эталоны не трогались. `demo/load/epic06`: `TestFramework::new`,
  `describe().get(..)`.
- **Документация:** `cqrsx/doc/components.md` — новые имена, `Label`, `EventFormat`, `DifferenceKind`,
  `ObservedProcessor`, `FailureCategory`, `describe()` как `BTreeMap`, `AggregateKey` закрытый, дерево файлов.
- **Ключ, описанный дважды:** см. раздел «Повторный ключ в `describe()`» ниже; проверка «ключ дважды» перенесена из
  `render` в `contracts.rs` в сборку движка.
- **Конструкторы с `Result` под именем `new`:** сделано, см. раздел «Конструкторы» ниже.

### Конструкторы (решение пользователя 2026-10-30, правило 22)

- **Без ошибки:** `TaskFormat::of::<T>()` возвращает `TaskFormat`; `T::TASK_TYPE` и `T::TASK_VERSION` проверяет
  `const { assert!(…) }` при инстанцировании, плохая константа не собирается (`cargo build`; `cargo check` может не
  сообщить) и у derive, и у ручного `TaskPayload` (doctest `compile_fail,E0080`). Поэтому без `Result`
  `JsonTaskAdapter::new`, `BoxedTaskAdapter::json`, а `Policies::with_task` без ошибки формата. `TaskPolicy::new(retry,
  NonZeroU16)`; тот же тип у `Module::policy`, `DEFAULT_MAX_ATTEMPTS`, `Policies::max_attempts`, `NewTask::max_attempts`,
  `NewTask::with_max_attempts` (без `Result`) и закрытого `PolicyEntry`; outbox пишет `i32::from(n.get())`. demo читает
  `ENCORE_TASK_MAX_ATTEMPTS` как `NonZeroU16` (по умолчанию 6): `0` теперь отказ разбора (`cannot parse "0": number would
  be zero for non-zero type`) вместо `must be positive`.
- **`try_new`:** `TaskFormat::try_new`, `NewTask::try_new`, `TaskAttempt::try_new`, `TaskAttempt::try_first` (было
  `first`), `PolicyName::try_new` (`const fn … -> Option<Self>`).
- **`TaskContext::new(TaskAttempt, Duration)`** сам кладёт попытку в `Arc`; `TaskContext::shared` и так `pub(crate)`.
- **Удалены ставшие невозможными ошибки:** `TaskValidationError::{Format, InvalidAttemptLimit}`,
  `PolicyError::{MaxAttempts, Format}`, `TaskDispatcherError::InvalidFormat`; тесты неверного формата во время работы
  удалены (их место — doctest `compile_fail`).
- **Оставлены после grep публичного API:** `NewTask::from_payload` (преобразование JSON, `from_*` с `Result`, как
  `String::from_utf8`); `from_env`/`from_lookup` у `LoggingConfig`, `TaskRuntimeConfig`, `TaskWorkerPoolConfig`,
  `TaskStorageConfig`, `Settings` (чтение окружения); `Causation::from_event_metadata` (`Result<Option<_>>`: причины может
  не быть). Методы builder с `Result` (`with_schedule`, `with_metadata`, `Policies::with_*`, `Executors::with`) —
  не конструкторы.

### Повторный ключ в `describe()` (решение пользователя 2026-10-30)

- Сборка движка падает с `BuildError::DescribedTwice { key, first, second }` (``the key `K` is described twice, as `a`
  and as `b` ``), если ключ описан дважды с разными значениями: настройки движка и модуль, два модуля (`Setup::describe`,
  `Setup::env`), модуль сам с собой, настройки движка между собой. То же значение дважды — одна запись (два модуля
  читают общую переменную). `describe()` остаётся `BTreeMap<String, String>`.
- Устройство: `Settings`, `PoolPlan`, `MongoPlan` отдают пары по порядку (закрытый `entries()`), `Setup` копит описания
  модулей списком, `merge_described` в `build.rs` сливает всё по ключу; `Cqrs::describe` отдаёт проверенную карту.
- `serve.rs` печатает ошибку без `invalid configuration:`: это ошибка кода (один ключ читают по-разному), которую
  значение переменной не исправит навсегда. Значения печатаются как есть — как и `describe()`, они могут содержать
  адрес базы с паролем.
- Тесты: модуль против движка, два модуля с разными умолчаниями, модуль дважды, то же значение принимается;
  `the_engine_describes_every_key_once` возвращает проверку «каждый ключ один раз». Эталон `effective_config.txt` тот же.

**Проверки** (все с кодом 0, журнал вне репозитория): build, fmt, clippy `-D warnings`, полные тесты workspace
(838 прошли, 0 упало), `-p cqrsx --no-default-features` без features и с каждой из `postgres`, `worker`, `host`,
`logging`, `mongodb`, `http`; покрытие строк — workspace 98,00 %, `cqrsx` 97,90 %; `cargo doc -p cqrsx` без
предупреждений. demo — fmt, build, clippy `-D warnings`, тесты (174 прошли, `contracts.rs` с эталонами — 9);
`demo/load/epic06` — build, fmt, clippy `-D warnings`, тесты. Первый прогон поймал три doctest с прежним
`load_with_context` и тест архитектуры demo: доменный тест назвал `Label`, которого нет в списке разрешённого домену;
тест переписан без `Label`, список не менялся.
