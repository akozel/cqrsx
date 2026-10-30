# 06. `Debug`, `#[must_use]`, параметры по значению

| | |
|---|---|
| Статус | сделано 2026-10-30, `144e0b2` |
| Ценность | низкая–средняя: результат builder и итог остановки нельзя молча потерять, любой публичный тип печатается |
| Цена | низкая: механические правки, около 100 мест |
| cqrsx | места ниже; `Cargo.toml` (`[lints]`) |
| cqrsx-macros | `src/*.rs` (`expand_*` по ссылке); `Cargo.toml` (`[lints]`) |
| demo | нет (кроме новых предупреждений `must_use`, если demo игнорирует результат) |
| Зависимости | 05 (те же типы после переименований) |
| Правила | 3, 16, 20, 32 |

## Сейчас

Аудит, [раздел «API и типы»](../../cqrsx-style-audit.html#api), lints по HEAD `e995a53`:
- `missing_debug_implementations` — 42 публичных типа без `Debug`;
- `return_self_not_must_use` — 42 метода builder без `#[must_use]`;
- `needless_pass_by_value` — 15 мест (F4 `persisted.rs:550` уходит в задаче 04).

## Станет

1. **`Debug`** у всех 42 типов: derive, где поля позволяют; иначе ручной с `finish_non_exhaustive()`. Данные, которые
   нельзя печатать (payload задач, lease token, пароли), скрываются, как в существующих ручных `Debug`.
2. **`#[must_use]`** (правило 20):
   - на типы builder (`engine/build.rs`, `engine/module.rs`, `task/handlers.rs`, `processor.rs`, `cqrs/command/executor.rs`
     и другие из списка lint) — это закрывает все их методы;
   - по смыслу, вне lint: `Stopped` (`host/lifecycle.rs:90`, итоги host и read side теряются при `..await;`),
     `Setup::invalid` и `SetupError::failed` (`engine/setup.rs:87`, `:37`), `AggregateResultValidator`
     (`cqrs/testing/validator.rs:6`, `testing.rs:519`), `stamp_task` и `task_id` (`causation.rs:110`, `:123`), чистые
     `Retry::pause`, `Policies::pause`, `Policies::max_attempts` (`task/policy.rs:122`, `:210`, `:216`).
3. **Параметры по значению** (правило 3): маленьким конфигурациям — `Copy` (`PostgresCommandConfig` в
   `postgres/command.rs:83`, `TaskStorageConfig` в `default_executor.rs:33`); `storage_error(&sqlx::Error, ..)`
   (`postgres/task_queue.rs:32`); семь `expand_*(input: &DeriveInput)` в макросах; четыре места в помощниках для
   тестов.
4. **Lints** в `[lints]` обоих crate: `missing_debug_implementations`, `clippy::return_self_not_must_use`,
   `clippy::needless_pass_by_value` — `deny`; `unsafe_code = "forbid"` (правило 32). Skill `rust-style-review`: эти
   lints убираются из команды шага 1.

## Этапы

1. Правки по трём группам; тесты не меняются, кроме вызовов, которые теперь требуют использовать результат.
2. Lints; `CLAUDE.md`-проверки cqrsx и сборка demo без предупреждений; коммит.

## Риски

- **Ручной `Debug` раскрывает секрет.** Для каждого ручного `Debug` проверить поля: payload, metadata, lease token,
  URL с паролем не печатаются.

## Как проверить

- `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` с новыми lints.

## Критерии приёмки

- [x] Три lint включены, срабатываний ноль; `unsafe_code = "forbid"`.
- [x] `#[must_use]` по смыслу на перечисленных типах и функциях.

## Результаты

Сделано 2026-10-30 поверх задачи 05 (в рабочем дереве), без коммита. Задача 04 не сделана, её правки сюда не попали.
Поведение, тексты SQL, эталоны `demo/back-end/tests/golden/`, JSON и тексты ошибок не менялись.

| Lint | Срабатываний было | Итог |
|---|---|---|
| `missing_debug_implementations` | 42 | 6 derive (`TaskEntry`, `TaskHandlers`, `PostgresCheckpointStore`, `PostgresEventRepository`, `PostgresEventRepositoryWithOutbox`, `PostgresTaskQueue`; ради них derive у закрытых `SqlQueryFactory` и `SnapshotPolicy`), 36 ручных |
| `return_self_not_must_use` | 43 | `#[must_use]` на 17 типах builder |
| `needless_pass_by_value` | 23: 15 в коде crate, 8 в тестах | 22 исправлены, 1 (F4) ждёт задачу 04 |
| `unsafe_code` | 0 | `forbid` |

Все четыре — в `[lints]` обоих crate (`[lints.rust]`: `missing_debug_implementations = "deny"`, `unsafe_code =
"forbid"`; `[lints.clippy]`: `needless_pass_by_value`, `return_self_not_must_use` — `deny`) и действуют и на тесты.
Срабатываний ноль: `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` и clippy каждой
feature по отдельности. Skill `rust-style-review`: в команде шага 1 остался только `wildcard_enum_match_arm`.

### Ручные `Debug`

Ручной там, где derive потребовал бы `Debug` от `A`, `Rules`, хранилища или обработчика, или напечатал бы лишнее. Все
кончаются `finish_non_exhaustive()`, кроме `ReactionsPlanner`, `MemViewRepository`, `AggregateResultValidator`,
`PostgresCommandExecutor`, `PostgresViewRepository` (скрыт только `PhantomData`). Что не печатается:
- **payload и metadata задач:** `Outcome`, `AnyOutcome` печатают задачи как их форматы, `TestCommandExecutor` не
  печатает опубликованные задачи;
- **metadata команды** (данные вызывающего) и сама команда (у неё нет `Debug`): `CommandReport`, `AnyCommandReport`
  (команда — её имя, отказ — его `code`), `Decision`, `Decided`;
- **URL с паролем:** `Cqrs`, `Builder`, `Setup` не печатают `Settings` и описанное модулями; пулы PostgreSQL печатают
  размер и опции, без URL (тест с паролем в URL); `MongoCheckpointStore`, `MongoViewRepository` — только имя коллекции
  (`Debug` коллекции печатает весь клиент);
- **остальное:** замыкания и trait objects (наблюдатели, upcasters, `transform`, переменные, корни модулей, адаптеры,
  очередь, хранилища), состояние агрегата и `Rules`.

Типы: `CqrsFramework`, `CommandExecutorCore`, `CommandReport`, `Outcome`, `AnyCommandReport`, `AnyOutcome`, `Decision`,
`Decided`, `ReactionsPlanner`, `ViewProjection`, `ProjectionProcessor`, `ProjectionRuntime`, `PersistedEventStore`,
`EventStoreAggregateContext`, `NumericVersionUpcaster`, `TestFramework`, `AggregateResultValidator`,
`TestCommandExecutor`, `MemStoreAggregateContext`, `MemViewRepository`, `Cqrs`, `Builder`, `Module`, `Setup`, `Source`,
`Views`, `ViewQuery`, `MongoCheckpointStore`, `MongoViewRepository`, `PostgresCommandExecutor`,
`PostgresViewRepository`, `BoxedTaskAdapter`, `JsonTaskAdapter`, `TaskDispatcher`, `DefaultTaskExecutor`,
`TaskRuntime`. Каждый проверен тестом (строка вывода целиком или её начало и отсутствие секрета).

### `#[must_use]`

- **Типы builder** (закрывают lint): `CqrsFramework`, `CommandExecutorCore`, `TestCommandExecutor`,
  `PostgresCommandExecutor`, `PersistedEventStore`, `PostgresEventRepository`, `ViewProjection`, `ProjectionProcessor`,
  `ProjectionRuntime`, `Builder`, `Module`, `Settings`, `TaskContext`, `Dependencies`, `DefaultTaskExecutor`,
  `TaskHandlers`, `TestFramework`. С пояснением: `Builder` («an engine is made by `build`»), `Module`,
  `ProjectionProcessor`, `ProjectionRuntime`, `TestFramework`. У `TestFramework` три `#[must_use]` на методах заменены
  одним на типе (иначе `double_must_use`).
- **По смыслу:** типы `Stopped` («the host's and the read side's failures are lost unless they are checked»),
  `AggregateResultValidator` («the result is checked only by a `then_expect_*` method»); функции `Setup::invalid` и
  `SetupError::failed` («return it as `Err`»), `Causation::stamp_task` («the stamped task is the one returned»),
  `Causation::task_id`, `Retry::pause`, `Policies::pause`, `Policies::max_attempts`.
- **Вызывающие, терявшие результат:** один тест (`engine/settings.rs`: `Settings::from_lookup(..)` теперь проверяет
  префикс). В demo и `demo/load/epic06` новых предупреждений нет.

### Параметры по значению

- **`Copy`** у `PostgresCommandConfig` и `TaskStorageConfig`: все поля `Copy`. `.clone()` у них убран в
  `engine/memory.rs`, тесте `task/runtime/config.rs`, demo `back-end/src/fan.rs` и `back-end/tests/contracts.rs`.
- **По ссылке:** `storage_error(&sqlx::Error, ..)`; `expand_*(&DeriveInput)` и `expand_task_handler(&TokenStream,
  &ItemFn)` (с ними `run`, `run_attribute` и помощники тестов макросов); `then_expect_event(&A::Event)` и
  `then_expect_error(&A::Error)` (вызовы: 20 в cqrsx, 18 в demo, 1 в `demo/load/epic06`); `MemStore::wrap_events`,
  `reason(&str)` в `MemTaskQueue`; помощники тестов `with_deadline`, `with_format`, `plan`, `test_serialized_event`.
  `io_dispatcher` в тестах runtime забирает `Arc`, который хранит, вместо копии.
- **F4** (`PersistedEventStore::wrap_events`, задача 04): `#[expect(clippy::needless_pass_by_value, reason = ..)]`.
  Когда задача 04 перенесёт карту в событие, ненужный `expect` уронит clippy — напоминание его убрать.

### Открыто

- `Settings` (derive `Debug`) печатает `PostgresSettings` и `MongoSettings` с URL и паролем — та же утечка, что
  `describe()` в разделе эпика «Вне эпика»; новые ручные `Debug` её не повторяют.
- derive `Debug` у `NewTask`, `TaskAttempt`, `Then` печатает payload и metadata задачи, как и раньше.

**Проверки** (все с кодом 0, журнал вне репозитория): build, fmt, clippy `-D warnings`, полные тесты workspace
(847 прошли, 0 упало), `-p cqrsx --no-default-features` без features и с каждой из `postgres`, `worker`, `host`,
`logging`, `mongodb`, `http`; покрытие строк — workspace 98,06 %, `cqrsx` 97,97 % (после 05 — 98,00 % и 97,90 %). demo —
fmt, build, clippy `-D warnings`, тесты (174 прошли); `demo/load/epic06` — build, clippy `-D warnings`, тесты (12
прошли, подсчёт аллокаций, как всегда, `ignored`). Первые прогоны упали на новой проверке в тесте
`PostgresViewRepository` (`Debug` экранирует кавычки в SQL) и на fmt того же теста; поправлен только тест.
