# 06. Исполнение команд: одна ступень, журнал без ошибок команды, без пресетов PostgreSQL

| | |
|---|---|
| Статус | предложено |
| Ценность | высокая: одна публичная ступень исполнения вместо четырёх; последний цикл внутри ядра снят |
| Цена | средняя–высокая: тесты PostgreSQL и демо, которые собирают исполнитель вручную |
| Зависимости | 05 |

## Сейчас

Команда проходит пять ступеней: `CqrsFramework` (одна попытка: загрузить, решить, спланировать, записать) →
`CommandExecutorCore` (блокировка, повтор при конфликте, пробуждение, наблюдатель) → пресеты `PostgresCommandExecutor` и
`TestCommandExecutor`, которые повторяют `with_*` ядра → `Executor` движка (enum Memory | Postgres) → `Source`/`Outcome`.
После появления движка пресеты PostgreSQL (`PostgresCommandExecutor`, `PostgresCommandConfig`, `prepare_task_runtime`)
нужны только тестам. `TestCommandExecutor` настраивает ядро через закрытые `set_*` и паникует, если его уже клонировали.

Порт журнала — наследие cqrs-es: `EventStore::{load_events, load_aggregate, commit}` возвращают
`AggregateError<A::Error>`, ошибку команды с вариантами `Domain` и `TaskPlan`. Журнал знает о решениях и планировании,
и `journal` зависит от `command`.

## Станет

1. **Порт журнала возвращает `PersistenceError`.** `EventStore` и `PersistedEventStore` не называют `AggregateError` и
   `TaskPlanError`; ошибки serde уже сводятся к `PersistenceError`. Исполнитель сводит `PersistenceError` к
   `AggregateError` тем же `From`, что сейчас (`OptimisticLockError` → `AggregateConflict` и так далее), поэтому
   категории ответа и логов прежние.
2. **`CqrsFramework` — закрытая часть исполнителя:** `command/attempt.rs`, без `pub`. Интеграционные тесты, которые
   собирают `CqrsFramework` (14 файлов в `cqrsx/tests`: `cqrs/{command,execution,history,planning}.rs`,
   `cqrs/projection/support.rs`, `log_fallback.rs`, `mongo/{support,journal_in_postgres}.rs`, `postgres.rs`,
   `postgres/{command,compatibility,planning}.rs`, `postgres/event_store/snapshots.rs`,
   `postgres/projection/processor.rs`), идут через исполнитель. Публичная ступень одна — `CommandExecutorCore`; план предлагает ему имя без `Core`, если оно не
   спорит с портом `CommandExecutor`.
3. **Пресеты PostgreSQL удаляются:** `postgres/command.rs` (`PostgresCommandExecutor`, `PostgresCommandConfig`),
   `postgres/task_runtime.rs` (`prepare_task_runtime`). Движок собирает исполнитель над
   `PersistedEventStore<PostgresEventRepositoryWithOutbox, A>` так же, как над журналом в памяти, и runtime задач —
   через `worker::runtime::prepare`. Тесты `tests/postgres/*` и `demo/back-end/tests/write_side.rs` собирают то же
   вручную; повторяющаяся сборка — помощник в их `support`.
4. **`TestCommandExecutor`** собирает ядро публичными `with_*` до того, как поделить его в `Arc`; закрытые `set_*` и
   паника «до клонирования» исчезают.
5. **`Executor` движка:** enum остаётся, если план не покажет, что `Arc<dyn CommandExecutor<A>>` проще; выбор — по
   простоте кода, без замера (README эпика, «Замеров производительности нет»).

После задачи `journal` не зависит от `command`: порядок модулей ядра из README эпика выполняется полностью.

## Поведение не меняется: чем доказано

Таблица гарантий эпика 05 (`guarantees.md`): блокировка на ключ и её лимиты, повтор при конфликте, отказ плана без
записи, задачи вместе с событием, пробуждение до ответа, наблюдатель после снятия блокировки — тесты прежние, меняется
сборка исполнителя в них. Категории логов (`connection`, `deserialization`, `unexpected`, `task_plan`) проверяют
прежние тесты логов. Эталоны демо байт в байт.

## Демо и документы

`tests/write_side.rs` демо; `tests/architecture.rs` перестаёт искать `PostgresCommandExecutor` (его нет).
`components.md`: строки «Command orchestration», «Command implementation», «Event store», «Backend assembly» и путь
команды в начале; README («Running on PostgreSQL»).
