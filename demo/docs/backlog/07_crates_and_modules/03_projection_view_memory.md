# 03. Ядро II: `projection`, `view`, `memory`, `testing` наверх

| | |
|---|---|
| Статус | предложено |
| Ценность | высокая: машина проекций (≈ 3 000 строк) выходит из-под модели витрин; бэкенд в памяти перестаёт быть «тестовым»; модуль `cqrs` исчезает |
| Цена | средняя |
| Зависимости | 02 |

## Сейчас

`cqrs::projection` держит и то, что пишет и читает приложение (`View`, `StoredView`, `ViewRepository`, `verify`), и
самую большую подсистему крейта — процессор с владельцем, арендой, доставкой и перестройкой. `EventHandler` лежит
отдельно на уровне `cqrs`, хотя его зовёт только процессор. Движок в памяти (`Cqrs::in_memory()`) собран из
`cqrs::testing::{MemEventRepository, MemCheckpointStore, MemViewRepository}` и `task::testing::MemTaskQueue`.

## Станет

| Сейчас | Станет | Что |
|---|---|---|
| `cqrs/event_handler.rs`, `cqrs/projection.rs` (`RebuildableProjection`) | `projection.rs` | контракты, которые ведёт процессор |
| `cqrs/projection/processor.rs`, `processor/*` | `projection/processor.rs`, `processor/*` | без изменений |
| `cqrs/projection/checkpoint.rs` | `projection/checkpoint.rs` | `CheckpointKey::of::<V, A>()` удаляется: вызывающий пишет `CheckpointKey::new(V::NAME, A::TYPE)` — так процессор не зависит от витрин |
| `cqrs/projection/runtime.rs` | `projection/runtime.rs` | |
| `cqrs/projection.rs` (`View`) | `view.rs` | контракт витрины |
| `cqrs/projection.rs` (`ViewProjection`, `apply_contiguous`, их тесты) | `view/projection.rs` | |
| `cqrs/projection/repository.rs` | `view/repository.rs` | `ViewRepository`, `ViewContext`, `StoredView` |
| `cqrs/projection/verify.rs` | `view/verify.rs` | |
| `cqrs/testing/mem_event_repository.rs` | `memory/event_repository.rs` | `MemEventRepository` |
| `cqrs/testing/mem_checkpoint_store.rs` | `memory/checkpoint_store.rs` | `MemCheckpointStore` |
| `cqrs/testing/mem_view_repository.rs` | `memory/view_repository.rs` | `MemViewRepository` |
| `task/testing.rs`, `task/testing/mem_task_queue.rs` | `memory/task_queue.rs` | `MemTaskQueue`, `QueuedTask`, `TaskStatus` |
| `cqrs/testing.rs`, `cqrs/testing/{validator,command_executor,mem_store,unavailable_store}.rs` | `testing.rs`, `testing/…` | на верхнем уровне ядра до задачи 09 |

`PersistenceErrorObserver` переезжает в `view`, если план подтвердит, что его зовёт только `ViewProjection`.

После задачи модуля `cqrs` нет. Каталог интеграционных тестов `cqrsx/tests/cqrs/` переименовывается по смыслу (план
предлагает: `tests/core.rs` с подкаталогами по модулям ядра), его подкаталог `projection/` — без изменений внутри.
Раскрытия макросов, которые называют `::cqrsx::cqrs::…`, к этой задаче уже переведены задачей 02.

## Поведение не меняется: чем доказано

Только пути и одна замена `CheckpointKey::of` на `CheckpointKey::new` с теми же аргументами (тест ключа витрины в
`checkpoint.rs` остаётся и проверяет то же: имя витрины и тип агрегата). Число тестов прежнее, эталоны демо байт в байт.

## Демо и документы

Импорты демо (`StoredView`, `Verification`, `RebuildRequest`, тестовые инструменты); `tests/architecture.rs`:
презентации — `cqrsx::view::repository::StoredView`, тестовые пути — `cqrsx::testing`. `components.md`, README,
`typical_scenario.md`.
