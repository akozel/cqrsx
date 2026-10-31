# 03. Ядро II: `projection`, `view`, `memory`, `testing` наверх

| | |
|---|---|
| Статус | сделано 2026-10-31 ([результаты](#результаты)) |
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
| `cqrs/projection/checkpoint.rs` | `projection/checkpoint.rs` | `CheckpointKey::of::<V, A>()` остаётся, но его `impl` переезжает в `view.rs` (inherent `impl` можно писать в любом модуле крейта): `checkpoint.rs` больше не называет `View`, процессор не зависит от витрин, вызовы в демо не меняются (решение оркестратора 2026-10-31 вместо удаления метода) |
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

Только пути и перенос `impl CheckpointKey { fn of }` в `view.rs` (тест ключа витрины переезжает вместе с ним и проверяет
то же: имя витрины и тип агрегата). Число тестов прежнее, эталоны демо байт в байт.

## Демо и документы

Импорты демо (`StoredView`, `Verification`, `RebuildRequest`, тестовые инструменты); `tests/architecture.rs`:
презентации — `cqrsx::view::repository::StoredView`, тестовые пути — `cqrsx::testing`. `components.md`, README,
`typical_scenario.md`.

## Результаты

2026-10-31, на HEAD `688e46d` плюс правки задачи. Поведение не менялось: переезды файлов и правка путей.

- **Дерево:** `cqrs/event_handler` и `RebuildableProjection` → `projection.rs` (центральный контракт процессора);
  `cqrs/projection/{checkpoint,runtime,processor}` → `projection/`; `View` и `PersistenceErrorObserver` → `view.rs`;
  `ViewProjection` → `view/projection.rs`; `repository`, `verify` → `view/`; `Mem*` →
  `memory/{event_repository,checkpoint_store,view_repository,task_queue}`; `testing` — на верхний уровень ядра.
  `impl CheckpointKey { fn of }` переехал в `view.rs`, поэтому `projection/checkpoint.rs` не называет `View`. Модуля
  `cqrs` нет; тесты `tests/cqrs/` → `tests/in_memory/`.
- **Доказательство:** 867 тестов рабочей области и 174 демо, ignored 0; списки имён `--list` совпадают по бинарникам (в
  doctest'ах отличаются только пути 19 имён); fmt, clippy, `cargo doc -Dwarnings`, матрица фич зелёные; рабочий код
  `projection/**` не называет `view`, `memory`, `testing`, рабочий код `memory/**` не называет `testing`; старых путей
  нет; `components.md` и остальные живые документы сверены с HEAD по строкам и заголовкам.
- **Правило архитектуры демо:** `cqrsx::memory` — только для тестов слоёв domain и application, как раньше `Mem*` под
  `cqrs::testing`; нарушение для рабочего кода application закреплено тестом. Синтетическая строка с несуществующим
  `cqrsx::cqrs::event_sink::EventSink` заменена на `cqrsx::projection::EventHandler`.
- **Приёмка, круг 1:** восстановлен `components.md`, обрезанный при правке на 175 строк; возвращено ограничение
  `cqrsx::memory` в правиле архитектуры.
- **Остаётся:** тестовые модули и doctest'ы под `projection/**` называют витрины и память (только тесты).
