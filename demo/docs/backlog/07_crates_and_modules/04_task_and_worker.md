# 04. Ядро III: `task` — данные, `worker` — исполнение; фичи `worker` больше нет

| | |
|---|---|
| Статус | сделано 2026-10-31 ([результаты](#результаты)) |
| Ценность | высокая: снимает цикл `cqrs ↔ task`; исчезают 22 `cfg(feature = "worker")` и правило «конфиг задач компилируется без фичи» |
| Цена | средняя |
| Зависимости | 03 |

## Сейчас

`task` склеивает две вещи. Задача как данные — то, что сторона записи кладёт в outbox: `NewTask`, расписание, формат,
попытка, политики. Её используют команды и реакции. Исполнение задач — очередь, исполнитель, диспетчер, контекст,
runtime — вызывает команды обратно (`ctx.execute`, `From<Result<_, CommandGatewayError>> for TaskResult`). Отсюда цикл:
команды зависят от `task`, `task` — от команд. Фича `worker` закрывает только runtime, а его настройки компилируются без
неё, потому что их держат настройки движка.

## Станет

| Сейчас | Станет | Что |
|---|---|---|
| `task.rs` | `task.rs` | `NewTask`, `TaskSchedule`, `TaskValidationError`, `DEFAULT_MAX_ATTEMPTS`, помощники labels |
| `task/{format,attempt,policy}.rs` | `task/…` | без изменений |
| `task/queue/retry.rs` (`redelivery_backoff`) | `task/policy.rs` | пауза повтора по умолчанию — часть политик; сейчас `policy` ради неё лезет в очередь |
| `task/handler.rs` (`TaskHandler`), `task/result.rs` (`TaskResult`) | `worker.rs` | то, что пишет приложение; путь `cqrsx::worker::TaskHandler` |
| `task/{context,dependencies,handlers,dispatcher}.rs` | `worker/…` | |
| `task/adapter.rs`, `adapter/{json,boxed}.rs` | `worker/adapter.rs`, `adapter/…` | |
| `task/executor.rs`, `executor/{default_executor,lease_budget}.rs` | `worker/executor.rs`, `executor/…` | |
| `task/queue.rs`, `queue/{lease,settlement,storage_config}.rs` | `worker/queue.rs`, `queue/…` | `TaskQueue` — порт исполнения |
| `task/runtime.rs`, `runtime/*` | `worker/runtime.rs`, `runtime/*` | |
| `task/test_support.rs` | `worker/test_support.rs` | |

После задачи `task` не зависит ни от одного модуля ядра, кроме закрытого `diagnostics`; `command` зависит от `task`,
`worker` — от `command`.

**Фича `worker` удаляется.** `async-channel`, `tokio/rt`, `tokio/sync` становятся обычными зависимостями;
`host = ["tokio/signal"]`, `http = ["host", …]`, `default = ["postgres"]`. `postgres/task_runtime.rs` перестаёт
зависеть от фичи (сам пресет удаляет задача 06). Из `CLAUDE.md` уходит прогон `--features worker`.

Раскрытия `#[task_handler]` и `derive(Task)` (`::cqrsx::task::{context, dependencies, handler, result, format}`)
переходят на новые пути.

## Поведение не меняется: чем доказано

Только пути и удаление фичи. Прогоны `--no-default-features` и по одной фиче — без `worker`; число тестов прежнее (тесты,
которые компилировались только с `worker`, теперь компилируются всегда — их число сверяется с полным прогоном «до»),
эталоны демо байт в байт.

## Демо и документы

Импорты демо (`TaskContext`, `TaskResult`, `Policies`, `TaskAttempt`); `tests/architecture.rs`: запреты слоя
application — `cqrsx::worker::{adapter, dispatcher, runtime}` вместо `cqrsx::task::{…}`. `components.md`, README,
`typical_scenario.md`, `CLAUDE.md` (фичи и прогоны).

## Результаты

2026-10-31, на HEAD `4fd439a` плюс правки задачи. Поведение не менялось: переезды, пути и удаление фичи.

- **`task`** — только данные: `NewTask`, `TaskSchedule`, `TaskValidationError`, `DEFAULT_MAX_ATTEMPTS`, `format`,
  `attempt`, `policy` с `redelivery_backoff` (из `task/queue/retry.rs`, который удалён); его рабочий код не называет ни
  одного модуля ядра, кроме закрытого `diagnostics`.
- **`worker`** — исполнение: `TaskHandler` и `TaskResult` в `worker.rs`, затем `context`, `dependencies`, `handlers`,
  `dispatcher`, `adapter`, `executor`, `queue`, `runtime`; цикл `command ↔ task` исчез (`worker` выше `command`, рабочий
  код `command/**` не называет `worker`). `decode_with_labels` в `task.rs` стала `pub(crate)`: её зовёт
  `worker::adapter::json`, который раньше был потомком `task`.
- **Фича `worker` удалена:** `async-channel` — обычная зависимость, `default = ["postgres"]`, `host = ["tokio/signal"]`;
  41 `cfg(feature = "worker")` убран; `CLAUDE.md`, README крейта и корневой, `components.md`, `.github/workflows/ci.yml`
  (из матрицы убран `--features worker`) поправлены.
- **Тесты:** бинарник `task` стал `worker` (29 тестов); раскрытия `#[task_handler]` называют `::cqrsx::worker::…`.
  Тестов 867, демо 174, ignored 0; имена совпадают с точностью до префикса. Матрица без фич теперь включает исполнение
  задач: 575 тестов (раньше столько давал прогон с `worker`). `compile_fail`-doctest'ы отказывают по своим причинам, не
  по путям (проверено выводом компилятора).
- **Правило архитектуры демо:** слою application запрещены `cqrsx::worker::{adapter, dispatcher, runtime}`, как раньше
  `cqrsx::task::{…}`.
- **Находки:** `clippy -p cqrsx --no-default-features --all-targets` (прогнан впервые) падает на старом `unused_mut` в
  `engine/settings.rs:389` — задача 07; в матрице CI нет `http` — задачи 07–08.
