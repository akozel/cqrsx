# 07. Модули, стиль, документация

| | |
|---|---|
| Статус | предложено |
| Ценность | средняя: дерево читается как модель, код без скрытых побочных эффектов, у точек расширения есть описание |
| Цена | средняя: пять переносов модулей, около 15 цепочек, около 25 путей, около 130 описаний |
| cqrsx | места ниже |
| cqrsx-macros | `src/attribute.rs`, `src/task.rs` |
| demo | пути импорта, если переносы меняют публичные пути |
| Зависимости | 06 |
| Правила | 25–31, 34 |

## Сейчас

Аудит, разделы [«Модули»](../../cqrsx-style-audit.html#modules), [«Стиль»](../../cqrsx-style-audit.html#style),
[«Документация»](../../cqrsx-style-audit.html#docs). Места — по HEAD `e995a53`; после задач 02–06 строки
сместятся, ищется по смыслу.

## Станет

### Родительские модули (правило 26)

| Где | Что вынести | Куда |
|---|---|---|
| `cqrs.rs:35–526` | `CqrsFramework`, `CommitReport` и их тесты | `cqrs/framework.rs`, re-export; `AggregateError` остаётся |
| `cqrs/projection.rs:39–624` | `ViewProjection`, `apply_contiguous`, doctests и тесты | `projection/view_projection.rs`; в родителе `View`, `RebuildableProjection` |
| `engine.rs:208–431` | сборка (`install_aggregate`, `executor`, `install_view`, `processor`…); enum `Executor` и адаптер `Commands` | сборка — `engine/build.rs`; `Executor`, `Commands` — `engine/executor.rs` |
| `task.rs:49–137`, `:156–193` | `NewTask`, `TaskSchedule`; помощники labels и проверок | `task/new_task.rs`, `task/labels.rs`; в родителе `TaskValidationError`, константы |
| `cqrs/projection/processor.rs:254–318` | `Context` и цикл захвата ключей | `processor/claim.rs` |

Плюс: пустой каталог `cqrs/event_handler/delivery/` удаляется.

### Цепочки итераторов (правило 29)

- В заголовке `for`: `owner.rs:727`, `verify.rs:184`, `persisted.rs:534`, `engine/build.rs:309`,
  `cqrsx-macros/src/attribute.rs:20`, `:73` — сначала связать с переменной.
- С изменением внешнего состояния или условиями: `command/planner.rs:257–266` (`ordinals`), `task/dispatcher.rs:87–97`,
  `task/testing/mem_task_queue.rs:210–233`, `owner.rs:382–384`, `engine/build.rs:316–326`
  (`insert(..).then_some(()).ok_or(..)?`), `delivery.rs:370–381` (если не ушло с F1), `cqrsx-macros/src/task.rs:103–114`
  — обычный `for`.

### Типы и импорты (правила 28, 30)

- Тип результата `.parse()`/`.into()` виден на строке: `logging.rs:27`, `task/handlers.rs:92`, `:119`, `env.rs:9`,
  `engine/views.rs:230`.
- Около 25 типов, записанных полным путём, импортируются: `cqrs/store.rs:59`, `:62`; `repository.rs:44`;
  `diagnostics.rs:26`, `:89`, `:195`; `task/dispatcher.rs:20`, `:30`; `worker_pool.rs:21`, `:29`; `supervisor.rs:38`,
  `:48`; `host/signal.rs:9`, `:20–21`; `outbox/write_plan.rs:32`, `:75`; `event_repository/write.rs:18`;
  `outbox.rs:86–92`; `engine/views.rs:63–69`; `mongodb/projection_checkpoints.rs:164`.

### Документация (правило 34)

- **Неверные комментарии:** `serialized_event.rs:112` («A serialized version of a snapshot» над `TryFrom` для envelope),
  `diagnostics.rs:99–101` (описание другой функции), опечатки `upcaster.rs:11–17`.
- **Пересказ имени** — переписать или удалить: `SerializedEvent::new` (если останется), `NumericVersionUpcaster::new`,
  `ViewProjection::new`, `ViewContext::new`, `TaskContext::with_dependencies`, `with_executors`,
  `postgres/event_repository.rs:40`, `:98`, `validator.rs:5`.
- **Модули без `//!`** (около 26): ядро — `aggregate`, `event`, `command/executor`, `command/retry`,
  `store/persisted`, `store/repository`, `store/serialized_event`, `store/upcaster`; проекции — `projection/repository`,
  `testing/mem_store`, `testing/validator`; задачи — `dispatcher`, `queue/retry`, `queue/storage_config`,
  `runtime/config`, `runtime/worker_pool_config`, `host/error`, `host/lifecycle`, `host/signal`; PostgreSQL — `postgres`,
  `postgres/command`, `postgres/event_repository`, `postgres/pool`, `postgres/task_runtime`, `postgres/view_repository`.
- **Функции и типы без описания** (около 130). Сначала точки расширения: `TaskAdapter::formats`, `invoke`;
  `TaskQueue::claim`, `settle`, `recover_expired` и их ошибки; `TaskFormat`; геттеры `ProcessorConfig`;
  `from_env`/`from_lookup` конфигураций хранилища, пула workers и runtime — с именами переменных `OUTBOX_*`, которые
  они читают. Остальное — по списку lint `missing_docs` (без вариантов и полей, правило 34), сам lint не включается.

## Этапы

1. Переносы модулей — отдельным шагом без правок содержимого, чтобы дифф читался как переезд.
2. Цепочки, типы, импорты.
3. Документация.
4. `CLAUDE.md`-проверки; коммит. Если дифф слишком велик для проверки, задача делится на 07a (модули) и 07b (стиль и
   документация) — решение в плане.

## Риски

- **Переносы ломают публичные пути.** Re-export из родителя сохраняет путь для пользователей; demo проверяется
  сборкой.

## Как проверить

- `CLAUDE.md`; `cargo doc --no-deps` без предупреждений о ссылках.

## Критерии приёмки

- [ ] Пять родительских модулей разнесены; пустой каталог удалён.
- [ ] Нет цепочек в заголовках `for` и цепочек с изменением внешнего состояния.
- [ ] Неверные комментарии исправлены; точки расширения и модули описаны.
