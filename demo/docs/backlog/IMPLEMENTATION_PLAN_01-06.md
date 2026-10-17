# План реализации тикетов 01–06

Статус: **в работе** на ветке `feat/cqrsx-extractions`; ход по шагам в `demo/docs/PROGRESS.md`. Тикеты описывают «что и зачем» (код до и после). Этот документ
описывает «как»: порядок, решения, файлы, тесты, проверки и критерий готовности каждого шага.

## 0. Коротко

- Шесть шагов, по одному на тикет, в порядке номеров: **01 → 02 → 03 → 04 → 05 → 06**. Единственная жёсткая
  зависимость: 02 раньше 03 (общий `Arc`-обработчик должен сразу отдавать имя).
- Один коммит на тикет. В коммит входят изменения `cqrsx`, тесты, `components.md` и перевод demo на новый API.
  После каждого коммита обе сборки зелёные: корневая (`cqrsx`) и demo.
- Ломающих изменений публичного API два: тип ошибки после коммита (02) и конструктор PostgreSQL-исполнителя (03).
  Остальные шаги только добавляют API.
- Из demo уйдёт около 330 строк, в `cqrsx` добавится около 1000 строк, примерно половина из них тесты.
- Всё, что в плане решено за вас, собрано в разделе 9. Если с чем-то не согласны, это правится до начала работы.

## 1. Подготовка (шаг 0)

1. Ветка `feat/cqrsx-extractions` от `refactor/clean-architecture`. Ветка рефакторинга не слита, а demo-часть
   каждого тикета опирается на новую структуру.
2. Первый коммит: `docs(demo): backlog of cqrsx extractions` (каталог `demo/docs/backlog/` целиком, сейчас untracked).
3. Базовая линия. Прогнать и записать счётчики тестов в `demo/docs/PROGRESS.md`:

   ```bash
   # root (cqrsx), from /workspace/event-sourcing
   cargo build --locked --workspace --all-features
   cargo fmt --all --check
   cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
   RUST_TEST_THREADS=2 PROPTEST_RNG_SEED=42 cargo test --locked --workspace --all-features
   cargo test --locked -p cqrsx --no-default-features
   for f in postgres worker host logging; do cargo test --locked -p cqrsx --no-default-features --features $f; done
   cargo llvm-cov --locked --workspace --all-features --fail-under-lines 97
   cargo llvm-cov --locked -p cqrsx --all-features --fail-under-lines 97

   # demo, from /workspace/event-sourcing/demo
   cargo fmt --all --check
   cargo clippy --workspace --all-targets -- -D warnings
   cargo test --workspace
   ```

   Окружение проверено 2026-10-17: `cargo 1.98.1`, оба `Cargo.lock` резолвятся офлайн, `DOCKER_HOST` указывает на
   сокет Podman. Интеграционные тесты поднимают свои контейнеры через testcontainers. Запущенный dev-стек
   (`make up` / `make dev`) не трогаем и проверки против него не гоняем.

## 2. Общие правила для каждого шага

- **Порядок внутри шага:** API и юнит-тесты в `cqrsx` → интеграционные тесты `cqrsx` → `components.md` → перевод demo
  → полный набор проверок обеих сборок → статус тикета.
- **Покрытие.** Порог 97 % проверяется для `cqrsx`. Каждая новая ветка кода, включая ошибочные, получает тест. Код без
  достижимой ошибки делаем инфаллибельным, а не прикрываем `expect`.
- **Матрица фич.** Шаги 01, 02, 04, 05, 06 трогают только ядро (`cqrs`, `task` без `runtime`) и должны собираться с
  `--no-default-features`. Шаг 03 живёт за `postgres`.
- **Wire-формат demo не меняется ни в одном шаге.** Сторожа уже есть: тесты JSON событий, снапшотов, payload задач,
  кодов ошибок, HTTP-тесты роутера и `tests/write_side.rs`. Если какой-то из них приходится править, это стоп-сигнал.
- **Архитектурный тест demo** (`tests/architecture.rs`) должен проходить без правки правил. Новые импорты из
  `cqrsx::cqrs::testing` допустимы только в тестовом коде application, это правило уже действует.
- **Завершение шага:** в тикете статус «сделано» и хеш коммита, строка в `README.md` бэклога, запись в `PROGRESS.md`.
- **Сообщение коммита:** `feat(cqrsx): <что>; adopt in demo`, тело с перечнем ломающих изменений, если они есть.

## 3. Шаг 01. Один тип адаптера задач вместо enum

Тикет: [01_boxed_task_adapter.md](01_boxed_task_adapter.md). Ломающих изменений нет.

### cqrsx

| Файл | Изменение |
|---|---|
| `src/task/adapter/boxed.rs` (новый) | приватный `DynTaskAdapter` с `Pin<Box<dyn Future + Send + 'a>>`, blanket-реализация для любого `T: TaskAdapter`, публичный `BoxedTaskAdapter` с `new(adapter)` и `json::<P, H>(handler) -> Result<Self, TaskFormatError>`, `impl TaskAdapter for BoxedTaskAdapter` |
| `src/task/adapter.rs` | `pub mod boxed;` |
| `doc/components.md` | строка «Wire adaptation»: добавить `BoxedTaskAdapter`; дерево исходников: `adapter/{json,boxed}.rs` |

Тесты в `boxed.rs`:

- два `JsonTaskAdapter` с разными payload в одном `TaskDispatcher<BoxedTaskAdapter>`: маршрутизация по формату,
  порядок `formats()` совпадает с порядком регистрации;
- дубликат формата отвергается (`TaskDispatcherError::DuplicateFormat`);
- `json` с невалидным форматом возвращает `TaskFormatError` до регистрации;
- `Ok`, `Retry { retry_after }` и `Permanent` проходят через обёртку без изменений;
- изменяемое состояние обработчика сохраняется между вызовами (счётчик внутри `&mut self`);
- `new` с пользовательским адаптером (не JSON).

Интеграционный тест в `tests/task/runtime/preparation.rs` (фича `worker`): `TaskDispatcherFactory<BoxedTaskAdapter>`
проходит `runtime::prepare`. Он ловит ошибки в границах `Send` и `'static` на настоящем пути рантайма.

### demo

| Файл | Изменение |
|---|---|
| `infrastructure/task_runtime/adapter.rs` | удалить |
| `infrastructure/task_runtime.rs` | убрать `pub mod adapter`, обновить doc-комментарий |
| `infrastructure/task_runtime/dispatcher.rs` | `EncoreAdapter` заменить на `BoxedTaskAdapter`; список регистраций через `BoxedTaskAdapter::json::<P, _>(…)?` (ошибка формата превращается в `TaskDispatcherError` через существующий `From`) |
| `bootstrap.rs` | `Wiring.dispatcher: TaskDispatcherFactory<BoxedTaskAdapter>` |
| `docs/ARCHITECTURE_OPTIONS.md` §11.3, `PLAN.md`, `README.md` | найти упоминания `EncoreTaskAdapter`, сократить чеклист «добавить задачу» до одной строки регистрации |

### Готово, когда

- [x] полный набор проверок обеих сборок зелёный;
- [x] в demo нет enum-адаптера, `grep -rn EncoreTaskAdapter demo/` пуст (кроме исторических документов, если решим их не трогать).
  Остались только исторические места: `API_MAPPING.md`, журнал `PROGRESS.md`, дерево §4 и журнал фаз в `PLAN.md`,
  анализ §3 и §9 в `ARCHITECTURE_OPTIONS.md` (там помечено, что снято).

### Не делаем в этом шаге

Номер воркера в фабрике диспетчера (опция из тикета). Причины в разделе 9, решение Р1.

## 4. Шаг 02. Имя обработчика в ошибке после коммита

Тикет: [02_handler_names_in_failures.md](02_handler_names_in_failures.md). **Ломающее изменение:** вариант
`CommandGatewayError::CommittedWithEventHandlerFailure` меняет поле `handler_indices: Vec<usize>` на
`handlers: Vec<FailedHandler>`.

### cqrsx

| Файл | Изменение |
|---|---|
| `src/cqrs/event_handler.rs` | в трейт `EventHandler` метод `fn name(&self) -> String` с дефолтом `std::any::type_name::<Self>()` |
| `src/cqrs/projection.rs` | поле `name: Option<String>`, `with_name(impl Into<String>)`; `name()` отдаёт заданное имя, иначе `type_name::<V>()` (имя типа view понятнее имени `ViewProjection<…>`) |
| `src/cqrs.rs` | `EventHandlerFailure` получает поле `handler_name: String`; имя берётся только на пути ошибки |
| `src/cqrs/command.rs` | новый `pub struct FailedHandler { pub index: usize, pub name: String }` (`Clone, Debug, PartialEq, Eq`); новое поле варианта; текст `Display` не меняется |
| `src/cqrs/command/executor.rs` | `map_aggregate_error` собирает `FailedHandler`; поправить два теста около строк 657–684 |
| `tests/cqrs/execution.rs:274`, `tests/postgres/projection/committed.rs:161,188` | утверждения дополнить именами |
| `doc/components.md` | строки «Post-commit failures» (индексы и имена) и «Event reaction» (`name`) |

Новые тесты: дефолтное имя равно имени типа; `ViewProjection` без `with_name` называется по типу view, с `with_name`
по заданному; при трёх обработчиках и падении первого и третьего ошибка содержит ровно их, в порядке регистрации.

### demo

| Файл | Изменение |
|---|---|
| `application/command_service.rs` | удалить `NamedHandler`, `HandlerFactory`, поле `projections` и параметр конструктора; `map_gateway_result(result)` строит предупреждения из `handlers[].name`; переписать тест около строки 345 |
| `application/projections/notifying.rs` | убрать параметр имени; имя берётся у внутреннего обработчика через `name()`, и `NotifyingHandler` сам отдаёт его наружу |
| `infrastructure/postgres.rs` | временно: `Executors::executor` принимает `Vec<HandlerFactory<A>>` (тип переезжает сюда) и возвращает только исполнитель. Этот код удаляет шаг 03 |
| `bootstrap.rs` | фабрики обработчиков строят `NotifyingHandler::new(ViewProjection::new(repo).with_name(CONCERT_HALLS), activity)` |
| `tests/write_side.rs`, `application/concert/test_support.rs`, `application/workflows/test_support.rs`, `application/fan.rs` | `CommandService::new` без списка имён |

Проверка поведения: события активити-лога `projection.updated` и `projection.failed` и поле
`warnings[].projection` в ответе команды по-прежнему содержат `concert_halls` и `fan_wallets`. Фронт на эти строки
завязан, поэтому сверяем их в существующих тестах use cases и роутера.

### Готово, когда

- [x] `grep -rn "handler_indices\|NamedHandler" cqrsx demo/back-end` пуст;
- [x] полный набор проверок обеих сборок зелёный.

Сделано с одним дополнением: в demo не было теста, который бы проверял имя проекции в событиях активити-лога,
поэтому добавлен юнит-тест `NotifyingHandler` (`projection.updated` и `projection.failed` с `fan_wallets`).

## 5. Шаг 03. PostgreSQL-исполнитель с services и обработчиками

Тикет: [03_postgres_executor_services_handlers.md](03_postgres_executor_services_handlers.md). **Ломающее
изменение:** `PostgresCommandExecutor::new(pool, config)` становится `new(pool, config, services)`. Функция
`command_executor(pool, config)` остаётся краткой формой для `Services = ()`.

### cqrsx

| Файл | Изменение |
|---|---|
| `src/cqrs/event_handler.rs` | `#[async_trait] impl<A, H> EventHandler<A> for Arc<H> where H: EventHandler<A> + ?Sized` с делегированием `handle` и `name`. Покрывает и `Arc<dyn EventHandler<A>>`, и `Arc<ViewProjection<…>>` |
| `src/postgres/command.rs` | структура `PostgresCommandExecutor<A: Aggregate> { pool, config, services: A::Services, event_handlers: Vec<Arc<dyn EventHandler<A>>> }`; `new(pool, config, services)`; `with_event_handler(Arc<dyn EventHandler<A>>)`; граница `A::Services: Clone` вместо `Services = ()`; в фабрике фреймворка каждый `Arc` клонируется в `Box<dyn EventHandler<A>>` |
| `tests/postgres/command.rs:328` | новый конструктор |
| `doc/components.md` | строки «Backend assembly» (services, обработчики) и «Event reaction» (разделение через `Arc`) |

Тесты:

- юнит-тест в `event_handler.rs` (ядро): `Arc<dyn EventHandler>` делегирует `handle` и `name`;
- интеграционные тесты в `tests/postgres/command.rs` (testcontainers):
  - services доходят до `handle` (правило агрегата зависит от значения в services);
  - обработчик вызывается после коммита с записанными событиями, один и тот же `Arc` обслуживает несколько команд;
  - падение обработчика даёт `CommittedWithEventHandlerFailure` с именем, а событие и задачи при этом записаны.

### demo

| Файл | Изменение |
|---|---|
| `infrastructure/postgres.rs` | удалить `Executors`, `BuildFramework`, `PostgresExecutor`, `OutboxStore`, `HandlerFactory`; `Executors::history()` заменить функцией `history_store(pool, snapshot_every)`. Остаются `HistoryStore`, `event_history`, `history_repositories`, `PostgresLedger` |
| `bootstrap.rs` | алиасы `ConcertApp` и `FanApp` над `PostgresCommandExecutor<_>`; сборка `PostgresCommandExecutor::new(pool.clone(), command_config.clone(), ConcertServices { … }).with_event_handler(Arc::new(NotifyingHandler::new(…)))` |
| `infrastructure/task_runtime/dispatcher.rs` | `PostgresExecutor<Fan>` → `PostgresCommandExecutor<Fan>` |
| `tests/write_side.rs` | библиотечный конструктор и `history_store` |

### Готово, когда

- [ ] в `infrastructure/postgres.rs` нет сборки фреймворка вручную;
- [ ] `tests/write_side.rs` зелёный против PostgreSQL в testcontainers;
- [ ] полный набор проверок обеих сборок зелёный.

## 6. Шаг 04. Тестовый инструментарий уровня use case

Тикет: [04_use_case_testing_toolkit.md](04_use_case_testing_toolkit.md). Ломающих изменений нет. Весь новый API
живёт в публичном `cqrs::testing` и в `task::attempt`, без новых зависимостей.

Отличие от эскиза в тикете: исполнитель называется `TestCommandExecutor<A, S = MemStore<A>>` и параметризован
хранилищем. Иначе тесты demo с `UnavailableStore` (отмена брони при недоступном журнале, закрытая касса) не
переедут. Решение Р5.

### cqrsx

| Файл | Изменение |
|---|---|
| `src/cqrs/testing/command.rs` (новый) | `TestCommandExecutor<A, S = MemStore<A>>`: `new(store, services)`, `with_event_handler(Arc<dyn EventHandler<A>>)`, `with_retry_policy`, `store()`, `take_bound_tasks()` (всё, что передано исполнителю), `take_published_tasks()` (только задачи вызовов, которые закоммитились: `Ok` или `CommittedWithEventHandlerFailure`); `impl CommandExecutor<A>` поверх `CqrsCommandExecutor` |
| `src/cqrs/testing/unavailable_store.rs` (новый) | `UnavailableStore` (`Clone, Copy, Debug, Default`): `EventStore<A>` для любого `A`, каждый вызов возвращает `DatabaseConnectionError` |
| `src/cqrs/testing/mem_view_repository.rs` (новый) | `MemViewRepository<V, A>` (`Clone, Default`): документы хранятся как JSON, поэтому `V: Clone` не нужен, а ошибки serde ловятся так же, как в настоящем хранилище. Правила версий как у `PostgresViewRepository`: вставка при версии 0 и конфликт при существующем документе, обновление только при совпадении версии, переполнение версии даёт ошибку. Методы для тестов: `put(view_id, version, &V)`, `list_all()`, `clear()`. Имена `list_all` и `clear` выбраны заранее: шаг 07 поднимет их в трейт без переименований |
| `src/cqrs/testing.rs` | объявить три модуля |
| `src/task/attempt.rs` | `TaskAttempt::first<P: TaskPayload>(payload: &P, metadata: Value) -> Result<Self, TaskValidationError>`: первая попытка, случайный `task_id`, номер 1 |
| `doc/components.md` | строки «Public testing tools» и «Handler input» |

Тесты: связанные и опубликованные задачи различаются для отклонённой команды; `UnavailableStore` даёт `Unavailable`
через `TestCommandExecutor`; `MemViewRepository` возвращает `OptimisticLockError` при устаревшей версии и при
повторной вставке, `list_all` отсортирован по id, `clear` очищает; `TaskAttempt::first` отвергает payload, который
не сериализуется в объект. Одна doc-проверка на `TestCommandExecutor` как пример для downstream.

### demo

| Файл | Изменение |
|---|---|
| `application/test_support.rs` | удалить целиком (`BoundTasks`, `executor`, `TestFramework`, `TestExecutor`, `UnreachableStore`) |
| `application.rs` | убрать объявление модуля |
| `application/concert/test_support.rs` | `Harness` держит `TestCommandExecutor<Concert, S>`; `h.bound.take()` → `h.executor.take_bound_tasks()` |
| `application/workflows/test_support.rs` | `fans()` возвращает приложение и исполнитель; `attempt(&payload, correlation_id)` через `TaskAttempt::first` |
| `application/fan.rs`, `application/concert/cancel_booking.rs` (тесты) | `UnreachableStore` → `UnavailableStore` |
| `testing.rs` | удалить `MemoryReadModelStore`; добавить `impl ReadModelStore<V> for MemViewRepository<V, A>` (локальный трейт, внешний тип, правило сирот соблюдено) |
| `presentation/http/tests.rs` | витрины сидируются через `MemViewRepository::put` |

Тест «связано, но не опубликовано» (`a_cancellation_that_overtook_its_ticket_is_retried_later`) становится точнее:
связанная задача одна, опубликованных ноль.

### Готово, когда

- [ ] в demo нет собственной тестовой обвязки исполнителя и хранилища витрин;
- [ ] число тестов demo не уменьшилось;
- [ ] полный набор проверок обеих сборок зелёный.

## 7. Шаг 05. Помощник причинности

Тикет: [05_causation_helper.md](05_causation_helper.md). Ломающих изменений нет.

Два отличия от эскиза в тикете, оба про устройство модулей:

- Метод `NewTask::caused_by` не делаем: модуль `task` не должен зависеть от `cqrs`, а `cqrs::command` уже зависит от
  `task`. Вместо него `Causation::stamp_task(task) -> NewTask`. Решение Р8.
- `stamp_task` инфаллибелен. Метаданные задачи по инварианту всегда объект, поэтому достижимой ошибки нет. Для этого
  в `NewTask` появляется `pub(crate)` сеттер объекта метаданных без проверки.

### cqrsx

| Файл | Изменение |
|---|---|
| `Cargo.toml` | `uuid`: добавить фичу `v7` (идентификаторы команд упорядочены по времени, как сейчас в demo). Проверить, что `cargo build --locked` проходит; если `Cargo.lock` изменился, коммитить его вместе с шагом |
| `src/cqrs/command/causation.rs` (новый) | константы ключей `COMMAND_ID`, `CORRELATION_ID`, `CAUSATION_ID`, `SOURCE`; `Causation { command_id, correlation_id, causation_id, source }`; `root(source)`; `from_task(&TaskAttempt)` (корреляция из метаданных задачи, иначе id задачи; причина равна id задачи; источник `task:<type>`); `event_metadata() -> HashMap<String, String>` (четыре ключа); `stamp_task(NewTask) -> NewTask` (три id, **без** `source`) |
| `src/cqrs/command.rs` | `pub mod causation;` |
| `src/task.rs` | `pub(crate) fn merge_metadata(&mut self, entries: Map<String, Value>)` |
| `doc/components.md` | новая строка «Causation» рядом с «Command boundary»: ключи и правило наследования |

Почему `stamp_task` не пишет `source`: сейчас метаданные задач в outbox содержат `command_id`, `correlation_id`,
`causation_id`, `concert_id`, `fan_id`. Новый ключ поменял бы wire-формат строк outbox, а sidecar и рентген их
показывают.

Тесты: у `root` три id равны и это v7; `from_task` наследует корреляцию; падает на id задачи, если ключа нет, если
значение не строка и если строка не UUID; источник `task:<type>`; `event_metadata` содержит ровно четыре ключа;
`stamp_task` перезаписывает только свои три ключа и сохраняет прикладные.

### demo

| Файл | Изменение |
|---|---|
| `application/command_service.rs` | `CommandMetadata { pub causation: Causation, pub recorded_at: String }`; `with_source` и `ui` через `Causation::root`; `from_task(&TaskAttempt)`; `to_map()` = `event_metadata()` плюс `recorded_at`; `publish_outcome` читает id из `causation` |
| `application/tasks.rs` | `new_task` через `metadata.causation.stamp_task(…)`; прикладные ключи `concert_id`, `fan_id` остаются в demo |
| `application/workflows.rs` | удалить `correlation_of` и его тест (покрыт в `cqrsx`); `run_chain_step` теряет параметр типа `P`, метаданные строит `CommandMetadata::from_task(attempt)` |
| `workflows/ticket_delivery/record_ticket.rs`, `workflows/refund/record_cancellation.rs` | вызов `run_chain_step` без турбофиша |
| `presentation/http/concerts/execute_command.rs`, тесты в `concert/book_seat.rs`, `concert/cancel_booking.rs`, `application/tasks.rs`, `tests/write_side.rs` | `metadata.command_id` → `metadata.causation.command_id` |

Wire-сторожа: `tests/write_side.rs` проверяет ключи в `events.metadata` и `correlation_id` в метаданных задачи;
тест `record_ticket` проверяет `causation_id` и `source`; тест `tasks.rs` проверяет метаданные задачи.

### Готово, когда

- [ ] в demo нет ручного разбора `correlation_id` из JSON;
- [ ] JSON метаданных событий и задач побайтно совпадает с прежним по существующим тестам;
- [ ] полный набор проверок обеих сборок зелёный.

## 8. Шаг 06. Прогресс и темп replay

Тикет: [06_replay_progress.md](06_replay_progress.md). Ломающих изменений нет.

### cqrsx

| Файл | Изменение |
|---|---|
| `src/cqrs/event_handler/replay.rs` | `pub struct ReplayProgress<'a> { aggregate_id, sequence, event_type, replayed }`; `pub type ReplayProgressObserver = dyn Fn(&ReplayProgress<'_>) + Send + Sync + 'static`; поля `progress_observer` и `pace: Duration`; `set_progress_observer(&mut self, …)` по образцу `set_error_observer`; `with_pace(self, Duration) -> Self`; `with_upcasters` переписать на `mut self`, чтобы он не терял новые поля. В `run_stream`: пауза перед каждым событием, если `pace > 0`; после успешного `handle` счётчик и вызов наблюдателя; `event_type()` вычисляется только при наличии наблюдателя |
| `doc/components.md` | строка «Event replay»: прогресс и темп |

Тесты: наблюдатель видит каждое успешное событие со счётчиком от 1, в `replay` и в `replay_all`; не вызывается для
события, на котором упал `handle` или декодирование; темп под `#[tokio::test(start_paused = true)]` даёт время не
меньше `n × pace`; нулевой темп не спит; порядок вызовов `with_upcasters` и `with_pace` не теряет настройки.

### demo

| Файл | Изменение |
|---|---|
| `application/projections/progress.rs` | удалить |
| `application/projections.rs` | убрать модуль, поправить doc-комментарий |
| `application/projections/maintenance.rs` | `MaintainedReadModel::rebuild(pace: Duration, on_event: Box<ReplayProgressObserver>)`; `replay_into` настраивает `EventReplay` темпом и наблюдателем; `verify` запускает replay без наблюдателя |
| `application/operations/rebuild.rs` | замыкание прогресса принимает `&ReplayProgress`; медленный режим передаёт `slow_delay`, быстрый `Duration::ZERO`; `FakeReadModel` в тестах вызывает наблюдатель с `ReplayProgress` |

Глобальный счётчик воспроизведённых событий по всем проходам остаётся в `RebuildService`: библиотечный `replayed`
считает в пределах одного replay, а у demo два прохода, по одному на тип агрегата.

### Готово, когда

- [ ] в demo нет собственного декоратора прогресса;
- [ ] тест `a_rebuild_wipes_replays_every_read_model_in_order_and_releases_the_lock` проходит с прежней
  последовательностью событий активити-лога;
- [ ] полный набор проверок обеих сборок зелёный.

## 9. Решения, принятые в плане

Каждое можно пересмотреть до начала работы.

| # | Решение | Альтернатива и почему не она |
|---|---|---|
| Р1 | Номер воркера в фабрике диспетчера (опция тикета 01) **не делаем** | Сигнатура фабрики меняется в 25 местах `cqrsx`, а demo всё равно нужен счётчик: перезапущенный воркер получает тот же номер и с фиксированным сидом повторил бы ту же последовательность сбоев почты |
| Р2 | В ошибке после коммита поле `handlers: Vec<FailedHandler>` вместо `handler_indices` | Добавить `handler_names` рядом: без поломки, но две параллельные коллекции, ровно та проблема, которую решает тикет |
| Р3 | `EventHandler::name()` возвращает `String`, по умолчанию имя типа; у `ViewProjection` имя типа view | `Cow<'static, str>`: экономит аллокацию, но имя нужно только на пути ошибки и в логе активности |
| Р4 | `PostgresCommandExecutor::new(pool, config, services)`, краткая форма `command_executor` остаётся | `new` с `Services: Default`: не ломает вызовы, но заставляет services иметь дефолт, а у реальных services (клиенты, ключи) его нет |
| Р5 | `TestCommandExecutor<A, S = MemStore<A>>`, параметризованный хранилищем, с раздельными «связанными» и «опубликованными» задачами и с обработчиками событий | Исполнитель только над `MemStore`: тесты недоступного журнала в demo не переедут |
| Р6 | `MemViewRepository` хранит JSON, методы `put`, `list_all`, `clear` | Хранить `V`: потребовал бы `V: Clone` и не ловил бы ошибки serde |
| Р7 | `TaskAttempt::first` в `task::attempt`, а не в `cqrs::testing` | Полезен любому, кто вызывает обработчик задач вне рантайма; модуля `task::testing` нет |
| Р8 | `Causation::stamp_task` вместо `NewTask::caused_by` | `caused_by` создал бы зависимость `task → cqrs` при уже существующей `cqrs → task` |
| Р9 | `stamp_task` пишет три id, без `source` | Писать `source`: новый ключ в строках outbox, то есть изменение wire-формата |
| Р10 | Фича `v7` у `uuid` в `cqrsx` | `root(command_id, source)` с id от вызывающего: каждый проект снова решал бы, какой UUID брать |
| Р11 | Один коммит на тикет, обе сборки в нём | Отдельные коммиты для `cqrsx` и demo: после ломающих шагов 02 и 03 demo не собиралась бы между ними |
| Р12 | Шаг 02 оставляет в demo временную `Executors::executor` с фабриками; шаг 03 её удаляет | Слить 02 и 03 в один коммит: меньше промежуточного кода, но крупнее ревью. Допустимо, если удобнее |

## 10. Объём (оценка)

| Шаг | `cqrsx`, строк (код + тесты) | demo, строк | Ломает API |
|---|---:|---:|---|
| 01 | +160 | −50 | нет |
| 02 | +90 | −40 | да |
| 03 | +220 | −60 | да |
| 04 | +380 | −110 | нет |
| 05 | +170 | −30 | нет |
| 06 | +130 | −40 | нет |

## 11. Риски

- **`async_trait` и `impl EventHandler for Arc<H: ?Sized>`** (шаг 03). Если макрос не примет `?Sized`, реализуем
  только для `Arc<dyn EventHandler<A>>`. Demo этого достаточно.
- **`Cargo.lock` после фичи `v7`** (шаг 05). Если фича тянет новую зависимость, обновляются оба lock-файла. Корневой
  CI собирает с `--locked`, поэтому lock коммитится в том же шаге.
- **Покрытие** (шаги 03 и 04). Интеграционные тесты PostgreSQL дают основную долю покрытия шага 03. Если контейнер
  в окружении не поднимается, шаг не закрывается: порог не снижаем и тесты не отключаем.
- **Порядок активити-лога** (шаги 02 и 06). Фронт рисует анимации по последовательности событий лога. Тесты
  use cases и `RebuildService` фиксируют последовательность, их не ослабляем.

## 12. Что не входит

- Тикеты 07–10.
- Изменения HTTP API, фронта, sidecar и схем SQL.
- Слияние `refactor/clean-architecture` в `main`: отдельное решение после этой серии.
