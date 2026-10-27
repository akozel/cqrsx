# 09. Задачи: `#[task_handler]`, `TaskContext`, зависимости, паузы по политике

| | |
|---|---|
| Статус | сделано 2026-10-27; решения пользователя 2026-10-27 (README эпика): общий обработчик без фабрики (вопрос 3), политика по типу задачи (вопрос 2), наблюдателя задач в движке пока нет (вопрос 4) |
| Ценность | высокая: обработчик — функция рядом с задачей, следующая команда — `ctx.execute`, повторы и паузы считает движок |
| Цена | средняя–высокая: контракт `TaskHandler` меняется (общий обработчик без фабрики), контекст задачи, реестр зависимостей, атрибутный макрос |
| cqrsx | `src/task/handler.rs` (`TaskHandler::handle(&self, task, &TaskContext) -> TaskResult`), новые `src/task/{result,context,dependencies,handlers}.rs` (`TaskResult`; `TaskContext`, `Executors`; `Dependencies`, `DependencyId`; `TaskHandlers`), новый `src/cqrs/command/metadata.rs` (`CommandMetadata` из демо), `src/task/adapter{,/json,/boxed}.rs` (`&self`), `src/task/dispatcher.rs`, `src/task/runtime{,/supervisor,/worker_pool,/worker}.rs` (общий диспетчер; `dispatcher_factory.rs` удалён), `src/task/executor{,/default_executor}.rs` (пауза по политике), `src/task/policy.rs` (`Policies::pause`), `src/task/queue/lease.rs` (попытка в `Arc`), `src/postgres/task_runtime.rs`, `src/host/{error,lifecycle}.rs` (тесты), `src/diagnostics.rs`, `cqrsx-macros` (`#[task_handler]`, `syn` с `full`), `tests/{task/**,postgres/{runtime,task_queue/*},derives,log_fallback}.rs`, `README.md`, `doc/components.md` |
| demo | `fan/application/{record_ticket,send_e_ticket,record_cancellation,issue_refund}.rs`, `fan/application.rs` (тест таблицы), `fan/application/test_support.rs`, `fan/application/chain.rs` (удаляется), `shared/application/{mail.rs, policies.rs, test_support.rs, metadata.rs (удаляется)}`, `shared/infrastructure/http_mailer.rs` (без своей формулы паузы), `bootstrap{,/tasks}.rs`, `process.rs`, `concert/presentation/execute_command.rs` и тесты на `CommandMetadata` cqrsx, `tests/{contracts,write_side,architecture}.rs` |
| Зависимости | 07, 08 |
| Гарантии | G-14, G-16, G-17, G-18, G-19, G-21, G-22, G-36 (`task.attempt.*`) |

## Сейчас

- Обработчик — `impl TaskHandler<P> for Handler { async fn handle(&mut self, payload, &TaskAttempt) -> Result<(),
  TaskHandlerError> }`; регистрация — `BoxedTaskAdapter::json(handler)?` в `TaskDispatcher::try_new(vec![..])`,
  фабрика диспетчеров строит свежие обработчики на каждый воркер и перезапуск (`bootstrap/tasks.rs`).
- Следующая команда: `run_chain_step` строит `CommandMetadata::from_task`, зовёт `FanApp::execute` и переводит исход
  в результат задачи (`map_fan_outcome`: принято/R9/R11 — готово, R10/конфликт/недоступно — повтор через 1 с).
- Паузу перед повтором считает обработчик: почта — `MailClientSettings::retry_after` (1, 2, 4, 8, 8 с), шаги —
  `CHAIN_RETRY_AFTER` 1 с; без `retry_after` исполнитель берёт `redelivery_backoff`.

## Станет

Дизайн [05](design/05_tasks.md).

**cqrsx**
- `TaskHandler<P>`: `fn handle(&self, task: P, ctx: &TaskContext) -> impl Future<Output = TaskResult> + Send`;
  обработчик — `Send + Sync`, общий для всех воркеров; `TaskResult` с конструкторами `done()`, `retry(reason)`,
  `fail(reason)` (пауза в результате не передаётся).
- `TaskContext`: `task_id()`, `attempt()` (номер, формат, метаданные), `retry_pause() -> Duration` (пауза, которую
  движок применит к повтору этой попытки по политике задачи — чтобы запись ленты `task.attempt.failed` сохранила
  `retry_after_ms`), `dependency::<T>()`, `execute::<A>(&A::Id, command) -> Result<CommitReport<A>,
  ExecuteFailed<A::Error>>`, где `ExecuteFailed::{Rejected(E), Conflict, Unavailable}`; `impl From<Result<…>> for
  TaskResult`: принято → готово, конфликт и недоступно → повтор, отказ → отказ задачи. Команда из `ctx.execute`
  несёт `Causation::from_task` и `recorded_at` (переезжает из `CommandMetadata::from_task` демо).
- `Executors` — реестр исполнителей по типу агрегата (до 11 заполняет bootstrap демо; в 11 — движок).
- `Dependencies` — реестр по `TypeId` (`provide(Arc<T>)`, `T: ?Sized + Send + Sync + 'static`), так что ключом
  может быть и trait object: `type Mailer = dyn MailService + Send + Sync`, параметр `mail: &Mailer`.
- `#[task_handler]` на `pub async fn name(task: T, ctx: &TaskContext, dep: &D, …) -> TaskResult` порождает
  одноимённое значение-обработчик (`impl TaskHandler<T>`), которое достаёт `D` из `ctx.dependency::<D>()`, и список
  `DEPENDENCIES: &[DependencyId]` для проверки при сборке: каждой зависимости обработчика должен соответствовать
  `.provide`. Параметры распознаются по типу: `T` — первый, `&TaskContext` — по типу, остальные — зависимости.
- Рантайм: один `TaskDispatcher` в `Arc`, общий для воркеров; `TaskDispatcherFactory`, `build_checked_dispatcher`,
  `WorkerSetupError::FormatsMismatch` удаляются; `JsonTaskAdapter::invoke(&self)`, `BoxedTaskAdapter` остаётся
  стиранием типов; перезапуск воркера после паники — с тем же диспетчером. `prepare_task_runtime(pool, dispatcher,
  config, policies)`.
- Пауза по политике: `DefaultTaskExecutor` на `Retry { reason }` берёт политику по формату попытки
  (`policies.pause(attempt.format(), attempt.number())`); формат без политики — `redelivery_backoff(n)`, как сейчас.

**demo**
- Четыре обработчика как функции: `record_ticket(task: RecordTicket, ctx: &TaskContext)` (`ctx.execute::<Fan>`,
  R9 → готово, R10 → повтор), `record_cancellation` (R11 → готово, R10 → повтор), `send_e_ticket(task, mail:
  &Mailer)`, `issue_refund(task, mail: &Mailer)`; `chain.rs` (`ChainStep`, `run_chain_step`, `map_fan_outcome`,
  `CHAIN_RETRY_AFTER`) удаляется.
- `HttpMailer`: без `retry_base`/`retry_max` и своей формулы; `retry_after_ms` в записи ленты — `ctx.retry_pause()`;
  `Mailer::send(&self, ctx, mail)`. `ENCORE_TASK_RETRY_*` читает политика `MAIL` (07).
- `bootstrap/tasks.rs`: `TaskHandlers::new().handler(record_ticket).policy(STEP).handler(…).provide(mail)
  .executors(fans).build()?` — временная сборка до 11; привязка типов задач к политикам переезжает сюда из 07.

**Решения ревью плана (2026-10-27, `work/09-plan-review-r1.md`):**
- привязка типа задачи к политике остаётся в `bootstrap::tasks::policies` (07), рантайм получает `policies`
  (`prepare_task_runtime(pool, dispatcher, config, policies)`); `.task(h).policy(P)` — модуль задачи 11;
- `ExecuteFailed` не вводится: `ctx.execute` отвечает `Result<CommitReport<A>, CommandGatewayError<A::Error>>`, как
  исполнитель; `From<Result<_, CommandGatewayError<E: Rejection>>> for TaskResult`: принято — готово, конфликт и
  недоступно — повтор (`conflict`, `unavailable`), отказ — отказ задачи с кодом;
- оба обработчика шагов разбирают все три кода книжки, как `map_fan_outcome`: R9 и R11 — готово, R10 — повтор
  `unknown_ticket`;
- `TaskContext` без параметра времени жизни (`ctx: &TaskContext` в `async fn`), `LeasedTask` хранит попытку в `Arc`;
- `#[task_handler]` даёт одноимённое значение с методом `call` (функция как написана), тест зовёт
  `send_e_ticket.call(task, &ctx, &mail)`; почтовые обработчики берут и `ctx`;
- `TaskResult` — `Done | Retry { reason } | Fail { reason }` с конструкторами `TaskResult::done()`, `retry(..)`,
  `fail(..)`, заменяет `TaskHandlerError`;
- исполнителя типа нет в контексте — `Unavailable` и запись в журнал; зависимость, которой нет в контексте, собранном
  руками, — отказ задачи;
- `CommandMetadata` — в cqrsx (`cqrs::command::metadata`), `ui()` → `root("ui")`, allowlist presentation +=
  `CommandMetadata`;
- `WorkerSetupError` и `TaskRuntimeError::WorkerSetup` удалены целиком, `prepare` без `Result`;
- `TaskHandlers` — `task/handlers.rs`, `.executor(..)` по одному на тип агрегата; `CHAIN_RETRY_AFTER` — в
  `shared/application/policies.rs`; контракты `…V1` не переименовываются.

## Этапы

1. `TaskResult`, `TaskContext`, `Dependencies`, `Executors`; `TaskHandler` с `&self`; адаптеры и диспетчер; тесты
   адаптеров и диспетчера переводятся (тесты с `Cell`-маркером «не `Sync`» заменяются на «обработчик общий»).
2. Рантайм без фабрики; тесты `tests/task/runtime/*`: `panic_restarts_without_poll_tick_with_a_new_dispatcher_and_
   task` → «…with_the_shared_dispatcher…», `sequential_deliveries_preserve_dispatcher_state_and_tokio_task_identity`
   → состояние обработчика общее; `validates_every_workers_dispatcher_before_polling` → проверка зависимостей при
   сборке; остальные без изменений.
3. Пауза по политике в исполнителе; тест эталона `retry_pauses.txt` через рантайм с очередью в памяти (`MemTaskQueue`
   появится в 11; здесь — приватная очередь из `task/test_support.rs`).
4. `#[task_handler]`: развёртка с тестами (порядок и типы параметров, зависимости, ошибка «нет параметра задачи»,
   `compile_fail` на не-`async`).
5. Демо: обработчики, почта, bootstrap; тесты обработчиков как обычных функций с `RecordingMailer` и тестовым
   контекстом над `TestCommandExecutor`.
6. `components.md`: «Business operation», «Handler input», «Wire adaptation», «Dispatcher factory» (удалить),
   «Prepared runtime», «Supervision», новые «Task context» и «Dependencies».

## Зачем, на примере

```rust
#[task_handler]
pub async fn record_cancellation(task: RecordCancellation, ctx: &TaskContext) -> TaskResult {
    let ledger = FanLedgerId::new(&task.fan_id, &task.concert_id);
    match ctx.execute::<Fan>(&ledger, task.command()).await {
        Err(Rejected(FanError::AlreadyCancelled { .. })) => done(),
        Err(Rejected(FanError::UnknownTicket { .. })) => retry("ticket not recorded yet"),
        other => other.into(),
    }
}
```

Вместо `RecordCancellationHandler { fans }` + `run_chain_step` + `map_fan_outcome` + `CommandMetadata::from_task`:
один файл с контрактом задачи и её обработчиком.

## Риски

- **Общий обработчик вместо свежего на воркер** — изменение механизма (не поведения демо: обработчики демо без
  состояния). Сохраняется: перезапуск после паники, дренаж при остановке (G-21). Решение пользователя 2026-10-27:
  да; записано в «Исключения» задачи 01.
- **Пауза переезжает из обработчика в исполнитель.** Значения те же (эталон 02); `handler timed out` и
  «недостаточно аренды» по-прежнему `redelivery_backoff`.
- **Зависимость по типу для trait object.** `TypeId` у `dyn Trait + Send + Sync` есть; параметр пишется `&Mailer`
  через `type Mailer = dyn …`. Проверить на демо; запасной вариант — обёртка `struct Mailer(Arc<dyn MailService>)`.
- **Атрибутный макрос на `async fn`** сложнее derive: развёртка тестируется на токенах; доступ к `ctx` внутри тела —
  по имени параметра пользователя.

## Как проверить

- Проверки `CLAUDE.md`; матрица фич (`worker` отдельно: рантайм без фабрики); покрытие.
- `tests/postgres/{runtime,task_queue/*}.rs` на PostgreSQL без ослабления.
- Демо: тесты обработчиков, `write_side.rs`, эталоны ленты и пауз.

## Поведение не меняется: чем доказано

- Доставка, claim, аренда, таймауты, восстановление и settle не трогаются (G-16, G-17, G-18, G-20): их тесты
  проходят как есть; меняется только то, как рантайм получает диспетчер.
- Паузы по попыткам совпадают с эталоном (G-19): политики `MAIL`/`STEP` дают 1, 2, 4, 8, 8… с и 1 с; умолчание —
  `redelivery_backoff`.
- Причинность команды из задачи — тот же `Causation::from_task` (G-14), тест `a_command_from_a_task_inherits_…` и
  тест `from_task` метаданных переезжают в cqrsx.
- Исходы агрегата → результат задачи по той же таблице (G-22: тест `outcomes_map_to_task_results_…` переезжает в
  обработчики).
- Записи `task.attempt.*` — те же поля, включая `retry_after_ms` (G-36, эталон ленты).

## Критерии приёмки

- [x] `TaskHandler` с `&self` и `TaskContext`; `TaskResult`; `Dependencies`; `Executors`; `#[task_handler]` с
      тестами и `compile_fail`.
- [x] Рантайм с общим диспетчером; фабрика удалена; проверка зависимостей при сборке.
- [x] Пауза по политике в исполнителе; эталон пауз проходит через рантайм.
- [x] Демо: четыре функции-обработчика; `chain.rs`, `metadata.rs`, формула паузы почты удалены.
- [x] `components.md`, guarantees.md.

## Результат

Сделано 2026-10-27.
- **cqrsx:**
  - `TaskHandler<P>::handle(&self, task, &TaskContext) -> TaskResult` (`Done | Retry { reason } | Fail { reason }`, без
    паузы), `DEPENDENCIES`. `TaskHandlerError` удалён.
  - `TaskContext`: `task_id`, `attempt`, `retry_pause`, `dependency::<T>()`, `execute::<A>` с
    `CommandMetadata::from_task`. Ответ `execute` — `CommandGatewayError`, `From` в `TaskResult`.
  - `Dependencies` по типу, trait object тоже. `Executors` — по одному на тип агрегата.
  - `TaskHandlers` собирает один диспетчер и отказывает зависимости без `.provide`.
  - `#[task_handler]`: функция становится одноимённым значением с методом `call`.
  - Рантайм: один `Arc<TaskDispatcher>` на воркеры, перезапуск с ним же. Фабрика, `WorkerSetupError` и ошибки
    запуска удалены; `prepare(queue, dispatcher, config, policies)`.
  - Исполнитель паузит повтор по политике типа (`Policies::pause`); таймаут и нехватка аренды —
    `redelivery_backoff`.
  - `CommandMetadata` переехал из демо в `cqrs::command::metadata`.
- **Демо:**
  - `record_ticket`, `record_cancellation`, `send_e_ticket`, `issue_refund` — функции рядом с контрактами;
  - `chain.rs` и `metadata.rs` удалены;
  - `MailService`, `type Mailer = dyn MailService`;
  - клиент почты без формулы паузы, `retry_after_ms` — `ctx.retry_pause()`;
  - сборка — `bootstrap::tasks::handlers`.
- **Неизменность:**
  - значения записей `task.attempt.*` и паузы очереди на каждой ветке почты (S0-1), исходы и паузы шагов (S0-2) —
    тесты, написанные и пройденные до замены, после неё утверждения не менялись;
  - эталон пауз проходит через рантайм;
  - эталоны 02 без изменений.
- **Сверка утверждений:** 803 / 24 / 31 / 7 / 6 / 37.
- **Тесты и покрытие:**
  - cqrsx: lib 374, интеграционные 245, doctest'ы 51 + 22, macros 44;
  - демо: 145 + 7 + 9 + 5, sidecar 14;
  - покрытие: workspace 97,75 %, cqrsx 97,62 %.
