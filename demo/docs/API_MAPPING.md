# API_MAPPING.md — Фаза 0: план ↔ реальный API `cqrsx`

**Статус:** Фаза 0 завершена. Есть **контрольная точка** (§1) — требуется решение человека до начала Фазы 1.
Код демо не написан; этот документ — единственный артефакт фазы.

**Основание:** `cqrsx` 0.1.0, коммит `4d8e695` репозитория `event-sourcing`. Прочитаны целиком `cqrsx/src/**`
(15 508 строк), `cqrsx/tests/**` (9 894 строки), `cqrsx/sql/*.sql`, `cqrsx/doc/components.md`, корневые
`Cargo.toml` / `Cargo.lock` / `compose.yaml` / `rust-toolchain.toml` / `.github/workflows/ci.yml`.

**Метод проверки:** чтение исходников и интеграционных тестов библиотеки (тесты компилируются и проходят в CI
библиотеки, поэтому их сборочные паттерны — надёжный образец). В песочнице нет сети и реестра cargo, собрать
пробный код было нельзя. Утверждения про внешние крейты (axum, mongodb, Vite/Vue) **не проверялись** и помечены.

Пути ниже — относительно `cqrsx/src/`, если не сказано иное; `файл:строка` — место в исходнике.

---

## 0. Главное за минуту

| | Вывод |
|---|---|
| **Совпадает с планом** | набросок агрегата §2.6 верен дословно (`async fn handle(&mut self, cmd, &svc, &EventSink<Self>)`, `sink.write(event, self).await`, `apply`); маппинг `CommandGatewayError` → HTTP (§6.6); контракт `ViewRepository`/`ViewContext` (§6.7); `EventReplay`/`ReplayStream` (§2.9); `TestFramework` given/when/then (§6.9); `host::lifecycle::run` (§6.3); снапшоты через интервал (§2.3); SQL-схемы как файлы (§5.2); prebuilt-задачи (U1) |
| **Расходится, решение без потери поведения** | U3 — порт `CommandExecutor` не принимает метаданные (нужен декоратор `EventStore`); U5 — статусы outbox проще, чем в плане; U6 — backoff задаёт обработчик через `retry_after`; U7 — два `JsonTaskAdapter` оборачиваются в enum; U10 — lease ≥ 25 с, а не 10; U11 — нужны features `host`, `logging` и крейт `async-trait`; U13 — SSE-потоки должны завершаться по отмене; U15 — сортировка по `metadata.recorded_at`; `connect_pool` принимает `PgConnectOptions`, а не URL; `RUST_LOG` — один уровень без директив |
| **Расходится, поведение меняется → контрольная точка** | **U2:** репозиторий с outbox коммитит ровно одно событие на команду — `ConcertSoldOut` вместе с `SeatBooked` невозможен. **U3:** выбор варианта A/B/C. Имена форматов задач: точки в `task_type` ломают диагностику библиотеки |
| **Блокеры** | нет |

---

## 1. Контрольная точка — решения человека до Фазы 1

> **Итог обсуждения (17.10.2026, решения внесены в PLAN.md v1.1, §15):**
> - КТ-1 → принят вариант «два агрегата»: `Concert` (места, R1–R8 синхронно) и `Fan` (билеты зрителя), связанные
>   цепочкой outbox `SeatBooked → encore_record_ticket → Fan.RecordTicket → TicketRecorded → encore_send_e_ticket`.
>   `ConcertSoldOut` снят, `sold_out` производное. Правило: принятая команда даёт ровно одно событие (D-01…D-03).
> - КТ-2 → вместо декоратора правится библиотека: `CommandExecutor::execute_with_metadata` (Фаза 0.5 плана, D-04).
>   `committed_sequence` и счётчик конфликтов из ответа команды убраны (P2).
> - КТ-3 → имена задач в snake_case приняты; форматов четыре (D-05).
> Текст ниже сохранён как протокол анализа.

### КТ-1 (U2). Событие `ConcertSoldOut` исчезает из журнала

**Факт.** `postgres/outbox/write_plan.rs:73-93`: `WritePlan::new` принимает `[]` (NoOp) или ровно `[event]`;
два и более события → `WriteValidationError::MultipleEvents` («a command may commit at most one domain event») →
`PersistenceError::UnknownError` → `AggregateError::UnexpectedError` → `CommandGatewayError::Unavailable` → по §6.6
это **HTTP 503**, и ничего не записано. Подтверждено тестом
`tests/postgres/outbox/publication.rs::rejects_multi_event_snapshot_only_and_tasks_without_event_before_writing`.
`PersistedEventStore::commit` передаёт в `persist` **все** события из `EventSink` одним вызовом, разбить нельзя.
Обычный `PostgresEventRepository` многособытийные батчи поддерживает, но у него нет outbox.

**Ловушка.** `TestFramework` этот предел не проверяет: доменный тест «`SeatBooked` + `ConcertSoldOut`» прошёл бы,
а рантайм вернул бы 503. В Фазе 2 нужен страховочный тест «любая принятая команда → ровно одно событие».

**Рекомендация (fallback из §13):** убрать `ConcertSoldOut` из событий. `sold_out` остаётся производным полем —
в агрегате `is_sold_out()` (для R4) и в витрине `concert_halls.sold_out = sold == capacity`. Конфетти (§8.8) и
бейдж уже привязаны к переходу `sold_out: false → true` в витрине, поэтому UI-сценарий не меняется.

**Альтернатива (по желанию):** добавить в `SeatBooked` поле `sold_out: bool` («эта бронь закрыла зал») — факт на
момент события, стоит ноль, виден в X-ray на строке последней брони. Это не второе событие, ограничение не нарушает.

**Что меняется в плане:** §2.4 (таблица команд и заметка про «несколько событий»), §2.6 (`apply` — три
варианта), §2.7 и §14 (P2 `encore.band.notify` не на что повесить; можно снять или сделать P3 с проверкой
«39/40 → 40/40» в application-слое), §3.2 (диаграмма: `SeatBooked (+ ConcertSoldOut)`), §6.9 (тест «ConcertSoldOut
при последнем месте» → «после последней брони `is_sold_out()`»), §8.8 (триггер только по витрине), §9 шаг 5
(«событие `ConcertSoldOut`» → «`sold_out: true` в витрине»).

### КТ-2 (U3). Как передавать метаданные событий

**Факт.** `CqrsFramework::execute_with_metadata(&self, aggregate_id, command, metadata: HashMap<String, String>)`
существует (`cqrs.rs:522`) и кладёт карту в `EventEnvelope.metadata` каждого события → колонка `events.metadata`.
Но порт `CommandExecutor::execute(&self, aggregate_id, command, tasks)` (`cqrs/command.rs:34`) **не имеет**
параметра метаданных, а `CqrsCommandExecutor::execute` вызывает `framework.execute(id, command.clone())`
(`cqrs/command/executor.rs:51-53`), то есть `HashMap::new()` (`cqrs.rs:398-403`). Через командный порт библиотеки
`events.metadata` всегда `{}`. Повторить retry самостоятельно «как в библиотеке» нельзя:
`retry_on_aggregate_conflict` и `AggregateConflictRetryPolicy::backoff` — `pub(super)`.

| Вариант | Суть | Что видно в демо | Цена |
|---|---|---|---|
| **A (рекомендую)** | Декоратор `CommandContextStore<ES>: EventStore<Concert>` (≈40 строк в демо): делегирует `load_*`, в `commit` добавляет метаданные команды, запоминает `last_sequence` закоммиченного батча и считает `AggregateConflict`. `CqrsCommandExecutor` создаётся **на каждый запрос** с замыканием, захватившим метаданные (так же делает `PostgresCommandExecutor::execute`, `postgres/command.rs:110-118`). Образец оборачивания стора внутри `CqrsCommandExecutor` — `tests/postgres/command.rs::contracts::CommitBoundary` | всё как в плане: `metadata = {command_id, correlation_id, recorded_at, source}` в X-ray; `committed_sequence` в ответе 200 (иначе недоступен — успех возвращает `()`); бонус: число конфликтов для «Гонки» (U16) | один вспомогательный тип с комментарием «почему»; библиотечный retry и диагностика сохраняются |
| B | Демо-сервис зовёт `CqrsFramework::execute_with_metadata` напрямую, свой цикл retry (3 попытки, 5–50 мс) | то же, что A, плюс полные тексты ошибок обработчиков | обходит `CommandExecutor`/`CqrsCommandExecutor`/политику retry библиотеки — демо перестаёт быть образцом командного порта; своё логирование отказов |
| C (fallback §13) | Без метаданных: `announced_at`/`booked_at`/`cancelled_at` в payload, `command_id` только в задачах | `events.metadata = {}` в X-ray; трассировка событие ↔ задача только по `aggregate_id + aggregate_sequence`; U15 сортировка «последних событий» — по `ctid` или полю payload | меньше кода, но наблюдаемое поведение беднее плана |

**Рекомендация: A.** Сохраняет наблюдаемое поведение плана и библиотечный командный путь. Вариант A заодно
закрывает U16 (конфликты считаются в декораторе) и даёт `committed_sequence`.

### КТ-3 (U7). Имена форматов задач

`diagnostics.rs:3` `safe_type` пропускает в логи только `[A-Za-z0-9_]`, иначе печатает `unknown`. С именами
`encore.e_ticket.send` все строки `task failed` в логах back-end будут `task_type=unknown`. Предложение:
`encore_send_e_ticket` v1 и `encore_issue_refund` v1 (стиль библиотечных тестов: `dummy_task`). Видно в X-ray
(колонка `task_type`) и в поле `tasks` ответа API; механику не меняет. `Aggregate::TYPE = "Concert"` — безопасно.

### Информационно (решения не требует, но меняет цифры плана)

- **U10.** Бюджет lease проверяется формулой `lease > 2·S + H + 5 с + ceil(Q/C)·(H+S)`. Для демо: C=2, Q=2,
  poll 300 мс, H=5 с, S=3 с → требуется > 24 с → **lease 30 с** (в плане «≥ 10 с» — не пройдёт валидацию).
- **U6.** Короткий backoff 1 → 2 → 4 → 8 → 8 с реализуется самим обработчиком через `retry_after`.
- **U8.** `replay_all` отдаёт события **всех** концертов, отсортированные по `sequence` (поперёк агрегатов): при
  Encore! места «загораются» по очереди в трёх залах, а не зал за залом. Для LWW-проекций это корректно.

---

## 2. Сводная таблица U1–U16

| # | Вопрос | Факт | Решение | Поведение | Правка плана |
|---|---|---|---|---|---|
| U1 | привязка задач | prebuilt `Vec<NewTask>` в `execute(id, cmd, tasks)`; пишутся с `aggregate_type/id/sequence` события | как в плане; `task_id` генерирует библиотека (v4), стабилен при retry | = | уточнение |
| U2 | несколько событий | **нельзя** с outbox-репозиторием (503) | убрать `ConcertSoldOut`; `sold_out` производное; страховочный тест | **меняется** | КТ-1 |
| U3 | метаданные | есть во framework, нет в порту executor | вариант A: декоратор `EventStore` + executor на запрос | = при A | КТ-2 |
| U4 | view-id из события | нет: `view_id == aggregate_id` | свой `EventHandler` `FanWalletsProjection` (как в плане) | = | уточнение |
| U5 | схема outbox | `transactional_outbox`, статусы `pending/processing/completed/failed`, строки не удаляются, `last_error` есть | константы sidecar и `outboxStatus.ts` проще плана | = | упростить §8.5 |
| U6 | backoff | `redelivery_backoff` фиксирован (5 с·2ⁿ⁻¹, до 300 с), но `Retry{retry_after: Some}` уважается | обработчик отдаёт 1–8 с; две env-переменные | = | §2.7, §5.4 |
| U7 | TaskHandler/Attempt/Factory | native `async fn handle(&mut self, P, &TaskAttempt)`; `Retry{reason, retry_after}` / `Permanent{reason}`; `number()` = номер claim; фабрика `Arc<dyn Fn() -> Result<TaskDispatcher<A>, _> + Send + Sync>` | enum-адаптер над двумя `JsonTaskAdapter`; `reason` = код причины (попадает в `last_error`) | = (имена → КТ-3) | §6.2 |
| U8 | EventReplay/ReplayStream | `new(repo, handler)` по значению; `replay(id)`, `replay_all()`; по одному событию; стоп на первой ошибке | как в плане; машина времени — `PersistedEventStore::new_event_store(..).load_events(id)` | порядок replay_all | §3.4, §2.10 |
| U9 | SQL и compose | файлы `cqrsx/sql/001-event-store.sql`, `002-transactional-outbox.sql`; константы `EVENT_STORE_SCHEMA` и др. — `include_str!` тех же файлов; том `/var/lib/postgresql`; `:ro,Z` в корне | монтировать файлы; для общих с корнем файлов `z` | = | §5.2 |
| U10 | env cqrsx | 7 ключей `OUTBOX_*`, читаются только явным `from_env()`/`from_lookup()`; кросс-валидация бюджета | `from_lookup` с демо-дефолтами; lease 30 с | = | §5.4, §2.7 |
| U11 | features/версии | default `postgres,worker`; `host`, `logging` — опциональны; sqlx 0.8.6; edition 2024; rust 1.98.1 | `features = ["host","logging"]`; добавить `async-trait` | = | §6.1 |
| U12 | снапшоты | `PersistedEventStore::new_snapshot_store(repo, n)`; `PostgresCommandConfig::new(n, policy)`; одна строка на агрегат, обновляется на месте | как в плане (`ENCORE_SNAPSHOT_EVERY=10`) | = | §8.3 маркер 📸 |
| U13 | lifecycle::run | `run(inbound: io::Result<()>, task_runtime: Send+'static, token, signal)` | как в плане + SSE завершать по токену | = | §6.3 п.11 |
| U14 | event_version | строка хранится как есть, сверяется на равенство; `NumericVersion` парсит `1`, `1.0`, `1.0.0` | `"1.0"` | = | — |
| U15 | порядок событий | ни глобальной позиции, ни timestamp; `payload/metadata` — `json` | `ORDER BY metadata->>'recorded_at' DESC` (фиксированная ширина RFC 3339) при варианте A; иначе `ctid` | = при A | §7.3 |
| U16 | наблюдаемость конфликтов | нет: retry приватен, конфликты не логируются | при A — счётчик в декораторе → `conflicts` в ответе/activity | лучше плана при A | §6.4 |

---

## 3. Карточки U1–U16

### U1. Как задачи привязываются к команде

**Факт.**
- `cqrs/command.rs:29-40`: `trait CommandExecutor<A> { fn execute(&self, aggregate_id: &str, command: A::Command, tasks: Vec<NewTask>) -> impl Future<Output = Result<(), CommandGatewayError<A::Error>>> + Send; }`.
- `cqrs/command/executor.rs:17-57`: `CqrsCommandExecutor::new(build_framework, retry_policy)`; `execute` вызывает фабрику **один раз** на вызов с `tasks`, повторяет только `AggregateConflict` клонами той же команды; список задач неизменяем между попытками (тест `executor_builds_once_and_retries_the_unchanged_command_and_tasks`).
- Задачи попадают в репозиторий через `PostgresEventRepositoryWithOutbox::new(pool).with_outbox(tasks)` (`postgres/outbox.rs:40`); писатель (`postgres/outbox/write.rs:92-109`) вставляет каждую с `aggregate_type = A::TYPE`, `aggregate_id`, `aggregate_sequence = sequence события`, `task_type`, `task_version`, `payload`, `metadata`, `available_at` (`Now` / `At(t)` / `After(d)`), `max_attempts` — одной CTE-инструкцией с событием (и снапшотом).
- Вывод задач из событий или через `Services` не предусмотрен. Отклонённая команда ничего не пишет (framework возвращает до `commit`). Задачи при нуле событий → `TasksWithoutEvent` → `Unavailable`.
- `NewTask` (`task.rs:41-104`): `new<T: TaskPayload>(payload) -> Result<Self, TaskValidationError>` (payload должен сериализоваться в JSON-объект), `.with_metadata(Value)?` (объект), `.with_max_attempts(i32)?` (> 0, по умолчанию 10), `.scheduled(TaskSchedule)?`; `id()` — `Uuid::new_v4()`, задаётся библиотекой, стабилен при conflict-retry.

**Решение.** Как в плане: application-слой строит задачи до выполнения команды. `booking_id`/`cancellation_id` = `command_id` (поле payload), `task_id` ≠ `booking_id`. Связь «событие ↔ задача» в UI есть и без метаданных: строка outbox несёт `aggregate_id` + `aggregate_sequence`. `AnnounceConcert` → `tasks = []` обязательно.

**Правка плана.** §2.7: добавить, что `task_id` генерирует библиотека; §6.4: `tasks` в ответе — `TaskFormat` отображается как `"{type} v{version}"` (`task/format.rs:46`).

### U2. Несколько событий в одном коммите — см. КТ-1

Дополнительно: `components.md` формулирует контракт прямо — «One event, optional checkpoint and explicit tasks in one atomic SQL statement»; `postgres/command.rs:58-59` — «at most one event is supported per command».

### U3. Метаданные при выполнении команды — см. КТ-2

Дополнительно:
- `EventEnvelope.metadata: HashMap<String, String>` (`cqrs/event.rs:189`) — только строковые значения; `recorded_at` — строка RFC 3339.
- Метаданные одинаковы для всех событий одного коммита (`persisted.rs:509-525` клонирует `base_metadata`).
- Успешный `execute` возвращает `()`; `last_sequence` доступен только в `CommandGatewayError::CommittedWithEventHandlerFailure { last_sequence, handler_indices }`. Поэтому `committed_sequence` из §6.4 без декоратора (A) или прямого вызова framework (B) получить нельзя.

Эскиз декоратора для варианта A (документация, не код демо):

```rust
// demo/back-end/src/app/command_context.rs (эскиз)
struct CommandContextStore<ES> { inner: ES, metadata: HashMap<String, String>, outcome: Arc<Mutex<CommitOutcome>> }
#[derive(Default)] struct CommitOutcome { last_sequence: Option<usize>, conflicts: u32 }
impl<ES: EventStore<Concert>> EventStore<Concert> for CommandContextStore<ES> {
    type AC = ES::AC;
    async fn load_events(&self, id: &str) -> Result<Vec<EventEnvelope<Concert>>, AggregateError<ConcertError>> { self.inner.load_events(id).await }
    async fn load_aggregate(&self, id: &str) -> Result<Self::AC, AggregateError<ConcertError>> { self.inner.load_aggregate(id).await }
    async fn commit(&self, events: Vec<ConcertEvent>, ctx: Self::AC, mut metadata: HashMap<String, String>)
        -> Result<Vec<EventEnvelope<Concert>>, AggregateError<ConcertError>> {
        metadata.extend(self.metadata.iter().map(|(k, v)| (k.clone(), v.clone())));
        match self.inner.commit(events, ctx, metadata).await {
            Ok(committed) => { /* outcome.last_sequence = committed.last().map(|e| e.sequence) */ Ok(committed) }
            Err(AggregateError::AggregateConflict) => { /* outcome.conflicts += 1 */ Err(AggregateError::AggregateConflict) }
            Err(other) => Err(other),
        }
    }
}
// на каждый запрос:
let executor = CqrsCommandExecutor::<Concert, _>::new(move |tasks| CqrsFramework::new(
    CommandContextStore { inner: PersistedEventStore::new_snapshot_store(
        PostgresEventRepositoryWithOutbox::new(pool.clone()).with_outbox(tasks), snapshot_every), metadata: meta.clone(), outcome: outcome.clone() },
    handlers(), services.clone()), config.conflict_retry());
executor.execute(&concert_id, command, tasks).await   // -> Result<(), CommandGatewayError<ConcertError>>
```

Ограничения, которые нужно соблюсти: `A: Aggregate + Clone + 'static`, `A::Command: Clone + Send + 'static`,
`A::Error: Send + 'static`, `ES: EventStore<A> + 'static`, `ES::AC: Send` (`cqrs/command/executor.rs:33-48, 76-83`).
`CqrsFramework::new` забирает `Services` по значению → `ConcertServices` должен быть `Clone` (он тривиален).

### U4. Умеет ли `ViewProjection` вычислять view-id из события

**Факт.** `cqrs/projection.rs:509-521`: `impl EventHandler<A> for ViewProjection` — `handle(&self, view_id: &str, events)`; framework передаёт сюда `aggregate_id` (`cqrs.rs:546`). Вычисления id из события нет. `apply_events` — `pub(crate)` (`projection.rs:488`), переиспользовать цикл retry нельзя. Публичны: `ViewProjection::new(Arc<R>)`, `with_max_attempts(NonZeroUsize)` (по умолчанию 16), `set_error_observer(Box<PersistenceErrorObserver>)`, `load(view_id)`.

**Решение.** Как в плане: `FanWalletsProjection` — собственный `#[async_trait] impl EventHandler<Concert>`; группирует события по `fan_id`; на каждый кошелёк: `load_with_context` → `Default` при `None` с `ViewContext::new(id, 0, 0)` → `View::update` → `update_view`; при `OptimisticLockError` повтор (≤ 16 попыток, `tokio::task::yield_now().await` между ними — как в `projection.rs:489-499`). Остальные ошибки — наружу.

**Правка плана.** §6.2: уточнить, что крейту нужен `async-trait` (трейт `EventHandler` объявлен через `#[async_trait]`, `cqrs/event_handler.rs:14`); `TaskHandler`, `TaskAdapter`, `ViewRepository`, `EventStore`, `Aggregate` — native `async fn`.

**Обновление 2026-10-18 (D-14).** Поток `Fan` стал журналом на пару «фанат + концерт» (`aggregate_id = <fan_id>:<concert_id>`), `fan_wallets` — документ на журнал с тем же `_id`; `ViewProjection` остаётся. `GET /api/fans/{fan_id}/wallet` собирает кошелёк из диапазона `_id` `[<fan>:, <fan>;)` (`ReadModelStore::list_range`, `application/fan/wallet.rs`) и отвечает `{ fan_id, ledgers: [{ concert_id, version, last_sequence }], view }`. Причина — нагрузочный прогон 03b (`docs/discussions/06_fan_ledger_growth.md`).

### U5. Схема outbox

**Факт** (`cqrsx/sql/002-transactional-outbox.sql`): таблица `transactional_outbox`:

| Колонка | Тип | Семантика |
|---|---|---|
| `id` | uuid PK | `NewTask::id()` |
| `aggregate_type`, `aggregate_id`, `aggregate_sequence` | text, text, bigint > 0 | ключ события, с которым задача записана атомарно |
| `task_type`, `task_version` | text ≠ '', integer > 0 | `TaskFormat` |
| `payload`, `metadata` | jsonb (объекты) | из `NewTask` |
| `status` | text | **`pending` \| `processing` \| `completed` \| `failed`** |
| `created_at`, `available_at` | timestamptz (default `statement_timestamp()`) | создание; момент готовности к claim |
| `finished_at` | timestamptz | заполнено ⇔ `status ∈ {completed, failed}` |
| `attempts`, `max_attempts` | integer | `attempts` растёт при **claim** (`claim.rs:70`); `max_attempts` из `NewTask` |
| `last_error` | text (≤ 2048 симв., `settlement.rs:89`) | `reason` из `TaskHandlerError`, либо `worker lease expired`, `handler timed out`, `insufficient lease budget before handler execution` |
| `locked_by`, `lock_token`, `locked_until` | text, uuid, timestamptz | заполнены ⇔ `processing`; `locked_by` = owner-UUID одного запуска runtime (`supervisor.rs:73`), не воркера |

Инварианты закреплены CHECK-ограничениями (тест `tests/postgres/schema.rs`). Индексы: `(task_type, task_version, available_at, id) WHERE status='pending'`, `(locked_until, id) WHERE status='processing'`.

Переходы: claim → `processing`, `attempts+1`, lease = `clock_timestamp() + lease_ms`; settle `Completed` → `completed`; settle `Retry{delay}` → `pending`, `available_at = now + delay`, `last_error = reason` (если `attempts >= max_attempts` — сразу `failed` с тем же `last_error`; в логах cause `retry limit reached`); settle `Failed` → `failed`, `last_error = reason`; recovery просроченных lease → `pending` с `redelivery_backoff` (или `failed`), `last_error = 'worker lease expired'`. **Завершённые строки не удаляются** (тест `tests/postgres/runtime.rs::old_terminal_tasks_are_retained_...`). Requeue: `REQUEUE_FAILED_OUTBOX_TASK` с одним uuid-параметром, только из `failed`.

**Решение.** `lib/outboxStatus.ts` упрощается — статус в колонке явный:

```ts
export function deriveTaskStatus(row: OutboxRow, dbNow: Date): UiTaskStatus {
  switch (row.status) {
    case 'completed':  return 'delivered';
    case 'failed':     return 'failed';
    case 'processing': return 'in_progress';                 // lease истёк, но recovery ещё не прошёл — тоже in_progress (≤ poll)
    case 'pending':    return row.attempts > 0 ? 'retry_scheduled' : 'queued';
  }
}
// обратный отсчёт: available_at − dbNow; ≤ 0 → «ждёт воркера»
```

«Призрачные» строки (§8.5) не нужны. Причина 🎲 — из `last_error` (код причины); activity остаётся источником для анимации. Sidecar: `ORDER BY created_at DESC, id`; счётчики `GROUP BY status`; «попытка n/max» = `attempts/max_attempts`.

### U6. Настраивается ли backoff

**Факт.** `task/queue/retry.rs:3-8`: `redelivery_backoff(attempt) = min(5 с · 2^min(attempt−1, 6), 300 с) × U(0.8, 1.2)` — не настраивается. Используется, когда обработчик вернул `Retry { retry_after: None }`, при таймауте обработчика, при `insufficient lease budget` и при восстановлении просроченного lease. Если обработчик вернул `Retry { retry_after: Some(d) }`, задержка `d` применяется как есть (`default_executor.rs:93-113`; тест `worker_test_aggregate_covers_success_retry_exhaustion_and_permanent_failure`: `available_at ≈ now + 300 с` при `retry_after = 300 с`).

**Решение.** `FlakyMailServer` возвращает `TaskHandlerError::Retry { reason: reason_code, retry_after: Some(min(base · 2^(attempt−1), max)) }` с `ENCORE_TASK_RETRY_BASE_MS = 1000`, `ENCORE_TASK_RETRY_MAX_MS = 8000` → 1, 2, 4, 8, 8 с. В README: библиотечный backoff 4–300 с действует только для аварийных путей (истёкший lease, таймаут обработчика).

**Правка плана.** §2.7 («если настраивается») → настраивается обработчиком; §5.4 — две переменные.

### U7. API `TaskHandlerError`, `TaskAttempt`, `TaskDispatcherFactory`

**Факт.**
- `task/handler.rs:8-16`: `enum TaskHandlerError { Retry { reason: String, retry_after: Option<Duration> }, Permanent { reason: String } }`.
- `task/handler.rs:60-66`: `trait TaskHandler<P>: Send where P: TaskPayload + DeserializeOwned + Send + 'static { fn handle(&mut self, payload: P, attempt: &TaskAttempt) -> impl Future<Output = Result<(), TaskHandlerError>> + Send; }` — `&mut self`, без `async_trait`, обработчику не нужны `Sync`/`Clone`.
- `task/format.rs:6-9`: `trait TaskPayload: Serialize { const TASK_TYPE: &'static str; const TASK_VERSION: i32; }`.
- `task/attempt.rs:9-54`: `TaskAttempt { task_id() -> Uuid, format() -> &TaskFormat, payload() -> &Value, metadata() -> &Value, number() -> i32 }`; `number()` — номер claim (1-based, включая claim-ы без вызова обработчика); lease-токен обработчику недоступен.
- `task/adapter/json.rs:36`: `JsonTaskAdapter::<P, H>::new(handler: H) -> Result<Self, TaskFormatError>`; неверный формат/payload → `Permanent` без вызова обработчика.
- `task/dispatcher.rs:44`: `TaskDispatcher::<A: TaskAdapter>::try_new(Vec<A>)` — **один тип адаптера** на диспетчер; для двух разных payload-типов нужен enum, делегирующий `formats()`/`invoke()` (образец: `WorkerAdapter` в `tests/postgres/runtime.rs:128-145`).
- `task/runtime/dispatcher_factory.rs:13`: `type TaskDispatcherFactory<A> = Arc<dyn Fn() -> Result<TaskDispatcher<A>, TaskDispatcherError> + Send + Sync>`; вызывается C раз при `prepare` и при каждом перезапуске воркера; у всех экземпляров должен совпадать **упорядоченный** список форматов (`WorkerSetupError::FormatsMismatch`). Индекс воркера в фабрику не передаётся.

**Решение.**
- `enum EncoreTaskAdapter { ETicket(JsonTaskAdapter<SendETicketV1, SendETicketHandler>), Refund(JsonTaskAdapter<IssueRefundV1, IssueRefundHandler>) }` + `impl TaskAdapter`.
- Фабрика создаёт свежие обработчики с собственным RNG; детерминированный seed — `ENCORE_CHAOS_SEED + счётчик (AtomicU64)` внутри замыкания, потому что индекс воркера недоступен.
- `reason` = код причины (`smtp_timeout` и т. д.) — он попадает в `last_error` и виден в X-ray без activity.
- Имена форматов — КТ-3.

### U8. API `EventReplay` / `ReplayStream`

> 03/03 (2026-10-24): рядом с `EventReplay` появился `cqrs::projection::processor::ProjectionProcessor` — проекция
> читает журнал сама от закладки (`CheckpointStore`, `PostgresCheckpointStore`), пересборка — `request_rebuild` /
> `request_rebuild_key` через того же рабочего ключа; `EventReplay` уходит в 03/05.

**Факт.**
- `cqrs/event_handler/replay.rs:158-390`: `EventReplay::<R: PersistedEventRepository, H: EventHandler<A>, A>::new(repository: R, handler: H)` — оба **по значению**; `with_upcasters(Vec<Box<dyn EventUpcaster>>)`; `set_error_observer(&mut self, Box<PersistenceErrorObserver>)`; `async fn replay(&self, aggregate_id) -> Result<(), PersistenceError>`; `async fn replay_all(&self)`. Каждое событие передаётся **отдельно**: `handler.handle(&event.aggregate_id, &[event])`. Останавливается на первой ошибке открытия/чтения/декодирования/обработчика; применённый префикс остаётся (док: «rebuild into a clean projection»).
- `postgres/event_repository.rs:61-79` + `sql_query.rs`: `stream_events::<A>(id)` = `SELECT … WHERE aggregate_type=$1 AND aggregate_id=$2 ORDER BY sequence`; `stream_all_events::<A>()` = `… WHERE aggregate_type=$1 ORDER BY sequence` — **сортировка только по `sequence` поперёк агрегатов** (порядок внутри каждого агрегата сохраняется; проверено тестом `replay_isolates_ids_and_types_...`).
- `postgres/event_repository/stream.rs`: поток держит одно соединение пула до конца; при `drop(ReplayStream)` SQL отменяется, соединение закрывается, а не возвращается в пул; ошибка БД приходит терминальным элементом `Err` и затем EOF. Буфер канала — 200 (`with_streaming_channel_size`).
- `ReplayStream::next::<A>(&mut self, upcasters: &[Box<dyn EventUpcaster>]) -> Option<Result<EventEnvelope<A>, PersistenceError>>` (`stream.rs:24`).

**Решение.**
- Rebuild: `EventReplay::new(PostgresEventRepository::new(pool.clone()), ProgressHandler::new(FanOutHandler::new(vec![concert_halls, fan_wallets]), ...)).replay_all().await` — репозиторий и обработчик создаются на каждый rebuild (проекции держат `Arc<MongoViewRepository>`, дорогого состояния нет). Ошибку → `rebuild.failed`, lock снимается guard-ом.
- Машина времени / timeline: `PersistedEventStore::<PostgresEventRepository, Concert>::new_event_store(PostgresEventRepository::new(pool.clone())).load_events(id).await` → `Vec<EventEnvelope<Concert>>` (снапшоты не участвуют — как требует §2.10), далее `Concert::apply` до нужного `sequence`. Альтернатива — `repo.stream_events::<Concert>(id)` + `next`.
- `rebuild.started` может сразу нести `total` (`SELECT count(*) FROM events WHERE aggregate_type='Concert'`) — прогресс не зависит от sidecar.

**Правка плана.** §3.4/§8.3: порядок событий при Encore! — поперёк концертов по `sequence`.

### U9. SQL-файлы схем, том PG 18, SELinux

**Факт.** Файлы: `cqrsx/sql/001-event-store.sql` (`events`, `snapshots`), `cqrsx/sql/002-transactional-outbox.sql`, `cqrsx/sql/requeue-failed-outbox.sql`; все `CREATE TABLE IF NOT EXISTS` (идемпотентны), схема `public`. Константы `postgres::{EVENT_STORE_SCHEMA, TRANSACTIONAL_OUTBOX_SCHEMA, REQUEUE_FAILED_OUTBOX_TASK}` — `include_str!` тех же файлов (`postgres.rs:9-15`); тесты применяют их через `sqlx::raw_sql(..)`. Корневой `compose.yaml`: образ `docker.io/library/postgres:18.6-alpine`, том `cqrsx_postgres_data:/var/lib/postgresql`, монтирование `./cqrsx/sql/00X-*.sql:/docker-entrypoint-initdb.d/00X-*.sql:ro,Z`, healthcheck `pg_isready -U cqrsx -d cqrsx`, порт `${CQRSX_POSTGRES_PORT:-5433}`.

**Решение.** В `demo/compose.yaml`: `../cqrsx/sql/001-event-store.sql:/docker-entrypoint-initdb.d/10-event-store.sql:ro,z` и `../cqrsx/sql/002-transactional-outbox.sql:/docker-entrypoint-initdb.d/20-outbox.sql:ro,z`. Для этих двух файлов — строчная `z` (shared label): их же монтирует корневой compose, а `Z` перемаркирует эксклюзивно. Порядок init-скриптов алфавитный: `00-inspector-role`, `10-…`, `20-…`, `30-grants` ✓. Запасной путь (константы через `raw_sql` при старте) допустим и безопасен благодаря `IF NOT EXISTS`, но не нужен.

### U10. Переменные окружения `cqrsx`, дефолты, кросс-валидация

**Факт.** Библиотека не читает env неявно; только `*::from_env()` / `from_lookup(|name| ...)`:

| Ключ | Читает | Дефолт | Ограничение |
|---|---|---|---|
| `OUTBOX_WORKER_CONCURRENCY` (C) | `TaskWorkerPoolConfig` | 128 | > 0 |
| `OUTBOX_QUEUE_CAPACITY` (Q = `buffer_capacity`) | `TaskWorkerPoolConfig` | 128 | > 0 |
| `OUTBOX_POLL_INTERVAL_MS` | `TaskWorkerPoolConfig` | 1000 | > 0 |
| `OUTBOX_HANDLER_TIMEOUT_SECS` (H) | `TaskWorkerPoolConfig` | 15 | 0 < H < lease |
| `OUTBOX_LEASE_SECS` (L) | `TaskWorkerPoolConfig` | 60 | см. формулу |
| `OUTBOX_STORAGE_TIMEOUT_MS` (S) | `TaskStorageConfig` | 6000 | > 0 |
| `OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS` | `PostgresTaskQueueConfig` | 4000 | 1..=2147483647 |
| `RUST_LOG` | `LoggingConfig` | `info` | **один уровень** (`off/error/warn/info/debug/trace` или `0..5`); директивы вида `info,crate=debug` — ошибка парсинга (`logging.rs:100-102`) |

Кросс-валидация (`worker_pool_config.rs:126-152`, вызывается в `TaskRuntimeConfig::new` и в `supervisor::run`):
`L > 2·S + H + 5 с (LEASE_SAFETY_MARGIN) + ceil(Q/C)·(H+S)`, иначе `LeaseTooShort`. Перед вызовом обработчика
`execution_plan` требует `L − 5 с − прошло ≥ H + S`, иначе задача сразу переводится в retry (`lease_budget.rs`).
Drain при остановке ждёт активные задачи не дольше `H + S` (`worker_pool.rs:81`).

**Решение.** Значения для демо и расчёт:

```
OUTBOX_WORKER_CONCURRENCY=2   OUTBOX_QUEUE_CAPACITY=2   OUTBOX_POLL_INTERVAL_MS=300
OUTBOX_HANDLER_TIMEOUT_SECS=5 OUTBOX_LEASE_SECS=30      OUTBOX_STORAGE_TIMEOUT_MS=3000
OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS=4000 (дефолт)
требуется: 2·3 + 5 + 5 + 1·(5+3) = 24 с  <  30 с ✓;   H=5 с > ENCORE_TASK_LATENCY_MAX_MS=1.8 с ✓;   drain ≤ 8 с ✓
(с дефолтным S=6 с требовалось бы > 33 с — lease 30 с не пройдёт; тогда L=40 с)
```

Чтение — `TaskRuntimeConfig::from_lookup(|name| std::env::var(name).ok().or_else(|| demo_default(name)))`: ключи
библиотеки сохраняются, дефолты подходят демо даже без `.env`. `Config::from_env()` демо дополнительно проверяет
`latency_max < handler_timeout`. Логирование: `LoggingConfig::from_env()` с `RUST_LOG=info` (один уровень).

**Правка плана.** §5.4: `RUST_LOG` → `info` (без директив); строку «ключи cqrsx … по документации» заменить таблицей выше; §2.7: «lease ≥ 10 с» → 30 с; «poll ≈ 300 мс» ✓.

### U11. Feature-флаги, версии

**Факт** (`cqrsx/Cargo.toml`, корневой `Cargo.lock`): `default = ["postgres", "worker"]`, `host = ["worker", "tokio/signal"]`, `logging = ["dep:tracing-subscriber"]`; `edition = "2024"`, `rust-version = "1.98"`, toolchain `1.98.1`. Версии: sqlx **0.8.6** (features `json, chrono, uuid, postgres, runtime-tokio-rustls`, `default-features = false`), tokio 1.53.1, tokio-util 0.7.19, chrono 0.4.45 (+serde), uuid 1.26.1 (serde, v4), serde 1.0.229, serde_json 1.0.151, async-trait 0.1.92, thiserror 2.0.20, rand 0.10.2, tracing 0.1.44, tracing-subscriber 0.3.23, async-channel 2.5.0, futures 0.3.34; dev: testcontainers 0.28.0, proptest =1.12.0. Корневой `rustfmt.toml`: `max_width = 130`. CI: `cargo build/check/clippy/test --locked --workspace --all-features`, покрытие 97 %, матрица features.

**Решение.**
- `cqrsx = { path = "../cqrsx", features = ["host", "logging"] }` (без них нет `cqrsx::host` и `cqrsx::logging`).
- Добавить `async-trait = "0.1"` (для `EventHandler`), `sqlx = "0.8"` (не 0.9 — §7.3 плана неточен), `rand = "0.10"` (API: `rand::rng()`, `Rng::random_bool`, `StdRng::seed_from_u64`), `uuid` с `v7`, `tokio-util` 0.7 (`CancellationToken`).
- В `demo/` — свой `.gitignore` (`target/`, `node_modules/`, `.env`): корневой `.gitignore` игнорирует только `/target/` (якорь на корень).
- `workspace = true` в `cqrsx/Cargo.toml` не используется — замечание §5.1 о наследуемых полях снимается.

### U12. Снапшоты

**Факт.** `PersistedEventStore::new_snapshot_store(repo, snapshot_interval: usize)` (`persisted.rs:372`) — публичный способ включить снапшоты; `SnapshotPolicy` приватна, но параметр проходит через конструктор. `PostgresCommandConfig::new(snapshot_interval, AggregateConflictRetryPolicy)` (дефолт 25) — держатель конфигурации с геттерами `snapshot_interval()`, `conflict_retry()`. Снапшот создаётся, когда `sequence % interval == 0` (при одном событии на команду — ровно на кратных), пишется в той же CTE (`written_snapshot`), версия `current_snapshot` растёт на 1; на агрегат — **одна строка**, обновляемая на месте (`UPDATE … WHERE current_snapshot = version−1`, `write.rs:40-53`). Таблица `snapshots(aggregate_type, aggregate_id, last_sequence, current_snapshot, payload json)`, PK `(aggregate_type, aggregate_id, last_sequence)`. Загрузка: `get_snapshot` + `get_events_after(last_sequence)`; несовместимый снапшот → полный replay без ошибки (`persisted.rs:428-437`).

**Решение.** Как в плане: `ENCORE_SNAPSHOT_EVERY = 10`. Панель снапшотов: `aggregate_id`, `last_sequence`, `current_snapshot` («версия закладки»), `payload` (serde-форма `Concert`). Маркер 📸 у события: `sequence % 10 == 0`.

### U13. `host::lifecycle::run`

**Факт** (`host/lifecycle.rs:34-84`):
```rust
pub async fn run<Inbound, RuntimeFuture, Signal>(inbound: Inbound, task_runtime: RuntimeFuture, shutdown: CancellationToken, signal: Signal) -> Result<(), HostError>
where Inbound: Future<Output = io::Result<()>>, RuntimeFuture: Future<Output = Result<(), TaskRuntimeError>> + Send + 'static, Signal: Future<Output = io::Result<()>>;
```
`task_runtime` спавнится на Tokio (поэтому `Send + 'static`); `inbound`/`signal` живут внутри, могут быть не-`Send`. Приоритет: сигнал → отмена токена → выход inbound → выход runtime; затем `shutdown.cancel()` и ожидание **обоих**. Штатный выход inbound до отмены — ошибка `HostError::InboundStopped`; штатный выход runtime до отмены — `TaskRuntimeStopped`. Сигнал: `cqrsx::host::signal::shutdown_signal()` (SIGINT/SIGTERM). Ошибки логируются `HostError::log(msg)` без panic-payload.

**Решение.** `host::lifecycle::run(axum::serve(listener, router).with_graceful_shutdown(shutdown.clone().cancelled_owned()), prepared.run(shutdown.clone()), shutdown.clone(), shutdown_signal()).await` (`axum::serve(..).with_graceful_shutdown(..)` отдаёт `io::Result<()>` — по документации axum 0.8, **не проверено** в песочнице). После `run` — `pool.close().await`.
Важно: graceful shutdown axum ждёт закрытия открытых соединений, поэтому **SSE-потоки `/api/activity` должны завершаться по `shutdown.cancelled()`** (например, `take_until`), иначе Ctrl+C не завершится, пока открыта вкладка браузера. Это критерий приёмки Фазы 4 («корректный drain»).

### U14. Формат `event_version`

**Факт.** Хранится строка `DomainEvent::event_version()` как есть; при чтении требуется **точное равенство** с `event_version()` текущего кода после апкастинга, иначе `DeserializationError` (`serialized_event.rs:116-124`). `NumericVersion::from_str` (`upcaster.rs:53-67`): `"1"` → 1.0.0, `"1.0"` → 1.0.0, `"1.0.0"`, четвёртый компонент игнорируется; `NumericVersionUpcaster::new(event_type, target_version, transform)` применяется, когда `target` строго больше сохранённой. Апкастеры передаются и в стор (`with_upcasters`), и в `EventReplay` (`with_upcasters`).

**Решение.** `"1.0"` (как в плане и doc-примерах). P2-апкаст `SeatBooked` 1.0 → 2.0: код возвращает `"2.0"`, апкастер `NumericVersionUpcaster::new("SeatBooked", "2.0", ...)` в обоих местах.

### U15. Глобальная позиция / метка времени в `events`

> 03/03 (2026-10-24): позицию читает `ProjectionProcessor`; строка закладки на пару «проекция + тип» лежит в
> `projection_checkpoints` (`PROJECTION_CHECKPOINTS_SCHEMA`, PostgreSQL ≥ 18), исполнитель будит процессоры через
> `CommitWakeup` (`with_commit_wakeup`).
>
> 03/04a (2026-10-24): закладка может лежать рядом с витринами — `cqrsx::mongodb::projection_checkpoints::MongoCheckpointStore`
> (фича `mongodb`, коллекция `projection_checkpoints` в базе витрин, документ на пару «проекция + тип»); позиция в ней —
> номер транзакции PostgreSQL, поэтому журнал и витрины сбрасываются вместе.

**Факт** (`cqrsx/sql/001-event-store.sql`): `events(aggregate_type text, aggregate_id text, sequence bigint, event_type text, event_version text, payload json, metadata json)`, PK `(aggregate_type, aggregate_id, sequence)`. Ни глобальной позиции, ни `created_at` (с эпика 03/02 есть `tx xid8` — номер записавшей транзакции, индекс `events_feed_idx` и лента `feed_after`; `recorded_at` в метаданных остаётся для X-ray). Вставка — `ON CONFLICT DO NOTHING` + проверка `rows_affected` (или 23505 на `events_pkey`) → `OptimisticLockError`.

**Решение.** При U3 = A: `recorded_at` пишется фиксированной шириной (`to_rfc3339_opts(SecondsFormat::Micros, true)` → `2026-10-17T09:12:03.123456Z`), и sidecar сортирует строкой без приведения типов:
`SELECT row_to_json(t) FROM (SELECT * FROM events WHERE aggregate_type = 'Concert' [AND aggregate_id = $2] ORDER BY metadata->>'recorded_at' DESC NULLS LAST, aggregate_id, sequence DESC LIMIT $1) t`. При U3 = C: `ORDER BY ctid DESC` (физический порядок вставки; для демо без `VACUUM FULL` приемлемо, но недокументировано) или по полю payload.

### U16. Наблюдаемость конфликт-ретраев

**Факт.** `retry_on_aggregate_conflict` — `pub(super)`; `CqrsCommandExecutor` отдаёт только итог; конфликты не логируются (`log_aggregate_error` возвращает раньше, `executor.rs:71`); в БД проигравший ничего не оставляет. Политика по умолчанию: 3 попытки, backoff 5 → 50 мс, джиттер ±20 % (`retry.rs:71-75`); `AggregateConflictRetryPolicy::new(max_attempts, initial, max, jitter) -> Result<_, _>`.

**Решение.** При U3 = A декоратор считает `Err(AggregateConflict)` → поле `conflicts` в ответе 200/422 и в `command.accepted`/`command.rejected`; `RaceResult` показывает «🐼 Bob: конфликт ×1 → повтор → seat_taken». Иначе — fallback §13 (только исходы).

---

## 4. Дополнительные расхождения и находки

| # | Находка | Следствие для демо |
|---|---|---|
| D1 | `postgres::pool::connect_pool(options: PgConnectOptions) -> Result<PgPool, sqlx::Error>` (`pool.rs:9`) — не URL; 10 соединений; без ожидания готовности. **2026-10-18:** добавлен `connect_pool_with_size(options, NonZeroU32)`, демо берёт размер из `ENCORE_DATABASE_POOL_SIZE` (нагрузочные тесты показали пул как потолок пути записи) | `config.database_url.parse::<PgConnectOptions>()?` (+ `.application_name("encore-backend")`); готовность БД — `scripts/wait-db.sh` |
| D2 | `postgres::command::command_executor` / `PostgresCommandExecutor` требуют `Aggregate<Services = ()>` и не регистрируют обработчиков (`command.rs:68-75`, док-комментарий отсылает к `CqrsCommandExecutor::new`) | §6.3 п.7: использовать `CqrsCommandExecutor::new` с фабрикой; `PostgresCommandConfig` остаётся держателем интервала и политики |
| D3 | Успешный `execute` → `()`; `last_sequence` только при `CommittedWithEventHandlerFailure` | `committed_sequence` — из декоратора (A) |
| D4 | `handler_indices` в `CommittedWithEventHandlerFailure` — индексы регистрации (0 = `concert_halls`, 1 = `fan_wallets`); framework вызывает **все** обработчики даже после сбоя первого (`cqrs.rs:545-549`) | `warnings: [{ code: "projection_failed", projection }]` по индексу |
| D5 | `ViewProjection::set_error_observer(Box<dyn Fn(&PersistenceError)>)` — синхронный наблюдатель ошибок | альтернатива `NotifyingHandler` для `projection.failed`; `projection.updated` — всё равно декоратором |
| D6 | `ViewRepository` — native async fn с `Send`-фьючами; `update_view(view, ViewContext { view_instance_id, version, last_sequence })`: `version == 0` → insert, иначе update `WHERE version = ctx.version`, новая версия `ctx.version + 1`, `last_sequence` пишется тем же CAS (эпик 03/01) (`view_repository.rs:187-210` как образец) | §6.7 верен; `MongoViewRepository` повторяет контракт, конфликт → `PersistenceError::OptimisticLockError`. С 03/04a репозиторий библиотечный (`cqrsx::mongodb::view_repository`, фича `mongodb`); `infrastructure/mongo/read_model.rs` демо (`MongoReadModel`) делегирует ему запись и добавляет запросы и wipe |
| D7 | `View<A>: Debug + Default + Serialize + DeserializeOwned + Send + Sync`, `update(&mut self, &EventEnvelope<A>)` — синхронно и безошибочно | LWW реализуется в `update`; порядок и однократность применения обеспечивает библиотека (`last_sequence`, эпик 03/01) |
| D8 | `TestFramework::<A>::with(services).given(vec)/.given_no_previous_events().and(vec).when(cmd)` (sync, свой runtime) или `.when_async(cmd).await`; валидатор: `then_expect_events(vec)`, `then_expect_error(err)` (нужен `PartialEq` у ошибки), `then_expect_error_message(&str)`, `inspect_result()` | `ConcertError: thiserror::Error + PartialEq + Clone`; тесты R1–R8 — по одному на правило |
| D9 | `Aggregate: Default + Serialize + DeserializeOwned + Send + Sync` (+ `Clone` для framework); `Services: Send + Sync`; `DomainEvent: Clone + PartialEq + Debug + Serialize + DeserializeOwned` | `Concert` сериализуется в снапшот → ключи `BTreeMap<SeatId, _>` должны сериализоваться строкой (newtype над `String`) |
| D10 | `EventSink::write` применяет событие к рабочей копии сразу (`event_sink.rs:23-26`) | комментарий в §2.6 верен |
| D11 | `TaskDispatcher::dispatch` неизвестного формата → `Permanent("unsupported task format")`; при `prepare` форматы всех воркеров сверяются | enum-адаптер фиксирует порядок форматов |
| D12 | Supervisor: один producer, claim по форматам по кругу, recovery просроченных lease на каждом тике (`min(C, 32)` строк), restart воркера с новым диспетчером из фабрики | при poll 300 мс — ~3 лёгких транзакции/с в простое, приемлемо |
| D13 | `PostgresViewRepository` существует (PG-таблица `view_id text PK, version bigint, payload json`) | не используется (витрины в MongoDB), но это готовый образец для `MongoViewRepository` (с 03/04a — в cqrsx рядом с ним) |
| D14 | Rule 12.4 vs §2.9 verify P1 («in-memory репозиторий») | предложение: verify пересобирает во временные коллекции `*_verify` тем же `MongoViewRepository` и сравнивает документы — без in-memory фейков в рантайме (с 03/04a — `MongoReadModel` над библиотечным репозиторием) |
| D15 | Корневой CI и workspace не задеты: `members = ["cqrsx"]`, демо — отдельный workspace; `exclude = ["demo"]` в корне — единственная правка вне `demo/` (§12.1) | без изменений |
| D16 | Канал стрима replay держит соединение пула; пул 10 соединений | 2 воркера + poller + HTTP + 1 стрим rebuild — запас есть |
| D17 | `LEASE_SAFETY_MARGIN = 5 с` публична (`task/queue/lease.rs:8`) | используется в расчёте §7 |
| D18 | `NewTask::scheduled(TaskSchedule::{Now, After(d), At(t)})` | задачи демо — `Now` |
| D19 | `HostError` реализует `Debug` без panic-payload; `HostError::log` | в `main`: `if let Err(e) = result { e.log("encore stopped with an error") }` |
| D20 | Не проверено в песочнице: сигнатуры axum 0.8 (`serve`, `with_graceful_shutdown`, SSE), mongodb 3.9, версии фронтовых пакетов | проверить в Фазе 1 при первой сборке |
| D21 | **Найдено в Фазе 3.** В axum 0.8.9 `axum::serve(..).with_graceful_shutdown(..)` возвращает `WithGracefulShutdown`, который реализует `IntoFuture`, а не `Future`; `host::lifecycle::run` требует `Future<Output = io::Result<()>>` | в `main.rs`: `.with_graceful_shutdown(token.cancelled_owned()).into_future()` (`use std::future::IntoFuture`) |

---

## 5. Справочник сигнатур (то, что нужно демо)

Все пути — `cqrsx::…`; re-экспортов в крейте нет, импортировать из определяющих модулей.

### cqrs

```rust
// cqrs/aggregate.rs:125
pub trait Aggregate: Default + Serialize + DeserializeOwned + Sync + Send {
    const TYPE: &'static str;
    type Command; type Event: DomainEvent; type Error: std::error::Error; type Services: Send + Sync;
    fn handle(&mut self, command: Self::Command, service: &Self::Services, sink: &EventSink<Self>) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn apply(&mut self, event: Self::Event);
}
// cqrs/event.rs:158,178
pub trait DomainEvent: Serialize + DeserializeOwned + Clone + PartialEq + Debug + Sync + Send { fn event_type(&self) -> String; fn event_version(&self) -> String; }
pub struct EventEnvelope<A: Aggregate> { pub aggregate_id: String, pub sequence: usize, pub payload: A::Event, pub metadata: HashMap<String, String> }
// cqrs/event_sink.rs:23
impl<A: Aggregate> EventSink<A> { pub async fn write(&self, event: A::Event, aggregate: &mut A); }
// cqrs.rs:40-557
pub struct CqrsFramework<A, ES: EventStore<A>>;
impl CqrsFramework<A, ES> {
    pub fn new(store: ES, event_handlers: Vec<Box<dyn EventHandler<A>>>, service: A::Services) -> Self;
    pub fn append_event_handler(self, handler: Box<dyn EventHandler<A>>) -> Self;
    pub async fn execute(&self, aggregate_id: &str, command: A::Command) -> Result<(), AggregateError<A::Error>> where A: Clone;
    pub async fn execute_with_metadata(&self, aggregate_id: &str, command: A::Command, metadata: HashMap<String, String>) -> Result<(), AggregateError<A::Error>> where A: Clone;
}
pub enum AggregateError<T: Error> { Domain(T), AggregateConflict, DatabaseConnectionError(Box<dyn Error + Send + Sync>), DeserializationError(..), UnexpectedError(..), CommittedWithEventHandlerFailure(CommittedWithEventHandlerFailure) }
pub struct CommittedWithEventHandlerFailure { pub last_sequence: usize, pub errors: Vec<EventHandlerFailure> }
pub struct EventHandlerFailure { pub handler_index: usize, pub error: PersistenceError }
// cqrs/command.rs:29,44
pub trait CommandExecutor<A>: Send + Sync { fn execute(&self, aggregate_id: &str, command: A::Command, tasks: Vec<NewTask>) -> impl Future<Output = Result<(), CommandGatewayError<A::Error>>> + Send; }
pub enum CommandGatewayError<D> { Domain(D), Conflict, Unavailable, CommittedWithEventHandlerFailure { last_sequence: usize, handler_indices: Vec<usize> } } // Clone, Debug, PartialEq
// cqrs/command/executor.rs:17-57
pub struct CqrsCommandExecutor<A, BuildFramework>;
impl CqrsCommandExecutor<A, BuildFramework> {
    pub fn new(build_framework: BuildFramework, retry_policy: AggregateConflictRetryPolicy) -> Self;
    pub async fn execute<ES>(&self, aggregate_id: &str, command: A::Command, tasks: Vec<NewTask>) -> Result<(), CommandGatewayError<A::Error>>
        where BuildFramework: Fn(Vec<NewTask>) -> CqrsFramework<A, ES> + Send + Sync, ES: EventStore<A> + 'static, ES::AC: Send;
}
// cqrs/command/retry.rs:18-75
pub struct AggregateConflictRetryPolicy; // Copy
impl AggregateConflictRetryPolicy { pub fn new(max_attempts: usize, initial_backoff: Duration, max_backoff: Duration, jitter_ratio: f64) -> Result<Self, AggregateConflictRetryPolicyError>; }
impl Default for AggregateConflictRetryPolicy { /* 3, 5 ms, 50 ms, 0.2 */ }
// cqrs/event_handler.rs:14-19
#[async_trait] pub trait EventHandler<A: Aggregate>: Send + Sync { async fn handle(&self, aggregate_id: &str, events: &[EventEnvelope<A>]) -> Result<(), PersistenceError>; }
// cqrs/event_handler/replay.rs:158-375
pub struct EventReplay<R: PersistedEventRepository, H: EventHandler<A>, A: Aggregate>;
impl EventReplay<R, H, A> {
    pub fn new(repository: R, handler: H) -> Self;
    pub fn with_upcasters(self, event_upcasters: Vec<Box<dyn EventUpcaster>>) -> Self;
    pub fn set_error_observer(&mut self, error_observer: Box<PersistenceErrorObserver>);
    pub async fn replay(&self, aggregate_id: &str) -> Result<(), PersistenceError>;
    pub async fn replay_all(&self) -> Result<(), PersistenceError>;
}
// cqrs/projection.rs:20-521
pub trait View<A: Aggregate>: Debug + Default + Serialize + DeserializeOwned + Send + Sync { fn update(&mut self, event: &EventEnvelope<A>); }
pub struct ViewProjection<R: ViewRepository<V, A>, V: View<A>, A: Aggregate>;
impl ViewProjection<R, V, A> {
    pub fn new(view_repository: Arc<R>) -> Self;
    pub fn with_max_attempts(self, attempts: NonZeroUsize) -> Self;         // default 16
    pub fn set_error_observer(&mut self, error_observer: Box<PersistenceErrorObserver>);
    pub async fn load(&self, view_id: &str) -> Result<Option<V>, PersistenceError>;
}
impl EventHandler<A> for ViewProjection<R, V, A>; // handle(view_id = aggregate_id, events)
// cqrs/projection/repository.rs:7-41
pub trait ViewRepository<V: View<A>, A: Aggregate>: Send + Sync {
    fn load(&self, view_id: &str) -> impl Future<Output = Result<Option<V>, PersistenceError>> + Send;
    fn load_with_context(&self, view_id: &str) -> impl Future<Output = Result<Option<(V, ViewContext)>, PersistenceError>> + Send;
    fn update_view(&self, view: V, context: ViewContext) -> impl Future<Output = Result<(), PersistenceError>> + Send;
}
pub struct ViewContext { pub view_instance_id: String, pub version: i64, pub last_sequence: usize }  impl ViewContext { pub fn new(view_instance_id: String, version: i64, last_sequence: usize) -> Self; }
// cqrs/store.rs:15-129
pub trait EventStore<A: Aggregate>: Send + Sync {
    type AC: AggregateContext<A>;
    fn load_events(&self, aggregate_id: &str) -> impl Future<Output = Result<Vec<EventEnvelope<A>>, AggregateError<A::Error>>> + Send;
    fn load_aggregate(&self, aggregate_id: &str) -> impl Future<Output = Result<Self::AC, AggregateError<A::Error>>> + Send;
    fn commit(&self, events: Vec<A::Event>, context: Self::AC, metadata: HashMap<String, String>) -> impl Future<Output = Result<Vec<EventEnvelope<A>>, AggregateError<A::Error>>> + Send;
}
pub trait AggregateContext<A: Aggregate> { fn aggregate(&mut self) -> &mut A; }
pub enum PersistenceError { OptimisticLockError, ConnectionError(Box<dyn Error + Send + Sync>), DeserializationError(..), UnknownError(..), SequenceGap { expected: usize, found: usize } }
pub type PersistenceErrorObserver = dyn Fn(&PersistenceError) + Send + Sync + 'static;
// cqrs/store/persisted.rs:46-569
pub struct PersistedEventStore<R: PersistedEventRepository, A: Aggregate>;
impl PersistedEventStore<R, A> { pub fn new_event_store(repo: R) -> Self; pub fn new_snapshot_store(repo: R, snapshot_interval: usize) -> Self; pub fn with_upcasters(self, Vec<Box<dyn EventUpcaster>>) -> Self; }
impl EventStore<A> for PersistedEventStore<R, A> { type AC = EventStoreAggregateContext<A>; }
pub struct EventStoreAggregateContext<A> { pub aggregate_id: String, pub aggregate: A, pub current_sequence: usize, pub snapshot_version: Option<usize> }
// cqrs/store/repository.rs:10-45
pub trait PersistedEventRepository: Send + Sync {
    fn get_events<A: Aggregate>(&self, aggregate_id: &str) -> …Result<Vec<SerializedEvent>, PersistenceError>;
    fn get_events_after<A: Aggregate>(&self, aggregate_id: &str, after_sequence: usize) -> …;
    fn get_snapshot<A: Aggregate>(&self, aggregate_id: &str) -> …Result<Option<SerializedSnapshot>, PersistenceError>;
    fn persist<A: Aggregate>(&self, events: &[SerializedEvent], snapshot_update: Option<SnapshotUpdate>) -> …Result<(), PersistenceError>;
    fn stream_events<A: Aggregate>(&self, aggregate_id: &str) -> …Result<ReplayStream, PersistenceError>;
    fn stream_all_events<A: Aggregate>(&self) -> …Result<ReplayStream, PersistenceError>;
    // эпик 03/02: журнал типа в порядке коммитов (cqrs/store/feed.rs: Position, FeedEntry)
    fn feed_after<A: Aggregate>(&self, after: Position, limit: usize) -> …Result<Vec<FeedEntry>, PersistenceError>;
    fn head<A: Aggregate>(&self) -> …Result<Position, PersistenceError>;
    fn aggregate_ids_after<A: Aggregate>(&self, after: &str, limit: usize) -> …Result<Vec<String>, PersistenceError>;
}
// cqrs/store/stream.rs:12-50
impl ReplayStream { pub fn new(queue_size: usize) -> (ReplayFeed, Self); pub async fn next<A: Aggregate>(&mut self, upcasters: &[Box<dyn EventUpcaster>]) -> Option<Result<EventEnvelope<A>, PersistenceError>>; }
// cqrs/store/serialized_event.rs:14, snapshot.rs:8-26
pub struct SerializedEvent { pub aggregate_id: String, pub sequence: usize, pub aggregate_type: String, pub event_type: String, pub event_version: String, pub payload: Value, pub metadata: Value }
impl<A: Aggregate> TryFrom<&EventEnvelope<A>> for SerializedEvent; impl<A: Aggregate> TryFrom<SerializedEvent> for EventEnvelope<A>;
pub struct SerializedSnapshot { pub aggregate_id: String, pub aggregate: Value, pub last_sequence: usize, pub snapshot_version: usize }
// cqrs/store/upcaster.rs:13-177
pub trait EventUpcaster: Send + Sync { fn can_upcast(&self, event_type: &str, event_version: &str) -> bool; fn upcast(&self, event: SerializedEvent) -> SerializedEvent; }
pub struct NumericVersionUpcaster; impl NumericVersionUpcaster { pub fn new(event_type: &str, target_version: &str, transform: Box<dyn Fn(Value) -> Value + Send + Sync>) -> Self; }
// cqrs/testing.rs, testing/executor.rs, testing/validator.rs
TestFramework::<A>::with(service) -> TestFramework<A>;  .given_no_previous_events() | .given(Vec<A::Event>) -> AggregateTestExecutor<A>;  .and(Vec<A::Event>);
.when(A::Command) -> AggregateResultValidator<A>  |  .when_async(A::Command).await;
.then_expect_events(Vec<A::Event>);  .then_expect_error(A::Error) /* A::Error: PartialEq */;  .then_expect_error_message(&str);  .inspect_result() -> Result<Vec<A::Event>, A::Error>;
```

### task

```rust
// task.rs:24-104
pub enum TaskSchedule { #[default] Now, After(Duration), At(DateTime<Utc>) }
pub struct NewTask; // Clone, Debug, PartialEq
impl NewTask {
    pub fn new<T: TaskPayload>(payload: T) -> Result<Self, TaskValidationError>;           // id = Uuid::new_v4(), max_attempts = 10, metadata = {}
    pub fn from_payload(format: TaskFormat, payload: Value) -> Result<Self, TaskValidationError>;
    pub fn scheduled(self, schedule: TaskSchedule) -> Result<Self, TaskValidationError>;
    pub fn with_metadata(self, metadata: Value) -> Result<Self, TaskValidationError>;      // объект
    pub fn with_max_attempts(self, max_attempts: i32) -> Result<Self, TaskValidationError>; // > 0, считает claim-ы
    pub fn id(&self) -> Uuid; pub fn format(&self) -> &TaskFormat; pub fn payload(&self) -> &Value; pub fn metadata(&self) -> &Value; pub fn schedule(&self) -> &TaskSchedule; pub fn max_attempts(&self) -> i32;
}
// task/format.rs:6-50
pub trait TaskPayload: Serialize { const TASK_TYPE: &'static str; const TASK_VERSION: i32; }
pub struct TaskFormat; impl TaskFormat { pub fn new(task_type: impl Into<String>, task_version: i32) -> Result<Self, TaskFormatError>; pub fn of<T: TaskPayload>() -> Result<Self, TaskFormatError>; pub fn task_type(&self) -> &str; pub fn task_version(&self) -> i32; } // Display: "{type} v{version}"
// task/attempt.rs:9-54
pub struct TaskAttempt; impl TaskAttempt { pub fn task_id(&self) -> Uuid; pub fn format(&self) -> &TaskFormat; pub fn payload(&self) -> &Value; pub fn metadata(&self) -> &Value; pub fn number(&self) -> i32; }
// task/handler.rs:8-66
pub enum TaskHandlerError { Retry { reason: String, retry_after: Option<Duration> }, Permanent { reason: String } }
pub trait TaskHandler<P>: Send where P: TaskPayload + DeserializeOwned + Send + 'static { fn handle(&mut self, payload: P, attempt: &TaskAttempt) -> impl Future<Output = Result<(), TaskHandlerError>> + Send; }
// task/adapter.rs:27, adapter/json.rs:24-71
pub trait TaskAdapter: Send { fn formats(&self) -> Vec<TaskFormat>; fn invoke(&mut self, attempt: &TaskAttempt) -> impl Future<Output = Result<(), TaskHandlerError>> + Send; }
pub struct JsonTaskAdapter<P, H>; impl JsonTaskAdapter<P, H> { pub fn new(handler: H) -> Result<Self, TaskFormatError>; }
// task/dispatcher.rs:6-82
pub struct TaskDispatcher<A>; impl<A: TaskAdapter> TaskDispatcher<A> { pub fn try_new(adapters: Vec<A>) -> Result<Self, TaskDispatcherError>; pub fn formats(&self) -> &[TaskFormat]; pub async fn dispatch(&mut self, delivery: &TaskAttempt) -> Result<(), TaskHandlerError>; }
pub enum TaskDispatcherError { Empty, DuplicateFormat(TaskFormat), InvalidFormat(TaskFormatError) }
// task/runtime.rs:32-93, runtime/dispatcher_factory.rs:13-26
pub type TaskDispatcherFactory<A> = Arc<dyn Fn() -> Result<TaskDispatcher<A>, TaskDispatcherError> + Send + Sync>;
pub struct TaskRuntime<Q: TaskQueue, A>; impl TaskRuntime<Q, A> { pub async fn run(self, shutdown: CancellationToken) -> Result<(), TaskRuntimeError>; }
pub fn prepare<Q: TaskQueue + 'static, A: TaskAdapter + 'static>(queue: Arc<Q>, factory: TaskDispatcherFactory<A>, config: TaskRuntimeConfig) -> Result<TaskRuntime<Q, A>, WorkerSetupError>;
pub enum TaskRuntimeError { Configuration(TaskWorkerPoolConfigError), WorkerSetup(WorkerSetupError) }
pub enum WorkerSetupError { Build { worker_index: usize, source: TaskDispatcherError }, FormatsMismatch { worker_index: usize } }
// task/runtime/config.rs, worker_pool_config.rs, queue/storage_config.rs
impl TaskRuntimeConfig { pub fn new(storage: TaskStorageConfig, worker: TaskWorkerPoolConfig) -> Result<Self, TaskRuntimeConfigError>; pub fn from_env() -> …; pub fn from_lookup(lookup: impl FnMut(&str) -> Option<String>) -> …; pub fn storage(&self) -> &TaskStorageConfig; pub fn worker_pool(&self) -> &TaskWorkerPoolConfig; }
impl TaskWorkerPoolConfig { pub fn new(concurrency: usize, buffer_capacity: usize, poll_interval: Duration, handler_timeout: Duration, lease_duration: Duration) -> Result<Self, TaskWorkerPoolConfigError>; pub fn validate_for_storage_timeout(&self, storage_timeout: Duration) -> Result<(), TaskWorkerPoolConfigError>; /* геттеры */ }
impl TaskStorageConfig { pub fn new(storage_timeout: Duration) -> Result<Self, TaskStorageConfigError>; pub fn storage_timeout(&self) -> Duration; }
// task/queue/retry.rs:3, queue/lease.rs:8
pub fn redelivery_backoff(attempt: i32) -> Duration;   pub const LEASE_SAFETY_MARGIN: Duration = 5 s;
```

### postgres, host, logging

```rust
// postgres.rs:9-15
pub const EVENT_STORE_SCHEMA: &str; pub const TRANSACTIONAL_OUTBOX_SCHEMA: &str; pub const REQUEUE_FAILED_OUTBOX_TASK: &str; // include_str!("../sql/…")
// postgres/pool.rs:9
pub async fn connect_pool(options: PgConnectOptions) -> Result<PgPool, sqlx::Error>;   // max_connections(10)
// postgres/command.rs:16-136
pub struct PostgresCommandConfig; impl PostgresCommandConfig { pub fn new(snapshot_interval: usize, conflict_retry: AggregateConflictRetryPolicy) -> Result<Self, PostgresCommandConfigError>; pub fn snapshot_interval(&self) -> usize; pub fn conflict_retry(&self) -> AggregateConflictRetryPolicy; } // Default: 25, policy default
pub fn command_executor<A: Aggregate<Services = ()> + Clone + 'static>(pool: PgPool, config: PostgresCommandConfig) -> PostgresCommandExecutor<A>; // без обработчиков — для демо не подходит (D2)
// postgres/outbox.rs:24-49
pub struct PostgresEventRepositoryWithOutbox; impl { pub fn new(pool: PgPool) -> Self; pub fn with_outbox(self, tasks: Vec<NewTask>) -> Self; pub fn with_streaming_channel_size(self, capacity: usize) -> Self; } // impl PersistedEventRepository
// postgres/event_repository.rs:27-137
pub struct PostgresEventRepository; impl { pub fn new(pool: Pool<Postgres>) -> Self; pub fn with_streaming_channel_size(self, usize) -> Self; pub fn with_tables(self, events_table: &str, snapshots_table: &str) -> Self; } // impl PersistedEventRepository
// postgres/view_repository.rs:16-148
pub struct PostgresViewRepository<V, A>; impl { pub fn new(table_name: &str, pool: Pool<Postgres>) -> Self; } // impl ViewRepository<V, A>
// postgres/task_queue.rs:19-117
pub struct PostgresTaskQueue; impl { pub fn new(pool: PgPool) -> Self; pub fn with_config(pool: PgPool, config: PostgresTaskQueueConfig) -> Self; }
pub struct PostgresTaskQueueConfig; impl { pub fn new(statement_timeout: Duration) -> Result<Self, PostgresTaskQueueConfigError>; pub fn from_env() -> …; pub fn from_lookup(..) -> …; } // Default 4 s
// postgres/task_runtime.rs:19
pub fn prepare_task_runtime<A: TaskAdapter + 'static>(pool: PgPool, factory: TaskDispatcherFactory<A>, queue_config: PostgresTaskQueueConfig, runtime_config: TaskRuntimeConfig) -> Result<TaskRuntime<PostgresTaskQueue, A>, WorkerSetupError>;
// host/lifecycle.rs:34, host/signal.rs:8, host/error.rs:11-56
pub async fn run<Inbound, RuntimeFuture, Signal>(inbound: Inbound, task_runtime: RuntimeFuture, shutdown: CancellationToken, signal: Signal) -> Result<(), HostError>
    where Inbound: Future<Output = io::Result<()>>, RuntimeFuture: Future<Output = Result<(), TaskRuntimeError>> + Send + 'static, Signal: Future<Output = io::Result<()>>;
pub async fn shutdown_signal() -> io::Result<()>;
pub enum HostError { ShutdownSignal(io::Error), Inbound(io::Error), InboundStopped, TaskRuntimeJoin(JoinError), TaskRuntime(TaskRuntimeError), TaskRuntimeStopped } impl HostError { pub fn log(&self, message: &'static str); }
// logging.rs:10-48
pub struct LoggingConfig; impl { pub fn new(level_filter: LevelFilter) -> Self; pub fn from_env() -> Result<Self, ParseLevelFilterError>; pub fn from_lookup(..) -> …; pub fn level_filter(&self) -> LevelFilter; pub fn try_init(&self) -> Result<(), LoggingInitError>; } // with_target(false), with_thread_names(true)
```

---

## 6. Схемы PostgreSQL и запросы sidecar

### `events` и `snapshots` (`cqrsx/sql/001-event-store.sql`)

```sql
events(aggregate_type text, aggregate_id text, sequence bigint CHECK (>= 0), event_type text, event_version text,
       payload json, metadata json, tx xid8 NOT NULL DEFAULT pg_current_xact_id(),  -- эпик 03/02
       PRIMARY KEY (aggregate_type, aggregate_id, sequence))
CREATE INDEX events_feed_idx ON events (aggregate_type, tx, aggregate_id, sequence)  -- эпик 03/02
snapshots(aggregate_type text, aggregate_id text, last_sequence bigint, current_snapshot bigint, payload json,
          PRIMARY KEY (aggregate_type, aggregate_id, last_sequence))
```

Для `Concert`: `aggregate_type = 'Concert'`, `event_type ∈ {ConcertAnnounced, SeatBooked, BookingCancelled}`,
`event_version = '1.0'`, `payload` — serde-форма варианта enum (внешне-тегированная по умолчанию: `{"SeatBooked": {...}}`;
если нужна плоская форма — `#[serde(tag = "type")]` на `ConcertEvent`, это не влияет на библиотеку), `metadata` —
объект строк (при U3 = A).

### Запросы sidecar (константы, параметры только bind)

```sql
-- последние события (все концерты / один)
SELECT row_to_json(t) AS row FROM (SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata
  FROM events WHERE aggregate_type = 'Concert' ORDER BY metadata->>'recorded_at' DESC NULLS LAST, aggregate_id, sequence DESC LIMIT $1) t;
SELECT row_to_json(t) AS row FROM (SELECT … FROM events WHERE aggregate_type = 'Concert' AND aggregate_id = $2 ORDER BY sequence DESC LIMIT $1) t;
-- снапшоты
SELECT row_to_json(t) AS row FROM (SELECT aggregate_type, aggregate_id, last_sequence, current_snapshot, payload FROM snapshots WHERE aggregate_type = 'Concert' ORDER BY aggregate_id LIMIT $1) t;
-- outbox
SELECT row_to_json(t) AS row FROM (SELECT * FROM transactional_outbox ORDER BY created_at DESC, id LIMIT $1) t;
-- stats
SELECT count(*) FROM events WHERE aggregate_type = 'Concert';
SELECT aggregate_id, count(*) FROM events WHERE aggregate_type = 'Concert' GROUP BY aggregate_id;
SELECT status, count(*) FROM transactional_outbox GROUP BY status;
-- время БД (та же транзакция REPEATABLE READ READ ONLY)
SELECT now();
```

Таймстемпы outbox (`created_at`, `available_at`, `locked_until`, `finished_at`) — `timestamptz`, выставляются
`statement_timestamp()`/`clock_timestamp()` сервера, поэтому обратный отсчёт по `db_time` корректен (§7.3 плана).

---

## 7. Конфигурация демо: сводка

| Что | Значение | Откуда |
|---|---|---|
| зависимость | `cqrsx = { path = "../cqrsx", features = ["host", "logging"] }` | U11 |
| доп. крейты | `async-trait 0.1`, `sqlx 0.8`, `tokio-util 0.7`, `rand 0.10`, `uuid 1 (v4, v7, serde)`, `chrono 0.4 (serde)` | U11, D1 |
| снапшоты | `PostgresCommandConfig::new(ENCORE_SNAPSHOT_EVERY=10, AggregateConflictRetryPolicy::default())` | U12, U16 |
| задачи | `NewTask::new(payload)?.with_metadata(json!({command_id, correlation_id, concert_id, recorded_at}))?.with_max_attempts(ENCORE_TASK_MAX_ATTEMPTS=6)?` | U1, U7 |
| runtime | C=2, Q=2, poll 300 мс, H=5 с, L=30 с, S=3000 мс, statement_timeout 4000 мс (по умолчанию) | U10 |
| retry обработчика | `retry_after = min(1 с · 2^(n−1), 8 с)` (`ENCORE_TASK_RETRY_BASE_MS`, `ENCORE_TASK_RETRY_MAX_MS`) | U6 |
| логи | `LoggingConfig::from_env()`, `RUST_LOG=info` (один уровень) | U10 |
| compose | образ `postgres:18.6-alpine`, том `/var/lib/postgresql`, схемы — файлы `../cqrsx/sql/*.sql` с `:ro,z` | U9 |

---

## 8. Сборка back-end — пересмотр §6.3 под реальный API (эскиз)

1. `dotenvy::dotenv().ok(); LoggingConfig::from_env()?.try_init()?;` — один раз, до любого `tracing`.
2. `Config::from_env()` — ENCORE_* и OUTBOX_* (через `TaskRuntimeConfig::from_lookup` с демо-дефолтами), валидация `latency_max < handler_timeout`, `0 ≤ failure_rate ≤ 1`.
3. `let pool = connect_pool(config.database_url.parse::<PgConnectOptions>()?.application_name("encore-backend")).await?;`
4. MongoDB-клиент (не проверено в песочнице), базы `encore_read` / `mail_service`.
5. `ActivityHub::new(1024, 100)`.
6. Фабрика обработчиков `handlers() -> Vec<Box<dyn EventHandler<Concert>>>`: `[NotifyingHandler(ViewProjection::new(halls_repo.clone())), NotifyingHandler(FanWalletsProjection::new(wallets_repo.clone()))]` — вызывается на каждый framework.
7. `CommandService { pool, snapshot_every, retry: PostgresCommandConfig::conflict_retry(), services: ConcertServices { ticket_limit }, handlers }`; на запрос — `CqrsCommandExecutor::new(move |tasks| CqrsFramework::new(CommandContextStore{…}, handlers(), services.clone()), retry).execute(id, cmd, tasks)` (U3 = A).
8. `let runtime = prepare_task_runtime(pool.clone(), dispatcher_factory, PostgresTaskQueueConfig::default(), runtime_config)?;` — без фоновой работы.
9. `AppState { command_service, read_db, mail_db, pool, activity, rebuild, config }`.
10. `seed::run(&state).await` при `ENCORE_SEED=true` (`already_announced` игнорируется).
11. `let shutdown = CancellationToken::new(); host::lifecycle::run(axum::serve(listener, router).with_graceful_shutdown(shutdown.clone().cancelled_owned()), runtime.run(shutdown.clone()), shutdown.clone(), shutdown_signal()).await` → при `Err(e)` `e.log("encore-backend stopped with an error")`; затем `pool.close().await`. SSE-хендлеры завершают поток по `shutdown.cancelled()`.

Rebuild (§6.8): `EventReplay::new(PostgresEventRepository::new(pool.clone()), ProgressHandler(FanOutHandler(vec![ViewProjection::new(halls_repo.clone()), FanWalletsProjection::new(wallets_repo.clone())]))).replay_all().await`.
Машина времени (§2.10): `PersistedEventStore::<_, Concert>::new_event_store(PostgresEventRepository::new(pool.clone())).load_events(id).await`.

---

## 9. Предлагаемые правки PLAN.md (сводно)

1. **§2.4, §2.6, §2.7, §3.2, §6.9, §8.8, §9 (шаг 5), §14** — убрать событие `ConcertSoldOut`; `sold_out` — производное поле; добавить страховочный тест «одна команда → одно событие» (КТ-1).
2. **§2.4 (метаданные), §3.2, §4, §6.2, §6.3 п.7, §6.4 п.4–6** — метаданные через декоратор `CommandContextStore` и `CqrsCommandExecutor` на запрос; `executor.execute(concert_id, command, tasks)` без параметра `metadata`; добавить файл `app/command_context.rs`; `committed_sequence` и `conflicts` берутся из декоратора (КТ-2, U16).
3. **§2.7** — форматы `encore_send_e_ticket` v1 / `encore_issue_refund` v1 (КТ-3); `task_id` генерирует библиотека; backoff — через `retry_after` (1–8 с); lease 30 с; «claim-attempt limit» = `with_max_attempts(6)`.
4. **§5.4** — `RUST_LOG=info` (один уровень); таблица `OUTBOX_*` с демо-дефолтами (U10); `ENCORE_TASK_RETRY_BASE_MS=1000`, `ENCORE_TASK_RETRY_MAX_MS=8000`.
5. **§6.1** — `cqrsx` с `features = ["host", "logging"]`; добавить `async-trait`; `sqlx 0.8`; `rand 0.10`.
6. **§6.3 п.3** — `connect_pool(PgConnectOptions)`; **п.7** — `CqrsCommandExecutor::new` вместо `command_executor` (D2); **п.11** — SSE завершаются по токену; после `run` закрыть пул.
7. **§6.2** — `outbox::dispatcher` (сейчас `infrastructure::task_runtime::dispatcher`): enum-адаптер над двумя `JsonTaskAdapter`; seed RNG через счётчик.
8. **§3.4, §6.8** — порядок `replay_all`: по `sequence` поперёк концертов; `rebuild.started` несёт `total`.
9. **§7.3** — убрать замечание про sqlx 0.9; запросы из §6 этого документа; сортировка событий по `metadata->>'recorded_at'` (фиксированная ширина).
10. **§8.5** — упрощённый `deriveTaskStatus` по колонке `status`; «призрачные» строки не нужны; причина 🎲 — из `last_error`.
11. **§8.3** — маркер 📸: `sequence % ENCORE_SNAPSHOT_EVERY == 0`; `SnapshotsPanel` показывает `current_snapshot`.
12. **§5.1** — убрать фразу про `workspace = true`; добавить `demo/.gitignore`.
13. **§5.2** — для двух SQL-файлов, общих с корневым compose, использовать `:ro,z`.
14. **§2.9 / §6.2 (verify P1)** — временные коллекции вместо in-memory репозитория (D14).
15. **§13** — U1–U16 закрыты этим документом.

---

## 10. Что не проверялось и риски

- **Не компилировалось:** нет сети и реестра cargo. Сигнатуры `cqrsx` сверены с исходниками и его тестами; интеграция с axum 0.8 (`serve`, `with_graceful_shutdown`, SSE), mongodb 3.9 и версии фронтовых пакетов проверяются при первой сборке Фазы 1.
- **Параллельные команды на один концерт** (🌊 Толпа последовательна, ⚡ Гонка P1): конфликт решает `CqrsCommandExecutor` (3 попытки, 5–50 мс); при трёх подряд конфликтах — `409 conflict`, в демо маловероятно.
- **Пропускная способность пула** (10 соединений): достаточно для 2 воркеров + poller + HTTP + 1 стрим rebuild; долгие SSE соединения пул не занимают.
- **`LoggingConfig` + sqlx:** при `RUST_LOG=debug` sqlx логирует каждый запрос; при необходимости `PgConnectOptions::log_statements(LevelFilter::Trace)`.
