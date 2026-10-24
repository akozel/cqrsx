# 03. Процессор проекций с закладкой

| | |
|---|---|
| Статус | предложено |
| Ценность | высокая: событие из журнала доходит до витрины при любом сбое; одна проекция на несколько нод без двойного применения; пересборка и самолечение тем же рабочим ключа |
| Цена | высокая: новый компонент ядра, таблица, рантайм, модель сбоев с тестами |
| cqrsx | новые `cqrs/projection/checkpoint.rs`, `cqrs/projection/processor.rs` (при росте — `processor/{config,rebuild}.rs`), `sql/003-projection-checkpoints.sql` (две таблицы), `postgres/projection_checkpoints.rs`, `postgres/projection_runtime.rs`, `cqrs/testing/mem_checkpoint_store.rs`; правки `cqrs/command/executor.rs` и `postgres/command.rs` (`with_commit_wakeup`), `cqrs/testing/command.rs`, `postgres.rs` (ассет), `diagnostics.rs`, `doc/components.md`; тесты `tests/cqrs/projection/*`, `tests/postgres/projection/processor/*` |
| demo | нет (перевод демо — [04](04_demo_on_processor.md)) |
| Зависимости | [01](01_contiguous_views.md), [02](02_event_position.md) |
| Где выигрыш | G1–G5 из README эпика |

## Что делаем

Проекция перестаёт получать события от команды. Процессор проекции читает журнал страницами от своей закладки,
применяет события тем же `EventHandler` через существующую `OrderedDelivery` (рабочий на ключ, пачки, квитанции,
изоляция паники) и двигает закладку. Исполнитель команд после коммита только будит процессоры своего типа агрегата.
Пересборка — задание через того же рабочего ключа: «прочитать историю агрегата, свернуть, заменить документ» —
для всех ключей журнала (Encore!), одного ключа по запросу или документа, отвергшего событие как дыру (D-19).

```text
CommandExecutor ── commit ──▶ events (tx) ◀──────────── page after position ──┐
       └── CommitWakeup.wake() ──▶ ProjectionProcessor ◀── poll / wakeup ────┘
                                        │ register / claim / extend / advance / release (аренда)
                                        ▼
                               projection_checkpoints
                                        │ задание на ключ: живая пачка или пересборка
                                        ▼
                     OrderedDelivery ──▶ EventHandler (ViewProjection) ──▶ хранилище вида
```

### Схема

```sql
-- sql/003-projection-checkpoints.sql; константа PROJECTION_CHECKPOINTS_SCHEMA в postgres.rs
CREATE TABLE IF NOT EXISTS projection_checkpoints
(
    projection_name   text        NOT NULL CHECK (projection_name <> ''),
    aggregate_type    text        NOT NULL,
    position          xid8        NOT NULL DEFAULT '0',   -- всё до неё включительно применено
    applied           bigint      NOT NULL DEFAULT 0,     -- событий применено живой доставкой
    rebuild_requested boolean     NOT NULL DEFAULT false,
    rebuild_pace_ms   integer     NOT NULL DEFAULT 0 CHECK (rebuild_pace_ms >= 0),
    rebuild_cursor    text,                               -- NULL = пересборки нет; '' = начата; иначе последний готовый ключ
    rebuilt           bigint      NOT NULL DEFAULT 0,     -- ключей пересобрано в текущей пересборке
    owner             text,
    lock_token        uuid,
    locked_until      timestamptz,
    failures          integer     NOT NULL DEFAULT 0 CHECK (failures >= 0),
    last_error        text,
    updated_at        timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (projection_name, aggregate_type),
    CHECK ((owner IS NULL) = (lock_token IS NULL) AND (owner IS NULL) = (locked_until IS NULL))
);

-- запросы пересборки одного ключа с любой ноды; владелец забирает их каждым оборотом
CREATE TABLE IF NOT EXISTS projection_rebuild_keys
(
    projection_name  text        NOT NULL,
    aggregate_type   text        NOT NULL,
    aggregate_id     text        NOT NULL,
    requested_at     timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (projection_name, aggregate_type, aggregate_id)
);
```

### Хранилище закладок

```rust
// cqrs/projection/checkpoint.rs — контракт; postgres/projection_checkpoints.rs — PostgresCheckpointStore;
// cqrs/testing/mem_checkpoint_store.rs — для тестов. Каждая операция PostgreSQL — один автокоммитный оператор
// под StatementTimeout пула.
pub struct CheckpointKey { pub projection: String, pub aggregate_type: &'static str }
pub struct LeaseToken(Uuid);
pub struct Claimed { pub position: Position, pub rebuild: Option<RebuildState>, pub pending_rebuild: Option<Duration>, pub failures: u32, pub token: LeaseToken }
pub struct RebuildState { pub cursor: String, pub pace: Duration, pub rebuilt: u64 }
pub enum RebuildRequest { Accepted, AlreadyRunning }

pub trait CheckpointStore: Send + Sync {
    async fn register(&self, key: &CheckpointKey) -> Result<(), CheckpointError>;                     // INSERT … ON CONFLICT DO NOTHING, позиция 0
    async fn claim(&self, key, owner: &str, lease: Duration) -> Result<Option<Claimed>, CheckpointError>;   // свободна или аренда истекла
    async fn extend(&self, key, token: &LeaseToken, lease: Duration) -> Result<Option<Pending>, CheckpointError>; // None = аренда потеряна; Pending несёт запрос пересборки
    async fn advance(&self, key, token, to: Position, applied: u64) -> Result<bool, CheckpointError>;  // false = аренда потеряна
    async fn record_failure(&self, key, token, error: &str) -> Result<bool, CheckpointError>;
    async fn request_rebuild(&self, key, pace: Duration) -> Result<RebuildRequest, CheckpointError>;  // с любой ноды; одна за раз
    async fn begin_rebuild(&self, key, token) -> Result<bool, CheckpointError>;                       // флаг снят, cursor = '', rebuilt = 0
    async fn advance_rebuild(&self, key, token, cursor: &str, keys: u64) -> Result<bool, CheckpointError>; // страница ключей готова
    async fn finish_rebuild(&self, key, token) -> Result<bool, CheckpointError>;                      // cursor = NULL
    async fn request_rebuild_key(&self, key, aggregate_id: &str) -> Result<(), CheckpointError>;      // строка в projection_rebuild_keys
    async fn take_rebuild_keys(&self, key, token, limit: usize) -> Result<Vec<String>, CheckpointError>; // DELETE … RETURNING, только владельцем
    async fn release(&self, key, token) -> Result<(), CheckpointError>;
    async fn status(&self, projection: Option<&str>) -> Result<Vec<CheckpointStatus>, CheckpointError>;
}
```

```sql
-- claim: только свободную или просроченную; срок — по часам базы
UPDATE projection_checkpoints
   SET owner = $3, lock_token = gen_random_uuid(), locked_until = clock_timestamp() + $4 * interval '1 millisecond'
 WHERE projection_name = $1 AND aggregate_type = $2
   AND (locked_until IS NULL OR locked_until < clock_timestamp())
RETURNING position::text, rebuild_cursor, rebuild_pace_ms, rebuild_requested, rebuilt, failures, lock_token;

-- advance: только владельцем и только вперёд
UPDATE projection_checkpoints
   SET position = $4::text::xid8, applied = applied + $5, failures = 0, last_error = NULL,
       updated_at = statement_timestamp()
 WHERE projection_name = $1 AND aggregate_type = $2 AND lock_token = $3 AND position < $4::text::xid8;

-- request_rebuild: одна пересборка за раз
UPDATE projection_checkpoints SET rebuild_requested = true, rebuild_pace_ms = $3
 WHERE projection_name = $1 AND aggregate_type = $2 AND NOT rebuild_requested AND rebuild_cursor IS NULL;

-- take_rebuild_keys: только владельцем (fencing через строку закладки)
DELETE FROM projection_rebuild_keys k
 USING projection_checkpoints c
 WHERE c.projection_name = $1 AND c.aggregate_type = $2 AND c.lock_token = $3
   AND k.projection_name = c.projection_name AND k.aggregate_type = c.aggregate_type
   AND k.aggregate_id IN (SELECT aggregate_id FROM projection_rebuild_keys
                           WHERE projection_name = $1 AND aggregate_type = $2 ORDER BY requested_at LIMIT $4)
RETURNING k.aggregate_id;
```

Срок аренды сравнивается с часами базы (`clock_timestamp()`); локальный дедлайн процессор считает консервативно,
с момента до отправки запроса, как `task_queue::claim`. Разница часов нод роли не играет.

### Процессор

```rust
// cqrs/projection/processor.rs
pub const DEFAULT_PROCESSOR_POLL_INTERVAL: Duration = Duration::from_millis(250);
pub const DEFAULT_PROCESSOR_PAGE_SIZE: usize = 1000;
pub const DEFAULT_PROCESSOR_LEASE: Duration = Duration::from_secs(10);
pub const DEFAULT_PROCESSOR_RETRY_BASE: Duration = Duration::from_millis(500);
pub const DEFAULT_PROCESSOR_RETRY_MAX: Duration = Duration::from_secs(10);
pub const DEFAULT_PROCESSOR_CONCURRENCY: usize = 16;    // ключей в работе одновременно
pub const DEFAULT_PROCESSOR_REBUILD_PAGE: usize = 100;  // ключей на страницу пересборки между живыми страницами

pub struct ProcessorConfig { poll_interval, page_size, rebuild_page, lease, retry_base, retry_max, concurrency, delivery: DeliveryConfig }
// new(..) -> Result<Self, ProcessorConfigError>: всё положительно, retry_base <= retry_max, lease >= 1 s

/// Проекция, которую процессор умеет пересобирать по ключу; `ViewProjection` реализует её: свёртка `events`
/// с нуля заменяет документ `key` с проверкой версии, `last_sequence` = последнее событие истории.
pub trait RebuildableProjection<A: Aggregate>: EventHandler<A> {
    async fn rebuild(&self, key: &str, events: &[EventEnvelope<A>]) -> Result<(), PersistenceError>;
}

/// Наблюдатель; все методы с пустым умолчанием.
pub trait ProjectionObserver: Send + Sync {
    fn applied(&self, report: &Applied);      // ключ, последний sequence, событий, позиция
    fn failed(&self, report: &Failed);        // ключ, имя обработчика, категория ошибки, позиция, попытка
    fn rebuilt(&self, report: &Rebuilt);      // ключ, событий в истории, причина: Requested | Healed
    fn rebuild(&self, event: &RebuildEvent);  // Started { pace } | Progress { keys } | Finished { keys, elapsed } | Interrupted
    fn lease(&self, event: &LeaseEvent);      // Acquired | Lost | Released
    fn waiting(&self, reason: Waiting);       // Horizon (голова выше закладки, страница пуста) | Idle
}

#[derive(Clone)] pub struct CommitWakeup(Arc<Notify>);  // wake(): notify_waiters(); wait(): notified()

pub struct ProjectionProcessor<A, R: PersistedEventRepository, C: CheckpointStore> { .. }
impl ProjectionProcessor {
    pub fn new(name: impl Into<String>, repository: Arc<R>, checkpoints: Arc<C>,
               projection: Arc<dyn RebuildableProjection<A>>, config: ProcessorConfig) -> Self;
    pub fn with_upcasters(self, Vec<Box<dyn EventUpcaster>>) -> Self;
    pub fn with_observer(self, Arc<dyn ProjectionObserver>) -> Self;
    pub fn wakeup(&self) -> CommitWakeup;                                  // отдать исполнителю
    pub async fn run(self, shutdown: CancellationToken) -> Result<(), ProcessorError>;  // до остановки
}
// Пересборка запрашивается через CheckpointStore с любой ноды: request_rebuild (все ключи, pace) и
// request_rebuild_key (один ключ); выполняет владелец.
```

Исполнитель: `CqrsCommandExecutor::with_commit_wakeup(CommitWakeup)` и то же у `PostgresCommandExecutor` и
`TestCommandExecutor`. После успешного коммита — `wake()`, до возврата клиенту. Это не доставка: процессор всё
равно читает журнал; пробуждение убирает ожидание опроса на той же ноде. Для другой ноды — опрос.

PostgreSQL-сборка: `postgres::projection_runtime::prepare_projection_runtime(pool, processors) -> ProjectionRuntime`
с `run(shutdown)`, который запускает все процессоры и завершается, когда остановились все; схема
`PROJECTION_CHECKPOINTS_SCHEMA` в `postgres.rs`. Без фоновой работы до `run`, как `prepare_task_runtime`.

### Алгоритм

```text
run(shutdown):
  register(key)
  loop:
    claim → None: ждать poll_interval или shutdown, снова claim; Some → владение
  владение:
    keeper: фоновая задача продлевает аренду каждые lease / 3; потеря → флаг lost, выдача заданий прекращается
    heal = ∅                                                  // ключи, отвергшие событие как дыру
    loop:
      if shutdown: drain (дождаться квитанций заданий в полёте, не дольше lease), advance по сделанному, release, return
      if lost: observer.lease(Lost), break (назад к claim)
      if pending rebuild: begin_rebuild() → false: lost; rebuilding = { cursor: "", pace }; observer.rebuild(Started)
      -- живая страница
      page = repository.feed_after(position, page_size)      // ошибка: лог, пауза, continue
      jobs = события страницы по ключам, в порядке страницы
      for job in jobs: permit = concurrency.acquire(); slot = delivery.reserve(key); receipts.push(slot.deliver(events))
      outcomes = await всех квитанций (ошибка и паника задания — из квитанции)
      new_position = наибольшая позиция страницы, до которой включительно применены все задания, по целым транзакциям
      if new_position > position: advance(new_position, applied) → false: lost; position = new_position; observer.applied(..)
      heal ∪= ключи, чьё задание вернуло SequenceGap
      -- пересборка: лечение, явные запросы, затем страница курсора
      keys = heal ∪ take_rebuild_keys(rebuild_page) ∪ (rebuilding ? repository.aggregate_ids_after(cursor, rebuild_page) : ∅)
      for key in keys: permit; slot = delivery.reserve(key); receipts.push(slot.rebuild(key, pace))
      outcomes = await; heal −= успешные; observer.rebuilt(..) на ключ
      if rebuilding: страница курсора пуста → finish_rebuild() → false: lost; rebuilding = None; observer.rebuild(Finished)
                     иначе cursor = последний ключ страницы; advance_rebuild(cursor, keys) → false: lost; observer.rebuild(Progress)
      if были сбои: record_failure(category); failures += 1; observer.failed(..); sleep(backoff(failures)) под keeper
      else failures = 0
      if живая страница пуста и пересборки нет:
          observer.waiting(Horizon или Idle по repository.head() раз в N опросов)
          select { wakeup.wait(), sleep(poll_interval), shutdown }
```

- **Задание «пересобрать ключ»** читает историю агрегата (`get_events`, с upcaster-ами, как живой путь),
  сворачивает с нуля и заменяет документ (`RebuildableProjection::rebuild`): версия растёт, `last_sequence` =
  последнее событие истории. С `pace > 0` первое событие заменяет документ, остальные применяются по одному с
  паузой — медленный Encore! демо. Задание идёт через рабочего своего ключа, поэтому с живой пачкой того же ключа
  не пересекается: живое событие, вставшее в очередь раньше, применится к старому документу, а пересборка затем
  включит его в историю; вставшее позже — применится поверх, если истории в нём ещё не было, или будет пропущено
  правилом непрерывности. Закладку задание не трогает.
- **Самолечение.** Ключ, чьё живое задание вернуло `SequenceGap`, пересобирается на следующем обороте; до этого
  закладка стоит перед дырой, после — догоняет. Падение между ними безвредно: дыра повторится при следующей
  доставке. Так чинится документ, удалённый wipe, и документ, отставший из-за сбоя хранилища.
- **Encore!** — `request_rebuild(pace)` с любой ноды; владелец ставит курсор и проходит ключи журнала страницами
  `aggregate_ids_after` ([02](02_event_position.md)), по странице между живыми страницами, поэтому касса открыта
  и отставание ограничено. Падение продолжает с курсора: ключи до него уже пересобраны, ключ в полёте
  пересобирается снова. Wipe не нужен: документы заменяются на месте; документ без агрегата в журнале пересборка
  не удалит (кнопка wipe остаётся демо-операцией sidecar).
- **Один ключ** — `request_rebuild_key`: строка в `projection_rebuild_keys`, владелец забирает их каждым оборотом.
- **Многопоточная витрина** (D-19): структурно допустима — две строки закладок с одним обработчиком; её
  пересборка требует wipe один раз на поколение, прохода ключей всех типов, карты `last_sequence` по потокам и
  коммутативной свёртки. В эпик не входит.
- **Закладка не обгоняет сбой.** События других ключей с позицией выше сбоя могли примениться; при повторе их
  отсеет правило непрерывности ([01](01_contiguous_views.md)). Процессору не нужен список «что уже применено».
- **Пауза при сбое**: `min(retry_base * 2^(n-1), retry_max)`; аренду на паузе держит keeper.
- **Ошибка журнала или закладок** (PostgreSQL недоступна): та же пауза; если аренда истекла, владельца перехватит
  другая нода, которой база тоже недоступна, — безопасно.
- **Остановка**: владелец дорабатывает задания в полёте (не дольше аренды), двигает закладку и отпускает аренду
  сразу, чтобы другая нода продолжила без ожидания `locked_until`.
- **Verify** процессора не касается: свёртка истории ключа в памяти против документа ([04](04_demo_on_processor.md)).
- **Логи**: один `warn` на неудачную пачку с именем обработчика и категорией ошибки, без идентификаторов
  (`diagnostics::log_projection_failure`); `info` на смену владельца, начало/конец пересборки и каждое
  самолечение (без идентификатора). `last_error` — та же категория и имя, без содержимого событий.

### Инварианты

- На пару «проекция + тип» не больше одного владельца аренды; только владелец пишет закладку и витрину.
- `position` не убывает; каждая транзакция ниже закладки применена целиком. Пересборка закладку не меняет.
- Для каждого документа: равен свёртке `1..=last_sequence` ([01](01_contiguous_views.md)); никакое событие не
  применяется к документу дважды — и при пересборке рядом с живой доставкой.
- Закладка никогда не выше первого неприменённого события своего типа.
- В покое процессор не держит соединений и блокировок; число строк закладок не растёт.

## Модель сбоев

Каждая строка — тест. Единичные тесты идут против `MemStore` + `MemCheckpointStore` + `MemViewRepository` с
управляемыми сбоями; интеграционные — PostgreSQL через testcontainers с `PostgresViewRepository` (MongoDB в cqrsx
нет). «Падение» в тестах — `JoinHandle::abort` задачи процессора в выбранной точке (точки сообщает тестовый хук
`ProcessorProbe` из `testing`) и запуск нового процессора над теми же хранилищами. Сбои хранилища вида — обёртка
над `ViewRepository`, которая отказывает заданное число раз. Аренда в тестах короткая (200–500 мс).

| # | Сбой | Ожидание | Тест |
|---|---|---|---|
| F1 | Команда закоммичена, процессор ещё не запущен или не разбужен | событие применено при следующем опросе | `an_event_committed_before_the_processor_started_is_applied` |
| F2 | Падение во время чтения страницы | ничего не применено; после перезапуска страница прочитана заново | `a_crash_while_reading_a_page_loses_nothing` |
| F3 | Падение после записи части документов, до `advance` | страница повторена, документы без двойного применения, закладка догоняет | `a_crash_between_the_view_writes_and_the_advance_applies_nothing_twice` |
| F4 | Падение во время `advance` | закладка либо сдвинута, либо нет; оба исхода безопасны | `a_crash_during_the_advance_is_safe_either_way` |
| F5 | Аренда истекла у живого владельца (завис в хранилище вида) | вторая нода продолжает с закладки; поздняя запись первой — конфликт версии и пропуск по `last_sequence`, либо `SequenceGap`; её `advance` возвращает `false`, она выходит | `a_stale_owner_cannot_damage_the_view_or_the_checkpoint` (gated `update_view`) |
| F6 | Хранилище вида отказывает N попыток | закладка стоит перед первым сбоем, `failures` растёт, пауза растёт до `retry_max`, аренда держится; после восстановления догон одной страницей | `a_failing_view_store_stalls_the_projection_before_the_first_failure_and_recovers` |
| F7 | Сбой на одном ключе, остальные успешны | закладка до события перед сбоем; успешные ключи при повторе отсеяны; `failed` с именем обработчика | `a_failure_of_one_key_moves_the_checkpoint_up_to_the_event_before_it` |
| F8 | Битое событие (не декодируется) | остановка на нём: `last_error`, `failures`; соседняя проекция того же типа идёт дальше; с upcaster — продолжение | `a_poison_event_stalls_only_its_projection_until_an_upcaster_arrives` |
| F9 | Паника обработчика | как F7, рабочий ключа жив | `a_panic_in_the_handler_is_a_failure_of_its_key_only` |
| F10 | PostgreSQL недоступна (закладки или журнал) | лог, пауза, продолжение после восстановления; при истёкшей аренде — повторный claim | `a_lost_database_connection_is_retried_and_the_claim_is_renewed` (`pg_terminate_backend` по `application_name` пула) |
| F11 | Две ноды стартуют одновременно | владеет одна, вторая ждёт; `lease(Acquired)` один раз | `two_processors_share_one_checkpoint_one_works_the_other_waits` |
| F12 | Владелец остановлен штатно посреди страницы | дорабатывает, двигает закладку, отпускает аренду; вторая нода берёт её сразу | `a_graceful_stop_hands_the_checkpoint_over_at_once` |
| F13 | Владелец убит (без `release`) | вторая нода берёт аренду после `locked_until`; ни одного пропуска | `a_killed_owner_is_replaced_after_the_lease_expires` |
| F14 | Encore! под живой нагрузкой | все ключи пересобраны между живыми страницами; команды не отказывались; итог = свёртка журнала; закладка не откатывалась | `a_rebuild_under_concurrent_commits_ends_in_the_fold_of_the_journal` |
| F15 | Encore! запрошен, пока владелец в паузе после сбоя | подхвачен на следующем обороте, не позже `retry_max` | `a_rebuild_request_reaches_an_owner_in_backoff` |
| F16 | Падение посреди пересборки | новый владелец продолжает с курсора; ключи до курсора не тронуты, ключ в полёте пересобран заново; `rebuilt` не убывает | `a_crash_during_a_rebuild_continues_from_the_cursor` |
| F17 | Живое событие ключа во время его пересборки | применено ровно один раз: либо вошло в историю, либо поверх; документ = свёртка | `a_live_event_during_its_key_rebuild_is_applied_exactly_once` |
| F18 | Пишущая транзакция другого клиента зависла | страницы пусты, закладка стоит, `waiting(Horizon)`; после её конца — продолжение без пропуска | `an_idle_writing_transaction_pauses_the_projection_without_skipping` |
| F19 | `advance` отклонён (аренда перехвачена между продлением и `advance`) | старый владелец выходит; документы, записанные им до этого, корректны | `a_rejected_advance_stops_the_old_owner` |
| F20 | Второй Encore! во время первого | `AlreadyRunning`, первый не прерван | `a_second_rebuild_request_is_refused_while_one_runs` |
| F21 | Страница на 1000 ключей | не больше `concurrency` ключей в работе; порядок внутри ключа | `concurrency_bounds_the_keys_in_flight` |
| F22 | Пересборка одного ключа под нагрузкой (`request_rebuild_key` с другой ноды) | документ = свёртка; соседние документы и закладка не тронуты | `a_single_key_rebuild_touches_nothing_else` |
| F23 | Дыра в документе: документ удалён или отстал из-за сбоя хранилища | живое задание отвергает `SequenceGap`, закладка стоит перед ним; на следующем обороте ключ пересобран из истории, закладка догоняет; остальные ключи не ждали | `a_gap_heals_itself_from_the_journal_without_stalling_the_projection` |
| F24 | Модельный тест | случайная последовательность: коммиты, падения в точках F2–F4 и F16, потери аренды, сбои ключей, удаления документов, пересборки целиком и по ключу; инварианты после каждого шага; сходимость в конце | `processor_matches_a_model_under_random_faults` (proptest, как `ordered_delivery_matches_a_fifo_batch_model`) |

## Пограничные случаи

| # | Случай | Поведение |
|---|---|---|
| 1 | Новая проекция на старом журнале | строка с позицией 0: полная сборка с начала истории при первом старте |
| 2 | Журнал пуст | `head = START`; пересборка завершается сразу: страница ключей пуста |
| 3 | Несколько проекций одного типа | свои строки и процессоры, журнал читают независимо |
| 4 | Проекция по двум типам агрегатов | две строки и два процессора с одним обработчиком — структурно допустимо; правила и пересборка такой витрины — D-19 в README, в эпике не реализуются |
| 5 | `page_size` меньше транзакции | страница дочитывается до конца транзакции ([02](02_event_position.md)) |
| 6 | `max_batch` в `OrderedDelivery` | склеивает задания соседних страниц одного ключа, как сейчас |
| 7 | Два разных имени проекции на одну коллекцию | не обнаруживается; правило в `components.md` |
| 8 | Обработчик выполняет команду | по-прежнему запрещено: ждал бы собственную закладку |
| 9 | Медленный Encore! с `pace` 80 мс на горячем ключе в 20 000 событий | keeper держит аренду; живые страницы идут между ключами; рабочий ключа занят заданием, живые события этого ключа ждут его конца и затем отсеиваются или применяются; падение повторяет ключ с курсора |
| 10 | `request_rebuild` на строке без владельца (процессор не запущен) | флаг ждёт первого владельца |
| 11 | Ключ из `projection_rebuild_keys`, которого нет в журнале | история пуста: `rebuild` ничего не пишет, ключ снят; документ-сирота остаётся (wipe — демо-операция) |

## Тесты вне модели сбоев

- Конфигурация: границы и умолчания равны константам; `ProcessorConfigError` с понятными сообщениями.
- `CommitWakeup`: пробуждение сокращает ожидание; без процессора безвредно; клоны делят одно уведомление.
- Observer: `applied` на пачку с последним `sequence` ключа; `rebuilt` с причиной; `waiting(Horizon)` против `Idle`.
- `status()`: позиция, `applied`, владелец, `failures`, `last_error`, `rebuild_cursor`, `rebuilt`.
- `ViewProjection::rebuild`: свёртка с нуля заменяет документ любой версии; пустая история ничего не пишет;
  конфликт версии повторяется, как в `apply_events`.
- Матрица фич: ядро процессора компилируется без `postgres`; хранилище — за фичей.
- Doc-тест quick start с `MemStore`, `MemCheckpointStore`, `MemViewRepository`.

## Риски

- Горизонт снимка зависит от чужих зависших пишущих транзакций (F18): `waiting(Horizon)` в статусе и
  `idle_in_transaction_session_timeout` в compose демо.
- Один больной ключ держит проекцию (F7): осознанно, с видимым статусом; dead-letter — вне рамок эпика. Дыра —
  исключение: лечится сама (F23).
- Полная пересборка читает историю каждого ключа отдельным запросом: больше обращений, чем один поток по журналу,
  зато продолжаемо после падения и параллельно по ключам; список ключей — skip scan по первичному ключу.
- Стоимость опроса при многих проекциях: один запрос по индексу на проекцию в `poll_interval`; общий читатель на
  тип — вне рамок.
- Пробуждение после коммита идёт до ответа клиенту; стоимость — `notify_waiters`, без ожидания.

## Критерии приёмки

- [ ] Схема (две таблицы); `CheckpointStore` с PostgreSQL и in-memory реализациями; все операции одним
      оператором с fencing по `lock_token`.
- [ ] `ProjectionProcessor` с `run`, пересборкой (Encore!, один ключ, самолечение), observer, keeper аренды,
      `CommitWakeup`; `RebuildableProjection` у `ViewProjection`; исполнители с `with_commit_wakeup`;
      `prepare_projection_runtime`.
- [ ] Строки модели сбоев F1–F24 и пограничные случаи покрыты тестами; покрытие ≥ 97 %.
- [ ] `components.md`: инвентарь (Event position, Projection checkpoint, Projection processor, Commit wake-up),
      путь команды и путь проекции, правила имён (Processor читает журнал; Delivery — его очередь на ключ;
      Checkpoint — закладка, Lease — аренда, Rebuild — пересборка по ключу), правило «одна коллекция — одна
      проекция», правила D-19 для многопоточных витрин.
