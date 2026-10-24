# 03. Процессор проекций с закладкой

| | |
|---|---|
| Статус | предложено |
| Ценность | высокая: событие из журнала доходит до витрины при любом сбое; одна проекция на несколько нод без двойного применения; replay тем же кодом |
| Цена | высокая: новый компонент ядра, таблица, рантайм, модель сбоев с тестами |
| cqrsx | новые `cqrs/projection/checkpoint.rs`, `cqrs/projection/processor.rs` (при росте — `processor/{config,replay}.rs`), `sql/003-projection-checkpoints.sql`, `postgres/projection_checkpoints.rs`, `postgres/projection_runtime.rs`, `cqrs/testing/mem_checkpoint_store.rs`; правки `cqrs/command/executor.rs` и `postgres/command.rs` (`with_commit_wakeup`), `cqrs/testing/command.rs`, `postgres.rs` (ассет), `diagnostics.rs`, `doc/components.md`; тесты `tests/cqrs/projection/*`, `tests/postgres/projection/processor/*` |
| demo | нет (перевод демо — [04](04_demo_on_processor.md)) |
| Зависимости | [01](01_contiguous_views.md), [02](02_event_position.md) |
| Где выигрыш | G1–G5 из README эпика |

## Что делаем

Проекция перестаёт получать события от команды. Процессор проекции читает журнал страницами от своей закладки,
применяет события тем же `EventHandler` через существующую `OrderedDelivery` (рабочий на ключ, пачки, квитанции,
изоляция паники) и двигает закладку. Исполнитель команд после коммита только будит процессоры своего типа агрегата.

```text
CommandExecutor ── commit ──▶ events (tx) ◀──────────── page after position ──┐
       └── CommitWakeup.wake() ──▶ ProjectionProcessor ◀── poll / wakeup ────┘
                                        │ register / claim / extend / advance / release (аренда)
                                        ▼
                               projection_checkpoints
                                        │ задание на ключ
                                        ▼
                     OrderedDelivery ──▶ EventHandler (ViewProjection) ──▶ хранилище вида
```

### Схема

```sql
-- sql/003-projection-checkpoints.sql; константа PROJECTION_CHECKPOINTS_SCHEMA в postgres.rs
CREATE TABLE IF NOT EXISTS projection_checkpoints
(
    projection_name  text        NOT NULL CHECK (projection_name <> ''),
    aggregate_type   text        NOT NULL,
    position         xid8        NOT NULL DEFAULT '0',   -- всё до неё включительно применено
    replayed         bigint      NOT NULL DEFAULT 0,     -- событий применено с начала работы или последнего replay
    replay_requested boolean     NOT NULL DEFAULT false,
    replay_pace_ms   integer     NOT NULL DEFAULT 0 CHECK (replay_pace_ms >= 0),
    replay_until     xid8,                               -- NULL = живой режим
    owner            text,
    lock_token       uuid,
    locked_until     timestamptz,
    failures         integer     NOT NULL DEFAULT 0 CHECK (failures >= 0),
    last_error       text,
    updated_at       timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (projection_name, aggregate_type),
    CHECK ((owner IS NULL) = (lock_token IS NULL) AND (owner IS NULL) = (locked_until IS NULL))
);
```

### Хранилище закладок

```rust
// cqrs/projection/checkpoint.rs — контракт; postgres/projection_checkpoints.rs — PostgresCheckpointStore;
// cqrs/testing/mem_checkpoint_store.rs — для тестов. Каждая операция PostgreSQL — один автокоммитный оператор
// под StatementTimeout пула.
pub struct CheckpointKey { pub projection: String, pub aggregate_type: &'static str }
pub struct LeaseToken(Uuid);
pub struct Claimed { pub position: Position, pub replay: Option<ReplayState>, pub pending_replay: Option<Duration>, pub failures: u32, pub token: LeaseToken }
pub enum ReplayRequest { Accepted, AlreadyRunning }

pub trait CheckpointStore: Send + Sync {
    async fn register(&self, key: &CheckpointKey) -> Result<(), CheckpointError>;                     // INSERT … ON CONFLICT DO NOTHING, позиция 0
    async fn claim(&self, key, owner: &str, lease: Duration) -> Result<Option<Claimed>, CheckpointError>;   // свободна или аренда истекла
    async fn extend(&self, key, token: &LeaseToken, lease: Duration) -> Result<Option<Pending>, CheckpointError>; // None = аренда потеряна; Pending несёт запрос replay
    async fn advance(&self, key, token, to: Position, applied: u64, replay_done: bool) -> Result<bool, CheckpointError>; // false = аренда потеряна
    async fn record_failure(&self, key, token, error: &str) -> Result<bool, CheckpointError>;
    async fn begin_replay(&self, key, token, head: Position) -> Result<bool, CheckpointError>;         // позиция 0, replay_until = head, флаг снят, replayed = 0
    async fn request_replay(&self, key, pace: Duration) -> Result<ReplayRequest, CheckpointError>;
    async fn release(&self, key, token) -> Result<(), CheckpointError>;
    async fn status(&self, projection: Option<&str>) -> Result<Vec<CheckpointStatus>, CheckpointError>;
    async fn remove(&self, key) -> Result<(), CheckpointError>;                                        // scratch-строки verify
}
```

```sql
-- claim: только свободную или просроченную; срок — по часам базы
UPDATE projection_checkpoints
   SET owner = $3, lock_token = gen_random_uuid(), locked_until = clock_timestamp() + $4 * interval '1 millisecond'
 WHERE projection_name = $1 AND aggregate_type = $2
   AND (locked_until IS NULL OR locked_until < clock_timestamp())
RETURNING position::text, replay_until::text, replay_requested, replay_pace_ms, failures, lock_token;

-- advance: только владельцем и только вперёд (равенство — для снятия replay_until без сдвига)
UPDATE projection_checkpoints
   SET position = $4::text::xid8, replayed = replayed + $5, failures = 0, last_error = NULL,
       replay_until = CASE WHEN $6 THEN NULL ELSE replay_until END, updated_at = statement_timestamp()
 WHERE projection_name = $1 AND aggregate_type = $2 AND lock_token = $3 AND position <= $4::text::xid8;

-- request_replay: один replay за раз
UPDATE projection_checkpoints SET replay_requested = true, replay_pace_ms = $3
 WHERE projection_name = $1 AND aggregate_type = $2 AND NOT replay_requested AND replay_until IS NULL;
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

pub struct ProcessorConfig { poll_interval, page_size, lease, retry_base, retry_max, concurrency, delivery: DeliveryConfig }
// new(..) -> Result<Self, ProcessorConfigError>: всё положительно, retry_base <= retry_max, lease >= 1 s

/// Чистит витрину перед replay (wipe); идемпотентен.
pub trait ProjectionReset: Send + Sync { async fn reset(&self) -> Result<(), PersistenceError>; }

/// Наблюдатель; все методы с пустым умолчанием.
pub trait ProjectionObserver: Send + Sync {
    fn applied(&self, report: &Applied);     // ключ, последний sequence, событий, позиция, ReplayStatus
    fn failed(&self, report: &Failed);       // ключ, имя обработчика, категория ошибки, позиция, попытка
    fn replay(&self, event: &ReplayEvent);   // Started { until, pace } | Finished { replayed, elapsed } | Interrupted
    fn lease(&self, event: &LeaseEvent);     // Acquired | Lost | Released
    fn waiting(&self, reason: Waiting);      // Horizon (голова выше закладки, страница пуста) | Idle
}

#[derive(Clone)] pub struct CommitWakeup(Arc<Notify>);  // wake(): notify_waiters(); wait(): notified()

pub struct ProjectionProcessor<A, R: PersistedEventRepository, C: CheckpointStore> { .. }
impl ProjectionProcessor {
    pub fn new(name: impl Into<String>, repository: Arc<R>, checkpoints: Arc<C>,
               handler: Arc<dyn EventHandler<A>>, reset: Arc<dyn ProjectionReset>, config: ProcessorConfig) -> Self;
    pub fn with_upcasters(self, Vec<Box<dyn EventUpcaster>>) -> Self;
    pub fn with_observer(self, Arc<dyn ProjectionObserver>) -> Self;
    pub fn wakeup(&self) -> CommitWakeup;                                  // отдать исполнителю
    pub async fn run(self, shutdown: CancellationToken) -> Result<(), ProcessorError>;  // до остановки
    pub async fn catch_up(self) -> Result<Position, ProcessorError>;        // до головы на момент старта и выйти (verify)
}
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
    loop:
      if shutdown: drain (дождаться квитанций заданий в полёте, не дольше lease), advance по сделанному, release, return
      if lost: observer.lease(Lost), break (назад к claim)
      if pending replay: drain; reset.reset() (ошибка → пауза, повтор на следующем обороте);
                         head = repository.head(); begin_replay(head) → false: lost;
                         replaying = { until: head, pace }; observer.replay(Started)
      page = repository.feed_after(position, page_size)      // ошибка: лог, пауза, continue
      if page пуста:
          if replaying && position >= until: advance(position, 0, replay_done = true); replaying = None; observer.replay(Finished)
          observer.waiting(Horizon или Idle по repository.head() при replaying == None раз в N опросов)
          select { wakeup.wait(), sleep(poll_interval), shutdown }; continue
      jobs = события страницы по ключам, в порядке страницы;
             replaying с pace > 0: одно событие на задание, sleep(pace) перед каждой передачей
      for job in jobs: permit = concurrency.acquire(); slot = delivery.reserve(key); receipts.push(slot.deliver(events))
      outcomes = await всех квитанций (ошибка и паника задания — из квитанции)
      new_position = наибольшая позиция страницы, до которой включительно применены все задания, по целым транзакциям
      if new_position > position:
          advance(new_position, applied, replay_done = replaying && new_position >= until) → false: lost
          position = new_position; observer.applied(..)
      if были сбои: record_failure(category); failures += 1; observer.failed(..); sleep(backoff(failures)) под keeper
      else failures = 0
```

- **Закладка не обгоняет сбой.** События других ключей с позицией выше сбоя могли примениться; при повторе их
  отсеет правило непрерывности ([01](01_contiguous_views.md)). Процессору не нужен список «что уже применено».
- **Пауза при сбое**: `min(retry_base * 2^(n-1), retry_max)`; аренду на паузе держит keeper.
- **Ошибка журнала или закладок** (PostgreSQL недоступна): та же пауза; если аренда истекла, владельца перехватит
  другая нода, которой база тоже недоступна, — безопасно.
- **Replay**: сброс делает владелец между страницами, поэтому у коллекции один писатель и гонки replay с живыми
  событиями нет: события после `replay_until` применяются после истории, по порядку. `reset` идемпотентен:
  падение между wipe и `begin_replay` повторит wipe; падение после `begin_replay` продолжает replay с позиции без
  повторного wipe.
- **Остановка**: владелец дорабатывает задания в полёте (не дольше аренды), двигает закладку и отпускает аренду
  сразу, чтобы другая нода продолжила без ожидания `locked_until`.
- **`catch_up`**: как `run`, но без ожидания: голова берётся при старте, выход, когда позиция её достигла; аренда
  отпускается. Для verify и для разовых пересборок из командной строки.
- **Логи**: один `warn` на неудачную пачку с именем обработчика и категорией ошибки, без идентификаторов
  (`diagnostics::log_projection_failure`); `info` на смену владельца и начало/конец replay. `last_error` — та же
  категория и имя, без содержимого событий.

### Инварианты

- На пару «проекция + тип» не больше одного владельца аренды; только владелец пишет закладку и витрину.
- `position` не убывает иначе как через `begin_replay`; каждая транзакция ниже закладки применена целиком.
- Для каждого документа: равен свёртке `1..=last_sequence` ([01](01_contiguous_views.md)); никакое событие не
  применяется к документу дважды.
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
| F14 | Encore! под живой нагрузкой | wipe, replay 1..head, затем новые события по порядку; итог = свёртка; команды не отказывались | `a_replay_under_concurrent_commits_ends_in_the_fold_of_the_journal` |
| F15 | Encore! запрошен, пока владелец в паузе после сбоя | подхвачен на следующем обороте, не позже `retry_max` | `a_replay_request_reaches_an_owner_in_backoff` |
| F16 | Падение между wipe и `begin_replay` | флаг ещё стоит: wipe повторён, replay с 0 | `a_crash_between_the_wipe_and_begin_replay_repeats_the_wipe` |
| F17 | Падение после `begin_replay` | новый владелец продолжает replay с позиции, без wipe; `replay_until` сохранён | `a_crash_during_a_replay_continues_it_without_a_second_wipe` |
| F18 | Пишущая транзакция другого клиента зависла | страницы пусты, закладка стоит, `waiting(Horizon)`; после её конца — продолжение без пропуска | `an_idle_writing_transaction_pauses_the_projection_without_skipping` |
| F19 | `advance` отклонён (аренда перехвачена между продлением и `advance`) | старый владелец выходит; документы, записанные им до этого, корректны | `a_rejected_advance_stops_the_old_owner` |
| F20 | Второй replay во время первого | `AlreadyRunning`, первый не прерван | `a_second_replay_request_is_refused_while_one_runs` |
| F21 | Страница на 1000 ключей | не больше `concurrency` ключей в работе; порядок внутри ключа | `concurrency_bounds_the_keys_in_flight` |
| F22 | `catch_up` | читает до головы на момент старта, отпускает аренду, строку можно удалить | `catch_up_stops_at_the_head_and_releases` |
| F23 | `reset` отказал | флаг стоит, пауза, повтор; витрина не тронута закладкой | `a_failed_reset_is_retried_before_the_replay_starts` |
| F24 | Модельный тест | случайная последовательность: коммиты, падения в точках F2–F4 и F16–F17, потери аренды, сбои ключей, replay; инварианты после каждого шага; сходимость в конце | `processor_matches_a_model_under_random_faults` (proptest, как `ordered_delivery_matches_a_fifo_batch_model`) |

## Пограничные случаи

| # | Случай | Поведение |
|---|---|---|
| 1 | Новая проекция на старом журнале | строка с позицией 0: полная сборка с начала истории при первом старте |
| 2 | Журнал пуст | `head = START`; replay завершается сразу |
| 3 | Несколько проекций одного типа | свои строки и процессоры, журнал читают независимо |
| 4 | Проекция по двум типам агрегатов | две строки и два процессора с одним обработчиком |
| 5 | `page_size` меньше транзакции | страница дочитывается до конца транзакции ([02](02_event_position.md)) |
| 6 | `max_batch` в `OrderedDelivery` | склеивает задания соседних страниц одного ключа, как сейчас |
| 7 | Два разных имени проекции на одну коллекцию | не обнаруживается; правило в `components.md` |
| 8 | Обработчик выполняет команду | по-прежнему запрещено: ждал бы собственную закладку |
| 9 | Медленный replay с `pace` 80 мс на странице в 1000 событий | keeper держит аренду, `advance` раз в страницу; падение повторяет страницу с пропуском по `last_sequence` |

## Тесты вне модели сбоев

- Конфигурация: границы и умолчания равны константам; `ProcessorConfigError` с понятными сообщениями.
- `CommitWakeup`: пробуждение сокращает ожидание; без процессора безвредно; клоны делят одно уведомление.
- Observer: `applied` на пачку с последним `sequence` ключа и `ReplayStatus`; `waiting(Horizon)` против `Idle`.
- `status()`: позиция, `replayed`, владелец, `failures`, `last_error`, состояние replay.
- Матрица фич: ядро процессора компилируется без `postgres`; хранилище — за фичей.
- Doc-тест quick start с `MemStore`, `MemCheckpointStore`, `MemViewRepository`.

## Риски

- Горизонт снимка зависит от чужих зависших пишущих транзакций (F18): `waiting(Horizon)` в статусе и
  `idle_in_transaction_session_timeout` в compose демо.
- Один больной ключ держит проекцию (F7): осознанно, с видимым статусом; dead-letter — вне рамок эпика.
- Стоимость опроса при многих проекциях: один запрос по индексу на проекцию в `poll_interval`; общий читатель на
  тип — вне рамок.
- Пробуждение после коммита идёт до ответа клиенту; стоимость — `notify_waiters`, без ожидания.

## Критерии приёмки

- [ ] Схема; `CheckpointStore` с PostgreSQL и in-memory реализациями; все операции одним оператором с fencing по
      `lock_token`.
- [ ] `ProjectionProcessor` с `run`, `catch_up`, replay-протоколом, observer, keeper аренды, `CommitWakeup`;
      исполнители с `with_commit_wakeup`; `prepare_projection_runtime`.
- [ ] Строки модели сбоев F1–F24 и пограничные случаи покрыты тестами; покрытие ≥ 97 %.
- [ ] `components.md`: инвентарь (Event position, Projection checkpoint, Projection processor, Commit wake-up),
      путь команды и путь проекции, правила имён (Processor читает журнал; Delivery — его очередь на ключ;
      Checkpoint — закладка, Lease — аренда), правило «одна коллекция — одна проекция».
