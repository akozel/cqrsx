# 03. Процессор проекций с закладкой

| | |
|---|---|
| Статус | сделано 2026-10-24 (см. «Отклонения»; замер — «Как измерить») |
| Ценность | высокая: событие из журнала доходит до витрины при любом сбое; одна проекция на несколько нод без двойного применения; пересборка и самолечение тем же рабочим ключа |
| Цена | высокая: новый компонент ядра, таблица, рантайм, модель сбоев с тестами |
| cqrsx | новые `cqrs/projection/checkpoint.rs`, `cqrs/projection/processor.rs` + `processor/{config,observer}.rs` (публичные) и `processor/{owner,lease,rebuild}.rs` (приватные), `cqrs/projection/runtime.rs`, `cqrs/command/wakeup.rs`, `cqrs/testing/mem_checkpoint_store.rs`, `postgres/projection_checkpoints.rs`, `sql/003-projection-checkpoints.sql`; правки `cqrs/projection.rs` (`RebuildableProjection`, `ViewProjection::rebuild`), `cqrs/event_handler/delivery/ordered.rs` (задание пересборки, `Done`), `cqrs/command/executor.rs`, `postgres/command.rs`, `cqrs/testing/command.rs` (`with_commit_wakeup`), `postgres.rs` (ассет), `diagnostics.rs`, `Cargo.toml` (`tokio-util` обязателен), `doc/components.md`, `README.md`; тесты `tests/cqrs/projection/*`, `tests/postgres/projection/{checkpoints,processor}.rs`, `tests/postgres/schema.rs` |
| demo | нет (перевод демо — [04](04_demo_on_processor.md)); корневой `compose.yaml` монтирует третью схему |
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
                               projection_checkpoints (+ projection_rebuild_keys)
                                        │ задание на ключ: живая пачка или пересборка
                                        ▼
                     OrderedDelivery ──▶ EventHandler / RebuildableProjection ──▶ хранилище вида
```

### Схема

`sql/003-projection-checkpoints.sql`, константа `PROJECTION_CHECKPOINTS_SCHEMA` в `postgres.rs`; умолчания
колонок — семантика `register` (`INSERT (projection_name, aggregate_type)`), не совместимость. PostgreSQL ≥ 18
(`RETURNING old` в `request_rebuild`).

```sql
CREATE TABLE IF NOT EXISTS projection_checkpoints
(
    projection_name   text        NOT NULL CHECK (projection_name <> ''),
    aggregate_type    text        NOT NULL CHECK (aggregate_type <> ''),
    position          xid8        NOT NULL DEFAULT '0',   -- всё до неё включительно применено
    applied           bigint      NOT NULL DEFAULT 0 CHECK (applied >= 0),   -- событий доставлено живой доставкой
    rebuild_requested boolean     NOT NULL DEFAULT false,
    rebuild_pace_ms   integer     NOT NULL DEFAULT 0 CHECK (rebuild_pace_ms >= 0),
    rebuild_cursor    text,                               -- NULL = пересборки нет; '' = начата; иначе последний записанный ключ
    rebuilt           bigint      NOT NULL DEFAULT 0 CHECK (rebuilt >= 0),   -- ключей в текущей/последней пересборке
    owner             text,
    lock_token        uuid,
    locked_until      timestamptz,
    failures          integer     NOT NULL DEFAULT 0 CHECK (failures >= 0),
    last_error        text,
    updated_at        timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (projection_name, aggregate_type),
    CHECK ((owner IS NULL) = (lock_token IS NULL) AND (owner IS NULL) = (locked_until IS NULL)),
    CHECK (NOT rebuild_requested OR rebuild_cursor IS NULL)
);

-- запросы пересборки одного ключа с любой ноды; владелец забирает их в порядке подачи
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
// под StatementTimeout пула; всё, что пишет строку, ограждено токеном: false / None = аренда потеряна.
pub struct CheckpointKey { projection: String, aggregate_type: &'static str }
pub struct LeaseToken(Uuid);
pub struct Claimed { pub token: LeaseToken, pub position: Position, pub failures: u32, pub rebuild: Option<RebuildState> }
pub struct RebuildState { pub cursor: String, pub pace: Duration, pub rebuilt: u64 }
pub enum RebuildRequest { Accepted, AlreadyRunning }
pub struct RebuildRequests { pub rebuild: Option<Duration>, pub keys: Vec<String> }
pub struct CheckpointStatus { projection, aggregate_type, position, applied, rebuild_requested, rebuild_pace, rebuild_cursor, rebuilt, owner, locked_until, failures, last_error, updated_at }
pub enum CheckpointError { Storage(..), Corrupt(..), Invalid(&'static str) }   // category(): storage | corrupt | invalid

pub trait CheckpointStore: Send + Sync {
    async fn register(&self, key) -> Result<(), CheckpointError>;                              // INSERT … ON CONFLICT DO NOTHING
    async fn claim(&self, key, owner: &str, lease: Duration) -> Result<Option<Claimed>, _>;     // свободна или просрочена (clock_timestamp())
    async fn extend(&self, key, token, lease) -> Result<bool, _>;                              // false = аренда потеряна
    async fn advance(&self, key, token, to: Position, applied: u64) -> Result<bool, _>;        // только вперёд; повтор позиции — true без изменений
    async fn record_failure(&self, key, token, error: &str) -> Result<bool, _>;
    async fn request_rebuild(&self, key, pace: Duration) -> Result<RebuildRequest, _>;         // с любой ноды; одна за раз; нет строки — Invalid
    async fn request_rebuild_key(&self, key, aggregate_id: &str) -> Result<(), _>;             // строка в projection_rebuild_keys; нет строки — Invalid
    async fn take_requests(&self, key, token, limit: usize) -> Result<Option<RebuildRequests>, _>; // флаг + ключи одним оператором, удаляет только владелец
    async fn begin_rebuild(&self, key, token) -> Result<bool, _>;                              // флаг снят, cursor = '', rebuilt = 0
    async fn advance_rebuild(&self, key, token, cursor: &str, keys: u64) -> Result<bool, _>;   // курсор только вперёд; повтор — true, счёт один раз
    async fn finish_rebuild(&self, key, token) -> Result<bool, _>;                             // cursor = NULL, rebuilt остаётся
    async fn release(&self, key, token) -> Result<(), _>;
    async fn status(&self, projection: Option<&str>) -> Result<Vec<CheckpointStatus>, _>;
}
```

Операторы PostgreSQL — в `postgres/projection_checkpoints.rs`: `claim` обновляет только свободную или просроченную
строку и возвращает токен; `advance` — `WHERE lock_token = $3 AND position <= $4` с подсчётом `applied` только при
движении вперёд; `request_rebuild` — один оператор с `RETURNING (NOT old.rebuild_requested AND old.rebuild_cursor IS NULL)`;
`take_requests` — CTE: `DELETE … USING projection_checkpoints … LIMIT $4 RETURNING` плюс чтение флага той же строки.
Срок аренды сравнивается с часами базы; локальный дедлайн процессор считает консервативно, с момента до отправки
запроса, как `task_queue::claim`. Разница часов нод роли не играет.

### Процессор

```rust
// cqrs/projection/processor/config.rs
pub const DEFAULT_PROCESSOR_POLL_INTERVAL: Duration = Duration::from_millis(250);
pub const DEFAULT_PROCESSOR_PAGE_SIZE: usize = 1000;
pub const DEFAULT_PROCESSOR_REBUILD_PAGE: usize = 100;  // ключей на страницу пересборки между живыми страницами
pub const DEFAULT_PROCESSOR_LEASE: Duration = Duration::from_secs(10);
pub const MIN_PROCESSOR_LEASE: Duration = Duration::from_secs(1);
pub const DEFAULT_PROCESSOR_RETRY_BASE: Duration = Duration::from_millis(500);
pub const DEFAULT_PROCESSOR_RETRY_MAX: Duration = Duration::from_secs(10);
pub const DEFAULT_PROCESSOR_CONCURRENCY: usize = 16;    // документов пишется одновременно

pub struct ProcessorConfig { .. }   // new(poll_interval, page_size, lease, delivery) -> Result; with_retry(base, max), with_rebuild_page(n), with_concurrency(n)
                                    // pages_in_flight() = delivery.capacity() − 1; backoff(n) = min(base · 2^(n−1), max); capacity ≥ 2, lease ≥ 1 s

// cqrs/projection.rs — проекция, которую процессор умеет пересобирать по ключу; ViewProjection реализует её
pub trait RebuildableProjection<A: Aggregate>: EventHandler<A> {
    /// Заменяет документ свёрткой history[..prefix]; документ, ушедший дальше прочитанной истории, отвергается
    /// OptimisticLockError (читатель перечитывает историю); пустая история ничего не пишет.
    async fn rebuild(&self, aggregate_id: &str, history: &[EventEnvelope<A>], prefix: usize) -> Result<(), PersistenceError>;
}

// cqrs/projection/processor/observer.rs — все методы с пустым телом
pub trait ProjectionObserver: Send + Sync {
    fn applied(&self, &Applied);      // ключ, последний sequence, событий, позиция страницы (квитанция ок)
    fn advanced(&self, &Advanced);    // закладка сдвинута: позиция, событий
    fn failed(&self, &Failed);        // ключ (если есть), обработчик, категория, позиция, сбоев подряд
    fn rebuilt(&self, &Rebuilt);      // ключ, событий в истории, причина: Requested | Healed
    fn rebuild(&self, RebuildEvent);  // Started { pace } | Progress { keys } | Finished { keys, elapsed } | Interrupted
    fn lease(&self, LeaseEvent);      // Acquired | Lost | Released
    fn waiting(&self, Waiting);       // Horizon (голова выше прочитанного, страница пуста) | Idle
}

// cqrs/command/wakeup.rs
#[derive(Clone)] pub struct CommitWakeup(Arc<Notify>);  // new(); wake(): notify_waiters(); wait()

// cqrs/projection/processor.rs
pub struct ProjectionProcessor<A, R: PersistedEventRepository, C: CheckpointStore> { .. }
impl ProjectionProcessor {
    pub fn new(name, repository: Arc<R>, checkpoints: Arc<C>, projection: Arc<dyn RebuildableProjection<A>>, config) -> Self;
    pub fn with_upcasters(self, Vec<Box<dyn EventUpcaster>>) -> Self;
    pub fn with_observer(self, Arc<dyn ProjectionObserver>) -> Self;
    pub fn with_wakeup(self, CommitWakeup) -> Self;      // общий с исполнителем типа
    pub fn with_owner(self, impl Into<String>) -> Self;  // имя владельца в строке; умолчание — UUID
    pub async fn run(self, shutdown: CancellationToken);  // до остановки; ошибки хранилищ повторяются, фатальных нет
}

// cqrs/projection/runtime.rs
pub struct ProjectionRuntime;  // new().with_processor(p)…; run(shutdown) -> Result<(), ProjectionRuntimeError::Panicked { projection }>
```

Исполнитель: `CqrsCommandExecutor::with_commit_wakeup(CommitWakeup)` и то же у `PostgresCommandExecutor` и
`TestCommandExecutor`. После успешного коммита с событиями — `wake()`, до передачи обработчикам и до возврата
клиенту. Это не доставка: процессор всё равно читает журнал; пробуждение убирает ожидание опроса на той же
ноде. Для другой ноды — опрос. Один `CommitWakeup` на тип агрегата создаёт приложение и отдаёт исполнителю и
каждому процессору типа.

### Алгоритм

```text
run(shutdown):
  register(key) с паузой backoff(n) до успеха
  loop: claim → None: ждать poll_interval или shutdown; Some → владение; потеря аренды → снова claim; остановка → return
  владение (owner.rs):
    keeper продлевает аренду каждые lease / 3; потеря = отказ хранилища или локальный дедлайн без продления
    окно: frontier (выдано), checkpoint (подтверждено), pages — страницы с квитанциями в полёте, ≤ capacity − 1
    оборот:
      settle: пока голова окна завершена без сбоев — advance(позиция, событий), observer.advanced
      сбой в голове (или сбой без страницы: журнал, закладки, пересборка): дождаться живых квитанций;
        advance до транзакции перед первой транзакцией сбойного ключа; record_failure("{обработчик}: {категория}");
        failures += 1; observer.failed; окно очищено, frontier = checkpoint; пауза backoff(failures)
      живой оборот (не в паузе, без сбоя):
        take_requests раз в poll_interval → ключи по запросу; флаг Encore! → begin_rebuild, Started
        пересборка: ключи по запросу не в полёте → задание (pace 0); страница курсора (aggregate_ids_after(cursor,
          rebuild_page)) → задания с pace строки, когда прошлая страница записана; пустая страница → finish_rebuild, Finished
        страница журнала, если окно не полно: feed_after(frontier, page_size); битое событие режет страницу по своей
          транзакции (префикс доставляется, само событие — сбой «journal: deserialization»); группы по ключу в
          порядке появления → reserve всех ключей, затем deliver без ожидания; frontier = последняя позиция
        пустая страница: каждый 8-й опрос head(): выше frontier → waiting(Horizon), иначе Idle
      ждать: shutdown | потеря аренды | квитанция живая | квитанция пересборки | таймер паузы | wakeup | poll_interval
        (окно с местом и непустая страница — без ожидания; сбой без квитанций в полёте — сразу к паузе)
      квитанция живая: ок → observer.applied; SequenceGap у ключа без более раннего сбоя в окне → лечение на месте:
        задание пересборки ключа (Healed) встаёт в FIFO ключа за сбойным, страница ждёт его квитанцию как свою;
        иной сбой (или дыра после сбоя) → страница сбойна, чтение остановлено
      квитанция пересборки: ок → observer.rebuilt, ждавшие страницы → applied, член страницы курсора → счёт;
        сбой → ждавшие страницы сбойны / страница курсора сбойна (перечитается с прежнего курсора) / ключ по
        запросу возвращается в очередь; сбой оборота
      страница курсора записана целиком → advance_rebuild(last_key, keys), Progress
    остановка: stop отменён (медленные пересборки заканчиваются на ближайшем шаге), ждать квитанции ≤ lease,
      settle, записать завершённую страницу курсора, Interrupted для активной пересборки, release
    потеря аренды: stop отменён, состояние сброшено; рабочие ключей дорабатывают короткие задания — их поздние
      записи защищены CAS, непрерывностью и границей истории у пересборки
```

- **Задание «пересобрать ключ»** читает историю агрегата (`get_events`, с upcaster-ами, как живой путь),
  сворачивает с нуля и заменяет документ (`RebuildableProjection::rebuild`): версия растёт, `last_sequence` =
  последнее событие истории. С `pace > 0` первое событие заменяет документ, остальные применяются по одному с
  паузой — медленный Encore! демо; пауза не держит разрешение `concurrency`, а `stop` владения прерывает её.
  Задание идёт через рабочего своего ключа, поэтому с живой пачкой того же ключа не пересекается: живое событие,
  вставшее в очередь раньше, применится к старому документу, а пересборка затем включит его в историю; вставшее
  позже — применится поверх, если истории в нём ещё не было, или будет пропущено правилом непрерывности.
  Документ новее прочитанной истории (устаревший читатель) отвергается, история перечитывается (до трёх раз).
  Закладку задание не трогает.
- **Самолечение.** Ключ, чьё живое задание вернуло `SequenceGap`, пересобирается сразу: страница ждёт квитанцию
  пересборки, окно не опустошается, сбой не считается. Так чинится документ, удалённый wipe, и документ, отставший
  из-за сбоя хранилища. Дыра у ключа, уже сбойного в этом окне, — обычный сбой (повтор после паузы).
- **Encore!** — `request_rebuild(pace)` с любой ноды; владелец ставит курсор и проходит ключи журнала страницами
  `aggregate_ids_after` ([02](02_event_position.md)), по странице между живыми страницами, поэтому касса открыта
  и отставание ограничено. Падение продолжает с курсора: ключи до него уже пересобраны, страница в полёте
  пересобирается снова. Wipe не нужен: документы заменяются на месте; документ без агрегата в журнале пересборка
  не удалит (кнопка wipe остаётся демо-операцией sidecar).
- **Один ключ** — `request_rebuild_key`: строка в `projection_rebuild_keys`, владелец забирает их раз в
  `poll_interval` одним оператором вместе с флагом Encore!.
- **Многопоточная витрина** (D-19): структурно допустима — две строки закладок с одним обработчиком; её
  пересборка требует wipe один раз на поколение, прохода ключей всех типов, карты `last_sequence` по потокам и
  коммутативной свёртки. В эпик не входит.
- **Закладка не обгоняет сбой.** События других ключей с позицией выше сбоя могли примениться; при повторе их
  отсеет правило непрерывности ([01](01_contiguous_views.md)). Процессору не нужен список «что уже применено».
- **Пауза при сбое**: `min(retry_base * 2^(n-1), retry_max)`; аренду на паузе держит keeper; `advance` обнуляет
  `failures` (и в строке, и локально).
- **Ошибка журнала или закладок** (PostgreSQL недоступна): та же пауза; если аренда истекла, владельца перехватит
  другая нода, которой база тоже недоступна, — безопасно. `advance`/`advance_rebuild` идемпотентны: потерянный
  ответ повторяется без «потери аренды».
- **Остановка**: владелец дорабатывает задания в полёте (не дольше аренды), двигает закладку и отпускает аренду
  сразу, чтобы другая нода продолжила без ожидания `locked_until`.
- **Verify** процессора не касается: свёртка истории ключа в памяти против документа ([04](04_demo_on_processor.md)).
- **Логи**: на сбойную пачку — `error` рабочего доставки с именем обработчика и операцией (`deliver` / `rebuild`),
  без идентификаторов; на уход в паузу — один `warn` (`log_projection_failure`: операция, категория, `failures`);
  `info` на смену владельца, начало/конец/прерывание пересборки и каждое самолечение (без идентификатора).
  `last_error` — `"{обработчик}: {категория}"`, без содержимого событий.

### Инварианты

- На пару «проекция + тип» не больше одного владельца аренды; только владелец пишет закладку и витрину.
- `position` не убывает; каждая транзакция ниже закладки применена целиком. Пересборка закладку не меняет.
- Для каждого документа: равен свёртке `1..=last_sequence` ([01](01_contiguous_views.md)); никакое событие не
  применяется к документу дважды — и при пересборке рядом с живой доставкой; документ никогда не откатывается
  назад устаревшим читателем.
- Закладка никогда не выше первого неприменённого события своего типа (после wipe — события, закоммиченного
  после wipe).
- В покое процессор не держит соединений и блокировок; число строк закладок не растёт.

## Модель сбоев

Каждая строка — тест. Единичные тесты идут против `MemEventRepository` + `MemCheckpointStore` +
`MemViewRepository` за обёртками с воротами и сценариями сбоев (`tests/cqrs/projection/support.rs`); интеграционные —
PostgreSQL через testcontainers с `PostgresViewRepository` (MongoDB в cqrsx нет). «Падение» в тестах —
`JoinHandle::abort` задачи процессора у открытых ворот (рабочие ключей при этом доживают — «медленный устаревший
владелец») и запуск нового процессора над теми же хранилищами. Аренда в тестах 1 с, время под паузой `tokio`.

| # | Сбой | Ожидание | Тест |
|---|---|---|---|
| F1 | Команда закоммичена, процессор ещё не запущен или не разбужен | событие применено при следующем опросе | `live::an_event_committed_before_the_processor_started_is_applied` |
| F2 | Падение во время чтения страницы | ничего не применено; после перезапуска страница прочитана заново | `live::a_crash_while_reading_a_page_loses_nothing` |
| F3 | Падение после записи части документов, до `advance` | страница повторена, документы без двойного применения, закладка догоняет | `live::a_crash_between_the_view_writes_and_the_advance_applies_nothing_twice` |
| F4 | Падение во время `advance` | закладка либо сдвинута, либо нет; оба исхода безопасны | `live::a_crash_during_the_advance_is_safe_either_way` |
| F5 | Аренда истекла у живого владельца (завис в хранилище вида, хранилище закладок недоступно) | вторая нода продолжает с закладки; поздняя запись первой — конфликт версии и пропуск по `last_sequence`; её `advance` не зовётся | `lease::a_stale_owner_cannot_damage_the_view_or_the_checkpoint`, `rebuild::a_stale_rebuild_never_rolls_a_document_back` |
| F6 | Хранилище вида отказывает N попыток | закладка стоит перед первым сбоем, `failures` растёт, пауза растёт, аренда держится; после восстановления догон одной страницей | `live::a_failing_view_store_stalls_the_projection_before_the_first_failure_and_recovers` |
| F7 | Сбой на одном ключе, остальные успешны | закладка до транзакции перед сбоем; успешные ключи при повторе отсеяны; `failed` с именем обработчика | `live::a_failure_of_one_key_moves_the_checkpoint_up_to_the_event_before_it` |
| F8 | Битое событие (не декодируется) | остановка на нём: `last_error`, `failures`; соседняя проекция того же типа с upcaster идёт дальше; перезапуск с upcaster — продолжение | `live::a_poison_event_stalls_only_its_projection_until_an_upcaster_arrives` |
| F9 | Паника обработчика | как F7, рабочий ключа жив | `live::a_panic_in_the_handler_is_a_failure_of_its_key_only` |
| F10 | PostgreSQL недоступна (закладки или журнал) | лог, пауза, продолжение после восстановления; при истёкшей аренде — повторный claim; потерянный ответ `advance` повторяется | `lease::a_lost_store_is_retried_and_the_claim_is_renewed`; PG: `processor::a_lost_database_connection_is_retried_and_the_claim_is_renewed` (`pg_terminate_backend` по `application_name`) |
| F11 | Две ноды стартуют одновременно | владеет одна, вторая ждёт; `lease(Acquired)` один раз | `lease::two_processors_share_one_checkpoint_one_works_the_other_waits`; PG: то же имя |
| F12 | Владелец остановлен штатно посреди страницы | дорабатывает, двигает закладку, отпускает аренду; вторая нода берёт её в ближайший опрос | `lease::a_graceful_stop_hands_the_checkpoint_over_at_once`; застрявший ключ — `lease::a_stop_with_a_stuck_key_releases_after_the_drain_deadline`; остановка в паузе и потеря аренды во время drain — `lease::a_stop_during_backoff_or_a_lost_lease_during_drain_ends_cleanly` |
| F13 | Владелец убит (без `release`) | вторая нода берёт аренду после `locked_until`; ни одного пропуска | `lease::a_killed_owner_is_replaced_after_the_lease_expires`; PG: то же имя |
| F14 | Encore! под живой нагрузкой | все ключи пересобраны между живыми страницами; команды не отказывались; итог = свёртка журнала; закладка не откатывалась | `rebuild::a_rebuild_under_concurrent_commits_ends_in_the_fold_of_the_journal`; PG: то же имя |
| F15 | Encore! запрошен, пока владелец в паузе после сбоя | подхвачен после паузы, не позже `retry_max` | `rebuild::a_rebuild_request_reaches_an_owner_in_backoff` |
| F16 | Падение посреди пересборки | новый владелец продолжает с курсора; ключи до курсора не тронуты, страница в полёте пересобрана заново; `rebuilt` не убывает | `rebuild::a_crash_during_a_rebuild_continues_from_the_cursor` |
| F17 | Живое событие ключа во время его пересборки | применено ровно один раз: поверх пересобранного документа | `rebuild::a_live_event_during_its_key_rebuild_is_applied_exactly_once` |
| F18 | Пишущая транзакция другого клиента зависла | страницы пусты, закладка стоит, `waiting(Horizon)`; после её конца — продолжение без пропуска | PG: `processor::an_idle_writing_transaction_pauses_the_projection_without_skipping`; в памяти — `live::waiting_distinguishes_the_horizon_from_an_idle_journal` |
| F19 | `advance` отклонён (аренда перехвачена между продлением и `advance`) | старый владелец выходит; документы, записанные им до этого, корректны | `lease::a_rejected_advance_stops_the_old_owner` |
| F20 | Второй Encore! во время первого | `AlreadyRunning`, первый не прерван | `rebuild::a_second_rebuild_request_is_refused_while_one_runs`; хранилища: `checkpoints::rebuild_requests_run_one_at_a_time_…` |
| F21 | Страница на 1000 ключей | не больше `concurrency` документов пишется одновременно; порядок внутри ключа | `live::concurrency_bounds_the_keys_in_flight` |
| F22 | Пересборка одного ключа под нагрузкой (`request_rebuild_key` с другой ноды) | документ = свёртка; соседние документы и закладка не тронуты | `rebuild::a_single_key_rebuild_touches_nothing_else` |
| F23 | Дыра в документе: документ удалён или отстал из-за сбоя хранилища | живое задание отвергает `SequenceGap`, страница ждёт пересборку ключа, соседи не ждут; `failures` не растёт; закладка догоняет | `rebuild::a_gap_heals_itself_from_the_journal_without_stalling_the_projection` |
| F24 | Модельный тест | случайная последовательность: коммиты, падения в точках F2–F4 и F16, потери аренды, сбои ключей, удаления документов, пересборки целиком и по ключу; инварианты после каждого шага; сходимость в конце | `model::property_processor_matches_a_model_under_random_faults` (proptest, 128 кейсов; CI гоняет с сидом коммита) |

## Пограничные случаи

| # | Случай | Поведение |
|---|---|---|
| 1 | Новая проекция на старом журнале | строка с позицией 0: полная сборка с начала истории при первом старте |
| 2 | Журнал пуст | `head = START`; пересборка завершается сразу: страница ключей пуста (`rebuild::an_empty_journal_finishes_a_rebuild_at_once`) |
| 3 | Несколько проекций одного типа | свои строки и процессоры, журнал читают независимо (F8) |
| 4 | Проекция по двум типам агрегатов | две строки и два процессора с одним обработчиком — структурно допустимо; правила и пересборка такой витрины — D-19 в README, в эпике не реализуются |
| 5 | `page_size` меньше транзакции | страница дочитывается до конца транзакции (`live::a_page_smaller_than_a_transaction_is_delivered_whole`) |
| 6 | `max_batch` в `OrderedDelivery` | склеивает задания соседних страниц одного ключа (`live::queued_pages_of_one_key_are_handled_in_one_call`) |
| 7 | Два разных имени проекции на одну коллекцию | не обнаруживается; правило в `components.md` |
| 8 | Обработчик выполняет команду | по-прежнему запрещено: ждал бы собственную закладку |
| 9 | Медленный Encore! с `pace` 80 мс на ключе в 200 событий | keeper держит аренду; живые страницы идут мимо, пока окно не заполнится страницами с этим ключом (`capacity − 1`), потом чтение ждёт его конца; падение повторяет ключ с курсора (`rebuild::a_paced_rebuild_of_one_key_does_not_hold_the_other_keys_back`) |
| 10 | `request_rebuild` на строке без владельца (процессор остановлен) | флаг ждёт следующего владельца (случай 2); на незарегистрированной строке — `Invalid` |
| 11 | Ключ из `projection_rebuild_keys`, которого нет в журнале | история пуста: `rebuild` ничего не пишет, ключ снят; документ-сирота остаётся (`rebuild::a_requested_key_without_history_is_dropped_without_a_write`) |
| 12 | Окно страниц | не глубже `capacity − 1` (`live::the_window_is_bounded_by_the_buffer_capacity`); пробуждение коммитом раньше опроса (`live::a_commit_wakes_an_idle_processor_before_the_poll`) |

## Тесты вне модели сбоев

- Конфигурация: границы и умолчания равны константам; `ProcessorConfigError` с понятными сообщениями
  (`processor/config.rs`).
- `CommitWakeup`: исполнитель будит после коммита с событиями и не будит после отказа или пустого коммита;
  клоны делят одно уведомление; регистрация до чтения (`command/wakeup.rs`, `executor.rs`, `testing/command.rs`).
- Observer: умолчания пусты; отчёты в тестах модели сбоев.
- `status()`: позиция, `applied`, владелец, `failures`, `last_error`, `rebuild_cursor`, `rebuilt`
  (`mem_checkpoint_store.rs`, `tests/postgres/projection/checkpoints.rs`).
- `ViewProjection::rebuild`: свёртка с нуля заменяет документ любой версии; `prefix = 1`; пустая история ничего не
  пишет; конфликт версии повторяется, как в `apply_events`; документ впереди истории отвергается без записи, в том
  числе при повторе конфликта; дыра в истории — ошибка (`projection.rs`, `mod rebuild`).
- `OrderedDelivery`: задание пересборки в FIFO ключа, пачка не смешивает виды, `Done`, сбой и паника пересборки,
  `stop`; модель `ordered_delivery_matches_a_fifo_batch_model` с op `Rebuild`.
- `HistoryRebuilder`: пустая история, `pace`, разрешение не держится во время паузы, `stop`, перечитывание устаревшей
  истории до трёх раз; `LeaseKeeper`: продление каждые `lease / 3`, отказ, недоступность до и после дедлайна.
- Матрица фич: ядро процессора компилируется и тестируется без `postgres`; хранилище — за фичей.
- Doc-тест quick start в `processor.rs` с `MemEventRepository`, `MemCheckpointStore`, `MemViewRepository`.
- Схема: `CHECK`-ограничения через `jsonb_populate_record`, повторное применение трёх схем (`schema.rs`).

## Как измерить

- Перенесено из [02](02_event_position.md) (решение пользователя 2026-10-24): горячий ключ `01-hot-key-vus40` в
  профиле `db` до и после колонки `tx` с индексом; ожидание — CPU PostgreSQL на команду в пределах шума, WAL на
  команду чуть выше (одна запись в индекс). Стенд `02_postgres_performance/perf/`,
  `./run.sh hot-key NAME=01-hot-key-vus40 VUS=40 POOL=20 PROFILE=db VERIFY=1`; «до» — оверлей `compose.before-feed.yaml`
  (`postgres/15-before-feed.sql` снимает индекс и колонку после схемы стенда; демо колонку не читает); серии
  `perf-03-before-` / `perf-03-after-` вперемешку, три прохода; `node tools/compare.mjs … perf-03-before- perf-03-after- --md`;
  метрики `efficiency.cpu_ms_per_1k_accepted.postgres` и `db.wal_bytes_per_command`.
- Результат 2026-10-24 (медиана трёх проходов, в скобках разброс; стенд под CPU-квотой, один образ back-end,
  прогоны «до»/«после» вперемешку; каталоги `demo/load/results/20261024-19*-perf-03-*`):

  | Метрика | до (`perf-03-before-`) | после (`perf-03-after-`) | Δ |
  |---|---|---|---|
  | принятых/с | 705 (658–734) | 710 (708–714) | +1 % |
  | p50 / p99, мс | 56.3 / 66 (65–84) | 55.6 / 86 (67–89) | −1 % / шум квоты |
  | CPU PostgreSQL, мс на 1000 команд | 1755 (1586–2243) | 1689 (1681–1730) | −4 % (шум) |
  | CPU back-end / MongoDB, мс на 1000 команд | 1584 / 727 | 1509 / 709 | −5 % / −2 % (шум) |
  | WAL на команду, байт | 4231 (4206–4641) | 4433 (4417–4446) | **+5 %** (≈ 200 байт: запись в `events_feed_idx` и колонка) |
  | операторов / пакетов на команду | 8.52 / 34.5 | 8.43 / 34.1 | −1 % |
  | `verify ok`, 409, 503, `projection_failed` | 1, 0, 0, 0 | 1, 0, 0, 0 | — |

  Вывод: G6 выполнена — CPU базы и пропускная способность в пределах шума, цена колонки и индекса — около 200
  байт WAL на команду.
- Полная пересборка на 1 и 5 млн событий — в [04](04_demo_on_processor.md), когда процессор работает в демо и
  стенд может нагенерировать историю (решение пользователя 2026-10-24).

## Риски

- Горизонт снимка зависит от чужих зависших пишущих транзакций (F18): `waiting(Horizon)` в статусе и
  `idle_in_transaction_session_timeout` в compose демо.
- Один больной ключ держит проекцию (F7): осознанно, с видимым статусом; dead-letter — вне рамок эпика. Дыра —
  исключение: лечится сама (F23).
- Медленная пересборка горячего ключа останавливает чтение, когда в окне `capacity − 1` страниц с этим ключом:
  граница, не отсутствие остановки; `ENCORE_PROJECTION_BUFFER` в демо — глубина окна плюс один.
- Полная пересборка читает историю каждого ключа отдельным запросом под `StatementTimeout` главного пула: длинная
  история без снапшотов упирается в него, как команда; зато продолжаемо после падения и параллельно по ключам.
- Стоимость опроса при многих проекциях: страница журнала в `poll_interval` плюс `take_requests` раз в
  `poll_interval` на проекцию; общий читатель на тип — вне рамок.
- Удалённый документ начинает версии заново: поздняя запись владельца, потерявшего аренду до wipe, может лечь на
  пересозданный документ. Wipe — демо-операция; под нагрузкой на нескольких нодах его не делать.
- Пробуждение после коммита идёт до ответа клиенту; стоимость — `notify_waiters`, без ожидания.

## Критерии приёмки

- [x] Схема (две таблицы); `CheckpointStore` с PostgreSQL и in-memory реализациями; все операции одним
      оператором с fencing по `lock_token`; `advance`/`advance_rebuild` идемпотентны.
- [x] `ProjectionProcessor` с `run`, окном страниц, пересборкой (Encore!, один ключ, самолечение на месте),
      observer, keeper аренды, `stop` владения; `CommitWakeup` общий у исполнителя и процессоров;
      `RebuildableProjection` у `ViewProjection`; `ProjectionRuntime`.
- [x] Строки модели сбоев F1–F24 и пограничные случаи покрыты тестами; покрытие ≥ 97 %.
- [x] `components.md`: инвентарь (Event position, Projection checkpoint, Projection processor, Commit wake-up,
      Projection runtime), путь команды и путь проекции, правила имён (Processor читает журнал; Delivery — его
      очередь на ключ; Checkpoint — закладка, Lease — аренда, Rebuild — пересборка по ключу, Wake-up — не доставка),
      правило «одна коллекция — одна проекция», правила D-19 для многопоточных витрин, ограничение wipe.
- [x] Замер горячего ключа до/после `tx` записан здесь и в `PROGRESS.md`.

## Отклонения

- **Окно страниц** (решение пользователя 2026-10-24): владелец читает следующие страницы, пока квитанции прежних в
  полёте (`capacity − 1` страниц), закладка идёт по завершённому префиксу; алгоритм тикета ждал все квитанции страницы.
- **Лечение на месте.** `SequenceGap` не копится в `heal` до следующего оборота: пересборка ключа ставится сразу за
  сбойным заданием, страница ждёт её квитанцию; `failures` и backoff не трогаются. Дыра у ключа, уже сбойного в окне,
  — обычный сбой (транзиторный сбой горячего ключа не превращается в полную пересборку его истории).
- **`take_requests`** вместо `take_rebuild_keys` и флага в `extend`: один fenced-оператор раз в `poll_interval` отдаёт
  флаг Encore! с темпом и ключи; `extend` возвращает `bool`; `Claimed` несёт только начатую пересборку.
- **`CommitWakeup::new()`** создаёт приложение, один на тип агрегата (исполнитель `with_commit_wakeup`, процессоры
  `with_wakeup`), вместо `processor.wakeup()`; живёт в `cqrs/command/wakeup.rs`.
- **`ProjectionRuntime`** в ядре (`cqrs/projection/runtime.rs`) вместо `postgres::projection_runtime::prepare_projection_runtime`:
  с пулом он ничего не делал бы; `run` процессора возвращает `()` — фатальных ошибок нет.
- **`ProcessorConfig::new(poll_interval, page_size, lease, delivery)` + `with_retry`/`with_rebuild_page`/`with_concurrency`**:
  восемь позиционных параметров — `clippy::too_many_arguments`.
- **`RebuildableProjection::rebuild(id, history, prefix)`** в `cqrs/projection.rs` (контракт обработчика, не процессора) с
  границей истории: документ новее прочитанной истории отвергается, читатель перечитывает — иначе устаревший владелец
  откатывал бы документ нового (находка критики плана). `pace` реализован как `prefix = 1` плюс `handle` по событию.
- **`stop` владения** у задания пересборки: медленный Encore! заканчивается при потере аренды и остановке.
- **Ворота в тестовых двойниках** (`tests/cqrs/projection/support.rs`) вместо `ProcessorProbe` в `testing`; падение —
  `JoinHandle::abort`.
- **`tokio-util` обязателен**: `CancellationToken` — сигнал остановки процессора в ядре.
- **Диагностика**: `log_delivery_failure`/`log_delivery_panic` получили параметр операции; новые `log_projection_event`
  (info) и `log_projection_failure` (warn, «operation failed», `failures` = 0 без паузы).
- **Имя модельного теста** `property_processor_matches_a_model_under_random_faults`: CI перегоняет `property_` с сидом
  коммита.
- **`request_rebuild_key`** для незарегистрированной проекции — `Invalid` (строк-сирот нет).
- **Замер** выполнен из песочницы агента на стенде `demo/load` под CPU-квотой (cpuset не делегирован); оверлей
  «до» лежит в `02_postgres_performance/perf/compose.before-feed.yaml` + `postgres/15-before-feed.sql` и пригоден
  для повтора на машине с пиннингом ядер.
