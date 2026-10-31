# 08b. Упорядоченная доставка событий в проекции (шаг B)

| | |
|---|---|
| Статус | сделано; по замеру умолчание demo — `per_aggregate` + `ordered`: горячий ключ ~720–770 принятых/с без 409; в [08c](08c_aggregate_lock_only.md) режим `concurrent` удалён |
| Ценность | высокая для горячих ключей: проекции уходят из-под блокировки агрегата, обновления идут пачками; проекции применяются в порядке коммитов и не теряют события ушедших клиентов |
| Цена | средняя: новый компонент ядра, разделение `CqrsFramework` на запись и уведомление, обработчики исполнителя, интеграция в demo |
| cqrsx | `cqrs.rs` (`decide_and_commit`, `notify`), `cqrs/event_handler/delivery.rs` и `delivery/ordered.rs` (новые), `cqrs/event_handler.rs`, `cqrs/command/executor.rs`, `postgres/command.rs`, `diagnostics.rs`, `doc/components.md` |
| demo | `back-end/src/config.rs`, `bootstrap.rs` (`drain_projections`), `main.rs` (остановка), `application/operations/rebuild.rs` (Encore!), `application/lock.rs` (комментарий), `.env.example`, `README.md`; стенд `demo/load` (`run.sh` `DELIVERY=`, `PROJECTION_BUFFER=`, `PROJECTION_MAX_BATCH=`, `VERIFY=1`; `compose.yaml`; `collect.mjs`), `perf/tools/compare.mjs` |
| Зависимости | [08a](08a_aggregate_locks.md): без блокировки агрегата порядок передачи не определён; общий план — [08](08_per_aggregate_queues.md) |
| Где выигрыш | 01 (горячий ключ); корректность проекций при конкуренции |

> **Имена после [08c](08c_aggregate_lock_only.md).** «Ход» (turn) из 08a теперь называется «блокировка агрегата»
> (lock), режима `concurrent` больше нет. Поэтому `OrderedDeliveryWithoutTurns` удалён, а `with_event_handler_via`
> возвращает исполнитель, а не `Result`; `ENCORE_COMMAND_ORDERING` удалён, без переменной доставка — `ordered`.
> Код, тесты и варианты замера ниже — в именах 08b. В [08d](08d_ordered_delivery_only.md) удалены `Inline`,
> `HandlerDelivery`, `with_event_handler_via`, обработчики `CqrsFramework` и `ENCORE_PROJECTION_DELIVERY`:
> все обработчики — упорядоченные, `with_event_handler(handler, OrderedDeliveryConfig)`. После
> [08e](08e_executor_api.md): `OrderedDeliveryConfig` → `DeliveryConfig`; `OrderedDelivery`, `DeliverySlot`,
> `DeliveryReceipt` и `DeliveryError` стали внутренними, наружу — `DeliveryConfig`, `DeliveryBacklog` и константы.

## Что делаем

У каждого обработчика исполнителя свой способ доставки. Упорядоченный обработчик получает события через буфер на
ключ агрегата, а не в задаче запроса. Команда отпускает блокировку агрегата сразу после передачи событий и потом ждёт квитанцию.

```rust
// cqrs/event_handler/delivery.rs
pub const DEFAULT_DELIVERY_CAPACITY: NonZeroUsize = 64;     // commands of one key in one handler's buffer
pub const DEFAULT_DELIVERY_MAX_BATCH: NonZeroUsize = 32;    // commands per handler call
pub enum HandlerDelivery { Inline, Ordered(OrderedDeliveryConfig) }
pub struct OrderedDeliveryConfig { capacity: NonZeroUsize, max_batch: NonZeroUsize } // new(..) -> Result, Default
pub struct OrderedDeliveryWithoutTurns;                     // Ordered under CommandOrdering::Concurrent
#[derive(Clone)] pub struct DeliveryBacklog;                // len(), drained(): handed over, not handled

// cqrs/event_handler/delivery/ordered.rs
#[derive(Clone)] pub struct OrderedDelivery<A>;             // per-key buffers and workers in front of one handler
impl<A> OrderedDelivery<A> {
    pub fn new(handler: Arc<dyn EventHandler<A>>, config: OrderedDeliveryConfig) -> Self;
    pub async fn reserve(&self, aggregate_id: &str) -> DeliverySlot<A>;   // waits for space; cancel-safe
    pub fn backlog(&self) -> DeliveryBacklog;
}
impl<A> DeliverySlot<A> {
    pub fn deliver(self, events: Arc<[EventEnvelope<A>]>) -> DeliveryReceipt;   // never waits
}
impl DeliveryReceipt { pub async fn outcome(self) -> Result<(), DeliveryError> }
pub enum DeliveryError { Failed(Arc<PersistenceError>), Panicked, Stopped }

// executors (CqrsCommandExecutor and PostgresCommandExecutor)
.with_event_handler(handler)                                        // Inline, as before
.with_event_handler_via(handler, HandlerDelivery) -> Result<Self, OrderedDeliveryWithoutTurns>
.backlog() -> DeliveryBacklog                                       // every ordered handler of the executor
```

`CqrsFramework::execute_with_metadata` = `decide_and_commit` (загрузка, решение, запись; возвращает закоммиченные
события) + `notify` (обработчики самого фреймворка). Прямые пользователи фреймворка и `TestCommandExecutor` не
изменились.

## Отклонения от эскиза тикета

1. **Ящик ключа — своя структура под одним std-мьютексом, а не `tokio::mpsc` + `recv_many`.**
   - С mpsc отправитель, взявший клон `Sender`, может вставить задание после того, как рабочая задача увидела пустой
     канал. Тогда задание либо теряется вместе с `Receiver`, либо на ключ появляются две рабочие задачи, и порядок
     ломается. «Повторная передача в новый канал» из эскиза этого не закрывает: старая задача ещё дорабатывает
     вставленное.
   - Сделано: `Lane { jobs: VecDeque<Job>, worker: bool, users: usize, space: Arc<Semaphore> }`. Постановка задания
     и решение задачи завершиться идут под одной блокировкой, без `.await`. Пути «канал закрыт — передать заново»
     нет.
2. **Место резервируется до коммита.** Блокировка → резерв места → решение и запись → передача.
   - Передача синхронная и выполняется в том же опросе, в котором завершилась запись. Клиент, ушедший после
     коммита, не может её потерять.
   - Клиент, ушедший, пока ждал места, ничего не записал.
3. **Ожидание места ограничено сроком блокировки.** Блокировка и место делят один срок: `max_wait` от прихода команды. Если
   места нет к сроку — 503 до коммита, ничего не записано, `warn` `reason="buffer_full"` без id. В эскизе ожидание
   не было ограничено.
4. **Ошибка в квитанции** — `DeliveryError::{Failed(Arc<PersistenceError>), Panicked, Stopped}`.
   - Исполнителю нужны только индекс и имя (`FailedHandler`).
   - Ошибку пишет в лог рабочая задача, один раз на пачку.
   - Обёртка ошибки внутри `PersistenceError` не понадобилась.
5. **Изоляция паники** — `catch_unwind` на каждый опрос вызова обработчика вместо подзадачи с `JoinHandle`: без
   лишней задачи на пачку и без `futures` в ядре. После паники будущее больше не опрашивается.
6. **API:** `with_event_handler_via(handler, HandlerDelivery)` вместо `with_ordered_event_handler(handler, config)`.
   Явный enum-рычаг, demo передаёт значение из env без `match`. Хэндл ожидания —
   `backlog() -> DeliveryBacklog`, его берут до того, как исполнитель уходит в `CommandService`.
7. **Ключ ящика — `aggregate_id`:** одна `OrderedDelivery<A>` обслуживает один тип агрегата.
8. **Под блокировкой передача идёт раньше inline-обработчиков**, чтобы рабочая задача стартовала раньше.
   Inline-обработчики (сначала фреймворка, потом исполнителя) идут по порядку регистрации, как прежде.
9. **Параметры стенда — в `demo/load`** (как в 08a: `backend-2` наследует env через `extends`). Добавлен шаг
   `VERIFY=1`: verify после прогона, когда цепочечные задачи закончены.
10. **В замере есть вариант `concurrent` + `inline`** — текущее умолчание demo, решение об умолчании принимается
    против него.
11. **Витрины demo уже не зависят от порядка** (LWW по `sequence`, `concert_hall.rs`). Для demo выигрыш 08b — пачки и
    выход проекции из-под блокировки; порядок — гарантия cqrsx для произвольных `View`.

## Алгоритм

### Ящик ключа и реестр

```rust
struct Lane<A> {
    jobs: VecDeque<Job<A>>,  // commands handed over and not taken by the worker yet
    worker: bool,            // a worker runs for this key
    users: usize,            // commands holding a reservation and not delivered yet
    space: Arc<Semaphore>,   // `capacity` FIFO permits: reserved + queued + in the handler
}
struct Job<A> { events: Arc<[EventEnvelope<A>]>, receipt: oneshot::Sender<..>, _space: OwnedSemaphorePermit, _pending: PendingJob }
type Lanes<A> = Mutex<HashMap<String, Lane<A>>>;   // only busy keys; never held across .await
```

- **`reserve(id)`.** Под блокировкой найти или открыть `Lane`, `users += 1`; без блокировки ждать разрешение
  семафора. Брошенное ожидание отпускает `LaneRef`, и пустой ящик закрывается.
- **`deliver(events)`.** `backlog += 1` (страж в задании). Под блокировкой: задание в конец очереди; если рабочей
  задачи нет, `worker = true`. После блокировки — `tokio::spawn` рабочей задачи. Затем `users -= 1`.
- **Рабочая задача, цикл.**
  1. Под блокировкой: очередь пуста → `worker = false`; если ещё и `users == 0` — удалить ящик; выйти. Иначе снять
     до `max_batch` заданий.
  2. Без блокировки: события пачки — одним срезом (для одного задания без копии), один вызов обработчика под
     `catch_unwind`.
  3. Итог отправляется в каждую квитанцию пачки. Задание уничтожается: место возвращается в семафор, `backlog`
     уменьшается.
- **Рабочая задача уничтожена посреди работы** (остановка рантайма): её `Drop` забирает очередь и уничтожает
  задания. Квитанции отвечают `Stopped`, `backlog` уменьшается, следующая передача запустит новую задачу.

### Путь команды в исполнителе

```rust
let arrived = Instant::now();
let turn = turns.enter(key).await?;                                       // 08a
let reserved = timeout_at(arrived + max_wait, handlers.reserve(id)).await?;  // ordered ones; else 503, nothing written
let framework = build(tasks);
let committed = retry(|| framework.decide_and_commit(..)).await?;         // every conflict attempt
let receipts = reserved.deliver(&committed.into());                        // same poll as the commit
let failures = framework.notify(..) + handlers.notify_inline(..);         // inline: under the turn, in order
drop(turn);                                                                // the next command of the key starts
for receipt in receipts { receipt.outcome().await }                        // → CommittedWithEventHandlerFailure
```

Индексы в `CommittedWithEventHandlerFailure`: сначала обработчики фреймворка, затем обработчики исполнителя в порядке
регистрации. У `PostgresCommandExecutor` фреймворк без своих обработчиков, индекс — позиция регистрации.

### Инварианты

- На (обработчик, ключ) не больше одной рабочей задачи: обработчик не выполняется дважды одновременно для ключа.
- Задания ключа обрабатываются в порядке передач, каждое один раз; пачка — непрерывный отрезок очереди не длиннее
  `max_batch`.
- Ящик есть тогда и только тогда, когда есть резерв, задание в очереди или работающая задача; в покое реестр пуст.
- Резервы, задания в очереди и задания в работе на ключ не превышают `capacity`.
- `backlog` считает каждую передачу до ответа или уничтожения задания.

## Пограничные случаи

| # | Случай | Поведение | Проверка |
|---|---|---|---|
| 1 | Порядок ключа | порядок коммитов, без пропусков и повторов, в том числе пачками | `events_of_a_key_reach_the_handler_in_delivery_order_across_batches`, модель, `twenty_parallel_commands_reach_an_ordered_projection_in_commit_order`, `ordered_projection_of_forty_parallel_commands_equals_the_replay` |
| 2 | Несколько проекций ключа | у каждой свой ящик, идут параллельно; inline — под блокировкой, по порядку регистрации | `keys_are_independent_a_blocked_key_does_not_delay_another`, `mixed_inline_and_ordered_report_registration_indices_inline_under_the_turn` |
| 3 | Клиент ушёл после коммита | события применяются | `a_dropped_receipt_or_caller_does_not_cancel_the_handling`, `a_caller_dropped_right_after_the_commit_still_reaches_the_ordered_handler`, E2E |
| 4 | Ошибка обработчика | `Failed` всем квитанциям пачки → `CommittedWithEventHandlerFailure` (индекс, имя); лог один раз | `a_failure_reaches_every_receipt_of_the_batch_and_is_logged_once`, `a_view_storage_failure_fails_only_its_batch` |
| 5 | Паника обработчика | `Panicked` пачке, ящик работает дальше | `a_panic_fails_the_batch_and_the_key_buffer_keeps_working` |
| 6 | Буфер полон | резерв ждёт места под блокировкой; к сроку блокировки — 503 до коммита | `the_reservation_past_capacity_waits_until_a_job_is_handled`, `a_full_buffer_refuses_before_the_commit_within_max_wait` |
| 7 | `max_batch = 1` | вызов на команду, но вне блокировки | `batches_never_exceed_max_batch_and_one_means_a_call_per_command`, замер `ord1` |
| 8 | Encore! при ожидающих доставках | `wait_idle`, затем `drained` каждого backlog | `a_rebuild_wipes_nothing_until_the_projections_have_applied_what_was_handed_over`, E2E |
| 9 | `Ordered` без блокировки | `OrderedDeliveryWithoutTurns` при регистрации; demo не стартует (в 08c снято: блокировка есть всегда) | `ordered_without_turns_is_a_configuration_error`, `ordered_handlers_need_per_aggregate_ordering_and_share_one_backlog`, `projection_delivery_names_the_variable_it_rejects`, `ordered_projections_are_wired_with_per_aggregate_ordering_and_refused_without` |
| 10 | Упорядоченный обработчик синхронно командует своим агрегатом | вечное ожидание квитанции; правило — outbox | `components.md` |
| 11 | Несколько нод | порядок только внутри процесса; между нодами — CAS вида | замер B (verify) |
| 12 | Ящик опустел, сразу новая передача | решение о выходе и постановка — под одной блокировкой; задача на ключ одна | `a_delivery_right_after_the_worker_left_starts_a_new_one_in_order` (многопоточный) |
| 13 | Остановка с ожидающими доставками | `drain_projections` до 5 с; остаток — `warn`, лечит Encore! | `draining_waits_for_the_projections_and_reports_what_is_left_at_the_limit`, E2E |
| 14 | Рантайм уничтожил рабочую задачу | квитанции `Stopped`, `backlog` уменьшается | `a_stopped_runtime_answers_stopped_and_the_backlog_drains` |
| 15 | Клиент ушёл, ожидая места | ничего не записано, ящик закрыт | `a_dropped_reservation_reserves_nothing_and_leaves_no_buffer` |
| 16 | Команда без событий | резерв отпущен, передачи нет | `a_command_without_events_hands_nothing_over` |
| 17 | Повтор при конфликте | резерв один до попыток, передача одна после успеха | `a_conflict_retry_reserves_once_and_delivers_once` |
| 18 | Пачка и ошибка | ошибка одного вызова — у всех команд пачки (цена пачки) | `a_view_storage_failure_fails_only_its_batch` |
| 19 | Память | ящик — только у занятых ключей; заданий ≤ `capacity`; события общие (`Arc`) | `idle_registry_is_empty`-проверки во всех тестах, модель |
| 20 | Два исполнителя одного типа | свои блокировки и ящики; порядок между ними не гарантирован | doc |
| 21 | Лента активности demo | `projection.updated` — на пачку, `sequence` — последний в пачке | ревью |
| 22 | `Send` будущего | квитанции и резервы переживают `.await` | `the_command_future_with_ordered_handlers_is_send` |
| 23 | Логи | без `aggregate_id` | тесты логов выше, `delivery_diagnostics_log_one_safe_event_per_batch_with_the_storage_category` |
| 24 | `deliver` вне рантайма | паника `spawn`; исполнитель всегда в рантайме | doc |

## Тесты

- **cqrsx, unit.**
  - `cqrs.rs`: `decide_and_commit` не вызывает обработчики, `notify` идёт по порядку регистрации, пустой срез — без
    вызовов.
  - `delivery.rs`: конфигурация, `drained`.
  - `delivery/ordered.rs`: тесты из таблицы и модельный тест `ordered_delivery_matches_a_fifo_batch_model`:
    - операции `Deliver`, `Finish(ok)`, `CancelReservation`, `DropReceipt` по 3 ключам, `capacity = 3`,
      `max_batch = 2`;
    - после каждой операции сверяются пачки вызовов, итоги квитанций, ждущие резервы, `backlog`, занятые ключи.
  - `executor.rs`: тесты из таблицы.
  - `diagnostics.rs`: лог доставки.
- **cqrsx, PostgreSQL.**
  - 40 параллельных команд, проекция в `PostgresViewRepository`, первый вызов удержан до 40 коммитов. Вид = свёртка
    журнала, вызовов 3 (1, 32, 7), версия вида 3.
  - То же со сбоем записи вида на втором вызове: команды 2–33 — `CommittedWithEventHandlerFailure`, остальные `Ok`,
    журнал 1..40 цел.
- **demo.**
  - Разбор переменных, сборка, Encore! ждёт backlog, `drain_projections`.
  - `write_side.rs`: 20 параллельных броней при `per_aggregate` + `ordered` в одну попытку — все приняты, витрина
    `ConcertHallView` = свёртка журнала.
- **E2E** (`encore-e2e`, временный скрипт), `per_aggregate` + `ordered`:
  - всплески без 409;
  - verify `ok` до и после Encore! посреди всплеска (3 раунда);
  - 16 броней при приостановленной MongoDB: клиенты ушли через 1 с, все 16 записаны, после возврата MongoDB
    verify `ok`;
  - то же и сразу SIGTERM: «projections drained», verify `ok` после перезапуска.
- **CI:** полный набор из корневого `CLAUDE.md` зелёный, покрытие 97.80 % строк (`delivery/ordered.rs` 98.15 %,
  `delivery.rs` 96.03 %).

## Замер

2026-10-21. Образы собраны заранее, во время серии ничего не компилировалось:
- `encore-load-backend:08a` — код `92d5735`;
- `encore-load-backend:08b` — рабочее дерево.

Варианты (образ `08b`, кроме `reg08a`):
- `conc` — `ORDERING=concurrent`, `DELIVERY=inline`: умолчание demo до 08b;
- `turn` — `per_aggregate` + `inline`: блокировка 08a, проекция под блокировкой;
- `ord1` — `per_aggregate` + `ordered`, `PROJECTION_MAX_BATCH=1`;
- `ord32` — `per_aggregate` + `ordered`, пачки до 32;
- `reg08a`, `reg08b` — `concurrent` + `inline` на образах `08a` и `08b`: регрессия пути без блокировки.

3 прохода по 38 прогонов, порядок внутри прохода перемешан, `POOL=20`, k6, CPU под квотой, `VERIFY=1` в каждом
прогоне. Добивка — `01-hot-key-vus1`: `conc`, `turn` и `ord1` ×3, у `ord32` ещё 3 повтора к трём из серии. Префиксы результатов:
`perf-08b-{conc,turn,ord1,ord32}-{app,db}-` (серия A, одна нода), `perf-08b-{n1lb,n2rr,n2key}-{conc,ord32}-`
(серия B, профиль `app`), `perf-08b-reg08{a,b}-`. Медианы повторов, в скобках — изменение, `≠` — диапазоны
повторов не пересекаются.

```bash
cd demo/docs/backlog/02_postgres_performance/perf
node tools/compare.mjs ../../../../load/results perf-08b-conc-app- perf-08b-ord32-app- --md
```

**Витрины.** verify `ok` во всех прогонах, включая две ноды по очереди. `projection_failed` и 503 — 0 везде.

**Серия A, горячий ключ, принято/с, профиль `app`** (бэкенд 2 ядра):

| VU | concurrent | turn | ord1 | ord32 | ord32 против concurrent | ord32 против turn |
|---|---|---|---|---|---|---|
| 1 | 309 | 419 | 313 | 316 | +2 % | −25 % ≠ |
| 5 | 531 | 533 | 739 | 723 | +36 % ≠ | +36 % ≠ |
| 10 | 617 | 543 | 711 | 755 | +22 % ≠ | +39 % ≠ |
| 20 | 642 | 526 | 748 | 741 | +15 % ≠ | +41 % ≠ |
| 40 | 681 | 469 | 725 | 717 | +5 % ≠ | +53 % ≠ |

**Профиль `db`** (PostgreSQL 2 ядра):

| VU | concurrent | turn | ord1 | ord32 | ord32 против concurrent | ord32 против turn |
|---|---|---|---|---|---|---|
| 10 | 627 | 532 | 774 | 685 | +9 % | +29 % ≠ |
| 40 | 464 | 501 | 707 | 709 | +53 % ≠ | +42 % ≠ |

**Цена принятой команды, 40 VU, профиль `app`** (в скобках — против `concurrent`):

| | concurrent | turn | ord1 | ord32 |
|---|---|---|---|---|
| 409 % | 69.2 | 0 | 0 | 0 |
| p50, мс | 20.4 | 78.7 (+286 % ≠) | 54.2 (+165 % ≠) | 54.7 (+168 % ≠) |
| p99, мс | 29 | 152 (+425 % ≠) | 73.6 (+154 % ≠) | 76.6 (+164 % ≠) |
| CPU бэкенда, мс на 1000 | 2488 | 1495 (−40 % ≠) | 1283 (−48 % ≠) | 1309 (−47 % ≠) |
| CPU PG, мс на 1000 | 3965 | 2017 (−49 % ≠) | 1567 (−60 % ≠) | 1600 (−60 % ≠) |
| CPU MongoDB, мс на 1000 | 732 | 809 (+11 % ≠) | 650 (−11 % ≠) | 649 (−11 % ≠) |
| запросов PG на команду | 33 | 10.4 (−68 % ≠) | 8.44 (−74 % ≠) | 8.56 (−74 % ≠) |

В профиле `db` при 40 VU картина та же: CPU PG на команду −64 %, бэкенда −57 %, MongoDB −25 % против
`concurrent`.

**Контроль, серия A:**
- `02b-spread-vus50` (`db`): `conc`, `turn` и `ord32` дают 3675, 3656 и 3664 команд/с, диапазоны пересекаются. CPU
  на команду совпадает: бэкенд 655–670, PG 556–561, MongoDB 385–390 мс на 1000. Ящики на тысячах ключей не видны.
- Регрессия пути `concurrent`, образ `08a` против `08b`: горячий ключ при 40 VU (`app`) — 652 и 667 принятых/с,
  `02b` — 3730 и 3558 команд/с (диапазоны 3453–3801 и 3499–3805). CPU на команду ±2 %. Разделение
  `CqrsFramework` ничего не стоит.

**Серия B, горячий ключ, 40 VU, профиль `app`, nginx:**

| Топология | принято/с: concurrent → ord32 | 409 % | p50, мс | p99, мс | CPU PG, мс на 1000 | CPU бэкенда, мс на 1000 |
|---|---|---|---|---|---|---|
| 1 нода за nginx | 646 → 723 (+12 % ≠) | 69.5 → 0 | 20.8 → 54.3 | 36.4 → 73.2 | 3942 → 1561 (−60 % ≠) | 2529 → 1276 (−50 % ≠) |
| 2 ноды, по очереди | 710 → 554 (−22 % ≠) | 68.0 → 5.1 | 20.3 → 68.9 | 29.9 → 174 | 4258 → 1899 (−55 % ≠) | 2274 → 1247 (−45 % ≠) |
| 2 ноды, по ключу | 563 → 682 (+21 % ≠) | 68.8 → 0 | 24.8 → 57.5 | 34.5 → 78.5 | 4080 → 1739 (−57 % ≠) | 2075 → 1159 (−44 % ≠) |

У `ord32` на двух нодах по очереди разброс повторов большой: 497–664 принятых/с.

### Выводы

1. **Проекция вышла из-под блокировки, и потолок ключа поднялся.**
   - `ord32` против `turn`: +36…+53 % принятых/с в `app`, +29…+42 % в `db`. p50 −26…−31 %, p99 при 40 VU — вдвое
     ниже (152 → 77 мс).
   - Потолок ключа — ~720–770 принятых/с при любом числе VU от 5 до 40. Значит, блокировка теперь держится ~1.3–1.4 мс: загрузка,
     решение, запись и передача. В 08a он был ~2 мс вместе с проекцией.
   - Ожидание до прогона было 800–1000/с. Под блокировкой осталось больше, чем оценивалось.
   - Фоновая работа за окно у всех вариантов сопоставима: цепочечных задач `encore_record_*` в секунду у
     `ordered` столько же или больше, чем у `concurrent` (при 40 VU ~830 против ~680). Выигрыш куплен не
     откладыванием работы на после окна.
2. **На одной ноде `ord32` лучше `concurrent` и по пропускной способности, и по цене.**
   - Принятых/с: в `app` +5…+36 % (все ≠), в `db` +9 % при 10 VU (диапазоны пересекаются) и +53 % ≠ при 40 VU.
   - 409 → 0. CPU на принятую команду при 40 VU: PG −60 %, бэкенд −47 %, MongoDB −11 %; запросов PG 33 → 8.6.
   - Цена — задержка ожидания блокировки: p50 ≈ VU / пропускная способность (закон Литтла), при 40 VU 55 мс против 20.
     `concurrent` отвечает быстрее, потому что 70 % его ответов — быстрые 409. p99 при 5–10 VU ниже или наравне,
     при 40 VU — 77 мс против 29.
3. **Пачки на этом стенде почти не собираются.**
   - `ord1` и `ord32` неразличимы во всех точках: принятых/с и CPU MongoDB на команду совпадают.
   - Проекция (загрузка вида и CAS в MongoDB) короче времени под блокировкой у следующей команды, и рабочая задача разбирает очередь
     раньше, чем та копится. CPU MongoDB на команду ниже, чем у `turn`, у обоих вариантов, то есть дело не в пачках.
   - Пачки — запас на случай, когда проекция медленнее записи: MongoDB тормозит или стоит на паузе (E2E), вид
     тяжелее. Здесь они ничего не стоят, умолчание 32 остаётся.
4. **Одиночная команда за передачу не платит; `turn` при 1 VU быстрее всех по невыясненной причине.**
   - При 1 VU `ord1`, `ord32` и `concurrent` неразличимы: ~310 принятых/с, p50 2.5–2.7 мс. Значит, переход
     с прежнего умолчания `concurrent` на `ordered` одиночный запрос не замедляет.
   - `turn` при 1 VU даёт 419/с, p50 2.1 мс и на 40 % меньше CPU бэкенда на команду (2130 против ~3500 мс на 1000).
     При 5 VU и выше этого преимущества уже нет.
   - Передача тут ни при чём: у `concurrent` её нет, а он так же медленен. Отложенная фоновая работа — тоже нет:
     цепочечных задач в секунду за окно у всех вариантов поровну (~320/с, у `ordered` ~357/с). Лишние ~0.2 ядра
     бэкенда у `concurrent` и `ordered` при одном клиенте — открытый вопрос. Профилировать стоит, если важна
     задержка одиночного клиента.
5. **Две ноды.** Блокировки и ящики живут внутри процесса.
   - Одна нода за nginx и две ноды с привязкой по ключу: +12 % и +21 % к `concurrent`, 409 нет.
   - Две ноды по очереди: −22 % ≠ с большим разбросом. Команды горячего ключа идут через две блокировки,
     ноды конфликтуют в базе (409 5 %), повторы выполняются под блокировкой и задерживают очередь ноды. Так же было
     у блокировки в 08a: 532 против 676.
   - Горячему ключу на нескольких нодах нужна маршрутизация по ключу (`BALANCE=key`, см. [15](15_sharding.md)).
6. **Умолчание demo — `ENCORE_COMMAND_ORDERING=per_aggregate` + `ENCORE_PROJECTION_DELIVERY=ordered`** (буфер 64,
   пачка 32). Правило плана выполнено:
   - на горячем ключе `ord32` по принятым/с не хуже `concurrent` в обоих профилях при 10–40 VU;
   - на `02b` нет `≠`;
   - verify `ok` везде.

   Без переменной доставка следовала режиму: `ordered` при `per_aggregate`, `inline` при `concurrent`. В
   [08c](08c_aggregate_lock_only.md) режим `concurrent` удалён: он выигрывал только у нескольких нод, которые делят
   горячий ключ по очереди, и платил за это 409 у двух клиентов из трёх.

## Критерии приёмки

- [x] `HandlerDelivery`, `OrderedDeliveryConfig`, `OrderedDelivery`, `DeliveryReceipt` (и `DeliverySlot`,
      `DeliveryError`, `DeliveryBacklog`) с комментариями и unit-тестами, включая модельный тест.
- [x] `CqrsFramework::decide_and_commit` и `notify`; `execute_with_metadata` через них, прежние тесты без правок.
- [x] Исполнитель: передача под блокировкой, ожидание квитанций после; `with_event_handler_via` с проверкой блокировки;
      индексы регистрации в ошибках.
- [x] Интеграционные тесты на PostgreSQL: вид = свёртка журнала; ошибка вида на пачке.
- [x] Константы по умолчанию в `cqrsx`; demo: цепочка env → config → cqrsx, сборка, Encore! и остановка ждут
      backlog; E2E smoke.
- [x] `components.md`: строка «Ordered delivery», дерево исходников, правила имён (delivery), правило о вложенных
      командах для упорядоченных обработчиков.
- [x] Стенд: `DELIVERY=`, `PROJECTION_BUFFER=`, `PROJECTION_MAX_BATCH=`, `VERIFY=1`; замер против 08a (одна и две
      ноды) записан в тикет.
- [x] Умолчание demo по правилу плана: `per_aggregate` + `ordered`; без переменной доставка следовала режиму (до 08c).
