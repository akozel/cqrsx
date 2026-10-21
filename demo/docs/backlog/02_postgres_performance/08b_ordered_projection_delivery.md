# 08b. Упорядоченная доставка событий в проекции (шаг B)

| | |
|---|---|
| Статус | предложено |
| Ценность | высокая для горячих ключей: обработчики уходят из последовательного участка, обновления проекции идут пачками; проекции применяются в порядке коммитов и не теряют события ушедших клиентов |
| Цена | средняя: новый компонент ядра, разделение `CqrsFramework` на запись и уведомление, интеграция в исполнитель и demo |
| cqrsx | `cqrs/event_handler/ordered.rs` (новый), `cqrs/event_handler.rs` (модуль), `cqrs.rs`, `cqrs/command/executor.rs`, `postgres/command.rs`, `doc/components.md` |
| demo | `back-end/src/config.rs`, `back-end/src/bootstrap.rs`, `.env.example`, `application/operations/rebuild.rs` (Encore!), остановка процесса; стенд `perf/run.sh` (`DELIVERY=`, `PROJECTION_BUFFER=`, `PROJECTION_MAX_BATCH=`) |
| Зависимости | [08a](08a_command_turns.md): без хода на ключ порядок передачи не определён; общий план — [08](08_per_aggregate_queues.md) |
| Где выигрыш | 01 (горячий ключ); корректность проекций при конкуренции |

## Что делаем

У каждого обработчика свой способ доставки. Упорядоченный обработчик получает события через буфер на ключ
агрегата, а не в задаче запроса. Команда отдаёт ход сразу после передачи событий и потом ждёт квитанцию.

```rust
// cqrs/event_handler/ordered.rs
/// How committed events reach one handler.
pub enum HandlerDelivery {
    /// In the command's task, after the commit and before its turn is released (the behaviour before 08b).
    Inline,
    /// Through a bounded buffer per aggregate key, drained in commit order by a background worker;
    /// the command releases its turn first and then awaits the handler's outcome.
    Ordered(OrderedDeliveryConfig),
}

/// Checked buffer capacity per key (commands) and the largest batch one handler call receives.
pub struct OrderedDeliveryConfig { capacity: NonZeroUsize, max_batch: NonZeroUsize }   // new(..) -> Result, Default

/// Per-key buffers and their workers in front of one handler.
pub struct OrderedDelivery<A: Aggregate> {
    handler: Arc<dyn EventHandler<A>>,
    config: OrderedDeliveryConfig,
    lanes: std::sync::Mutex<HashMap<AggregateKey, mpsc::Sender<Job<A>>>>,
}
impl<A: Aggregate> OrderedDelivery<A> {
    /// Hands one command's events over, after those handed over before; waits only for buffer space.
    pub async fn deliver(&self, key: AggregateKey, events: Vec<EventEnvelope<A>>) -> DeliveryReceipt { … }
    /// Resolves once every event handed over so far has been handled (Encore!, shutdown).
    pub async fn drained(&self) { … }
}

/// The handler's outcome for the events of one command.
pub struct DeliveryReceipt(oneshot::Receiver<Result<(), PersistenceError>>);
```

Путь команды в исполнителе:

```rust
let turn = turns.enter(key.clone()).await?;                              // 08a
let committed = retry(|| framework.decide_and_commit(…)).await?;           // загрузка, решение, запись
let inline = framework.notify(aggregate_id, &committed).await;           // обработчики самого фреймворка, как сейчас
let pending = handlers.deliver(&key, committed).await;                   // Inline — сразу; Ordered — передать по порядку
drop(turn);                                                              // следующая команда ключа может начинать
pending.outcome(inline).await                                            // квитанции → CommittedWithEventHandlerFailure
```

Изменения:
- **`CqrsFramework`** делится на `decide_and_commit` (загрузка, решение, запись; возвращает закоммиченные
  события) и `notify` (свои обработчики фреймворка). `execute_with_metadata` = оба шага: прямые пользователи
  фреймворка и `TestCommandExecutor` не меняются.
- **Обработчики исполнителя.** `PostgresCommandExecutor` хранит обработчики сам, в порядке регистрации, каждый со
  своим `HandlerDelivery`. Фреймворк на каждый вызов собирается без них. Индекс в `FailedHandler` — позиция
  регистрации, как сейчас.
  - `with_event_handler(handler)` — `Inline`.
  - `with_ordered_event_handler(handler, OrderedDeliveryConfig)` — `Ordered`. Возвращает ошибку, если ход на
    ключ выключен (`CommandOrdering::Concurrent`): без хода порядок передачи не определён.
  - Для своих хранилищ то же доступно через `CqrsCommandExecutor`. Рабочее имя набора обработчиков уточняется
    при реализации.
- **Рабочая задача ключа.**
  - Поднимается первой передачей. Берёт из канала до `max_batch` команд (`recv_many`), склеивает их события и
    один раз вызывает обработчик. `EventHandler::handle` уже принимает срез событий, поэтому пачка — это один
    `load` и один CAS в MongoDB вместо N.
  - Каждая квитанция пачки получает общий итог. Ошибка одна на пачку: квитанциям раздаётся ссылка на неё через
    обёртку с `Arc`, в лог она пишется один раз.
  - Когда канал пуст, задача удаляет свою запись под блокировкой реестра и завершается.
  - Отправитель, который успел взять канал завершившейся задачи, получает его закрытым и повторяет передачу в
    новый канал. Порядок не страдает: он всё ещё держит ход ключа.
- **Паника обработчика.** Вызов обработчика изолирован: рабочая задача ждёт его через `JoinHandle`. Квитанции
  пачки получают ошибку, буфер ключа продолжает работу.

demo:
- проекции `concert_halls` и `fan_wallets` подключаются как `Ordered`; `NotifyingHandler` оборачивает
  `ViewProjection`, как сейчас, а доставка — их обоих;
- цепочка env → config → cqrsx (таблица — в [08](08_per_aggregate_queues.md), «Параметры»):
  `ENCORE_PROJECTION_DELIVERY=inline|ordered`, `ENCORE_PROJECTION_BUFFER`, `ENCORE_PROJECTION_MAX_BATCH`
  разбираются в `config.rs` с проверкой. Значения по умолчанию — константы `DEFAULT_DELIVERY_CAPACITY` (64) и
  `DEFAULT_DELIVERY_MAX_BATCH` (32) из `cqrsx`. `ENCORE_PROJECTION_DELIVERY=ordered` при
  `ENCORE_COMMAND_ORDERING=concurrent` останавливает старт с понятной ошибкой;
- `.env.example`; стенд: `perf/run.sh DELIVERY= PROJECTION_BUFFER= PROJECTION_MAX_BATCH=`;
- **Encore!** после взятия замка ждёт `drained()` обеих проекций и только потом сбрасывает витрины;
- **остановка процесса:** после того как HTTP-сервер отпустил запросы, ждём `drained()` с ограничением по
  времени. Это доставки клиентов, которые ушли раньше.

## Пограничные случаи

| # | Случай | Ожидаемое поведение |
|---|---|---|
| 1 | Порядок | события ключа приходят в проекцию в порядке коммитов, без пропусков и повторов, в том числе пачками |
| 2 | Несколько проекций одного ключа | у каждой свой буфер, идут параллельно. Порядок *между* проекциями больше не гарантирован (сейчас — порядок регистрации); inline-обработчики по-прежнему идут по порядку под ходом |
| 3 | Клиент ушёл после коммита | события всё равно применяются (сейчас — нет) |
| 4 | Ошибка обработчика | квитанции всех команд пачки получают ошибку → `CommittedWithEventHandlerFailure` с индексом регистрации и именем; в лог — один раз |
| 5 | Паника обработчика | то же, буфер ключа работает дальше |
| 6 | Буфер ключа заполнен | `deliver` ждёт места, держа ход: команды ключа замедляются до скорости проекции, память не растёт |
| 7 | `max_batch = 1` | один вызов на команду, как inline, но вне хода |
| 8 | Encore! при ожидающих доставках | пересборка ждёт `drained()`; без этого событие могло бы потеряться или примениться дважды |
| 9 | `Ordered` без хода на ключ | ошибка конфигурации при сборке исполнителя: без хода команда N+1 может передать события раньше команды N, а восстановить порядок по номерам в буфере нельзя (события других нод туда не попадают). Пример — в [08](08_per_aggregate_queues.md), «Решения», п. 4 |
| 10 | Упорядоченный обработчик синхронно выполняет команду своего агрегата | тупик: команда ждёт квитанцию от того же обработчика. Правило в `components.md`: только через outbox |
| 11 | Несколько нод | порядок только внутри процесса; между нодами CAS `ViewProjection`, как сейчас |
| 12 | Буфер опустел | задача завершается, запись удаляется; гонка «взял канал — задача завершилась» закрыта повторной передачей |
| 13 | Остановка процесса с ожидающими доставками | ждём `drained()` с пределом; что не успело — как сейчас при падении процесса, лечится пересборкой |

## Тесты

**Unit, `cqrs/event_handler/ordered.rs`** (ручные `oneshot` и `Notify` вместо задержек):
- порядок: обработчик записывает номера событий. На многих передачах и пачках по одному ключу номера строго
  возрастают и идут без пропусков;
- пачки:
  - обработчик заблокирован на первом вызове, передаём ещё 5 команд, отпускаем — второй вызов получает события
    всех пяти по порядку;
  - при `max_batch = 2` вызовы не больше чем по 2 команды;
- независимость ключей: заблокированный обработчик ключа `a` не задерживает ключ `b`;
- обратное давление: при заблокированном обработчике `capacity + 1`-я передача ждёт, пока его не отпустят;
- отрыв от запроса: квитанция или вызывающий future уничтожены после `deliver` — обработчик всё равно вызван;
- ошибка: каждая квитанция пачки получает её; паника: квитанции получают ошибку, следующая передача работает;
- жизненный цикл: в покое реестр пуст;
- путь «канал закрыт — передать в новый» проверяется отдельно: шаг «отправить или переоткрыть» вынесен в функцию,
  в тесте канал закрывается до отправки;
- `drained()`: ждёт незавершённые доставки и сразу возвращается в покое;
- `OrderedDeliveryConfig::new` отвергает нули;
- модельный тест `proptest`: случайные передачи по нескольким ключам с блокировками обработчика; итог по
  каждому ключу совпадает с порядком передач.

**Unit, исполнитель и фреймворк** (`MemStore`):
- ход отпускается до конца упорядоченного обработчика. Обработчик первой команды заблокирован; вторая команда
  того же ключа успевает записаться. Первая возвращается только после того, как обработчик отпущен;
- смешанные `Inline` и `Ordered`: индексы в `CommittedWithEventHandlerFailure` — позиции регистрации; inline
  выполняются под ходом и в прежнем порядке;
- `CqrsFramework::execute_with_metadata` = `decide_and_commit` + `notify`: прежние тесты фреймворка проходят без
  правок;
- `with_ordered_event_handler` при `Concurrent` — ошибка;
- 20 параллельных команд к одному агрегату с проекцией в памяти, которая записывает порядок: порядок
  применения = порядок коммитов. Контроль с `Inline` при выключенном ходе показывает, что без очереди порядок
  может нарушаться (если это воспроизводится детерминированно).

**Интеграция на PostgreSQL** (проекция в `MemViewRepository` или `PostgresViewRepository`):
- 40 параллельных команд к одному агрегату, упорядоченная проекция с пачками: вид равен свёртке журнала (replay)
  после того, как все ответы получены;
- то же с ошибкой хранилища вида на одной пачке: ответы этих команд — `CommittedWithEventHandlerFailure`, у
  остальных `Ok`; журнал цел.

**demo:**
- разбор переменных;
- сборка;
- Encore! ждёт `drained()` (unit с доставкой, которую держит тест);
- E2E smoke (временный скрипт): брони при `ordered`, verify `ok` до и после пересборки, машина времени;
- пересборка во время потока броней — verify `ok` после.

**Проверки:** полный набор из корневого `CLAUDE.md`, покрытие ≥ 97 %, `--no-default-features`.

## Замер

`01-hot-key` 5 / 10 / 20 / 40 VU, профили `db` и `app`, k6, вперемешку, 3 прохода:
- 08a (`per_aggregate` + `inline`);
- 08b с пачкой 1;
- 08b с пачкой 32.

Метрики:
- принятых команд/с, p50/p99;
- CPU MongoDB и бэкенда на 1000 принятых;
- `projection_failed`;
- verify после прогона.

Контроль — `02b-spread-vus50`. Затем то же при `NODES=2` (задача [17](17_two_backend_nodes.md)): порядок
проекции между нодами не гарантирован, verify после прогона покажет, насколько это проявляется на практике.

Ожидание: потолок ключа растёт до 1 / max(ступень 1, ступень 2). Пачки снижают CPU MongoDB на горячем ключе
(один CAS на пачку вместо одного на команду). p99 может вырасти на глубине буфера.

## Критерии приёмки

- [ ] `HandlerDelivery`, `OrderedDeliveryConfig`, `OrderedDelivery`, `DeliveryReceipt` с комментариями и unit-тестами
      из списка выше, включая модельный тест.
- [ ] `CqrsFramework::decide_and_commit` и `notify`; `execute_with_metadata` через них, прежние тесты без правок.
- [ ] Исполнитель: передача под ходом, ожидание квитанций после; `with_ordered_event_handler` с проверкой хода;
      индексы регистрации в ошибках.
- [ ] Интеграционные тесты на PostgreSQL: вид = свёртка журнала; ошибка вида на пачке.
- [ ] Константы по умолчанию в `cqrsx`; demo: цепочка env → config → cqrsx, сборка, Encore! и остановка ждут
      `drained()`; E2E smoke.
- [ ] `components.md`: строка «Ordered delivery», дерево исходников, правила имён (delivery), правило о вложенных
      командах для упорядоченных обработчиков.
- [ ] Стенд: `DELIVERY=`, `PROJECTION_BUFFER=`, `PROJECTION_MAX_BATCH=`; замер против 08a (одна и две ноды)
      записан в тикет.
