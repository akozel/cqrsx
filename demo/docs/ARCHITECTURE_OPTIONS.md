# Структура `encore-backend`: анализ и варианты (Clean Architecture на `cqrsx`)

Статус: **реализовано** 2026-10-17 в ветке `refactor/clean-architecture` (вариант B с уточнениями §11);
итоговая таблица модулей — `PLAN.md` §6.2, журнал — `docs/PROGRESS.md`. Ниже — исходный анализ. Цель документа — выбрать
универсальную структуру каталогов, которую потом переиспользуем на реальных проектах на `cqrsx`.

## 0. Коротко

- Сейчас модули названы по техническим ролям (`app`, `http`, `outbox`, `projections`), слои
  перемешаны внутри каждого, бизнес-модель (концерт → билет → письмо) из дерева не читается.
- Рекомендация — **вариант B**: четыре слоя на верхнем уровне (`domain/`, `application/`,
  `presentation/`, `infrastructure/`); внутри `domain/` и `application/` — срезы по агрегату
  (`concert/`, `fan/`); внутри `presentation/` и `infrastructure/` — по технологии и ресурсу.
- Единица бизнес-логики: файл на команду в `domain/{aggregate}/commands/` (правила + тесты правил)
  и файл на use case в `application/{aggregate}/` (метаданные, outbox-задачи, исход). Листинг
  `application/` читается как сценарий демо: `book_seat` → `record_ticket` → `send_e_ticket`.
- Вариант C (контекст сверху, слои внутри) подходит проектам с многими контекстами. Переход
  B → C механический: `{layer}/{context}` ↔ `{context}/{layer}`, поэтому начинать можно с B.

## 1. Критерии (в порядке приоритета, как сформулировано)

1. Наглядность: из дерева каталогов видно, чем занимается приложение и какие у него use cases.
2. Удобство: «один use case — один файл», предсказуемое место для каждого типа кода.
3. Универсальность: та же схема для 1 и для 10 контекстов, с возможностью вынести контекст в крейт.
4. Чистота зависимостей (domain ← application ← presentation/infrastructure), но правила можно
   осознанно нарушать ради п. 1–3; каждое нарушение названо.
5. Соответствие контрактам `cqrsx` без борьбы с фреймворком.

## 2. Как устроено сейчас

| Модуль | Строк | Что внутри | Слои, которые смешаны |
| --- | ---: | --- | --- |
| `domain/` | 1 214 (466 тестов) | `Concert`, `Fan`, `ids`, `venue` | почти чистый домен |
| `app/` | 1 573 | `command_service` 625, `rebuild` 332, `tasks` 138, `activity` 137, `verify` 112, `time_travel` 99, `lock` 54, `read_side` 43, `personas` 33 | application + infrastructure (типы `cqrsx::postgres`, `PgPool`, raw SQL, Mongo-коллекции) |
| `http/` | 813 (255 тестов) | роутер, DTO команд, запросы, админ-эндпоинты, SSE | presentation + infrastructure (BSON → JSON) |
| `outbox/` | 820 | `flaky_mail` 285, `chain` 170, `chaos` 78, `dispatcher` 71, 4 обработчика ≈ 175, `adapter` 42 | use cases + infrastructure (почта, хаос, Mongo inbox) + wiring (adapter, dispatcher) |
| `projections/` | 699 | `concert_hall` 293, `fan_wallet` 188, `mongo_view_repository` 130, `notifying` 46, `progress` 42 | application (views, декораторы) + infrastructure (Mongo) |
| корень | 369 | `config` 175, `main` 157, `lib` 13, `testing` 24 | composition root |

Наблюдения (с точным местом):

1. **Направление зависимостей нарушено в application.** `app/command_service.rs` строит
   `PostgresEventRepositoryWithOutbox`, `PostgresEventRepository`, держит `PgPool`;
   `app/rebuild.rs` выполняет `SELECT count(*) FROM events`; `app/read_side.rs` — набор
   `Arc<MongoViewRepository>` и `Collection<Document>`; `http/queries.rs` конвертирует BSON.
2. **Циклы между модулями одного уровня.** `app::command_service` → `projections::notifying` →
   `app::activity`; `app::tasks` → `app::command_service::CommandMetadata` → `app::tasks`.
   Это признак того, что `app` и `projections` — один слой, разрезанный по техническому признаку.
3. **Use case размазан по четырём местам.** Для `BookSeat`: правила R1, R3–R6 — в
   `domain/concert.rs::handle` (один `match` на 120 строк на три команды); outbox-задача — в
   `app/command_service.rs::tasks_for`; DTO и нормализация — в `http/commands.rs`; продолжение
   цепочки — `outbox/record_ticket.rs` → `outbox/chain.rs` → `outbox/send_e_ticket.rs`.
4. **Имена первого уровня технические.** `concert` и `fan` появляются только внутри `domain/`.
   Названия `outbox` и `projections` описывают механизм доставки, а не то, что приложение делает.
5. **Домен не совсем чист.** `ConcertError::params()` и `FanError::params()` возвращают
   `serde_json::Value` (форма API); `ConcertCommand::name()` / `ConcertEvent::name()` дублируют
   `event_type()`; тесты правил — один файл на агрегат (326 и 140 строк), а не рядом с правилом.
6. **Composition root дублируется.** `main.rs` (157 строк) и `http/tests.rs::app()` собирают один и
   тот же граф вручную; при добавлении зависимости правятся оба.
7. **Что уже хорошо и должно сохраниться.** `Inbox` и `ChaosPolicy` — настоящие порты с тестовыми
   реализациями; `NamedHandler` жёстко связывает индекс обработчика с именем проекции;
   `outbox/chain.rs` выделяет общий шаг цепочки; LWW-проекции с тестами порядка и идемпотентности;
   `domain/ids.rs` валидирует идентификаторы на границе через serde.

## 3. Что диктует `cqrsx`

Фреймворк — наш, поэтому его трейты считаем частью ядра архитектуры, а не инфраструктурой.
Инфраструктура — только `cqrsx::postgres`, `cqrsx::task::{adapter, dispatcher, runtime}`,
`cqrsx::host`, `cqrsx::logging`.

1. `Aggregate` = один enum `Command`, один enum `Event`, один `handle`, один `apply`. «Обработчик
   на команду» — это делегирование из `handle` в функцию модуля команды; enum становится тонкой
   обёрткой: `ConcertCommand::BookSeat(BookSeat)`.
2. Outbox допускает **не более одного события на команду**, и задачи передаются в
   `execute_with_metadata(id, command, metadata, tasks)` **до** вызова `handle`. Следствие:
   политика «какую задачу публикуем» — функция команды (и для отмены — прочитанного состояния),
   а не события. Это код application-слоя, по одному на use case.
3. `CommandExecutor`, `EventStore`, `PersistedEventRepository`, `ViewRepository`, `TaskHandler`,
   `TaskAdapter` — native `async fn` (`impl Future`), то есть **не object-safe**. Абстрагировать
   инфраструктуру в application можно только generics'ами или алиасами на конкретные типы.
   `EventHandler` — `#[async_trait]`, `Box<dyn EventHandler<A>>` работает (и используется).
4. `TaskDispatcher<A>` принимает один тип адаптера, поэтому enum `EncoreTaskAdapter` неизбежен —
   это чистый wiring, ему место в infrastructure. *(Позже снято: `cqrsx` получил `BoxedTaskAdapter`,
   enum удалён, см. §11.3.)*
5. `TaskHandler<P>` — одна операция на формат задачи. Обработчики задач — use cases
   application-слоя, которые запускает рантайм, а не HTTP.
6. `TestFramework<A>` (given/when/then) работает на уровне агрегата, поэтому тесты правил можно
   класть в модуль команды.
7. `View<A>` привязан к типу агрегата: read model строится по одному агрегату.
8. Serde: enum с newtype-вариантами (`SeatBooked(SeatBooked)`) и `#[serde(tag = "type")]` даёт
   **тот же JSON**, что сейчас у struct-вариантов. Хранимые события и X-ray остаются совместимы.

Правила импортов `cqrsx` по слоям (проверяются grep'ом, см. D11):

| Слой | Разрешено из `cqrsx` | Запрещено |
| --- | --- | --- |
| `domain` | `cqrs::{aggregate, event, event_sink}`; в тестах `cqrs::testing` | всё остальное, а также `sqlx`, `mongodb`, `axum` |
| `application` | `cqrs::{command, event_handler, projection, store::{EventStore, PersistenceError}}`, `task::{NewTask, format, handler, attempt}` | `cqrsx::postgres`, `task::{adapter, dispatcher, runtime}`, `sqlx`, `mongodb`, `axum` |
| `presentation` | ничего (исходы приходят типами application) | любые типы `cqrsx`, `sqlx`, `mongodb` |
| `infrastructure` | всё | — |

## 4. Варианты

### Вариант A — переименование по ролям (минимум)

```text
src/
├── domain/                 без изменений
├── application/            ← app/ + projections/{concert_hall,fan_wallet,notifying,progress}
│                             + outbox/{chain,record_ticket,record_cancellation,send_e_ticket,issue_refund}
├── presentation/http/      ← http/
└── infrastructure/         ← projections/mongo_view_repository, outbox/{flaky_mail,chaos,adapter,dispatcher}
```

Плюсы: только `git mv` и правка `use`, риск ≈ 0. Минусы: бизнес-модель всё ещё не видна, use cases
не выделены, циклы и утечки инфраструктуры в application остаются. Годится только как первый шаг B.

### Вариант B — слои сверху, бизнес-срезы внутри (рекомендуется)

```text
src/
├── main.rs                          бинарник: env → Config → bootstrap → host::lifecycle::run
├── lib.rs
├── bootstrap.rs                     composition root: Config + соединения → AppState; общий для main и тестов
├── config.rs                        ENCORE_* / OUTBOX_*
│
├── domain.rs                        shared kernel: re-exports ids, venue
├── domain/
│   ├── ids.rs                       ConcertId, FanId
│   ├── venue.rs                     Venue, SeatId, Zone (справочные данные)
│   ├── concert.rs                   pub mod aggregate/commands/events/error + pub use
│   ├── concert/
│   │   ├── aggregate.rs             Concert (состояние), ConcertServices, impl Aggregate: handle = диспетчер, apply = state machine
│   │   ├── commands.rs              enum ConcertCommand { AnnounceConcert(AnnounceConcert), BookSeat(BookSeat), CancelBooking(CancelBooking) }
│   │   ├── commands/
│   │   │   ├── announce_concert.rs  struct + decide(&Concert, &Services) -> Result<ConcertEvent, ConcertError>; тесты R2
│   │   │   ├── book_seat.rs         R1, R3, R4, R5, R6; тесты
│   │   │   └── cancel_booking.rs    R1, R3, R7, R8; тесты
│   │   ├── events.rs                enum ConcertEvent { ConcertAnnounced(..), SeatBooked(..), BookingCancelled(..) }; impl DomainEvent
│   │   ├── events/
│   │   │   ├── concert_announced.rs факт: поля + док «кто порождает / кто потребляет»
│   │   │   ├── seat_booked.rs
│   │   │   └── booking_cancelled.rs
│   │   └── error.rs                 ConcertError R1–R8, code()
│   ├── fan.rs
│   └── fan/
│       ├── aggregate.rs             Fan, Ticket, TicketState
│       ├── commands.rs, commands/{record_ticket.rs, record_cancellation.rs}
│       ├── events.rs,   events/{ticket_recorded.rs, ticket_cancelled.rs}
│       └── error.rs                 FanError R9–R11
│
├── application.rs                   pub mod …; StorageError
├── application/
│   ├── command_service.rs           CommandService<A, E: CommandExecutor<A>>: lock → execute_with_metadata → outcome → activity;
│   │                                CommandMetadata, CommandOutcome, Executed, Warning, NamedHandler
│   ├── tasks.rs                     new_task(payload, metadata, …): общий конструктор NewTask
│   ├── ports.rs                     Mailer (+ Mail), опционально ReadModelStore<V>, EventLedgerStats
│   ├── activity.rs                  ActivityHub
│   ├── lock.rs                      BoxOfficeLock
│   ├── concert.rs                   ConcertApp<E, S> = CommandService<Concert, E> + reader S: EventStore<Concert>
│   ├── concert/
│   │   ├── announce_concert.rs      impl ConcertApp { pub async fn announce_concert(..) }   задач нет
│   │   ├── book_seat.rs             impl ConcertApp { pub async fn book_seat(..) }          публикует tasks::RecordTicketV1
│   │   ├── cancel_booking.rs        impl ConcertApp { pub async fn cancel_booking(..) }     читает committed booking → RecordCancellationV1
│   │   ├── tasks.rs                 RecordTicketV1, RecordCancellationV1 — контракты, которые публикует Concert
│   │   └── views/concert_hall.rs    ConcertHallView: View<Concert>, LWW
│   ├── fan.rs                       FanApp<E> = CommandService<Fan, E>
│   ├── fan/
│   │   ├── record_ticket.rs         TaskHandler<RecordTicketV1>: Fan.RecordTicket + follow-up SendETicketV1
│   │   ├── record_cancellation.rs   TaskHandler<RecordCancellationV1>: Fan.RecordCancellation + IssueRefundV1
│   │   ├── send_e_ticket.rs         TaskHandler<SendETicketV1> через порт Mailer
│   │   ├── issue_refund.rs          TaskHandler<IssueRefundV1> через порт Mailer
│   │   ├── chain.rs                 общий шаг цепочки: metadata из задачи, ожидание lock, FanError → результат задачи
│   │   ├── tasks.rs                 SendETicketV1, IssueRefundV1 — контракты, которые публикует Fan
│   │   └── views/fan_wallet.rs      FanWalletView: View<Fan>
│   ├── projections/
│   │   ├── notifying.rs             NotifyingHandler: EventHandler-декоратор → activity
│   │   └── progress.rs              ProgressHandler: прогресс и slow-motion при replay
│   └── operations/                  сценарии над обоими агрегатами
│       ├── rebuild.rs               RebuildService (Encore!)
│       ├── verify.rs                VerifyService
│       └── time_travel.rs           TimeTravel
│
├── presentation.rs
├── presentation/
│   ├── http.rs                      router(), TraceLayer, parse_concert_id / parse_fan_id
│   ├── http/
│   │   ├── error.rs                 ApiError { outcome, error: { code, params, message } }
│   │   ├── health.rs                GET /api/health
│   │   ├── config.rs                GET /api/config (+ personas — это UI-справочник)
│   │   ├── concerts/
│   │   │   ├── execute_command.rs   POST /api/concerts/{id}/commands: CommandDto → use case → ответ
│   │   │   ├── list.rs              GET /api/concerts
│   │   │   ├── get.rs               GET /api/concerts/{id}
│   │   │   ├── timeline.rs          GET /api/concerts/{id}/timeline
│   │   │   └── state_at.rs          GET /api/concerts/{id}/state-at/{n}
│   │   ├── fans/{wallet.rs, inbox.rs}
│   │   ├── projections/{wipe.rs, rebuild.rs, status.rs, verify.rs}
│   │   ├── activity/stream.rs       GET /api/activity (SSE)
│   │   └── tests.rs
│
├── infrastructure.rs
└── infrastructure/
    ├── postgres.rs                  алиасы OutboxStore<A>, HistoryStore<A>, PostgresExecutor<A>; connect; EventLedgerStats (count-запросы)
    ├── mongo.rs
    ├── mongo/
    │   ├── view_repository.rs       MongoViewRepository<V, A>: ViewRepository (+ ReadModelStore)
    │   ├── read_side.rs             имена коллекций, ReadSide (halls, wallets, inbox)
    │   └── inbox.rs                 MongoInbox: Inbox
    ├── mail.rs
    ├── mail/
    │   ├── flaky_mail_server.rs     DEMO ONLY: FlakyMailServer<I, C>: Mailer
    │   └── chaos.rs                 DEMO ONLY: ChaosPolicy, Never / Always / Random
    ├── tasks.rs
    └── tasks/
        ├── adapter.rs               EncoreTaskAdapter (enum над JsonTaskAdapter'ами)
        └── dispatcher.rs            dispatcher_factory → TaskDispatcherFactory
```

Файлов станет около 80 вместо 50, но каждый — об одном. Три эскиза, как это выглядит в коде:

```rust
// domain/concert/commands/book_seat.rs
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookSeat {
    /// Raw seat text from the UI; rule R3 validates it against the venue.
    pub seat: String,
    pub fan_id: FanId,
    pub fan_name: String,
    pub booking_id: Uuid,
}

impl BookSeat {
    /// R1, R3, R4, R5, R6 → exactly one `SeatBooked` (decision D-02 is now a signature).
    pub(in crate::domain::concert) fn decide(
        self,
        concert: &Concert,
        services: &ConcertServices,
    ) -> Result<ConcertEvent, ConcertError> {
        concert.info().ok_or(ConcertError::ConcertNotFound)?; // R1
        let seat = VENUE.seat(&self.seat).ok_or(ConcertError::UnknownSeat { seat: self.seat.clone() })?; // R3
        if concert.is_sold_out() {
            return Err(ConcertError::SoldOut); // R4
        }
        if let Some(booking) = concert.bookings().get(&seat.id) {
            return Err(ConcertError::SeatTaken { seat: seat.id, by: booking.fan_name.clone() }); // R5
        }
        if concert.tickets_of(&self.fan_id) >= services.ticket_limit {
            return Err(ConcertError::TicketLimit { limit: services.ticket_limit }); // R6
        }
        Ok(ConcertEvent::SeatBooked(SeatBooked { seat: seat.id, zone: seat.zone, price: seat.price,
            fan_id: self.fan_id, fan_name: self.fan_name, booking_id: self.booking_id }))
    }
}

#[cfg(test)]
mod tests { /* TestFramework given/when/then: happy path, r1_…, r3_…, r4_…, r5_…, r6_… */ }
```

```rust
// domain/concert/aggregate.rs
impl Aggregate for Concert {
    const TYPE: &'static str = "Concert";
    type Command = ConcertCommand;
    type Event = ConcertEvent;
    type Error = ConcertError;
    type Services = ConcertServices;

    async fn handle(&mut self, command: ConcertCommand, services: &ConcertServices, sink: &EventSink<Self>) -> Result<(), ConcertError> {
        let event = match command {
            ConcertCommand::AnnounceConcert(c) => c.decide(self, services)?,
            ConcertCommand::BookSeat(c) => c.decide(self, services)?,
            ConcertCommand::CancelBooking(c) => c.decide(self, services)?,
        };
        sink.write(event, self).await;
        Ok(())
    }

    fn apply(&mut self, event: ConcertEvent) {
        match event { /* the whole state machine stays in one place */ }
    }
}
```

```rust
// application/concert/book_seat.rs
impl<E: CommandExecutor<Concert>, S: EventStore<Concert>> ConcertApp<E, S> {
    /// UI use case. The booking id is the command id; the accepted event is published
    /// atomically with `RecordTicketV1`, the first link of the fan-ledger chain.
    pub async fn book_seat(&self, concert_id: &ConcertId, command: BookSeat, metadata: CommandMetadata) -> Executed<ConcertError> {
        let tasks = match VENUE.seat(&command.seat) {
            Some(seat) => vec![tasks::record_ticket(concert_id, &command, &seat, &metadata, self.task_max_attempts)?],
            None => vec![], // the aggregate rejects the command before anything is written
        };
        self.run(concert_id, ConcertCommand::BookSeat(command), tasks, metadata).await
    }
}
```

Плюсы B:

- Четыре слоя там, где их ожидают увидеть; внутри `domain/` и `application/` каталоги повторяют
  бизнес-модель; use cases перечислены именами файлов.
- Правило «куда класть» однозначно: правило бизнеса → `domain/{agg}/commands/{cmd}.rs`;
  оркестрация и задачи → `application/{agg}/{use_case}.rs`; endpoint → `presentation/http/{resource}/{action}.rs`;
  драйвер/хранилище → `infrastructure/{tech}/`.
- Тесты правил живут рядом с правилом; D-02 закодирован сигнатурой `decide`.
- Wire-совместимо: JSON событий, формат задач, HTTP API не меняются.
- Переход к C (контекст = крейт) механический.

Минусы B:

- Один use case по-прежнему проходит через три слоя (команда → use case → endpoint); для
  маленьких команд (`AnnounceConcert`) файл в application почти пуст. Это цена единообразия.
- Generics у сервисов (`ConcertApp<E, S>`) добавляют шума в сигнатурах; он гасится алиасами в
  `bootstrap.rs` (см. D7).

### Вариант C — вертикальные срезы (контекст сверху, слои внутри)

```text
src/
├── main.rs, lib.rs, bootstrap.rs, config.rs
├── shared/                          kernel: ids, venue, CommandMetadata/Outcome, ActivityHub, lock, ApiError
├── concert/                         контекст «касса»
│   ├── domain/{aggregate.rs, commands.rs, commands/, events.rs, events/, error.rs}
│   ├── application/{announce_concert.rs, book_seat.rs, cancel_booking.rs, tasks.rs, views/concert_hall.rs}
│   └── presentation/http/{execute_command.rs, list.rs, get.rs, timeline.rs, state_at.rs}
├── fan/                             контекст «кошелёк фана»
│   ├── domain/…
│   ├── application/{record_ticket.rs, record_cancellation.rs, send_e_ticket.rs, issue_refund.rs, chain.rs, tasks.rs, views/fan_wallet.rs}
│   ├── presentation/http/{wallet.rs, inbox.rs}
│   └── infrastructure/mail/{flaky_mail_server.rs, chaos.rs, inbox.rs}
├── operations/                      сквозные сценарии: rebuild, verify, time_travel + их HTTP + SSE
└── platform/                        postgres-алиасы, Mongo view repository, dispatcher/adapter, роутер
```

Плюсы: максимальная связность по фиче, естественный путь «контекст → крейт», privacy
через `pub(crate)` на уровне среза. Минусы: четырёх слоёв на верхнем уровне нет; для двух тесно
связанных контекстов большая доля кода уезжает в `shared/`, `operations/`, `platform/` (роутер,
диспетчер, rebuild — всё сквозное), и история «срезов» размывается; слои дублируются в каждом
контексте. Для демо с двумя агрегатами это тяжелее, чем B; для 5+ контекстов — лучше B.

## 5. Сравнение

| Критерий | A | B | C |
| --- | --- | --- | --- |
| Бизнес-модель видна из дерева | нет | да: `domain/{agg}/commands/`, `application/{agg}/` | да: `{context}/` |
| Четыре слоя на верхнем уровне | да | да | нет, внутри контекста |
| «Один use case — один файл» | нет | да | да |
| Универсальность (N контекстов, крейты) | низкая | средняя, B → C механически | высокая |
| Доля сквозного кода (`shared`/`platform`) | — | низкая | высокая при двух связанных контекстах |
| Чистота зависимостей | как сейчас | порты + generics, исключения названы | то же |
| Объём изменений | `git mv` + `use` | 7 шагов, каждый компилируется отдельно | как B плюс больше перемещений |
| Риск регрессии | ≈ 0 | низкий: wire-формат и API не меняются | тот же |

## 6. Решения внутри варианта B (с рекомендацией по умолчанию)

- **D1. Что такое `{name}`.** Агрегат. Bounded context = крейт (а при нескольких контекстах в
  одном крейте — родительская папка `domain/{context}/{aggregate}/`). Shared kernel — корень
  `domain/` (`ids`, `venue`). В Encore каждый контекст содержит один агрегат, поэтому уровень
  контекста опущен.
- **D2. Где `decide` и где `apply`.** `decide` — в модуле команды (правила рядом с тестами).
  `apply` — в `aggregate.rs`: весь автомат состояний читается одним `match`. Альтернатива —
  `SeatBooked::apply(self, &mut Concert)` в модуле события, тогда факт и его смысл для состояния
  лежат вместе, но автомат рассыпается по файлам. Модули событий в любом случае нужны: это каталог
  фактов с документацией «кто порождает, кто потребляет» и место для версий/upcaster'ов.
- **D3. Use cases как методы.** `impl ConcertApp` в каждом файле use case'а; общий `run`
  (lock → `execute_with_metadata` → outcome → activity) — в `command_service.rs`. Без трейта
  `UseCase` и без generic-диспетчеризации: список публичных методов = список файлов.
- **D4. Контракты задач — у публикующей стороны.** `application/concert/tasks.rs` держит
  `RecordTicketV1`, `RecordCancellationV1`; `application/fan/tasks.rs` — `SendETicketV1`,
  `IssueRefundV1`. Потребитель импортирует у публикатора. Альтернатива для маленьких проектов —
  один `application/tasks.rs` со всеми payload'ами.
- **D5. Три вида входов.** HTTP → `presentation/http`; обработчики задач (`TaskHandler`) →
  `application/{agg}/`; адаптер и фабрика диспетчера → `infrastructure/tasks` (это связывание
  рантайма с обработчиками, а не публичный интерфейс). Альтернатива — `presentation/tasks/` по
  аналогии с HTTP как «вторая точка входа».
- **D6. Порты.** Обязателен `Mailer` (иначе `send_e_ticket`/`issue_refund` зависят от
  `FlakyMailServer`). Желательны `ReadModelStore<V>` (типизированное чтение read model вместо
  BSON в HTTP) и `EventLedgerStats` (count-запросы rebuild). `ActivityHub`, `BoxOfficeLock`,
  `ReadSide` остаются конкретными типами. Правило: порт — там, где есть вторая реализация или она
  нужна тестам.
- **D7. Generics vs алиасы.** Сервисы параметризованы трейтами `cqrsx` (`E: CommandExecutor<A>`,
  `S: EventStore<A>`); конкретные типы и `AppState` собираются в `bootstrap.rs`. Выигрыш: use
  cases тестируются на `MemStore` без PostgreSQL (сейчас успешные ветки покрывает только один
  интеграционный тест на testcontainers). Прагматичная альтернатива — алиасы из
  `infrastructure::postgres` прямо в application (одна строка `use`, но направление нарушено).
- **D8. HTTP API не меняется.** Один `execute_command.rs` с `CommandDto` (enum по `type`), как
  требует фронт. Разбиение на REST-эндпоинты по командам — отдельное решение с правкой фронта.
- **D9. Тесты.** Правила — в модулях команд; D-02-инвариант — в `aggregate.rs`; use cases — на
  `MemStore`; `tests/write_side.rs` — как есть; роутер — `presentation/http/tests.rs` через
  `bootstrap` (убирает дублирование сборки).
- **D10. Чистка домена.** `params()` уходит из `ConcertError`/`FanError` в application
  (`describe(&E) -> (code, params)`), `name()` заменяется `event_type()`/одним источником.
  `code()` остаётся: это словарь домена, а не форма API.
- **D11. Архитектурный тест.** `tests/architecture.rs` читает `src/**` и проверяет таблицу
  импортов из §3 (`domain` не импортирует `application|presentation|infrastructure|sqlx|mongodb|axum`,
  `application` не импортирует `presentation|infrastructure|cqrsx::postgres|sqlx|mongodb|axum`, и т. д.).
  Дёшево, ловит дрейф, не требует разбиения на крейты. Разбиение на крейты — когда появится второй
  контекст.

## 7. Карта переноса (B)

| Сейчас | Станет |
| --- | --- |
| `main.rs` | `main.rs` (тонкий) + `bootstrap.rs` |
| `config.rs`, `lib.rs` | без изменений |
| `testing.rs` | `testing.rs` → постепенно заменяется фикстурами на `MemStore` |
| `domain.rs`, `domain/ids.rs`, `domain/venue.rs` | без изменений |
| `domain/concert.rs` | `domain/concert.rs` (список модулей) + `domain/concert/aggregate.rs` |
| `domain/concert/command.rs` | `domain/concert/commands.rs` + `commands/{announce_concert,book_seat,cancel_booking}.rs` |
| `domain/concert/event.rs` | `domain/concert/events.rs` + `events/{concert_announced,seat_booked,booking_cancelled}.rs` |
| `domain/concert/error.rs` | `domain/concert/error.rs` без `params()` |
| `domain/concert/tests.rs` | растворяется по `commands/*.rs`; D-02 тест → `aggregate.rs` |
| `domain/fan.rs`, `domain/fan/*` | по тому же образцу |
| `app.rs` | `application.rs` |
| `app/command_service.rs` | `application/command_service.rs` (generic сервис, metadata, outcome, `NamedHandler`) + `application/concert.rs` + `application/concert/{announce_concert,book_seat,cancel_booking}.rs` (бывший `tasks_for`) + `application/fan.rs` |
| `app/tasks.rs` | `application/tasks.rs` (`new_task`) + `application/concert/tasks.rs` + `application/fan/tasks.rs` |
| `app/activity.rs`, `app/lock.rs` | `application/activity.rs`, `application/lock.rs` |
| `app/personas.rs` | `presentation/http/config.rs` |
| `app/read_side.rs` | `infrastructure/mongo/read_side.rs` |
| `app/rebuild.rs` | `application/operations/rebuild.rs`; count-запросы → `infrastructure/postgres.rs` |
| `app/verify.rs`, `app/time_travel.rs` | `application/operations/{verify,time_travel}.rs` |
| `http.rs` | `presentation/http.rs`; `AppState` → `bootstrap.rs` |
| `http/commands.rs` | `presentation/http/concerts/execute_command.rs` |
| `http/queries.rs` | `presentation/http/{health,config}.rs`, `concerts/{list,get}.rs`, `fans/{wallet,inbox}.rs` |
| `http/projections.rs` | `presentation/http/projections/{wipe,rebuild,status,verify}.rs`, `concerts/{timeline,state_at}.rs` |
| `http/activity.rs` | `presentation/http/activity/stream.rs` |
| `http/error.rs`, `http/tests.rs` | `presentation/http/error.rs`, `presentation/http/tests.rs` |
| `outbox/chain.rs` | `application/fan/chain.rs` |
| `outbox/record_ticket.rs`, `outbox/record_cancellation.rs` | `application/fan/record_ticket.rs`, `application/fan/record_cancellation.rs` |
| `outbox/send_e_ticket.rs`, `outbox/issue_refund.rs` | `application/fan/send_e_ticket.rs`, `application/fan/issue_refund.rs` (через `Mailer`) |
| `outbox/flaky_mail.rs` | `infrastructure/mail/flaky_mail_server.rs` + `infrastructure/mongo/inbox.rs` (`Inbox`, `MongoInbox`); `Mail` → `application/ports.rs` |
| `outbox/chaos.rs` | `infrastructure/mail/chaos.rs` |
| `outbox/adapter.rs`, `outbox/dispatcher.rs` | `infrastructure/tasks/{adapter,dispatcher}.rs` |
| `projections/concert_hall.rs`, `projections/fan_wallet.rs` | `application/concert/views/concert_hall.rs`, `application/fan/views/fan_wallet.rs` |
| `projections/mongo_view_repository.rs` | `infrastructure/mongo/view_repository.rs` |
| `projections/notifying.rs`, `projections/progress.rs` | `application/projections/{notifying,progress}.rs` |
| `tests/write_side.rs` | без изменений, кроме путей импорта |

## 8. Порядок работ (каждый шаг компилируется и проходит тесты)

1. Ветка `refactor/clean-architecture`. Зафиксировать baseline: `cargo build`, `cargo test`,
   `cargo clippy -- -D warnings`, `cargo fmt --check` из `demo/`.
2. **Чистые перемещения (= вариант A).** `git mv` по карте §7 без изменения кода, только `use`.
3. **`bootstrap.rs`.** Вынести сборку графа из `main.rs`; `http/tests.rs::app()` переходит на него.
4. **Домен.** `commands/` и `events/` с `decide`, newtype-варианты enum'ов, перенос тестов правил
   в модули команд. Проверка wire-совместимости: тест сериализации каждого события против
   текущего JSON (`{"type":"SeatBooked","seat":"B4",…}`).
5. **Application.** `CommandService<A, E>`, `ConcertApp`/`FanApp`, use case на файл, `tasks.rs` у
   публикатора, `operations/`.
6. **Порты и generics.** `Mailer`; `ConcertApp<E, S>`; алиасы в `bootstrap.rs`; тесты use cases
   на `MemStore`. При желании — `ReadModelStore` и `EventLedgerStats`.
7. **Presentation.** Файл на endpoint, `AppState` в `bootstrap.rs`, `personas` в `config.rs`.
8. **Фиксация.** `tests/architecture.rs` (D11), обновить `PLAN.md` §6.2 и `README.md` демо,
   `cqrsx/doc/components.md` не трогаем.

Проверка возможна только там, где есть сеть или заполненный cargo-кэш: в этой песочнице
`cargo metadata --offline` падает на загрузке `combine v4.6.9`, поэтому шаги 2–8 нужно
прогонять на рабочей машине.

## 9. Что мог бы дать `cqrsx`, чтобы структура стала ещё чище (на будущее, не для этого рефакторинга)

1. **Политика задач от события.** Сейчас задачи строятся до `handle` из команды, из-за чего
   `cancel_booking` читает агрегат дважды. `TaskPolicy<A>: Fn(&EventEnvelope<A>) -> Vec<NewTask>`,
   вычисляемая фреймворком внутри транзакции, позволила бы писать «`SeatBooked` → `RecordTicketV1`»
   в одном месте и убрать повторное чтение.
2. **Гетерогенный диспетчер.** Object-safe обёртка над `TaskAdapter` (или `TaskDispatcher` над
   `Vec<Box<dyn …>>`) убрала бы enum `EncoreTaskAdapter`, который каждый проект пишет руками.
   *Сделано: `cqrsx::task::adapter::boxed::BoxedTaskAdapter` (бэклог, тикет 01).*
3. **Имена обработчиков в `CommittedWithEventHandlerFailure`** вместо индексов — `NamedHandler`
   перестал бы быть нужен. *Сделано: `EventHandler::name()`, `FailedHandler { index, name }` (бэклог,
   тикет 02); `NamedHandler` удалён.*
4. **Трейт `Decide`** (`fn decide(self, &A, &A::Services) -> Result<A::Event, A::Error>`) и
   blanket-реализация `handle` для enum'ов команд — тогда `aggregate.rs` сводится к `apply`.

## 10. Вопросы, которые надо решить

1. Вариант: B (рекомендую) или C?
2. Глубина: только шаги 2–3 (структура без изменения кода), 2–5 (структура + домен + use cases)
   или всё до 8 (порты, generics, архитектурный тест)?
3. D2: `apply` в `aggregate.rs` (рекомендую) или в модулях событий?
4. D7: generics по трейтам `cqrsx` (рекомендую) или алиасы инфраструктуры в application?
5. D5: wiring рантайма задач в `infrastructure/tasks` (рекомендую) или `presentation/tasks`?
6. Имена: `operations/` (или `admin/`), `views/` (или `read_models/`), `bootstrap.rs` (или `composition.rs`).

## 11. Уточнения после обсуждения (2026-10-17)

### 11.1 Value objects: что есть сейчас и куда их класть

Инвентарь домена Encore по типам DDD:

| Тип | Где сейчас | Что это | Замечание |
| --- | --- | --- | --- |
| `ConcertId`, `FanId` | `domain/ids.rs` | VO с инвариантом (slug), serde = plain string | общие для обоих агрегатов и для контрактов задач |
| `SeatId` | `domain/venue.rs` | VO с инвариантом `^[A-E][1-8]$` | общий: ключ мест в `Concert`, поле билета в `Fan`, поле payload'ов |
| `Zone` | `domain/venue.rs` | VO-перечисление с поведением: `price()`, `rows()` | `price()` — это прайс-лист, то есть бизнес-политика внутри справочника |
| `SeatInfo` | `domain/venue.rs` | составной VO (`id`, `zone`, `price`) | результат поиска в каталоге |
| `Venue` / `VENUE` | `domain/venue.rs` | каталог справочных данных: `capacity()`, `seat(raw)`, `seats()` | по сути доменный сервис над VO, не агрегат и не событие |
| `ConcertInfo`, `Booking` | `domain/concert.rs` | записи состояния агрегата; `Booking` неизменяем, удаляется при отмене | без собственных инвариантов |
| `ConcertServices { ticket_limit }` | `domain/concert.rs` | политика, передаваемая в `handle` через `Aggregate::Services` | конфигурация правила R6 |
| `Ticket`, `TicketState` | `domain/fan.rs` | `Ticket` — сущность внутри агрегата (ключ `booking_id`, меняет состояние); `TicketState` — VO | — |

Правило размещения для шаблона:

1. VO с инвариантом или поведением (парсинг, валидация, `price()`) получает собственный модуль.
2. VO, нужный двум и более агрегатам или контрактам задач, живёт в shared kernel `domain/shared/`.
3. Записи состояния без инвариантов (`ConcertInfo`, `Booking`, `Ticket`) остаются в `aggregate.rs`
   своего агрегата; когда файл перерастает ~200 строк, они переезжают в `domain/{agg}/value_objects.rs`
   (и `entities.rs` для сущностей вроде `Ticket`).
4. Справочник с поиском (`Venue`) — модуль по имени понятия рядом с его VO, не «services».

Для Encore:

```text
domain/
├── shared/                         shared kernel
│   ├── ids.rs                      ConcertId, FanId (+ private макрос для slug-newtype'ов)
│   └── venue.rs                    SeatId, Zone, SeatInfo, Venue / VENUE
├── concert/
│   ├── aggregate.rs                Concert, ConcertInfo, Booking, ConcertServices
│   ├── commands.rs, commands/…
│   ├── events.rs, events/…
│   └── error.rs
└── fan/
    ├── aggregate.rs                Fan, Ticket, TicketState
    └── …
```

Исправление примитивной одержимости (бесплатно для wire-формата). В `Fan`, `FanCommand`, `FanEvent`
и во всех четырёх payload'ах идентификаторы хранятся как `String` (`concert_id: String`,
`fan_id: String`); `FanId::parse` вызывается вручную в `run_chain_step`. Заменить на `ConcertId` /
`FanId`: их serde-представление — та же строка, значит JSON событий и задач не меняется, а невалидный
payload отклоняется адаптером как `Permanent` ещё до обработчика.

### 11.2 Почему обработчики outbox-задач не в domain, и как сохранить близость к событиям

Что делает обработчик `record_ticket`: берёт метаданные из `TaskAttempt`, строит follow-up задачу,
ждёт снятия `BoxOfficeLock` (`tokio::time::sleep`), вызывает `FanApp::execute` (исполнитель команд,
то есть персистентность), переводит исход в `TaskHandlerError::{Retry, Permanent}` с backoff'ом.
`send_e_ticket` — то же плюс порт `Mailer` с I/O. Ни один из этих шагов не является бизнес-правилом:
это оркестрация и семантика доставки. В domain они принесут `cqrsx::task`, `tokio`, исполнитель и
порты, и доменные тесты перестанут быть синхронными `TestFramework`-тестами без рантайма. Поэтому
обработчики — application.

Второй нюанс: в `cqrsx` задача привязана к **команде**, а не к событию (передаётся в
`execute_with_metadata` до `handle`). Благодаря D-02 (одно событие на команду) «рядом с событием»
означает «рядом с use case'ом, который его порождает»: публикация `RecordTicketV1` живёт в
`application/concert/book_seat.rs`.

Близость к потоку событий даёт группировка **по бизнес-процессу** (словарь `cqrsx`: *workflow* =
business sequencing). Вариант B-2 вместо раскладывания шагов по агрегатам:

```text
application/
├── command_service.rs, tasks.rs, ports.rs, activity.rs, lock.rs
├── concert.rs, concert/
│   ├── announce_concert.rs, book_seat.rs, cancel_booking.rs
│   ├── tasks.rs                    RecordTicketV1, RecordCancellationV1 — что публикуют use cases Concert
│   └── views/concert_hall.rs
├── fan.rs                          FanApp: исполнитель команд Fan (у Fan нет UI use cases)
├── fan/views/fan_wallet.rs
├── workflows.rs                    общий шаг: metadata из задачи, ожидание lock, FanError → результат задачи (бывший chain.rs)
├── workflows/
│   ├── ticket_delivery.rs          doc: SeatBooked → RecordTicket → TicketRecorded → SendETicket
│   ├── ticket_delivery/
│   │   ├── record_ticket.rs        TaskHandler<RecordTicketV1>: Fan.RecordTicket + публикует SendETicketV1
│   │   ├── send_e_ticket.rs        TaskHandler<SendETicketV1>: Mailer
│   │   └── tasks.rs                SendETicketV1
│   ├── refund.rs                   doc: BookingCancelled → RecordCancellation → TicketCancelled → IssueRefund
│   └── refund/
│       ├── record_cancellation.rs
│       ├── issue_refund.rs
│       └── tasks.rs                IssueRefundV1
├── projections/…
└── operations/…
```

Правило размещения: шаг, который запускает пользователь или API, лежит в папке своего агрегата;
шаги, которые запускают задачи, лежат в папке процесса в порядке выполнения. Папка `workflows/`
читается как список бизнес-процессов, каждый файл — как один шаг. Процесс из одного шага — один файл
`workflows/{name}.rs`.

Навигация от события к процессу без связывания слоёв: doc-комментарий модуля события с intra-doc
ссылкой, по которой IDE переходит кликом:

```rust
//! Fact: a seat has been sold. Starts [`crate::application::workflows::ticket_delivery`]
//! through the `RecordTicketV1` task published with this event.
```

Рекомендация: B-2. При желании оставить группировку по агрегату (B-1 из §4) правило то же, только
шаги процесса лежат в `application/fan/`, а цепочка видна лишь по ссылкам.

### 11.3 Зачем в infrastructure есть каталог про задачи

Здесь изначально лежали два файла, вместе около 110 строк. Первый, `adapter.rs` с enum
`EncoreTaskAdapter` из четырёх `JsonTaskAdapter`, существовал только потому, что `TaskDispatcher<A>`
принимает **один** тип адаптера, а трейт `TaskAdapter` native-async и не object-safe (§3, п. 3–4). Его
заменил библиотечный `BoxedTaskAdapter` (бэклог, тикет 01), файл удалён. Остался один:

- `dispatcher.rs` — `dispatcher_factory`: на каждого воркера создаёт свежие обработчики с их
  зависимостями (`FanApp`, `MongoInbox`, `FlakyMailServer` с seed'ом хаоса, `MailSettings`) и
  регистрирует форматы в `TaskDispatcher`. Это связывание рантайма очереди с обработчиками,
  аналог `router()` для HTTP.

Остальное про задачи (`PostgresTaskQueue`, воркеры, lease, retry) делает `cqrsx`. Приложение обязано
дать только фабрику (до тикета 01 ещё и enum-адаптер). Три возможных дома для них, как их сравнивали
до тикета 01:

| Дом | За | Против |
| --- | --- | --- |
| `infrastructure/task_runtime/` (рекомендую, с переименованием) | адаптер реализует трейт фреймворка и зависит от инфраструктурных типов; одно место для «добавить формат» | слово «tasks» уже занято контрактами в application |
| `bootstrap/task_runtime.rs` | фабрика — чистый wiring, то есть composition root | enum-адаптер в composition root выглядит чужим |
| `presentation/tasks/` | симметрия с HTTP как вторая точка входа | у рантайма нет внешнего интерфейса, который он «представляет» |

Переименование `infrastructure/tasks/` → `infrastructure/task_runtime/` убирает путаницу между
тремя смыслами слова: контракты (`application/{agg}/tasks.rs`), помощник `new_task`
(`application/tasks.rs`) и wiring рантайма.

Чеклист «добавить новую задачу» в шаблоне:

1. payload `XxxV1: TaskPayload` в `tasks.rs` публикующей стороны;
2. публикация в use case или шаге процесса, который её порождает;
3. шаг `TaskHandler<XxxV1>` в `application/workflows/{process}/`;
4. строка `BoxedTaskAdapter::json(XxxHandler { … })?` в `dispatcher_factory`
   (`infrastructure/task_runtime/dispatcher.rs`).

Пункт 4 — единственное касание инфраструктуры, и это одна строка.

## 12. Отличия реализации от плана миграции

Решения пользователя (вариант B, §11.1–11.3) реализованы полностью. Детали, которые исполнитель
решил иначе, чем в плане, и почему:

1. **`Rejection` — трейт, а не функция.** `code()` + `params()` для `ConcertError` и `FanError`
   реализованы в `application::{concert, fan}`; `CommandService` и HTTP вызывают одно и то же.
   `code()` остался и в домене: это словарь домена, а не форма API.
2. **Порт `Inbox` только читает.** Приложению нужен лишь `of_fan`; запись письма — внутреннее дело
   имитации почтового сервиса, поэтому контракт `MailStore::deliver` живёт в
   `infrastructure::mail::flaky_mail_server`. `MongoInbox` реализует оба.
3. **`MaintainedReadModel` и `ProjectionMaintenance` — в `application::projections::maintenance`,
   а не в `ports`.** Это абстракция приложения над любым read model, её не реализует инфраструктура.
4. **Типизированные id в payload'ах задач** сделаны на шаге 4 вместе с доменом, чтобы промежуточный
   код не парсил строки.
5. **`infrastructure::postgres::Executors`** сохранил имя и стал фабрикой (`executor()`, `history()`);
   рядом `event_history()`, `history_repositories()` и `PostgresLedger`. *Позже `Executors` удалён: его заменил
   библиотечный `PostgresCommandExecutor::new(pool, config, services).with_event_handler(…)` (бэклог, тикет 03),
   а `history()` стал функцией `history_store(pool, snapshot_every)`.*
6. **Тестовые фикстуры** — модули `test_support` (в `domain::{concert, fan}`, `application`,
   `application::{concert, workflows}`), сборка `Config` для тестов — `testing::config()`. *Позже
   `application::test_support` удалён: исполнитель use cases, недоступный журнал, витрины в памяти и первая
   попытка задачи теперь библиотечные (`cqrsx::cqrs::testing::{command::TestCommandExecutor,
   unavailable_store::UnavailableStore, mem_view_repository::MemViewRepository}`, `TaskAttempt::first`; бэклог,
   тикет 04). В demo остались только фикстуры предметной области.*
7. **Архитектурный тест раскрывает деревья `use`.** Поиск подстрок не видит
   `use crate::{infrastructure::…}`. Тест сразу нашёл BSON-тест view в application; он переехал в
   `infrastructure::mongo::view_repository`. Дополнительно запрещён `crate::config` в domain и
   application, `serde_json` в домене разрешён только тестам, `cqrsx` в домене — по allowlist.
8. **`AppState` потерял неиспользуемое поле `fan_commands`**; `ConcertApp`/`FanApp`/`TimeTravel` в
   `bootstrap` — алиасы конкретных типов над PostgreSQL.
