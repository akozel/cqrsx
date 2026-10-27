# 05. Команда решает сама: `Decide<A>`, `#[derive(Commands)]`, `type Rules`; `handle` и `EventSink` уходят

| | |
|---|---|
| Статус | предложено 2026-10-27; решение пользователя 2026-10-27 (README эпика, вопрос 8): `then_expect_event(event)` вместо `then_expect_events(vec![..])` |
| Ценность | высокая: правила команды — чистая синхронная функция в файле команды; одна команда — одно событие на уровне типов |
| Цена | высокая: контракт `Aggregate` меняется, 62 реализации в cqrsx (31 файл) и две в демо переводятся, `TestFramework` упрощается |
| cqrsx | `src/cqrs/aggregate.rs`, новый `src/cqrs/command/decide.rs`, `src/cqrs/event_sink.rs` (удаляется), `src/cqrs.rs`, `src/cqrs/command/{executor,planner}.rs` (`Services` → `Rules`), `src/cqrs/testing.rs`, `testing/{executor,validator,command}.rs`, `src/postgres/command.rs`, `cqrsx-macros` (`Command`, `Commands`), все doctest'ы и `README.md`, `doc/components.md`, `tests/**` (31 файл с агрегатами) |
| demo | `concert/domain/{concert,commands,commands/*}.rs`, `fan/domain/{fan,commands,commands/*}.rs`, `concert/application/app.rs` (`CommandLabel` из `#[label]`), `bootstrap/aggregates/*` (`ConcertRules`), `tests/architecture.rs` |
| Зависимости | 04 |
| Гарантии | G-01, G-03, G-04, G-05, G-06, G-12 (`command_id`), G-34 |

## Сейчас

`Aggregate::handle(&mut self, command, &Services, &EventSink<Self>) -> impl Future<Output = Result<(), Error>>`:
команда пишет события в `EventSink`, который сразу применяет их к рабочей копии; `CqrsFramework` собирает буфер и
коммитит. Демо уже пишет по-другому: `Concert::handle` — `match`, который зовёт `command.decide(self, services)` и
делает один `sink.write` (`concert.rs:89–102`); D-02 держат два теста-матрицы. В cqrsx 62 реализации `handle`, из
них несколько пишут больше одного события на команду (`tests/cqrs/support/customer.rs`: `CustomerDataPopulated` за
`NameAdded`; `tests/cqrs/projection/live.rs`: `a_command_with_several_events_is_one_transaction_and_one_job`), и
ни одна не ждёт в `handle` ничего, кроме `sink.write` (проверено 2026-10-26, задача 01).

## Станет

Дизайн [02](design/02_aggregate.md) и [03](design/03_commands.md).

**cqrsx**
- `Aggregate`: `const TYPE`, `type Command: Decide<Self> + Send`, `type Event: DomainEvent`, `type Error: Error`,
  `type Rules: Send + Sync`, `fn apply(&mut self, event)`. Метода `handle` нет; `event_sink.rs` удалён.
- `cqrs::command::decide::Decide<A: Aggregate>`: `fn decide(self, state: &A, rules: &A::Rules) -> Result<A::Event,
  A::Error>` — чистая, синхронная; одно событие или отказ (D-02 правилом типов).
- `cqrs::command::CommandInfo` (имя обсуждаемо): `fn name(&self) -> &'static str`, `fn command_id(&self) ->
  Option<Uuid>`, `fn labels(&self) -> Vec<(&'static str, String)>`. Нужен не ядру, а сборке и наблюдателю (08, 11).
- `cqrsx-macros`: `#[derive(Command)]` на struct команды — `CommandInfo` из имени типа, поля `#[command_id]` (не
  больше одного, тип `Uuid`) и полей `#[label]` (значение через `Display`); `#[derive(Commands)]` на enum newtype-
  вариантов — `impl<A> Decide<A> for Enum where каждый вариант: Decide<A>` (диспетчер-`match`) и `CommandInfo`
  делегированием. Derive не знает тип агрегата: обобщённый `impl` с `where`.
- `CqrsFramework::execute_with_metadata`: загрузка → `let event = command.decide(committed, &rules)?` → рабочая копия
  `after = committed.clone(); after.apply(event.clone())` (одна копия события на команду, тема 1 списка пересмотра) →
  `events = vec![event]` → планирование → коммит. С планировщиком команда клонируется один раз, как сейчас
  (`Planning::clone_command`): `decide` потребляет команду, а `Decision` её показывает.
- `Services` → `Rules` везде: `CqrsFramework::new(store, rules)`, `PostgresCommandExecutor::new(pool, config, rules)`,
  `TestCommandExecutor::new(store, rules)`, `TestFramework::with(rules)`.
- `TestFramework` — синхронный: `.given(events).when(command)` зовёт `decide` на свёртке; `then_expect_event(event)`,
  `then_expect_error(error)`, `then_expect_error_message(text)`, `inspect_result() -> Result<A::Event, A::Error>`;
  `AggregateTestExecutor` и асинхронный DSL (`async_dsl_observes_dependent_events…`) удаляются.
- Контракты хранилищ не меняются: `EventStore::commit(events: Vec<_>, …)` по-прежнему принимает список;
  `MemStore`, `MemEventRepository` и `PostgresEventRepository` без outbox хранят многособытийные записи для тестов
  истории, снапшотов и проекций, которые пишут через `commit`/`persist` напрямую.

**demo**
- `impl Decide<Concert> for CancelBooking` вместо `pub(in crate::domain::concert) fn decide`; `#[derive(Command)]` с
  `#[label] fan_id` и `#[command_id] cancellation_id` (у `BookSeat` — `booking_id`); `#[derive(Commands)]` на
  `ConcertCommand`, `FanCommand`; ручные `name()`, `fan_id()`, `From<…>` удаляются; `Concert::handle` и `Fan::handle`
  удаляются; `ConcertServices` → `ConcertRules`.
- `concert/application/app.rs`: `CommandLabel` строится из `CommandInfo::labels()` (`fan_id` — первая подпись).
- Тесты D-02 (`d02_every_accepted_*`) удаляются: правило держит сигнатура `decide`.
- `tests/architecture.rs`: `domain` может использовать `cqrsx::cqrs::command::{decide, CommandInfo}` и
  `cqrsx::{Command, Commands}`.

## Этапы

1. **Инвентарь** реализаций `handle` в cqrsx: для каждой — одна команда даёт одно событие или нет. Многособытийные
   (`customer.rs`, `live.rs::a_command_with_several_events…`, `scenario.rs`, doctest'ы `testing.rs`) переписать:
   событие на команду, а многособытийные записи тестов — через `commit`/`persist`. Инвентарь — в тикет.
2. cqrsx: трейты, derive, фреймворк, `TestFramework`; перевод 62 реализаций и ~190 вызовов `execute` там, где
   меняется тип правил; doctest'ы (`aggregate.rs`, `event.rs`, `cqrs.rs`, `projection.rs`, `testing.rs`,
   `processor.rs`, `planner.rs`, `testing/command.rs`) и быстрый старт `README.md` (компилируется).
3. Тесты: `decide` зовётся на зафиксированном состоянии, рабочая копия применяет событие один раз; отказ ничего не
   пишет; `Commands`-диспетчер с `compile_fail` на struct-вариант; `#[command_id]` дважды — ошибка компиляции;
   `labels()` в порядке полей.
4. Демо: домен, `app.rs`, bootstrap, архитектурный тест.
5. `components.md`: «Aggregate contract», «Command orchestration», новая строка «Command decision» (`Decide`,
   `CommandInfo`), «Public testing tools»; раскладка без `event_sink.rs`.

## Зачем, на примере

```rust
impl Decide<Concert> for CancelBooking {
    fn decide(self, concert: &Concert, _rules: &ConcertRules) -> Result<ConcertEvent, ConcertError> {
        concert.info().ok_or(ConcertError::ConcertNotFound)?;                                   // R1
        let seat = VENUE.seat(&self.seat).ok_or(ConcertError::UnknownSeat { seat: self.seat })?; // R3
        let booking = concert.bookings().get(&seat.id).ok_or(ConcertError::SeatNotBooked { seat: seat.id.clone() })?; // R7
        if booking.fan_id != self.fan_id { return Err(ConcertError::NotYourTicket); }            // R8
        Ok(ConcertEvent::BookingCancelled(BookingCancelled { seat: seat.id, fan_id: self.fan_id, booking_id: booking.booking_id, cancellation_id: self.cancellation_id, refund: booking.price }))
    }
}
```

Ни `async`, ни `sink`, ни `&mut self`: правила читают состояние и возвращают факт. В тесте — `TestFramework`
без рантайма.

## Риски

- **Многособытийные команды в тестах cqrsx.** Их мало, но каждая — осознанная переписка (этап 1). Правило D-02
  действует для outbox-репозитория и раньше; для `MemStore` и репозитория без outbox многособытийные записи остаются
  доступны через `commit`.
- **Сборка README** на каждом коммите: быстрый старт компилируется как doctest, поэтому правится здесь минимально, а
  целиком — в 14.
- **`Send` команд.** Клон команды для `Decision` остаётся (тест `a_planner_reads_a_send_only_command_and_the_future_
  stays_send`); `decide` синхронная, `await` в ней невозможен — часть контракта.
- **Объём.** 31 файл cqrsx; делать механически, по файлу, с `cargo test` после каждого каталога.

## Как проверить

- Все проверки `CLAUDE.md`; матрица фич; оба порога покрытия (новый код: трейты, derive, `TestFramework`).
- Тесты G-01, G-03, G-04, G-05, G-06 (ядро и PostgreSQL) проходят с переведёнными агрегатами без ослабления.
- Демо: все тесты; `write_side.rs`.

## Поведение не меняется: чем доказано

- Путь команды тот же: блокировка → загрузка → решение → планирование → коммит (G-01, G-03, G-04, G-05): тесты
  `executor.rs` и `tests/postgres/command.rs` переведены на `decide` без изменения утверждений.
- Одно событие на команду — правило типов; запись outbox по-прежнему отказывает нескольким событиям (G-06,
  `rejects_multi_event_snapshot_only_and_tasks_without_event_before_writing` не меняется).
- Журнал: те же события, версии и метаданные (G-34; `#[command_id]` даёт тот же `command_id`, что сегодня подставляет
  HTTP-обработчик — тест `booking_and_cancellation_ids_are_the_command_id`).
- Стенд не нужен: число операторов и загрузок не меняется (одна копия состояния, как сейчас).

## Критерии приёмки

- [ ] `Aggregate` без `handle`; `EventSink` удалён; `Decide<A>` и `CommandInfo` в cqrsx; `Command`/`Commands` в
      `cqrsx-macros` с тестами и `compile_fail`.
- [ ] 62 реализации cqrsx и демо переведены; инвентарь многособытийных команд в тикете, каждая переписана.
- [ ] `TestFramework` синхронный; `AggregateTestExecutor` удалён.
- [ ] Демо: `impl Decide` в файлах команд, `#[derive(Commands)]`, `ConcertRules`; D-02-тесты удалены.
- [ ] `components.md`, README cqrsx (компилируется), guarantees.md (переименованные тесты).
