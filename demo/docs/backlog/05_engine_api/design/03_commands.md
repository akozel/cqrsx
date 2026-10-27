# 03. Команды и их обработчики

Статус: согласовано, 2026-10-26.

## Назначение

Как пользователь описывает намерения и правила: команда как тип, обработчик команды с инвариантами и бизнес-правилами,
отказы с кодами для API.

## Как это выглядит

```rust
// concert/domain/commands.rs
/// The derive routes every command to its own `decide`: `ConcertCommand: Decide<Concert>`.
#[derive(Debug, Clone, Commands)]
pub enum ConcertCommand {
    AnnounceConcert(AnnounceConcert),
    BookSeat(BookSeat),
    CancelBooking(CancelBooking),
}
```

```rust
// concert/domain/commands/cancel_booking.rs
/// The owner gives a ticket back. Rules R1, R3, R7, R8.
#[derive(Debug, Clone, Command)]
pub struct CancelBooking {
    /// Raw seat text from the UI; checked here (R3).
    pub seat: String,
    #[label] // the activity feed shows and filters it
    pub fan_id: FanId,
    /// The cancellation is identified by its command: the engine takes the command id from here.
    #[command_id]
    pub cancellation_id: Uuid,
}

impl Decide<Concert> for CancelBooking {
    fn decide(self, concert: &Concert, _rules: &ConcertRules) -> Result<ConcertEvent, ConcertError> {
        concert.info().ok_or(ConcertError::ConcertNotFound)?; // R1
        let seat = VENUE.seat(&self.seat).ok_or(ConcertError::UnknownSeat { seat: self.seat })?; // R3
        let booking = concert.bookings().get(&seat.id).ok_or(ConcertError::SeatNotBooked { seat: seat.id.clone() })?; // R7
        if booking.fan_id != self.fan_id {
            return Err(ConcertError::NotYourTicket); // R8
        }
        Ok(ConcertEvent::BookingCancelled(BookingCancelled {
            seat: seat.id,
            fan_id: self.fan_id,
            booking_id: booking.booking_id,
            cancellation_id: self.cancellation_id,
            refund: booking.price, // what was paid, a fact from history
        }))
    }
}
```

Маршрут от enum к обработчику команды строит `#[derive(Commands)]`. Это тот же `match` по вариантам, только написанный
за пользователя.

```rust
// concert/domain/error.rs
/// Rules R1–R8 as the API reports them: the code is the variant in snake_case, the params are its fields.
#[derive(Debug, thiserror::Error, Rejection)]
pub enum ConcertError {
    #[error("the concert has not been announced")]
    ConcertNotFound, // concert_not_found, {} (R1)
    #[error("seat {seat} is taken by {by}")]
    SeatTaken { seat: SeatId, by: String }, // seat_taken, {seat, by}
    #[error("at most {limit} tickets per fan")]
    TicketLimit { limit: usize }, // ticket_limit, {limit}
    // …
}
```

## Что пишет пользователь

- **Команду** — данные намерения. Всё, что приходит извне, приходит в команде: id брони, текст места из интерфейса,
  цена от внешнего сервиса.
  - Поле с `#[command_id]` — если бизнес-id равен id команды, как id брони и id отмены. Иначе id выдаёт движок.
  - Поля с `#[label]` — подписи для ленты активности.
- **Обработчик команды** — `impl Decide<Concert>` в файле команды: инварианты и правила. Результат — одно событие или
  отказ.
- **Отказы** — enum ошибок с `#[derive(Rejection)]`. Код и параметры для текстов интерфейса выводятся из вариантов, а
  атрибут `#[rejection(code = "..")]` переопределяет код, когда это нужно.

## Что делает движок

- Ставит команду в очередь блокировки агрегата.
- Загружает агрегат и вызывает `decide` команды.
- При событии — реакция (04) и коммит. При отказе ничего не пишет.
- При конфликте с другим процессом повторяет: загружает агрегат заново и решает заново, на свежем состоянии.
- Отвечает одним из четырёх исходов: принято, отказ с ошибкой агрегата, конфликт (повторы исчерпаны), недоступно.
  Сейчас HTTP отвечает на них 200, 422, 409 и 503.

## Правила

- **`decide` чистая:** без ввода-вывода и часов. Она идёт под блокировкой и повторяется при конфликте.
- **Одна команда — не больше одного события** (D-02): запись в PostgreSQL принимает одно событие. Поэтому `decide`
  возвращает одно событие, а не список.
- **Отказ — исход, а не сбой.** Движок не пишет его в журнал как ошибку.

## Как тестировать

```rust
TestFramework::<Concert>::with(ConcertRules::default())
    .given(vec![announced(), booked("A1", "fox", 40)])
    .when(cancel("A1", "fox"))
    .then_expect_events(vec![cancelled("A1", "fox", 40)]);
```

## Решено

- **2026-10-26: маршрутизация — трейт `Decide<A>` на каждой команде и `#[derive(Commands)]` на enum.** `handle` писать
  не нужно. Удобство решения оценим после реализации — тема в [списке пересмотра](../revisit.md).
- **2026-10-26: одна команда — одно событие.** `decide` возвращает `Result<Event, Error>`, как требует D-02. Вернуться
  к этому перед закрытием эпика — тема в [списке пересмотра](../revisit.md).
- **2026-10-26: коды отказов — `#[derive(Rejection)]` на ошибке домена.**
  - Код — имя варианта в snake_case, параметры — поля варианта. Так устроены все нынешние коды R1–R8, поэтому
    контракт HTTP не меняется.
  - Ревью после реализации: удобно ли этим пользоваться и не нарушает ли это границы компонентов — коды для HTTP
    живут в домене. Тема в [списке пересмотра](../revisit.md).
- **2026-10-26: команда несёт свой id** — поле с `#[command_id]`; без такого поля id выдаёт движок.
  - Источник (`"ui"`) задаёт вызывающий.
  - Ревью после реализации — тема в [списке пересмотра](../revisit.md).
- **2026-10-26: пометки «внутренняя команда» нет.** Команда публична, только если её исполняет какой-то маршрут.
  Ревью после реализации: чем отличаются `cqrs.execute` и `ctx.execute` и нужна ли всё-таки пометка. Тема в
  [списке пересмотра](../revisit.md).

## Открытые вопросы

Нет: пять решений файла ждут ревью по списку пересмотра.
