# 02. Агрегат

Статус: согласовано, 2026-10-26.

## Назначение

Что пользователь пишет в `domain` модуля для агрегата: состояние, события, как события меняют состояние, тип id и
настройки правил. Команды — в [03](03_commands.md).

## Как это выглядит

Раскладка — как в демо сейчас; имена файлов внутри `domain` обсуждаемы:

```text
concert/domain/
├── concert.rs              состояние и apply
├── events.rs               enum событий
├── events/                 файл на событие
│   ├── concert_announced.rs
│   ├── seat_booked.rs
│   └── booking_cancelled.rs
├── commands.rs, commands/  команды (03)
└── error.rs                отказы (03)
```

```rust
// concert/domain/concert.rs
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Concert {
    info: Option<ConcertInfo>,           // None: not announced yet (R1)
    bookings: BTreeMap<SeatId, Booking>, // occupied seats only
}

impl Aggregate for Concert {
    const TYPE: &'static str = "Concert";
    type Id = ConcertId;
    type Command = ConcertCommand;
    type Event = ConcertEvent;
    type Error = ConcertError;
    type Rules = ConcertRules; // what the rules depend on: the ticket limit
    // No `handle`: every command decides itself, `ConcertCommand: Decide<Concert>` (03).

    /// The state machine: how every fact changes the concert. A replay calls only this.
    fn apply(&mut self, event: ConcertEvent) {
        match event {
            ConcertEvent::ConcertAnnounced(announced) => self.info = Some(announced.into()),
            ConcertEvent::SeatBooked(booked) => {
                self.bookings.insert(booked.seat.clone(), booked.into());
            }
            ConcertEvent::BookingCancelled(cancelled) => {
                self.bookings.remove(&cancelled.seat);
            }
        }
    }
}
```

```rust
// concert/domain/events.rs
/// Facts about one concert; the derive dispatches each variant's stored name and version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Events)]
#[serde(tag = "type")]
pub enum ConcertEvent {
    ConcertAnnounced(ConcertAnnounced),
    SeatBooked(SeatBooked),
    BookingCancelled(BookingCancelled),
}
```

```rust
// concert/domain/events/booking_cancelled.rs
/// The owner gave the ticket back. It carries everything what follows needs: the reactions read nothing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Event)]
#[event(name = "BookingCancelled", version = "1.0")] // stored with every event: never changes
pub struct BookingCancelled {
    pub seat: SeatId,
    pub fan_id: FanId,
    pub booking_id: Uuid,
    pub cancellation_id: Uuid,
    /// The price paid back then, not today's price list.
    pub refund: u32,
}
```

Отличия от нынешнего API cqrsx:
- **`type Id`** — тип id агрегата, например `ConcertId`, вместо строки. Требование к нему: `Display` и `FromStr`.
- **`handle` в агрегате больше нет.** Каждая команда решает сама (`Decide<Concert>`): синхронно, чисто, без
  `EventSink`. Маршрут к ней строит `#[derive(Commands)]` (03).
- **`type Rules`** вместо `type Services`: так проще сказать, что это настройки правил.
- **Тип и версия события — атрибут на типе события** (`#[derive(Event)]`, `#[event(name, version)]`), а не `match`
  в `DomainEvent`. Enum событий получает их через `#[derive(Events)]`.

## Что пишет пользователь

- **Состояние:** закрытые поля и методы чтения, которые нужны правилам.
- **События.** Каждое — факт с данными, по файлу на событие. Событие несёт всё, что понадобится следующим шагам,
  поэтому реакции ничего не читают (`BookingCancelled` несёт id брони и сумму возврата).
- **`apply`** — машина состояний. Повтор истории (replay) вызывает только её.
- **Тип id и настройки правил.**

## Что делает движок

- Загружает агрегат: снапшот и события после него.
- Пишет снапшот каждые N событий; N — настройка движка (07). Агрегат об этом не знает.
- Хранит события с типом, версией и метаданными: причинность, `recorded_at`.
- Блокирует, решает, коммитит — это путь команды (03, 04).

## Правила

- **`apply` полная и детерминированная:** без часов и ввода-вывода. На ней держатся повтор истории и снапшоты.
- **Опубликованное событие не меняется.** Изменить смысл — значит выпустить новую версию события.
- **Состояние сериализуется без потерь,** потому что снапшот — это сериализованное состояние.

## Как тестировать

«Дано события → состояние» через `apply`. Правила команд — given/when/then (03).

## Решено

- 2026-10-26: один агрегат на модуль по умолчанию (01).
- 2026-10-26: настройки правил — `type Rules` (`ConcertRules { ticket_limit }`), а не `Services`.
- **2026-10-26: тип и версия события задаются derive-макросом** `#[derive(Event)]` с атрибутом
  `#[event(name = "BookingCancelled", version = "1.0")]`, а для enum — `#[derive(Events)]`.
  - Записанные имена и версии остаются прежними.
  - Процедурные макросы приветствуются везде, где они делают код читаемее (README дизайна).
- **2026-10-26: `apply` принимает событие по значению,** как сейчас. Домен пишется без `.clone()`, а одну копию на
  команду делает движок. Тема — в [списке пересмотра](../revisit.md).
- **2026-10-26: перевод старых версий событий (upcasting) отложен.** Пока действует правило: новое поле добавляется со
  значением по умолчанию (`#[serde(default)]`). Тема — в [списке пересмотра](../revisit.md).

## Открытые вопросы

Нет: две темы отложены в список пересмотра.
