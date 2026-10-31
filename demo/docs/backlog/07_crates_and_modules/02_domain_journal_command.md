# 02. Ядро I: `domain`, `journal`, `command`, `reactions` вместо `cqrs`

| | |
|---|---|
| Статус | предложено |
| Ценность | высокая: контракты домена отделены от исполнения, журнал — от команд; путь `cqrsx::cqrs::…` исчезает |
| Цена | средняя: переезд файлов, пути в ядре, тестах, демо и документах |
| Зависимости | 01 |

## Сейчас

`cqrs/command/` собран по признаку «где вызывается»: решение команды (`decide`) — это домен, `executor`, `locks`,
`retry` — исполнение, `wakeup` — сигнал процессорам проекций, `causation` и `metadata` — сквозное правило «команда →
событие → задача → команда». `CqrsFramework`, `CommitReport` и `AggregateError` лежат в корне `cqrs.rs`.

## Станет

Чистые переезды: код и имена не меняются (имена — задача 05, ошибки журнала — задача 06).

| Сейчас | Станет | Что |
|---|---|---|
| `cqrs/aggregate.rs` | `domain/aggregate.rs` | `Aggregate`, `AggregateKey` |
| `cqrs/event.rs` | `domain/event.rs` | `DomainEvent`, `EventKind`, `EventEnvelope` |
| `cqrs/command.rs` (`CommandInfo`, `Label`), `cqrs/command/decide.rs` | `domain/command.rs` | `CommandInfo`, `Label`, `Decide` |
| `cqrs/rejection.rs` | `domain/rejection.rs` | `Rejection` |
| — | `domain.rs` | только `mod` |
| `cqrs/store.rs` | `journal.rs` | `EventStore`, `AggregateContext`, `PersistenceError`, `PersistenceErrorObserver`, `checked_counter_add` |
| `cqrs/store/{persisted,repository,serialized_event,snapshot,upcaster,feed}.rs` | `journal/…` | без изменений |
| `cqrs/command/wakeup.rs` | `journal/wakeup.rs` | `CommitWakeup`: «журнал типа записан»; команды его посылают, процессоры ждут |
| `cqrs.rs` (`CqrsFramework` и его тесты) | `command/framework.rs` | до задачи 06 |
| `cqrs.rs` (`CommitReport`, `AggregateError`), `cqrs/command.rs` (`CommandExecutor`, `CommandGatewayError`) | `command.rs` | контракт исполнения |
| `cqrs/command/{executor,locks,retry,observer,metadata,causation,planner}.rs` | `command/…` | без изменений |
| `cqrs/reactions.rs` | `reactions.rs` | `Reactions`, `Decided`, `Then`, `ReactionsPlanner` |

`impl From<PersistenceError> for AggregateError` и `From<serde_json::Error>` переезжают из `journal.rs` к
`AggregateError` в `command.rs`, чтобы журнал не называл ошибку команды хотя бы в своих `impl`; сигнатуры `EventStore`
меняет задача 06.

До задачи 03 в `cqrs` остаются `projection`, `event_handler` и `testing`.

Тесты `cqrsx/tests/cqrs/*` остаются на месте (каталог тестов переименует задача 03, когда `cqrs` исчезнет совсем);
меняются их импорты. Раскрытия макросов (`::cqrsx::cqrs::aggregate::Aggregate`, `::cqrsx::cqrs::command::…`,
`::cqrsx::cqrs::event::…`, `::cqrsx::cqrs::rejection::…`) переходят на новые пути.

## Поведение не меняется: чем доказано

Только пути. Число тестов прежнее, эталоны демо байт в байт, fmt, clippy и тесты рабочей области и демо зелёные.

## Демо и документы

Импорты демо; `tests/architecture.rs`: список домена — `cqrsx::domain::…` вместо `cqrsx::cqrs::{aggregate, event,
rejection}` и `cqrsx::cqrs::command::{CommandInfo, decide}`; презентации — `cqrsx::command::CommandGatewayError`.
`components.md` (таблица и «Source layout»), README, `typical_scenario.md`, `lib.rs` (описание модулей).
