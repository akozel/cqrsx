# 05. Имена derive и трейтов; derive рядом с трейтом

| | |
|---|---|
| Статус | предложено |
| Ценность | средняя–высокая: derive называется как трейт, который реализует, и импортируется вместе с ним одним путём |
| Цена | средняя: трейты, макросы, их сообщения об ошибках, все реализации в тестах ядра и в демо |
| Зависимости | 04 |
| cqrsx-macros | все раскрытия и сообщения об ошибках, README |

## Сейчас

Пять имён из семи не совпадают: `derive(Event)` реализует `EventKind`, `Events` — `DomainEvent`, `Command` —
`CommandInfo`, `Task` — `TaskPayload`, `Commands` — сразу `Decide<A>` и `CommandInfo`. Совпадают `Rejection` и, по
обычаю атрибутов, `#[task_handler]` → `TaskHandler`.
Derive реэкспортируются из корня крейта, трейты лежат в своих модулях: приложение импортирует одно и то же из двух мест.
Внутри `cqrsx` путь `::cqrsx` не разрешается, поэтому тесты ядра реализуют трейты вручную.

## Станет

| Derive | Трейт сейчас | Трейт после | Модуль (трейт и derive вместе) |
|---|---|---|---|
| `Event` (структура) | `EventKind` | `Event` | `domain::event` |
| `Events` (enum агрегата) | `DomainEvent` | `Events` | `domain::event` |
| `Command` — на структуре и **теперь на enum** (сведения варианта) | `CommandInfo` | `Command` | `domain::command` |
| `Commands` | `Decide<A>` + `CommandInfo` | **удаляется**: на enum пишут `#[derive(Command, Decide)]` | — |
| `Decide` (новый, только enum: решение варианта) | `Decide<A>` | `Decide<A>` | `domain::command` |
| `Task` | `TaskPayload` | `Task`, в `task.rs` | `task` |
| `Rejection` | `Rejection` | — | `domain::rejection` |
| `#[task_handler]` | `TaskHandler<P>` | `TaskHandler<T>` (правило 23) | `worker` |

- **Derive рядом с трейтом:** `pub use cqrsx_macros::Event;` в `domain/event.rs` рядом с `pub trait Event` (derive и
  трейт живут в разных пространствах имён, как `serde::Serialize`). Реэкспорт из корня `lib.rs` удаляется. `__private`
  остаётся в корне (решение 2 эпика).
- **`extern crate self as cqrsx;`** в `lib.rs`: внутри ядра `::cqrsx::…` разрешается (проверено на edition 2024),
  поэтому тесты и doctest ядра могут пользоваться своими derive. Ручные реализации меняются на derive только там, где
  тест и так правится; массовой замены нет.
- **Associated types не переименовываются:** `type Event: Events`, `type Command: Decide<Self> + Send`; документация
  `Aggregate` объясняет, что `A::Event` — enum событий, а трейт `Event` — один тип события.
- **Макросы:** раскрытия называют новые трейты; ошибки, которые называют `Commands`, `CommandInfo`, `EventKind`,
  `TaskPayload`, переписываются; `Command` на enum с вариантом не из одного поля — ошибка с объяснением; `Decide` на
  структуре — ошибка («реализуйте `Decide<A>` вручную»). `#[diagnostic::on_unimplemented]` у `Decide` советует
  `#[derive(Command, Decide)]`.

План сверяет, где нужны константы `EventKind::NAME`/`VERSION` и `TaskPayload::TASK_TYPE`/`TASK_VERSION`: имена констант
остаются, меняется только имя трейта.

## Поведение не меняется: чем доказано

Раскрытия делают то же: `tests/derives.rs` и юнит-тесты функций раскрытия проверяют тот же результат (имя события,
версия, коды и параметры отказов, сведения команды, формат задачи, labels) под новыми именами. Число тестов не меньше
прежнего (появляются тесты новых ошибок `Command`/`Decide`), эталоны демо байт в байт.

## Демо и документы

Домен демо: `#[derive(Command, Decide)]` на enum команд, импорты трейтов вместе с derive; `tests/architecture.rs`:
список домена без отдельных `cqrsx::Command`, `cqrsx::Event`… — они под путями модулей. `cqrsx-macros/README.md`,
`components.md` (строка «Derive macros» и упоминания трейтов), README, `typical_scenario.md`,
`.claude/rules/rust-style.md` (правило 33 называет `CommandInfo`, `EventKind`).
