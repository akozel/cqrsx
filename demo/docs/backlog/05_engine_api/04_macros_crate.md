# 04. Крейт `cqrsx-macros`: `Event`, `Events`, `Task`, `Rejection`

| | |
|---|---|
| Статус | сделано 2026-10-27 |
| Ценность | средняя–высокая: имя и версия события, формат задачи и код отказа — атрибут на типе, а не `match`; первый шаг к декларативному домену |
| Цена | средняя: новый крейт в workspace, четыре derive, тесты развёртки, перевод демо |
| cqrsx | `Cargo.toml` (workspace `members`), новый `cqrsx-macros/`, `cqrsx/Cargo.toml` (зависимость), `src/lib.rs` (реэкспорт), `src/cqrs/event.rs` (`EventKind`), новый `src/cqrs/rejection.rs`, `doc/components.md` |
| demo | `concert/domain/{events,events/*,error}.rs`, `fan/domain/{events,events/*,error}.rs`, `fan/application/{record_ticket,send_e_ticket,record_cancellation,issue_refund}.rs` (контракты задач), `concert/application/app.rs`, `fan/application/app.rs` (`impl Rejection` уходит), `tests/architecture.rs` (allowlist) |
| Зависимости | 03 |
| Гарантии | G-33, G-34, G-35 (коды отказов) |

## Сейчас

- Тип и версия события — `impl DomainEvent for ConcertEvent` с `match` по вариантам (`events.rs:23–36`), версия
  `"1.0"` строкой в одном месте на весь enum.
- Формат задачи — `impl TaskPayload for RecordCancellationV1 { const TASK_TYPE; const TASK_VERSION }` рядом с типом.
- Код отказа — `ConcertError::code()` с `match` в домене и `impl Rejection for ConcertError` с `params()` в
  `application` (трейт `Rejection` — демо, `command_service.rs:65`).
- Крейта процедурных макросов нет. `syn` 2.0.119, `quote` 1.0.47, `proc-macro2` 1.0.107 и `heck` 0.5.0 есть в обоих
  `Cargo.lock` и в офлайн-кэше; `trybuild` — нет (README дизайна).

## Станет

Новый крейт `cqrsx-macros` (`proc-macro = true`, workspace member), который cqrsx подключает всегда и реэкспортирует:
`cqrsx::{Event, Events, Task, Rejection}`.

| Derive | На чём | Что порождает |
|---|---|---|
| `#[derive(Event)]` + `#[event(name = "BookingCancelled", version = "1.0")]` | struct события | `impl cqrsx::cqrs::event::EventKind for T { const NAME: &str; const VERSION: &str; }` — новый трейт |
| `#[derive(Events)]` | enum из newtype-вариантов | `impl DomainEvent for Enum`: `event_type`/`event_version` через `<Payload as EventKind>::NAME` каждого варианта; вариант не newtype — ошибка компиляции с текстом «every variant carries one event type» |
| `#[derive(Task)]` + `#[task(name = "encore_record_cancellation", version = 1)]` | struct задачи | `impl TaskPayload` |
| `#[derive(Rejection)]` | enum ошибок домена | `impl cqrsx::cqrs::rejection::Rejection`: `code()` — вариант в snake_case (`heck`), переопределение `#[rejection(code = "…")]`; `params()` — объект из именованных полей (`serde_json::json!`), у unit-варианта `{}`, tuple-вариант — ошибка компиляции |

- Трейт `Rejection { fn code(&self) -> &'static str; fn params(&self) -> serde_json::Value }` переезжает из демо в
  `cqrsx::cqrs::rejection`: движку он нужен для наблюдателя команд (08) и для ответов HTTP.
- Атрибуты `#[label]` и `#[command_id]` для команд — в 05 (`Command`, `Commands`), `#[task_handler]` — в 09.
- Функции развёртки — `pub fn expand_event(input: DeriveInput) -> syn::Result<TokenStream>` и подобные над
  `proc_macro2`; точки входа `#[proc_macro_derive]` — одна строка каждая. Покрытие 97 % держат юнит-тесты развёртки
  (сравнение токенов, ошибки с их текстом); «не компилируется» — `compile_fail`-doctest'ы в cqrsx.
- Демо: все четыре derive вместо ручных `impl`; `ConcertError::code()` и `impl Rejection` в `application` удаляются;
  `ConcertEvent` сохраняет `#[serde(tag = "type")]` — derive его не трогает.

## Этапы

1. Крейт, зависимости, `cargo check --offline` в корне и в `demo/` (оба `Cargo.lock` получают запись `cqrsx-macros`
   и, если понадобится, `heck`); после этого `--locked` снова работает.
2. `EventKind`, `Event`, `Events` с тестами развёртки; doctest в `event.rs` на derive и `compile_fail` на enum с
   struct-вариантом.
3. `Task`; `Rejection` и трейт в cqrsx; `compile_fail` на tuple-вариант.
4. Демо: события, задачи, ошибки; `tests/architecture.rs`: `domain` может использовать `cqrsx::{Event, Events,
   Rejection}` и `cqrsx::cqrs::rejection`.
5. `components.md`: строка «Derive macros» в инвентаре и `cqrsx-macros/` в раскладке исходников; `README.md` cqrsx —
   событие в быстром старте через derive.

## Зачем, на примере

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Event)]
#[event(name = "BookingCancelled", version = "1.0")]
pub struct BookingCancelled { … }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Events)]
#[serde(tag = "type")]
pub enum ConcertEvent { ConcertAnnounced(ConcertAnnounced), SeatBooked(SeatBooked), BookingCancelled(BookingCancelled) }

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Rejection)]
pub enum ConcertError {
    #[error("seat {seat} is already taken by {by}")]
    SeatTaken { seat: SeatId, by: String }, // seat_taken, { seat, by }
    …
}
```

Имя и версия стоят у факта, которому принадлежат; новый вариант без `#[event]` не компилируется, вместо того чтобы
молча получить версию соседа.

## Риски

- **Коды отказов из snake_case имени варианта.** Все нынешние коды R1–R11 совпадают с snake_case вариантов
  (`rejection_codes_are_stable`), поэтому контракт HTTP не меняется. Для переопределения есть `#[rejection(code)]`.
- **`serde_json` в домене.** Сгенерированный `params()` строит `serde_json::Value` через путь `::cqrsx::…`, так что
  исходник домена `serde_json` не называет и `tests/architecture.rs` доволен. Граница компонентов — тема 5 списка
  пересмотра.
- **Покрытие крейта макросов** считается в `cargo llvm-cov --workspace`; функции развёртки тестируются напрямую,
  точки входа `proc_macro` недоступны тестам, поэтому в них не должно быть логики.
- **Офлайн-сборка.** Новые зависимости только из кэша; `trybuild` не используется.

## Как проверить

- Все проверки `CLAUDE.md`, включая матрицу фич (крейт макросов не зависит от фич) и оба порога покрытия.
- Демо: fmt, clippy, test; тесты G-33/G-34 (`*_keep_their_stored_json`, `task_payloads_keep_their_queued_json`) и
  G-35 (`rejection_codes_are_stable`, `rejections_carry_what_the_ui_texts_interpolate`) проходят без правок.

## Поведение не меняется: чем доказано

Derive порождает те же `event_type`/`event_version`, `TASK_TYPE`/`TASK_VERSION`, `code()`/`params()`, что
записаны руками: это держат тесты литералов G-33, G-34 и кодов G-35, которые не меняются. Записи в журнале, outbox и
ответы HTTP идентичны.

## Критерии приёмки

- [x] `cqrsx-macros` в workspace; `cqrsx::{Event, Events, Task, Rejection}` реэкспортированы; оба `Cargo.lock`
      обновлены, `--locked` работает.
- [x] Четыре derive с тестами развёртки и `compile_fail`-doctest'ами; покрытие ≥ 97 % по workspace и по cqrsx.
- [x] Демо использует derive; ручные `impl DomainEvent`, `impl TaskPayload`, `code()` и `impl Rejection` удалены.
- [x] `components.md`: строка инвентаря и раскладка.

## Результат

Сделано 2026-10-27.

- **Крейт `cqrsx-macros`** (`proc-macro`, член workspace, без фич). cqrsx подключает его всегда и реэкспортирует в
  корне `cqrsx::{Event, Events, Task, Rejection}`; трейты живут в своих модулях, скрытый `cqrsx::__private` даёт
  развёртке `serde_json`.
  - `Event` + `#[event(name, version)]` → `cqrs::event::EventKind` (`NAME`, `VERSION`; подсказка компилятора у
    трейта).
  - `Events` на enum из newtype-вариантов → `DomainEvent` через `EventKind` типа варианта.
  - `Task` + `#[task(name, version)]` → `TaskPayload`.
  - `Rejection` → `cqrs::rejection::Rejection`: код — вариант в snake_case (`#[rejection(code)]` переопределяет),
    параметры — именованные поля, у unit-варианта `{}`.
  - Ошибки при сборке: вариант не из одного типа события, тип варианта без `Event`, tuple-вариант отказа, неполный
    атрибут, чужой или повторённый ключ, пустые значения, версия задачи ≤ 0, повтор кода, `#[rejection]` на enum.
  - Функции развёртки над `proc_macro2`, точки входа — один вызов. 29 тестов развёртки, 3 теста скомпилированного
    кода (`cqrsx/tests/derives.rs`), 5 `compile_fail`-doctest'ов (тексты ошибок сверены во временном крейте).
    Покрытие крейта — 100 % строк.
- **Lock-файлы:** в обоих только пакет `cqrsx-macros` и строка о нём у `cqrsx`; `heck`, `proc-macro2`, `quote`,
  `syn 2.0.119` уже были.
- **Демо на derive.** Удалены 2 `impl DomainEvent`, 4 `impl TaskPayload`, 2 `fn code()`, 2 `impl Rejection` и трейт
  `Rejection` демо. Тестовые двойники `Counted` и `Added` остались с ручным `impl`: критерий — о домене модулей.
  - Тесты отказов переехали к ошибкам домена дословно. Два новых теста, добавленные до замены, держат код и
    параметры всех 11 вариантов R1–R11 литералами.
  - `tests/architecture.rs`: домен видит `cqrsx::{Event, Events, Rejection}` и `cqrsx::cqrs::rejection`,
    presentation — только `cqrsx::cqrs::rejection`.
  - Dockerfile'ы бэкенда и sidecar копируют `cqrsx-macros`.
- **Документы:** `components.md` (строки «Derive macros», «Rejection report», `EventKind`; раскладка `cqrsx-macros/`;
  правило о реэкспортах), быстрый старт README на derive, `revisit.md` (тема 5).
- **Поведение не изменилось** (G-22, G-33, G-34, G-35, G-36):
  - тесты гарантий проходят, тела без правок: 145 на месте, 4 перенесены дословно (`work/04-tools/test_bodies.py`);
  - эталоны 02 не тронуты;
  - SQL, ключи окружения и умолчания не менялись.
- **Переименованные тесты:** нет. В guarantees.md — новые пути двух перенесённых тестов и новые тесты (G-22, G-33,
  G-34, G-35, G-36).
- **Проверки:** workspace — 653 теста, матрица фич; покрытие — 97,73 % (workspace) и 97,65 % (`-p cqrsx`); демо —
  150 + 14 тестов.
