# 09. Крейт `cqrsx-testing`

| | |
|---|---|
| Статус | сделано 2026-10-31 ([результаты](#результаты)) |
| Ценность | средняя: рабочая сборка не содержит тестовых инструментов; разрешение паниковать — lints крейта, а не `#[allow]` на каждом элементе |
| Цена | средняя: юнит-тесты ядра, которые пользуются этими инструментами |
| Зависимости | 08 |

## Сейчас

`cqrsx::testing` (после задачи 03) — `TestFramework`, `AggregateResultValidator`, `MemStore`,
`MemStoreAggregateContext`, `TestCommandExecutor`, `UnavailableStore` — компилируется в каждой сборке ядра. Правило 8
разрешает им паниковать, поэтому каждый такой элемент несёт `#[allow(clippy::panic, reason = …)]`. Юнит-тесты ядра в
`command/framework.rs` (после 06 — `attempt.rs`), `command/executor.rs`, `command/observer.rs`, `domain/command.rs`,
`reactions.rs` пользуются `MemStore`, `TestFramework` или `TestCommandExecutor`.

## Станет

- **Крейт `cqrsx-testing`** (зависит от `cqrsx`): `src/lib.rs` — `mod`; `TestFramework` — в своём файле
  (`framework.rs`), остальное — по файлу на тип, как сейчас. Его `[lints]` — общие lints рабочей области без запрета
  паник (`lints.workspace` с переопределением Cargo не позволяет, поэтому таблица своя); `# Panics` в документации
  остаются.
- **Юнит-тесты ядра** перестают пользоваться тестовыми инструментами: через dev-dependency `cqrsx-testing` они
  получили бы вторую копию типов ядра. Где хватает журнала в памяти, — `PersistedEventStore<MemEventRepository, A>`;
  иначе тест переезжает в `cqrsx-testing/tests` (он проверяет ядро через публичный API).
- **Мьютекс опубликованных задач** `TestCommandExecutor` берётся через `cqrsx::mutex::lock` (задача 07).
- **Тесты самих инструментов** (`validator`, `mem_store`, `command_executor`, `unavailable_store`) переезжают с ними.
- **Демо:** `cqrsx-testing` в `[dev-dependencies]`; `tests/architecture.rs`: тестовые пути домена и application —
  `cqrsx_testing`.

## Поведение не меняется: чем доказано

Инструменты не меняются; тесты ядра проверяют то же, меняется хранилище под ними или место теста. Число тестов прежнее,
покрытие ядра ≥ 97 % без `cqrsx-testing`.

## Документы

`components.md` («Public testing tools»), README («Testing»), `.claude/rules/rust-style.md` (правило 8 называет
`cqrs::testing`).

## Результаты

2026-10-31, на HEAD `bc5b3e6` плюс правки задачи.

- **Крейт `cqrsx-testing`** зависит только от `cqrsx`. В нём `lib.rs` (`mod` и документация крейта), `framework.rs`
  (`TestFramework`) и по файлу на `validator`, `mem_store`, `command_executor`, `unavailable_store`. Пути —
  `cqrsx_testing::framework::TestFramework`, `cqrsx_testing::mem_store::MemStore` и так далее: фасада в корне нет,
  `cqrsx::testing` исчез.
- **Инструменты не изменились:** те же сигнатуры, паники, `# Panics`. Закрытая `checked_counter_add` ядра заменена в
  `MemStore` своим счётом с тем же текстом ошибки. `StoreCommandExecutor::store` стал публичным: его зовёт
  `TestCommandExecutor::store()`, а в тест ядра добавлено утверждение.
- **Тесты ядра** больше не берут инструменты, и dev-зависимости `cqrsx → cqrsx-testing` нет.
  - Юнит-тесты и doctest'ы перешли на `PersistedEventStore<MemEventRepository, A>` и `StoreCommandExecutor`.
  - В doctest'ах проверки `TestFramework` заменены сравнением `decide` со значением целиком.
  - 10 тестов самого `TestFramework` переехали в `cqrsx-testing/tests`.
  - Сняты три проверки `take_published_tasks` (принято): каждая дублирует проверку outbox журнала, которая осталась.
- **Паники.** В обоих тестовых крейтах — `lints.workspace = true` и `#![allow(…, reason)]` в `lib.rs` ровно с теми
  lints, без которых крейт не собирается:
  - `cqrsx-testing`: `unwrap_used`, `panic`;
  - `cqrsx-fixtures`: `unwrap_used`, `expect_used`, `panic` — два лишних lints из задачи 08 сняты.

  Поэлементных `#[allow]` нет. По правилу 8 `rust-style.md` оба крейта — тестовые помощники, а `cqrsx-testing` к тому
  же входит в перечень library code.
- **Лицензия:** у `cqrsx-testing` своя копия `licenses/` и строка атрибуции cqrs-es, как у `cqrsx-postgres`.
- **Демо и документы:**
  - `cqrsx-testing` подключён в dev-зависимостях `back-end`;
  - правило архитектуры называет `cqrsx_testing` в `test_only` и `cqrsx_allowed` (равнозначно), добавлен случай
    запрета для домена;
  - Dockerfile'ы копируют новый крейт;
  - корневой README перечисляет все семь крейтов (пропуск задачи 08).
- **Числа.** Тестов 877, ignored 0: 38 тестов ушли из ядра и 38 пришли в `cqrsx-testing`. Демо — 175 тестов. Покрытие
  строк:

  | Рабочая область | `cqrsx` | `cqrsx-postgres` | `cqrsx-mongodb` | `cqrsx-engine` |
  |---|---|---|---|---|
  | 98,13 % | 97,87 % (было 97,78 %) | 98,98 % | 97,88 % | 98,46 % |

  Эталоны демо без изменений.
- **Приёмка, круг 1:** копия лицензии и атрибуция у `cqrsx-testing`, «private» вместо «closed» в корневом README.
