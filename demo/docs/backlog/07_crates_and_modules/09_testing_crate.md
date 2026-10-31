# 09. Крейт `cqrsx-testing`

| | |
|---|---|
| Статус | предложено |
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
