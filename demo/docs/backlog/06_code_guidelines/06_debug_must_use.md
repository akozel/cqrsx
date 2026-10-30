# 06. `Debug`, `#[must_use]`, параметры по значению

| | |
|---|---|
| Статус | предложено |
| Ценность | низкая–средняя: результат builder и итог остановки нельзя молча потерять, любой публичный тип печатается |
| Цена | низкая: механические правки, около 100 мест |
| cqrsx | места ниже; `Cargo.toml` (`[lints]`) |
| cqrsx-macros | `src/*.rs` (`expand_*` по ссылке); `Cargo.toml` (`[lints]`) |
| demo | нет (кроме новых предупреждений `must_use`, если demo игнорирует результат) |
| Зависимости | 05 (те же типы после переименований) |
| Правила | 3, 16, 20, 32 |

## Сейчас

Аудит, [раздел «API и типы»](../../cqrsx-style-audit.html#api), lints по HEAD `e995a53`:
- `missing_debug_implementations` — 42 публичных типа без `Debug`;
- `return_self_not_must_use` — 42 метода builder без `#[must_use]`;
- `needless_pass_by_value` — 15 мест (F4 `persisted.rs:550` уходит в задаче 04).

## Станет

1. **`Debug`** у всех 42 типов: derive, где поля позволяют; иначе ручной с `finish_non_exhaustive()`. Данные, которые
   нельзя печатать (payload задач, lease token, пароли), скрываются, как в существующих ручных `Debug`.
2. **`#[must_use]`** (правило 20):
   - на типы builder (`engine/build.rs`, `engine/module.rs`, `task/handlers.rs`, `processor.rs`, `cqrs/command/executor.rs`
     и другие из списка lint) — это закрывает все их методы;
   - по смыслу, вне lint: `Stopped` (`host/lifecycle.rs:90`, итоги host и read side теряются при `..await;`),
     `Setup::invalid` и `SetupError::failed` (`engine/setup.rs:87`, `:37`), `AggregateResultValidator`
     (`cqrs/testing/validator.rs:6`, `testing.rs:519`), `stamp_task` и `task_id` (`causation.rs:110`, `:123`), чистые
     `Retry::pause`, `Policies::pause`, `Policies::max_attempts` (`task/policy.rs:122`, `:210`, `:216`).
3. **Параметры по значению** (правило 3): маленьким конфигурациям — `Copy` (`PostgresCommandConfig` в
   `postgres/command.rs:83`, `TaskStorageConfig` в `default_executor.rs:33`); `storage_error(&sqlx::Error, ..)`
   (`postgres/task_queue.rs:32`); семь `expand_*(input: &DeriveInput)` в макросах; четыре места в помощниках для
   тестов.
4. **Lints** в `[lints]` обоих crate: `missing_debug_implementations`, `clippy::return_self_not_must_use`,
   `clippy::needless_pass_by_value` — `deny`; `unsafe_code = "forbid"` (правило 32). Skill `rust-style-review`: эти
   lints убираются из команды шага 1.

## Этапы

1. Правки по трём группам; тесты не меняются, кроме вызовов, которые теперь требуют использовать результат.
2. Lints; `CLAUDE.md`-проверки cqrsx и сборка demo без предупреждений; коммит.

## Риски

- **Ручной `Debug` раскрывает секрет.** Для каждого ручного `Debug` проверить поля: payload, metadata, lease token,
  URL с паролем не печатаются.

## Как проверить

- `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` с новыми lints.

## Критерии приёмки

- [ ] Три lint включены, срабатываний ноль; `unsafe_code = "forbid"`.
- [ ] `#[must_use]` по смыслу на перечисленных типах и функциях.
