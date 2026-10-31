# 06. Типизированный id агрегата: `type Id`

| | |
|---|---|
| Статус | сделано 2026-10-27; решение пользователя 2026-10-27 (README эпика, вопрос 8): у тестовых агрегатов cqrsx `type Id = String` |
| Ценность | средняя: `execute(&ConcertId, ..)` вместо строки; реакции и обработчики получают `&ConcertId`, строки не разбираются |
| Цена | средняя: порт исполнителя и ~190 вызовов `execute` в тестах cqrsx; в демо — `FromStr`/`Display` у трёх id |
| cqrsx | `src/cqrs/aggregate.rs` (`type Id`), `src/cqrs/command.rs` (порт), `command/{executor,planner}.rs` (`Decision.aggregate_id: &A::Id`), `src/cqrs.rs`, `src/postgres/command.rs`, `src/cqrs/testing/command.rs`, `src/cqrs/aggregate.rs::AggregateKey`, тесты, `README.md`, `doc/components.md` |
| demo | `shared/domain/ids.rs` (`FromStr`), `concert/application/{reactions,fan_ledger}.rs`, `fan/application/reactions.rs` (без `parse`), `concert/application/*.rs`, `fan/application/app.rs`, `tests/write_side.rs` |
| Зависимости | 05 |
| Гарантии | G-01, G-03, G-13 (id задачи считается от того же текста id) |

## Сейчас

`CommandExecutor::execute(&self, aggregate_id: &str, command)`; `Decision.aggregate_id: &str`. Планировщики демо
разбирают строку обратно в тип (`ConcertId::parse(decision.aggregate_id).map_err(TaskPlanError::application)?`,
`refund.rs:34`), а тесты проверяют, что чужой id отказывает. В демо `ConcertId`, `FanId`, `FanLedgerId` — типы с
`parse`/`as_str`, но порт их не знает.

## Станет

- `Aggregate::type Id: Display + FromStr + Clone + Eq + Hash + Send + Sync + 'static`. Хранилища по-прежнему работают
  со строкой: исполнитель один раз вызывает `id.to_string()` (`AggregateKey::of::<A>(&A::Id)`,
  `EventStore::load_aggregate(&str)` не меняется).
- Порт: `execute(&self, id: &A::Id, command)`, `execute_with_metadata(&self, id: &A::Id, command, metadata)` —
  `CommandExecutorCore`, `PostgresCommandExecutor`, `TestCommandExecutor`, `CqrsFramework`.
- `Decision.aggregate_id: &A::Id`; планировщики демо перестают разбирать строки, `TaskPlanError::application` из них
  уходит.
- cqrsx: тестовые агрегаты получают `type Id = String`; вызовы `execute("c-1", ..)` → `execute(&"c-1".into(), ..)`
  на месте вызова, без помощника фикстур: помощник `id(..)` сталкивался бы с локальными `id` и требовал импорта в
  каждом файле (план, В-1); `README.md` — `Ledger::Id = String`.
- Демо: `impl FromStr for ConcertId/FanId/FanLedgerId` (через `parse`), `impl Display` уже есть; `to_string()`
  равен `as_str()` — тест.

## Этапы

1. cqrsx: трейт, порт, ядро, `Decision`, `AggregateKey::of`, пресеты; тесты `tests/**` (`&"c-1".into()` на месте
   вызова).
2. Тесты: `to_string` id уходит в хранилище и в `AggregateKey` без изменений (ключ блокировки — тот же текст);
   `Decision` держит `&A::Id`.
3. Демо: `FromStr`, реакции без `parse`, `write_side.rs`.
4. `components.md`: «Aggregate contract», «Command boundary», «Task planning».

## Зачем, на примере

```rust
let outcome = cqrs.execute::<Concert>(&concert_id, command).await;
// в реакции (07): decided.id: &ConcertId — без ConcertId::parse(..)?
```

## Риски

- **Текст id должен остаться тем же**: `Display` обязан давать строку, которую сегодня даёт `as_str()`; иначе
  меняются ключи журнала и id задач (G-13). Тест на равенство — часть задачи.
- **Объём механической правки** (~190 вызовов): по файлу, с `cargo test`.

## Как проверить

- Проверки `CLAUDE.md`, матрица фич, покрытие.
- Демо: тесты; `write_side.rs` на PostgreSQL.

## Поведение не меняется: чем доказано

Ключ блокировки и ключ хранилища — тот же текст (тест равенства `Display`/`as_str`), поэтому G-01, G-03 и G-13
(`Causation::task_id` от текста id) держат прежние тесты без изменений утверждений; журнал и outbox записывают те
же строки.

## Критерии приёмки

- [x] `Aggregate::Id`; порт и все исполнители принимают `&A::Id`; `Decision.aggregate_id: &A::Id`.
- [x] Планировщики демо без разбора строк; `TaskPlanError::Application` в демо не встречается.
- [x] Тест: `to_string()` каждого id демо равен `as_str()`.
- [x] `components.md`, README cqrsx.

## Результат

Сделано 2026-10-27.
- **cqrsx:**
  - `Aggregate::type Id: Display + FromStr + Clone + Eq + Hash + Send + Sync + 'static`;
  - порт `CommandExecutor` и `CommandExecutorCore`, `PostgresCommandExecutor`, `TestCommandExecutor`, `CqrsFramework`
    принимают `&A::Id`; `Decision.aggregate_id: &A::Id`.
- **Хранилища по-прежнему знают строку** — `Display` id:
  - ядро форматирует его один раз на команду (`AggregateKey::of`), до блокировки;
  - каждая попытка берёт текст у блокировки (`pub(crate) AggregateLockGuard::key`, `CqrsFramework::attempt`);
  - число операций на пути команды прежнее;
  - `CqrsFramework::execute_with_metadata`, вызванный напрямую, форматирует один раз на вызов.
- **У 65 тестовых агрегатов cqrsx** `type Id = String` (решение 8); вызовы — `execute(&"c-1".into(), ..)` на месте,
  без помощника.
- **Демо:**
  - `FromStr` у `ConcertId`, `FanId`, `FanLedgerId` (через `parse`);
  - `Concert::Id = ConcertId`, `Fan::Id = FanLedgerId`;
  - реакции не разбирают строк, `TaskPlanError::Application` в демо нет;
  - `CommandService::run(id, label, ..)`, подпись ленты — прежний `as_str()`.
- **Тесты:**
  - cqrsx `the_id_is_formatted_once_per_command_and_its_text_keys_the_lock_the_store_and_the_task_ids`: ключ
    блокировки, загрузки и `task_id` — `Display` id; одно форматирование на команду из двух попыток; решение держит
    id вызывающего;
  - демо `the_display_of_every_id_is_its_text_and_parses_back` (`to_string() == as_str()`);
  - демо `a_command_for_a_malformed_concert_id_is_refused_before_it_runs` (400).
- **Тесты «чужой id отказывает»** переименованы в `each_fact_plans_only_its_own_task`: отказ держат тип и граница
  HTTP.
- **Сверка утверждений:** 788 / 29 / 0 / 2 / 0 / 3. Эталоны 02 без изменений; SQL, окружение и умолчания не тронуты.
- **Тесты и покрытие:** cqrsx 667 тестов, демо 155 + 14; покрытие — workspace 97,80 %, cqrsx 97,70 %.
