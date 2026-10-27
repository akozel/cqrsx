# 06. Типизированный id агрегата: `type Id`

| | |
|---|---|
| Статус | предложено 2026-10-27; решение пользователя 2026-10-27 (README эпика, вопрос 8): у тестовых агрегатов cqrsx `type Id = String` |
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
  через помощник фикстур; `README.md` — `Ledger::Id = String`.
- Демо: `impl FromStr for ConcertId/FanId/FanLedgerId` (через `parse`), `impl Display` уже есть; `to_string()`
  равен `as_str()` — тест.

## Этапы

1. cqrsx: трейт, порт, ядро, `Decision`, `AggregateKey::of`, пресеты; тесты `tests/**` (помощник `id("c-1")`).
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

- [ ] `Aggregate::Id`; порт и все исполнители принимают `&A::Id`; `Decision.aggregate_id: &A::Id`.
- [ ] Планировщики демо без разбора строк; `TaskPlanError::Application` в демо не встречается.
- [ ] Тест: `to_string()` каждого id демо равен `as_str()`.
- [ ] `components.md`, README cqrsx.
