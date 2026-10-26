# 04. Мелкие траты CPU на команду

| | |
|---|---|
| Статус | предложено |
| Аудит | C-4, первые пункты (разбор JSON без промежуточного `Value` — задача [13](13_json_without_value.md)) |
| Ценность | низкая–средняя: часть этой работы идёт внутри блокировки горячего ключа; в профиле `app` CPU бэкенда — предел |
| Цена | низкая |
| cqrsx | `src/postgres/{command.rs, outbox.rs, event_repository.rs}`, `src/cqrs/store/persisted.rs`, `src/cqrs/event_sink.rs`, `doc/components.md` |
| demo | нет (API, которым пользуется демо, не меняется) |
| Зависимости | 01 |
| Где выигрыш | CPU бэкенда на команду (`02b` в `app`), горячий ключ |

## Сейчас

Три траты на каждую команду, которые ничего не дают.

1. **SQL собирается заново на каждую команду.** `PostgresFrameworkFactory::framework` (`postgres/command.rs:79`) создаёт
   `PostgresEventRepositoryWithOutbox::new(pool)`. Тот создаёт `PostgresEventRepository::new(pool)` (`outbox.rs:32`), а
   тот — `SqlQueryFactory::new("events", "snapshots")` (`event_repository.rs:131`). Это 12 `format!` строк SQL длиной в
   сотни байт на каждую команду, хотя тексты одни и те же для всего процесса.
2. **Снапшот копируется целиком перед разбором.** `TryFrom<SerializedSnapshot> for EventStoreAggregateContext`
   (`store/persisted.rs:593`) вызывает `serde_json::from_value(snapshot.aggregate.clone())`, хотя снапшот передан по
   значению. Для зала на 40 мест это копия дерева JSON из сотен узлов на каждую загрузку.
3. **Асинхронный мьютекс в `EventSink`.** `EventSink::write` (`event_sink.rs:24`) берёт `tokio::sync::Mutex` ради
   `Vec::push`. Мьютекс не держится через `.await`, поэтому хватит `std::sync::Mutex`.

## Станет

1. Тексты SQL строятся один раз. `PostgresEventRepository` держит `Arc<SqlQueryFactory>`. Фабрика исполнителя создаёт
   репозиторий при сборке, а на команду только клонирует его — копия пула и `Arc`. Открытый API
   (`PostgresEventRepository::new`, `with_tables`, `PostgresEventRepositoryWithOutbox::new`) остаётся.
2. `serde_json::from_value(snapshot.aggregate)` без клона.
3. `std::sync::Mutex` в `EventSink`. Сигнатуры `write` и `collect` остаются асинхронными: это API агрегатов.

## Зачем, на примере

На горячем концерте команды идут строго по одной, и всё, что команда делает под блокировкой, умножается на ~700 в
секунду. Копия снапшота и 12 строк SQL — мелочь на команду, но эта мелочь никому не нужна.

## Риски

Нет: поведение не меняется. Тесты репозитория на свои имена таблиц (`custom_tables_reach_every_query`) остаются.

## Как измерить

`02b-spread-vus50` и `01-hot-key-vus40`, профиль `app`, «до» и «после» вперемешку, `REPEAT=3`. Смотреть CPU бэкенда на
1000 команд и принятых в секунду. Эффект может оказаться в пределах шума. Тогда правка всё равно остаётся ради
простоты, если пользователь согласен.

## Критерии приёмки

- [ ] `SqlQueryFactory` создаётся один раз на репозиторий исполнителя; тест: две команды одного исполнителя делят
      один экземпляр.
- [ ] Клона снапшота нет; тест загрузки со снапшотом и с несовместимым снапшотом (откат к полной истории) проходит.
- [ ] `EventSink` на `std::sync::Mutex`.
- [ ] Замер записан в тикет.
