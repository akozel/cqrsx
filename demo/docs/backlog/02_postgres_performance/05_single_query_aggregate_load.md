# 05. Загрузка агрегата одним запросом

| | |
|---|---|
| Статус | предложено |
| Ценность | средняя: минус одно обращение на каждую команду, плюс короче окно гонки |
| Цена | средняя: новый метод трейта с реализацией по умолчанию, SQL с `UNION ALL` |
| cqrsx | `cqrs/store/repository.rs`, `cqrs/store/persisted.rs`, `postgres/event_repository.rs`, `postgres/event_repository/{read,sql_query}.rs`, `postgres/outbox.rs`, `doc/components.md` |
| demo | нет |
| Зависимости | 03 (желательно: запасной путь использует `get_events_after`) |
| Где выигрыш | все сценарии через М1, М2, М3 |

## Сейчас

```rust
// cqrsx: cqrs/store/persisted.rs:420
let snapshot = self.repo.get_snapshot::<A>(aggregate_id).await?;            // round trip 1
// ... restore context from the snapshot ...
SnapshotPolicy::Every(_) => self.load_events_after(aggregate_id, &context).await?,  // round trip 2
```

Второй запрос зависит от ответа первого (номер снапшота), поэтому выполнить их одновременно нельзя. Каждая команда
делает два последовательных обращения на чтение.

## Станет

Новый метод трейта с реализацией по умолчанию. Старые реализации (тестовые репозитории, будущие бэкенды)
продолжают работать без правок:

```rust
// cqrsx: cqrs/store/repository.rs
/// The current snapshot and the events after its watermark (every event when there is none).
fn get_snapshot_with_events<A: Aggregate>(
    &self,
    aggregate_id: &str,
) -> impl Future<Output = Result<(Option<SerializedSnapshot>, Vec<SerializedEvent>), PersistenceError>> + Send {
    async move {
        let snapshot = self.get_snapshot::<A>(aggregate_id).await?;
        let after = snapshot.as_ref().map_or(0, |s| s.last_sequence);
        Ok((snapshot, self.get_events_after::<A>(aggregate_id, after).await?))
    }
}
```

PostgreSQL переопределяет метод одним запросом, и связывает снапшот с событиями сам Postgres:

```sql
WITH snapshot AS (
    SELECT last_sequence, current_snapshot, payload
      FROM snapshots WHERE aggregate_type = $1 AND aggregate_id = $2
)
SELECT 'snapshot' AS kind, last_sequence AS sequence, current_snapshot,
       NULL AS event_type, NULL AS event_version, payload, NULL::json AS metadata
  FROM snapshot
UNION ALL
SELECT 'event', sequence, NULL, event_type, event_version, payload, metadata
  FROM events
 WHERE aggregate_type = $1 AND aggregate_id = $2
   AND sequence > COALESCE((SELECT last_sequence FROM snapshot), 0)
ORDER BY kind DESC, sequence          -- the snapshot row first
```

`PersistedEventStore::load_aggregate` с включённым снапшотом вызывает новый метод. Репозиторий с outbox
делегирует его читателю, как остальные чтения.

**Запасной путь.** Сейчас при ошибке десериализации снапшота (структура агрегата изменилась) контекст начинается с
нуля и читаются **все** события. Новый запрос вернёт только события после снапшота, поэтому в этом редком случае
нужен второй вызов `get_events_after(0)`. Поведение сохраняется, и его стережёт существующий тест
`load_aggregate_after_deserialize_error`.

## Зачем, на примере

Бронь: два чтения + запись = 3 обращения (без пингов, после задачи 02). После правки — 2. Для 02a, где предел —
Postgres, это на треть меньше сообщений на команду (М1). Для горячего ключа окно между чтением и записью короче на
одно обращение (М3).

## Риски

- **Корректность.** События неизменяемы, а снапшот и события читаются из одного снимка MVCC. Новый способ не менее
  корректен, чем два запроса.
- **Типы колонок.** `UNION ALL` требует согласованных типов, поэтому `NULL` нужно явно привести. Декодер строк
  получает поле `kind`. Тесты декодирования — на обе ветви.

## Как измерить

Матрица 01 / 02a / 02b против `perf-base-` (лучше уже после 02 и 03, иначе эффект теряется на фоне пингов).
Ожидание:
- в `pg_statements.tsv` вместо двух запросов на загрузку — один;
- `statements_per_command` у 02a и 02b меньше на 1.

## Критерии приёмки

- [ ] Метод `get_snapshot_with_events` в `PersistedEventRepository` с реализацией по умолчанию и юнит-тестом на
      фейковом репозитории.
- [ ] PostgreSQL-реализация одним запросом. Интеграционные тесты: нет снапшота, снапшот без хвоста, снапшот с
      хвостом, чужой тип агрегата не попадает в результат.
- [ ] Запасной путь при ошибке десериализации снапшота читает все события (существующий тест проходит, плюс тест на
      PostgreSQL).
- [ ] `PostgresEventRepositoryWithOutbox` делегирует метод читателю.
- [ ] `components.md`: строки «Repository-backed store» и «SQL operations».
- [ ] Замер против базовой линии записан в тикет.
