# 02. Позиция события в журнале

| | |
|---|---|
| Статус | предложено |
| Ценность | высокая: без сквозного порядка проекция не может читать журнал сама; попутно replay без сортировки (закрывает 02/14) |
| Цена | низкая–средняя |
| cqrsx | `sql/001-event-store.sql`, новый `sql/upgrade-event-position.sql`, `postgres.rs` (ассеты), `postgres/event_repository/{sql_query,read}.rs`, `cqrs/store/repository.rs` (`feed_after`, `head`), новый `cqrs/store/feed.rs` (`Position`, `FeedEntry`), `cqrs/testing/mem_store.rs`, `doc/components.md`; тесты `tests/postgres/event_store/*`, `tests/postgres/compatibility.rs` |
| demo | нет кода; сценарий обновления базы в README |
| Зависимости | нет |
| Где выигрыш | [03](03_projection_processor.md); rebuild и verify (вместо 02/14) |

## Сейчас

`events` упорядочен только внутри агрегата: первичный ключ `(aggregate_type, aggregate_id, sequence)`, колонки
времени или глобального номера нет. «Все события типа» читаются одним запросом `ORDER BY sequence` поперёк
агрегатов с сортировкой всей истории (02/14). Между агрегатами порядка нет, и вопрос «дай всё новое» задать некому.

## Станет

### Схема

```sql
-- sql/001-event-store.sql: новая база
CREATE TABLE IF NOT EXISTS events
(
    ...,
    tx xid8 NOT NULL DEFAULT pg_current_xact_id(),   -- транзакция, записавшая событие
    PRIMARY KEY (aggregate_type, aggregate_id, sequence)
);
CREATE INDEX IF NOT EXISTS events_feed_idx ON events (aggregate_type, tx, aggregate_id, sequence);
```

```sql
-- sql/upgrade-event-position.sql: существующая база.
-- Шаг 1 не переписывает таблицу: умолчание-константа хранится как «missing value». Старая история получает
-- tx = 1 (меньше любого настоящего номера транзакции) и читается первой, в порядке (aggregate_id, sequence).
-- Шаг 3 выполнять вне транзакции (CONCURRENTLY).
ALTER TABLE events ADD COLUMN IF NOT EXISTS tx xid8 NOT NULL DEFAULT '1';
ALTER TABLE events ALTER COLUMN tx SET DEFAULT pg_current_xact_id();
CREATE INDEX CONCURRENTLY IF NOT EXISTS events_feed_idx ON events (aggregate_type, tx, aggregate_id, sequence);
```

Путь записи не меняется: CTE в `outbox/write.rs` и `insert_event` колонку не перечисляют, умолчание заполняет её
внутри той же транзакции; все события одного коммита получают один номер. Цена — одна запись в индекс на событие,
в его правый край.

### Чтение

```rust
// cqrs/store/feed.rs
pub struct Position(u64);       // номер транзакции; Ord, Display/FromStr десятичные; Position::START = 0
pub struct FeedEntry { pub position: Position, pub event: SerializedEvent }

// cqrs/store/repository.rs
pub trait PersistedEventRepository {
    ...
    /// События типа `A` после `after` и ниже горизонта снимка, в порядке (tx, aggregate_id, sequence):
    /// не больше `limit` строк, но всегда до конца последней начатой транзакции.
    fn feed_after<A: Aggregate>(&self, after: Position, limit: usize)
        -> impl Future<Output = Result<Vec<FeedEntry>, PersistenceError>> + Send;
    /// Наибольшая позиция среди записанных событий типа `A` (голова журнала); `Position::START` для пустого.
    fn head<A: Aggregate>(&self) -> impl Future<Output = Result<Position, PersistenceError>> + Send;
}
```

```sql
-- postgres/event_repository/sql_query.rs. sqlx не знает тип xid8: параметры и результат идут текстом.
-- feed_after
SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata, tx::text AS position
  FROM events
 WHERE aggregate_type = $1 AND tx > $2::text::xid8 AND tx < pg_snapshot_xmin(pg_current_snapshot())
 ORDER BY tx, aggregate_id, sequence
 LIMIT $3;
-- дочитать последнюю транзакцию страницы, если LIMIT оборвал её внутри
SELECT ... FROM events
 WHERE aggregate_type = $1 AND tx = $2::text::xid8 AND (aggregate_id, sequence) > ($3, $4)
 ORDER BY aggregate_id, sequence;
-- head (max(xid8) есть с PostgreSQL 15; в compose — 18)
SELECT coalesce(max(tx), '0')::text FROM events WHERE aggregate_type = $1;
```

Почему горизонт, а не счётчик:
- Счётчик `bigserial` выдаёт номера в порядке запроса, а транзакции коммитятся в другом порядке: читатель,
  увидевший 506, ставит закладку за 505, который закоммитится позже, и 505 потерян навсегда. Marten и Axon
  лечат это эвристиками с таймаутами.
- `xmin` снимка — наименьший номер транзакции, которая в момент снимка ещё шла. Всё ниже либо закоммичено и видно
  этому же оператору, либо откачено навсегда. Поэтому закладка двигается до горизонта без дыр: читатель не
  пропускает, а ждёт.
- Горизонт и строки берутся из одного снимка, потому что это один оператор (autocommit, READ COMMITTED).
- Читающие транзакции номер не получают и горизонт не держат. Держит его пишущая транзакция, зависшая в
  `idle in transaction`: все проекции стоят до её конца. Запросы cqrsx однооператорные; для чужих клиентов —
  `idle_in_transaction_session_timeout` и признак «стою из-за горизонта» в статусе процессора ([03](03_projection_processor.md)).

Почему порядок внутри агрегата сохраняется: событие n+1 пишет команда, загрузившая событие n, то есть после его
коммита; номер транзакции выдаётся при INSERT, ещё позже. Значит `tx(n+1) > tx(n)`.

### `MemStore`

Счётчик коммитов: каждый `commit` получает следующую позицию, все события одного коммита — одну. `feed_after` и
`head` читают этот журнал. Нужно тестам процессора без PostgreSQL. Горизонта у `MemStore` нет: коммит атомарен.

### Контракт порядка

`feed_after` отдаёт события типа в порядке транзакций; внутри агрегата это порядок `sequence`; между агрегатами —
порядок выдачи номеров транзакций, один и тот же при каждом чтении. Старая история после обновления базы идёт
первой, в порядке `(aggregate_id, sequence)`. Контракт записывается в doc-комментарий и `components.md`. 02/14
закрывается этим тикетом: сортировки больше нет, длинной транзакции тоже — каждая страница согласована сама по себе.

## Пограничные случаи

| # | Случай | Поведение | Проверка |
|---|---|---|---|
| 1 | Транзакция с меньшим номером коммитится позже транзакции с большим | пока она идёт, страница не содержит ни одной из них; после — обе по порядку | `a_slower_transaction_with_a_lower_number_is_never_skipped` (в тесте открытая `BEGIN; INSERT` без коммита) |
| 2 | Открытая транзакция откатилась | горизонт уходит выше, события нет, дыры нет | `a_rolled_back_transaction_leaves_no_hole` |
| 3 | Долгая читающая транзакция | горизонт не держится, страницы идут | `a_read_only_transaction_does_not_hold_the_horizon` |
| 4 | Пишущая транзакция зависла | страницы пусты, позиция стоит; после `ROLLBACK` — продолжение без пропуска | `an_idle_writing_transaction_holds_the_feed_until_it_ends` |
| 5 | Страница оборвалась внутри транзакции из 3 событий | дочитана до конца транзакции | `a_page_ends_on_a_transaction_boundary` (`PostgresEventRepository::persist` с тремя событиями) |
| 6 | Параллельные команды разных агрегатов | внутри каждого порядок `sequence`, ни одного пропуска | `forty_parallel_commands_over_five_aggregates_feed_in_sequence_order` |
| 7 | Старая база | ассет обновления на схеме без колонки с данными `framework_v0_5.json`: старые строки первыми, новые после, индекс есть, путь записи работает | `the_upgrade_asset_orders_old_history_first` |
| 8 | Пустой журнал | `head` = `START`, страница пуста | `an_empty_journal_has_no_head` |
| 9 | Другой тип агрегата в той же таблице | не читается и не считается головой | `the_feed_filters_by_aggregate_type` |
| 10 | `with_tables` (свои имена таблиц) | запросы с именами таблицы и индекса репозитория | `custom_table_names_feed_too` |
| 11 | `limit` = 0 или позиция не число | `UnknownError` до запроса | `invalid_feed_arguments_are_refused` |

## Как измерить

Горячий ключ `01-hot-key-vus40` в профиле `db`, код до и после колонки с индексом: ожидание — CPU PostgreSQL на
команду в пределах шума, WAL на команду чуть выше (одна запись в индекс). Replay 1 и 5 млн событий счётчиком, как
задумано в 02/14: время до первого события и `pg_stat_database.temp_bytes = 0`.

## Критерии приёмки

- [ ] Колонка, индекс и ассет обновления; ассет проверен тестом на старой схеме с данными.
- [ ] `Position`, `FeedEntry`, `feed_after`, `head` в трейте, в PostgreSQL и в `MemStore`.
- [ ] Тесты таблицы; покрытие ≥ 97 %.
- [ ] `components.md`: «Event position» в инвентаре, контракт порядка, абзац про горизонт; 02/14 закрыт ссылкой
      сюда.
- [ ] Замер записан в тикет.
