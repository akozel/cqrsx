# 03. Номер события параметром запроса

| | |
|---|---|
| Статус | предложено |
| Ценность | средняя: до 2 лишних обращений на загрузку агрегата со снапшотом |
| Цена | низкая |
| cqrsx | `postgres/event_repository/sql_query.rs`, `postgres/event_repository/read.rs`, `postgres/event_repository.rs` |
| demo | нет |
| Зависимости | 01 для замера |
| Где выигрыш | 01 (горячий ключ), задачи outbox по большим агрегатам; 02b немного |

## Сейчас

```rust
// cqrsx: postgres/event_repository/sql_query.rs:83
pub fn select_events_after(&self, after_sequence: i64) -> String {
    format!("SELECT ... FROM {} WHERE aggregate_type = $1 AND aggregate_id = $2 AND sequence > {} ORDER BY sequence",
        self.event_table, after_sequence)
}
```

Тип и id агрегата передаются параметрами, а номер, после которого читать, вклеен в текст литералом.

sqlx держит на каждом соединении кэш разобранных запросов на 100 записей, и ключ кэша — текст запроса. Для него
`sequence > 30` и `sequence > 40` — два разных запроса. Каждый новый текст стоит отдельного обращения (`Parse` +
`Describe`, Postgres разбирает и планирует запрос). Когда кэш полон, нужно ещё одно — закрыть выселенную запись
(`sqlx-postgres 0.8.6`, `connection/executor.rs`, `get_or_prepare`).

## Станет

```rust
// sql_query.rs: one text for every watermark
select_events_after: format!(
    "SELECT ... FROM {event_table}
      WHERE aggregate_type = $1 AND aggregate_id = $2 AND sequence > $3
      ORDER BY sequence"),

// read.rs
sqlx::query(self.query_factory.select_events_after())
    .bind(A::TYPE)
    .bind(aggregate_id)
    .bind(encode_bigint(after_sequence)?)
```

`select_events` сейчас принимает готовый текст и привязывает два параметра. Ему нужен вариант с третьим параметром,
или общая функция, которая получает уже собранный `Query`.

## Зачем, на примере

Горячий концерт: номер снапшота растёт каждые 10 команд — 30, 40, 50… Команды раскиданы по 20–40 соединениям,
поэтому почти каждое соединение видит очередной текст впервые. Чтение «хвоста» стоит до трёх обращений вместо
одного: разбор, выселение, выполнение. Все три приходятся на окно гонки (М3).

В 02b эффект меньше. Номер вклеен, но id агрегата — параметр, поэтому текст `sequence > 30` общий для всех
концертов. В концерте не больше ~100 событий, различных текстов около десятка, и они быстро оседают в кэше.

## Риски

Нет: семантика запроса та же, меняется только способ передачи числа.

## Как измерить

- Матрица 01 и 03a против `perf-base-`: `packets_per_command` и 409/с ниже, принятых команд/с больше.
- В `pg_statements.tsv` вместо сотен строк `... sequence > <N>` одна строка с `$3`.

## Критерии приёмки

- [ ] Текст `select_events_after` не зависит от номера (юнит-тест в `sql_query.rs`: две разные отметки дают один
      текст с `$3`).
- [ ] Интеграционный тест: пул на одно соединение, `get_events_after` с десятью разными отметками, затем на том же
      соединении `SELECT count(*) FROM pg_prepared_statements WHERE statement LIKE '%sequence > $3%'` возвращает 1.
- [ ] Существующие тесты `get_events_after` (исключающая нижняя граница) проходят без правок.
- [ ] Замер против базовой линии записан в тикет.
