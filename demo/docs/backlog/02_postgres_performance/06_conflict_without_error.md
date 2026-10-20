# 06. Конфликт записи без исключения

| | |
|---|---|
| Статус | предложено |
| Ценность | низкая–средняя: заметна только при конфликтах, то есть на горячем ключе |
| Цена | низкая |
| cqrsx | `postgres/outbox/write.rs`, тесты `tests/postgres/outbox/concurrency.rs`, `tests/postgres/event_store/concurrency.rs` |
| demo | нет |
| Зависимости | 01 для замера. После 08 конфликтов внутри одного процесса почти не будет, но с несколькими экземплярами бэкенда задача остаётся полезной |
| Где выигрыш | 01 (горячий ключ) |

## Сейчас

Запись команды — один CTE (`outbox/write.rs:65`, `persist_one`). Конфликт версий распознаётся по **исключению**:
вставка события с занятым `(aggregate_type, aggregate_id, sequence)` падает с `23505` на `events_pkey`, и
`map_sqlx_error` превращает это в `OptimisticLockError`.

```sql
WITH written_events AS (INSERT INTO events (...) VALUES (...) RETURNING sequence),
     written_tasks  AS (INSERT INTO transactional_outbox ... WHERE EXISTS (SELECT 1 FROM written_events) ...)
SELECT 1::bigint AS snapshot_count            -- without a snapshot the conflict is only the error
```

Каждое исключение в PostgreSQL — это отменённый оператор и запись в лог сервера. По умолчанию
(`log_min_messages = warning`, `log_min_error_statement = error`) Postgres пишет строку `ERROR: duplicate key …`,
`DETAIL` с ключом и `STATEMENT` с полным текстом CTE.

## Станет

В записи **без снапшота** (9 команд из 10 при `ENCORE_SNAPSHOT_EVERY=10`) конфликт распознаётся по числу вставленных
строк:

```sql
WITH written_events AS (
    INSERT INTO events (...) VALUES (...)
    ON CONFLICT (aggregate_type, aggregate_id, sequence) DO NOTHING
    RETURNING sequence),
     written_tasks AS (... WHERE EXISTS (SELECT 1 FROM written_events) ...)
SELECT (SELECT count(*) FROM written_events)::bigint AS written   -- 0 → OptimisticLockError
```

Задачи и так защищены `WHERE EXISTS (written_events)`: при конфликте их тоже не будет.

Запись **со снапшотом** не меняется. Там исключение нужно: оно откатывает снапшот вместе с событием. Если бы
событие «тихо» не вставилось, а снапшот записался, в базе остался бы снапшот с состоянием, которого нет в журнале.
При одинаковом интервале снапшотов у всех писателей такого не бывает, но гарантировать это для чужих конфигураций
нельзя.

## Зачем, на примере

Горячий ключ, 40 VU (серия `c1-`): ~1025 ответов 409/с, и каждый — три неудачные попытки записи. Это больше 3 тыс.
исключений в секунду, столько же записей `ERROR` + `STATEMENT` в лог Postgres и столько же отменённых операторов. После
правки 9 из 10 таких попыток завершаются обычным ответом «0 строк», без лога и без отмены.

## Риски

- Ветвление в построителе SQL: два варианта финального `SELECT`. Тесты — на оба.
- `map_sqlx_error` остаётся, он нужен для записи со снапшотом.

## Как измерить

- Матрица 01 против `perf-base-`: `cpu_ms_per_1k_accepted` у PostgreSQL, принятых команд/с.
- Число строк `duplicate key` в логе контейнера PostgreSQL за прогон: стенд сохраняет лог, если `run.sh` ещё этого не
  делает — добавить.

## Критерии приёмки

- [ ] Запись без снапшота распознаёт конфликт по `written = 0`. Запись со снапшотом — без изменений.
- [ ] Тесты: конкурентная запись без снапшота даёт `OptimisticLockError` без задач; со снапшотом — по-прежнему
      `OptimisticLockError` без снапшота, без события и без задач.
- [ ] Тест: после конфликта без снапшота в логе сервера нет `duplicate key`, или, проще, `pg_stat_database.xact_rollback`
      не растёт.
- [ ] Замер против базовой линии записан в тикет.
