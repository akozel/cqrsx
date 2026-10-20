# 11. Уборка завершённых задач outbox

| | |
|---|---|
| Статус | предложено |
| Ценность | средняя: не ускоряет сейчас, а не даёт замедлиться через неделю работы |
| Цена | низкая–средняя |
| cqrsx | `postgres/task_queue.rs` (новая операция), `sql/002-transactional-outbox.sql` (индекс), `sql/` (ассет для оператора), `task/runtime/` (необязательный периодический вызов), `doc/components.md` |
| demo | `back-end/src/main.rs` или `config.rs` (включение), `sidecar` (проверить X-ray) |
| Зависимости | 10 для замера |
| Где выигрыш | долгая работа: размер таблицы, autovacuum, бэкапы |

## Сейчас

В `cqrsx` нет ни одного `DELETE FROM transactional_outbox`. Каждая задача за жизнь трижды записывает строку
(вставка, claim, settle), оставляет две мёртвые версии для autovacuum и навсегда остаётся в таблице со статусом
`completed`. Partial-индексы (`WHERE status = 'pending'` / `'processing'`) держат claim быстрым, но таблица, её
первичный ключ и работа autovacuum растут без предела.

Кто читает завершённые задачи: X-ray в сайдкаре показывает последние строки outbox и счётчики по статусам
(`demo/sidecar/src/postgres.rs:21`, `:203`). «Encore!» очищает таблицу целиком (`TRUNCATE`).

## Станет

Операция библиотеки — удалить завершённые задачи старше порога небольшими пачками:

```rust
// cqrsx: postgres/task_queue.rs
impl PostgresTaskQueue {
    /// Deletes up to `limit` completed tasks finished before `older_than`; failed tasks stay for operators.
    pub async fn purge_completed(&self, older_than: Duration, limit: usize) -> Result<u64, TaskProcessingError> { … }
}
```

```sql
DELETE FROM transactional_outbox WHERE id IN (
    SELECT id FROM transactional_outbox
     WHERE status = 'completed' AND finished_at < statement_timestamp() - $1 * interval '1 millisecond'
     ORDER BY finished_at LIMIT $2)
```

Варианты, решить при реализации:

| Вариант | Как | Плюс | Минус |
|---|---|---|---|
| A. Пачки `DELETE` + индекс | partial-индекс `(finished_at) WHERE status = 'completed'` | просто, схема почти та же | лишняя запись в индекс на каждый settle; удаление тоже даёт мёртвые строки |
| B. Партиции по дням | `PARTITION BY RANGE (created_at)`, удаление старой партиции целиком | мгновенно, без мусора | сложнее схема; `failed` и долгие `pending` в старой партиции нужно сначала перенести |

Предлагаю начать с A и измерить, сколько стоит индекс на settle (03a). B — если A не справится на soak.

Кто вызывает: библиотека даёт операцию и необязательную настройку runtime (`purge_completed_after`, по умолчанию
выключена). Поллер раз в N минут вызывает уборку пачками, пока она что-то удаляет. `failed` не трогаются никогда:
их перезапускает оператор (`requeue-failed-outbox.sql`).

## Зачем, на примере

3 тыс. броней/с × 2 задачи = 21.6 млн строк за час. Через сутки работы autovacuum постоянно сканирует сотни
миллионов строк, бэкапы и восстановление растут, а X-ray считает `count(*)` по всей таблице. С уборкой «старше
суток» таблица держится на уровне суточного объёма.

## Риски

- **Demo UX.** В X-ray пропадут старые завершённые задачи. Для dev-стенда уборку оставить выключенной или с большим
  порогом. На soak-стенде включить.
- **Индекс варианта A** — дополнительная запись на горячем пути settle. Замерить на 03a.
- **Совместимость схемы:** новый индекс через `CREATE INDEX IF NOT EXISTS` (для существующих баз —
  `CONCURRENTLY` в инструкции оператора).

## Как измерить

Soak (задача 10) с уборкой и без:
- размер `transactional_outbox` и индексов во времени;
- `n_dead_tup`, частота autovacuum;
- латентность settle и claim к концу часа.

Плюс 03a против `perf-base-`, чтобы увидеть цену индекса.

## Критерии приёмки

- [ ] `purge_completed` с интеграционными тестами: удаляет только `completed` старше порога, не трогает `failed`,
      `pending`, `processing`; соблюдает `limit`.
- [ ] Необязательная периодическая уборка в runtime: тест с паузой времени tokio.
- [ ] SQL-ассет для оператора, как `requeue-failed-outbox.sql`.
- [ ] Решение по варианту A/B записано в тикет. `components.md` обновлён.
- [ ] Soak с уборкой и без, выводы в тикете.
