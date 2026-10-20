# 04. Операции очереди одним запросом, свой пул

| | |
|---|---|
| Статус | предложено |
| Ценность | высокая для outbox: settle и claim — 1 обращение вместо 5, recover — 1 вместо N+3 |
| Цена | средняя: ломающее изменение API сборки очереди |
| cqrsx | `postgres/task_queue.rs`, `postgres/task_queue/{claim,settlement,recovery}.rs`, `postgres/pool.rs`, `postgres/task_runtime.rs`, тесты `tests/postgres/task_queue/*`, `doc/components.md` |
| demo | `back-end/src/main.rs`, `back-end/src/config.rs`, `.env.example`, `load/compose.yaml`, `load/run.sh` |
| Зависимости | 01 для замера; 02 усиливает эффект |
| Где выигрыш | 03a (слив outbox); косвенно API во время слива |

## Сейчас

Каждая операция очереди открывает транзакцию только ради таймаута:

```rust
// cqrsx: postgres/task_queue.rs:36
async fn begin_queue_transaction(&self, operation: &'static str) -> Result<Transaction<'_, Postgres>, _> {
    let mut tx = self.pool.begin().await?;                                    // ping + BEGIN
    sqlx::query("SELECT set_config('statement_timeout', $1, true)")          // + 1
        .bind(self.statement_timeout.as_millis().to_string())
        .execute(&mut *tx).await?;
    Ok(tx)
}
// claim / settle: one statement + COMMIT
```

Claim и settle — одиночные SQL-запросы, атомарные сами по себе. Транзакция нужна только для того, чтобы таймаут
действовал локально и не «протёк» в общий пул. В итоге 5 обращений (пинг, `BEGIN`, `set_config`, запрос, `COMMIT`)
вместо одного.

`recover_expired` хуже: `SELECT … FOR UPDATE SKIP LOCKED`, затем по отдельному `UPDATE` на каждую строку в цикле
(`recovery.rs`). Это N+3 обращения, и такая операция выполняется на каждом тике poller'а.

## Станет

У очереди свой пул, и таймаут задаётся на уровне его соединений, а не транзакции:

```rust
// cqrsx: postgres/pool.rs
/// A pool whose sessions carry the queue's statement timeout; only `connect_queue_pool` builds it.
pub struct QueuePool(PgPool);

pub async fn connect_queue_pool(
    options: PgConnectOptions,
    max_connections: NonZeroU32,
    config: &PostgresTaskQueueConfig,
) -> Result<QueuePool, sqlx::Error> {
    let options = options.options([("statement_timeout", config.statement_timeout().as_millis().to_string())]);
    // same pool settings as connect_pool_with_size (incl. ticket 02)
}

// cqrsx: postgres/task_queue.rs
impl PostgresTaskQueue {
    pub fn new(pool: QueuePool) -> Self { … }
}
// claim / settle: sqlx::query(...).fetch_all(&self.pool) — autocommit, one round trip
```

- **Тип `QueuePool`.** Гарантирует при компиляции, что очередь не получит пул без таймаута. Обычный `PgPool` в
  `PostgresTaskQueue::new` больше не принимается.
- **`prepare_task_runtime(queue_pool, dispatcher, runtime_config)`.** `PostgresTaskQueueConfig` переезжает в
  создание пула.
- **`recover_expired` одним запросом:** `WITH expired AS (SELECT … FOR UPDATE SKIP LOCKED) UPDATE … FROM expired
  RETURNING …`. Бэкофф сейчас считает Rust: `redelivery_backoff(attempts)`, 5 с × 2^(n−1), не больше 300 с, джиттер
  ±20 %. Варианты, решить при реализации:
  - формула в SQL с `random()` и тестом, что границы совпадают с `redelivery_backoff`;
  - массив задержек по номеру попытки (`$2::float8[]`), посчитанный на каждый вызов. Тогда у задач с одинаковым
    номером попытки в одной пачке джиттер общий.

Demo создаёт два пула:
- **командный** `ENCORE_DATABASE_POOL_SIZE`: HTTP и команды, которые выполняют обработчики задач;
- **очереди** `ENCORE_QUEUE_POOL_SIZE`: poller и settle, по умолчанию «воркеры + 1».

Воркеры пользуются обоими: claim и settle идут через пул очереди, команда `Fan` — через командный. Это закрывает п. 4
из [discussions/05](../../discussions/05_outbox_task_throughput.md): воркеры перестают отбирать соединения у HTTP.

## Зачем, на примере

Задача `encore_record_ticket` по разбору в discussions/05 стоит ~3 мс, пропускная способность = воркеры ÷ латентность.
Сейчас на задачу приходится settle (5 обращений) и доля claim (5 обращений на пачку). После правки settle — одно
обращение. Если команда `Fan` внутри задачи уже ускорена задачами 02 и 03, доля очереди в латентности задачи
заметна. Оценка: в 1.5–2 раза больше задач/с на воркер, пока база не насыщена. Проверит замер.

## Риски

- **Ломающее изменение API** (`PostgresTaskQueue::new`, `prepare_task_runtime`) — описать в теле коммита.
- **Сессионный таймаут.** Действует на всё, что выполняется на соединениях пула очереди. Пул отдельный, кроме
  очереди там ничего нет, и это нужно записать в doc-комментарий `QueuePool`.
- **Семантика lease, fencing и «исход неизвестен при таймауте» не меняется**: меняется только упаковка запросов.
  Тесты `tests/postgres/task_queue/{claim,settlement,timeouts,recovery}.rs` — сторожа.

## Как измерить

Матрица 03a (`WORKERS=2/8/32`) против `perf-base-`: задач/с, время слива, RPS API во время всплеска. В
`pg_statements.tsv` пропадают `BEGIN`, `COMMIT` и `set_config`.

## Критерии приёмки

- [ ] `QueuePool` и `connect_queue_pool`. Claim и settle — один запрос без явной транзакции, recover — один запрос.
- [ ] Таймаут действует: существующий тест таймаутов проходит на новом пуле, и есть тест, что
      `SHOW statement_timeout` на соединении пула очереди равен конфигу, а на обычном пуле — нет.
- [ ] Тест: recover одним запросом даёт те же переходы (`pending` с бэкоффом / `failed` на последней попытке), что и
      сейчас.
- [ ] Demo: два пула, `ENCORE_QUEUE_POOL_SIZE` в `config.rs` и `.env.example`. Стенд: параметр `QUEUE_POOL=` в
      `run.sh`.
- [ ] `components.md`: строки «PostgreSQL queue» и «Backend assembly».
- [ ] Замер против базовой линии записан в тикет.
