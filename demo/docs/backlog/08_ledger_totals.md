# 08. Размер журнала событий

| | |
|---|---|
| Статус | предложено |
| Ценность | низкая, но убирает raw SQL приложения по таблице библиотеки |
| Цена | низкая |
| cqrsx | `postgres/event_repository/read.rs` |
| demo | `infrastructure/postgres.rs::PostgresLedger` |
| Зависимости | нет |

## Сейчас в demo

Demo считает строки в таблице, которой владеет библиотека:

```rust
// demo: infrastructure/postgres.rs
async fn totals(&self) -> Result<LedgerTotals, StorageError> {
    let events: i64 = sqlx::query_scalar("SELECT count(*) FROM events").fetch_one(&self.pool).await.map_err(StorageError::new)?;
    let aggregates: i64 = sqlx::query_scalar("SELECT count(DISTINCT (aggregate_type, aggregate_id)) FROM events").fetch_one(&self.pool).await.map_err(StorageError::new)?;
    Ok(LedgerTotals { events: events.max(0) as u64, aggregates: aggregates.max(0) as u64 })
}
```

## Станет в cqrsx

```rust
// cqrsx: postgres/event_repository/read.rs
pub struct LedgerTotals { pub events: u64, pub aggregates: u64 }

impl PostgresEventRepository {
    /// Size of the saved history of one aggregate type, for progress reporting.
    pub async fn totals<A: Aggregate>(&self) -> Result<LedgerTotals, PersistenceError> {
        // SELECT count(*), count(DISTINCT aggregate_id) FROM events WHERE aggregate_type = $1
    }
}
```

## Станет в demo

```rust
let concerts = repository.totals::<Concert>().await?;
let fans = repository.totals::<Fan>().await?;
LedgerTotals { events: concerts.events + fans.events, aggregates: concerts.aggregates + fans.aggregates }
```

Порт `EventLedgerStats` остаётся в приложении (он object-safe и нужен `RebuildService`), его реализация сводится к
двум вызовам библиотеки.

## Зачем, на примере

Если библиотека переименует таблицу или добавит партиционирование, demo продолжит компилироваться и упадёт в рантайме
при первом Encore!. Кто владеет таблицей, тот и считает в ней строки.

## Критерии приёмки

- [ ] `PostgresEventRepository::totals::<A>()` с интеграционным тестом (пустая таблица, два агрегата одного типа, чужой тип не считается).
- [ ] `components.md`: строка «Event persistence» упоминает статистику.
- [ ] demo: raw SQL из `infrastructure/postgres.rs` удалён.
