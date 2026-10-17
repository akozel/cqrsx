# 07. Обслуживание витрин: перечислить, очистить, пересобрать, сверить

| | |
|---|---|
| Статус | предложено |
| Ценность | средняя: wipe, rebuild и verify одинаковы для любого хранилища витрин |
| Цена | средняя: новый трейт на репозиториях, реализация и тесты для PostgreSQL |
| cqrsx | `cqrs/projection/repository.rs` (`ViewStore`), `cqrs/projection/maintenance.rs` (новый), `postgres/view_repository.rs` |
| demo | `application/ports.rs` (`ReadModelStore::{list, wipe}`, `Stored`), `application/projections/maintenance.rs`, `infrastructure/mongo/view_repository.rs` |
| Зависимости | тикет 06 для прогресса; тикет 04 даёт `MemViewRepository` для тестов |

## Сейчас в demo

Репозиторий витрин в cqrsx умеет только загрузить и обновить один документ. Для Encore! и verify нужны «все
документы» и «очистить», поэтому demo завело свой порт и написало сверку само:

```rust
// demo: application/ports.rs
pub struct Stored<V> { pub id: String, pub version: i64, pub view: V }   // duplicates (ViewContext, V)

#[async_trait]
pub trait ReadModelStore<V>: Send + Sync {
    async fn load(&self, id: &str) -> Result<Option<Stored<V>>, StorageError>;
    async fn list(&self) -> Result<Vec<Stored<V>>, StorageError>;
    async fn wipe(&self) -> Result<(), StorageError>;
}

// demo: application/projections/maintenance.rs
async fn verify(&self, run_id: Uuid) -> Result<Verification, StorageError> {
    let scratch = (self.scratch)(run_id);
    scratch.wipe().await?;
    let replayed = self.replay_into(scratch.clone(), None, Box::new(|_, _, _| {})).await;
    let rebuilt = scratch.list().await;
    let _ = scratch.wipe().await;                      // always clean up
    replayed?;
    let expected = by_id(rebuilt?);
    let actual = by_id(self.live.list().await?);
    // only in `expected`: "missing"; only in `actual`: "unexpected"; unequal views: "differs"
}
```

## Станет в cqrsx

```rust
// cqrsx: cqrs/projection/repository.rs
/// A view repository that can also be enumerated and emptied.
pub trait ViewStore<V, A>: ViewRepository<V, A> {
    fn list_all(&self) -> impl Future<Output = Result<Vec<(ViewContext, V)>, PersistenceError>> + Send;
    fn clear(&self) -> impl Future<Output = Result<(), PersistenceError>> + Send;
}
impl<V: View<A>, A: Aggregate> ViewStore<V, A> for PostgresViewRepository<V, A> { /* SELECT … ORDER BY view_id; DELETE FROM … */ }

// cqrsx: cqrs/projection/maintenance.rs
pub async fn rebuild<R, S, V, A>(history: R, store: Arc<S>) -> Result<(), PersistenceError>
where R: PersistedEventRepository, S: ViewStore<V, A> + 'static, V: View<A> + 'static, A: Aggregate + 'static
{
    store.clear().await?;
    EventReplay::new(history, ViewProjection::new(store)).replay_all().await
}

pub enum DifferenceKind { Missing, Unexpected, Differs }
pub struct Difference { pub view_id: String, pub kind: DifferenceKind }
pub struct Verification { pub checked: usize, pub differences: Vec<Difference> }

/// Replays into `scratch`, compares with `live` ignoring versions, always clears `scratch`.
pub async fn verify<R, S, V, A>(history: R, live: &S, scratch: Arc<S>) -> Result<Verification, PersistenceError>
where V: View<A> + PartialEq, /* … */ { /* the demo body, made generic */ }
```

Прогресс и темп для `rebuild` подключаются через настройки `EventReplay` из тикета 06 (например, параметром-замыканием,
которое настраивает replay перед запуском).

## Станет в demo

Тело `ProjectionMaintenance` сводится к двум вызовам, а `MongoViewRepository` реализует библиотечный `ViewStore`
вместо порта `ReadModelStore`. Остаются в приложении: объектно-безопасная обёртка `MaintainedReadModel` для списка
витрин в `RebuildService` и порт для HTTP-чтения (`load`/`list` для `AppState`), потому что `ViewStore` native-async
и не object-safe.

## Зачем, на примере

Verify это «пересобери витрину в черновик и сравни с боевой». Операция одинакова для Mongo, Postgres и любого
будущего хранилища, в ней нет ничего про концерты. Единственная причина, почему она в demo: в контракте репозитория
не было двух методов.

## Критерии приёмки

- [ ] `ViewStore` с `list_all`/`clear`; реализация для `PostgresViewRepository` с интеграционным тестом.
- [ ] `maintenance::rebuild` и `maintenance::verify` с тестами на `MemViewRepository`: missing / unexpected / differs, черновик всегда очищен, в том числе после ошибки replay.
- [ ] `components.md`: новая строка «Projection maintenance».
- [ ] demo: `ReadModelStore::{list, wipe}` и `Stored` удалены или сведены к HTTP-порту, `ProjectionMaintenance` делегирует библиотеке, `GET /api/projections/verify` отвечает прежним JSON.
