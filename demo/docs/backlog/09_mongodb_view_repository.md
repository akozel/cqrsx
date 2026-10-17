# 09. Репозиторий витрин для MongoDB (условно)

| | |
|---|---|
| Статус | предложено, **требует решения**: будут ли реальные проекты хранить витрины в MongoDB |
| Ценность | условная: высокая при да, нулевая при нет |
| Цена | высокая: тяжёлая optional-зависимость, строка в матрице фич, контейнер MongoDB в CI ради порога покрытия 97 % |
| cqrsx | `Cargo.toml` (feature `mongodb`), `mongodb/view_repository.rs` (новый), CI |
| demo | `infrastructure/mongo/view_repository.rs` сводится к импорту |
| Зависимости | тикет 07 желателен (тогда реализуется и `ViewStore`) |

## Сейчас в demo

Файл в demo уже generic, он не знает ни про концерты, ни про фанатов:

```rust
// demo: infrastructure/mongo/view_repository.rs
pub struct MongoViewRepository<V, A> { collection: Collection<Document>, phantom: PhantomData<fn() -> (V, A)> }

impl<V: View<A>, A: Aggregate> ViewRepository<V, A> for MongoViewRepository<V, A> {
    async fn update_view(&self, view: V, context: ViewContext) -> Result<(), PersistenceError> {
        if context.version == 0 {
            // insert; duplicate key 11000 means somebody else was first
            return match self.collection.insert_one(doc! { "_id": &context.view_instance_id, "version": next, "view": view }).await {
                Ok(_) => Ok(()),
                Err(error) if is_duplicate_key(&error) => Err(PersistenceError::OptimisticLockError),
                Err(error) => Err(connection_error(error)),
            };
        }
        let result = self.collection
            .update_one(doc! { "_id": &context.view_instance_id, "version": context.version }, doc! { "$set": { "view": view, "version": next } })
            .await.map_err(connection_error)?;
        if result.matched_count == 0 { return Err(PersistenceError::OptimisticLockError); }
        Ok(())
    }
}
```

## Станет в cqrsx

Тот же файл под флагом:

```toml
# cqrsx/Cargo.toml
[features]
mongodb = ["dep:mongodb", "dep:futures"]

[dependencies]
mongodb = { version = "3", optional = true }
```

```rust
// cqrsx: lib.rs
#[cfg(feature = "mongodb")]
pub mod mongodb;   // mongodb::view_repository::MongoViewRepository<V, A>
```

Формат документа `{ _id, version, view }` и отображение ошибок становятся контрактом библиотеки.

## Станет в demo

Меняется только путь импорта; `infrastructure/mongo/view_repository.rs` остаётся лишь для HTTP-порта чтения (если
тикет 07 не сделан) или удаляется целиком.

## Зачем, на примере

Если следующий проект тоже хранит витрины в Mongo, это первое, что вы скопируете из demo, и копия начнёт жить своей
жизнью. Если витрины будут в Postgres, адаптер станет мёртвым кодом с дорогим CI. Решение зависит от планов, а не от
кода.

## Критерии приёмки

- [ ] Решение принято и записано здесь (да / нет / отложить).
- [ ] При «да»: feature `mongodb` компилируется и тестируется отдельно и вместе с остальными; интеграционный тест через testcontainers MongoDB: insert, конфликт версии, duplicate key, BSON round-trip.
- [ ] `components.md`: строка «Event persistence» или новая строка «MongoDB adapters».
- [ ] demo: импорт переведён на библиотеку, `make check` зелёный.
