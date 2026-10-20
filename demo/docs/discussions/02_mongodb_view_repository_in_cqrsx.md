# 02. Репозиторий витрин для MongoDB в `cqrsx`

Статус: обсуждается. Откуда: аудит C-3, [тикет 09](../backlog/01_cqrsx_extractions/09_mongodb_view_repository.md).
Решение зависит не от кода, а от планов: будут ли реальные проекты на `cqrsx` хранить витрины в MongoDB.

## 1. Контекст

`cqrsx` поставляет один репозиторий витрин — `PostgresViewRepository` (таблица `{ view_id, version, payload }`,
feature `postgres`). Demo хранит витрины в MongoDB, поэтому написало свой `MongoViewRepository`. Он уже generic
и не знает о домене:

```rust
// back-end: infrastructure/mongo/view_repository.rs (≈ 170 строк с тестами)
pub struct MongoViewRepository<V, A> { collection: Collection<Document>, phantom: PhantomData<fn() -> (V, A)> }

impl<V: View<A>, A: Aggregate> ViewRepository<V, A> for MongoViewRepository<V, A> {
    async fn load_with_context(&self, view_id: &str) -> Result<Option<(V, ViewContext)>, PersistenceError> { /* find_one { _id } */ }
    async fn update_view(&self, view: V, context: ViewContext) -> Result<(), PersistenceError> {
        // version == 0 → insert_one; duplicate key 11000 → OptimisticLockError
        // иначе update_one({ _id, version }, { $set: { view, version + 1 } }); matched_count == 0 → OptimisticLockError
    }
}
```

Документ: `{ _id: view_id, version: i64, view: <V as BSON> }`. Семантика оптимистической блокировки зеркалит
`PostgresViewRepository`. Плюс в demo у него есть вторая роль — порт чтения для HTTP (`ReadModelStore`: `load`,
`list`, `wipe`), см. обсуждение 01.

## 2. Варианты

### A. Feature `mongodb` в `cqrsx`

```toml
[features]
mongodb = ["dep:mongodb", "dep:futures"]
```

`cqrsx::mongodb::view_repository::MongoViewRepository<V, A>`; формат документа и отображение ошибок становятся
контрактом библиотеки.

Плюсы: следующий проект на Mongo получает готовый, протестированный репозиторий; demo импортирует.
Минусы: тяжёлая optional-зависимость (`mongodb` 3.x тянет много); строка в матрице фич корневого `CLAUDE.md`
(`--no-default-features --features mongodb` отдельно и вместе); контейнер MongoDB в CI (testcontainers) ради
порога 97 % — ещё одна минута на прогон и ещё один внешний образ.

### B. Отдельный крейт `cqrsx-mongodb` в корневом workspace

Тот же код, но своим крейтом с собственной зависимостью и собственными тестами.
Плюсы: `cqrsx` остаётся лёгким; порог покрытия и CI можно настроить отдельно; проект подключает только то, что
использует. Минусы: второй публичный крейт — версии, changelog, `components.md` для двух пакетов; код всё равно
нужно поддерживать.

### C. Остаётся в demo как эталонная реализация

Файл помечается «reference implementation: copy into your project», ссылка из `components.md`.
Плюсы: нулевая цена для библиотеки. Минусы: копии живут своей жизнью; если формат `{ _id, version, view }`
изменится в demo, старые копии об этом не узнают.

## 3. Что нужно учесть

1. **Формат документа как контракт.** `view` хранится как вложенный BSON; ключи `BTreeMap` — строки; `Option` —
   отсутствующие поля. BSON-round-trip тест в demo это фиксирует. В библиотеке это станет гарантией миграции.
2. **Ошибки Mongo → `PersistenceError`.** Сейчас всё, кроме duplicate key, — `ConnectionError`. Для библиотеки
   нужна более честная классификация (таймаут сервера vs. ошибка записи vs. десериализация).
3. **Тесты.** Интеграция через testcontainers MongoDB: insert, конфликт версии, duplicate key, BSON round-trip.
   Unit-тесты без Mongo невозможны (драйвер не имеет in-memory режима), значит, покрытие 97 % потребует
   контейнера в CI обязательно.
4. **Связь с 01.** Если `ViewStore` (list/clear) появится в библиотеке, реализовать его для Mongo нужно там же
   (`find({}).sort({_id:1})`, `drop()`).
5. **Зависимость `futures`** уже optional в `cqrsx` (feature `postgres`); Mongo тоже её использует (`TryStreamExt`).

## 4. Что нужно, чтобы решить

- Ответ на вопрос «есть ли планируемый проект с витринами в MongoDB». Если нет — C и закрыть тикет 09 как
  «не делаем». Если да — A или B; B предпочтительнее, если `cqrsx` хочет остаться маленьким.
- Готовность CI к контейнеру MongoDB (время прогона, доступность образа).
