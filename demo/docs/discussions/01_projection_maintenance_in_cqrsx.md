# 01. Обслуживание витрин (wipe, rebuild, verify) в `cqrsx`

Статус: обсуждается. Откуда: аудит C-2, [тикет 07](../backlog/01_cqrsx_extractions/07_projection_maintenance.md).
Решение нужно от владельца `cqrsx`: расширять ли контракт репозитория витрин и брать ли в библиотеку
операции над целой витриной.

## 1. Контекст

Витрина в `cqrsx` описывается двумя контрактами: `View<A>` (как документ меняется от события) и
`ViewRepository<V, A>` (как документ читается и сохраняется с оптимистической блокировкой). Оба работают с
**одним документом**: `load`, `load_with_context`, `update_view`. Операций над **всей коллекцией** нет.

Демо понадобились три такие операции:

| Операция | Что делает | Кнопка в UI |
|---|---|---|
| wipe | удалить все документы витрины | 🧨 (теперь sidecar) и первый шаг Encore! |
| rebuild | wipe + `EventReplay::replay_all()` через тот же `ViewProjection`, что в live | ⏪ Encore! |
| verify | пересобрать во **временную** копию и сравнить с боевой без `version` | 🔍 Проверить |

## 2. Что в коде сейчас

### В demo

```rust
// back-end: application/ports.rs — порт поверх ViewRepository, которого нет в библиотеке
pub struct Stored<V> { pub id: String, pub version: i64, pub view: V }     // ≈ (ViewContext, V)

#[async_trait]
pub trait ReadModelStore<V>: Send + Sync {
    async fn load(&self, id: &str) -> Result<Option<Stored<V>>, StorageError>;
    async fn list(&self) -> Result<Vec<Stored<V>>, StorageError>;        // all documents, ordered by id
    async fn wipe(&self) -> Result<(), StorageError>;                     // drop the collection
}

// back-end: application/projections/maintenance.rs — object-safe обёртка для списка витрин
#[async_trait]
pub trait MaintainedReadModel: Send + Sync {
    fn name(&self) -> &'static str;
    fn aggregate_type(&self) -> &'static str;
    async fn wipe(&self) -> Result<(), StorageError>;
    async fn rebuild(&self, pace: Duration, on_event: Box<ReplayProgressObserver>) -> Result<(), StorageError>;
    async fn verify(&self, run_id: Uuid) -> Result<Verification, StorageError>;
}

/// Реализация для любой view V агрегата A в репозитории R; H читает историю.
pub struct ProjectionMaintenance<H, R, V, A> {
    name: &'static str,
    history: HistoryFactory<H>,       // Arc<dyn Fn() -> H>: EventReplay::new забирает репозиторий по значению
    live: Arc<R>,
    scratch: ScratchFactory<R>,       // Arc<dyn Fn(Uuid) -> Arc<R>>: временная коллекция на один прогон verify
}
```

Тело `verify` (сокращённо): создать scratch → `EventReplay::new(history(), ViewProjection::new(scratch)).replay_all()` →
`scratch.list()` → `scratch.wipe()` → сравнить с `live.list()` по id → `missing` / `unexpected` / `differs`.
Реализации `ReadModelStore`: `MongoViewRepository` (прод) и `MemViewRepository` из `cqrsx::testing` (тесты demo,
через `impl` в `testing.rs`).

Ничто из этого не знает про концерты. Единственная прикладная часть: имена коллекций, порядок витрин
(`Concert` до `Fan`) и формат `Difference` для JSON ответа.

### В cqrsx

- `ViewRepository<V, A>`: `load`, `load_with_context`, `update_view`. Native async (`impl Future`), **не object-safe**.
- `PostgresViewRepository`: таблица `{ view_id, version, payload }`; ни `list`, ни `clear`.
- `MemViewRepository` (feature-less, `cqrs::testing`): уже имеет `list_all()`, `clear()`, `put()` — то есть
  тестовый двойник умеет то, чего нет в контракте.
- `EventReplay<R, H, A>`: `replay(id)`, `replay_all()`, `with_pace`, `set_progress_observer`, `set_error_observer`
  (тикет 06).

## 3. Варианты

### A. Тикет 07 как написан: `ViewStore` + `maintenance::{rebuild, verify}`

```rust
// cqrsx: cqrs/projection/repository.rs
pub trait ViewStore<V, A>: ViewRepository<V, A> {
    fn list_all(&self) -> impl Future<Output = Result<Vec<(ViewContext, V)>, PersistenceError>> + Send;
    fn clear(&self) -> impl Future<Output = Result<(), PersistenceError>> + Send;
}
// cqrsx: cqrs/projection/maintenance.rs
pub async fn rebuild<R, S, V, A>(history: R, store: Arc<S>, configure: impl FnOnce(&mut EventReplay<…>)) -> Result<(), PersistenceError>;
pub async fn verify<R, S, V, A>(history: R, live: &S, scratch: Arc<S>) -> Result<Verification, PersistenceError>;
```

Плюсы: demo теряет ~120 строк обобщённого кода; `verify` появляется у любого проекта на `cqrsx` «бесплатно»;
`PostgresViewRepository` получает `list_all`/`clear`, которых и так не хватает для эксплуатации.
Минусы: новая публичная поверхность, которую придётся поддерживать; `verify` требует **второго репозитория того
же типа** (scratch) — библиотеке нужно договориться, кто его создаёт (параметр, фабрика, `clone_as(name)`);
`PostgresViewRepository` нужна интеграция (testcontainers) и покрытие 97 %.

### B. Только `ViewStore` (`list_all`, `clear`)

Контракт расширяется двумя методами, `rebuild` и `verify` остаются в demo (и в каждом проекте, которому они нужны).
Плюсы: минимальная поверхность; `clear` + `EventReplay` уже достаточно, чтобы написать rebuild в три строки.
Минусы: сверка (verify) дублируется в проектах; demo сохраняет `ProjectionMaintenance` почти целиком.

### C. Ничего не менять

`ReadModelStore` остаётся портом приложения; `MongoViewRepository` реализует его сам.
Плюсы: нулевая цена. Минусы: у `PostgresViewRepository` нет `list`/`clear`, т. е. проект на Postgres не сможет
сделать Encore! без raw SQL по таблице библиотеки (та же проблема, что закрыл тикет 08 для `events`).

## 4. Что нужно учесть при решении

1. **Object-safety.** `ViewRepository` native-async, поэтому `Vec<Arc<dyn …>>` для списка витрин невозможен. В demo
   это решено обёрткой `MaintainedReadModel` (`async_trait`). Если библиотека даст `rebuild`/`verify` как
   свободные функции, обёртка всё равно останется в приложении — это нормально, но стоит записать как ожидание.
2. **Scratch-репозиторий для verify.** Варианты: `verify(history, live, scratch)` с готовым scratch (как в demo);
   трейт `ViewStore::scratch(&self, suffix) -> Self`; или verify без scratch — replay в память (`MemViewRepository`)
   и сравнение с live. Последнее проще всего, но нарушает правило demo §12.4 «без in-memory фейков в рантайме»
   и не масштабируется на большие витрины.
3. **`clear` для Postgres:** `TRUNCATE` (быстро, но требует привилегии и не транзакционен с остальным) или
   `DELETE FROM` (медленно на больших таблицах). Для Mongo это `drop()` коллекции (индексы тоже уходят).
4. **Сравнение в verify** требует `V: PartialEq`; у `View` этого bound нет. Добавлять bound только на `verify`.
5. **Прогресс и темп** rebuild уже есть в `EventReplay` (тикет 06); `rebuild` в библиотеке должен принимать
   конфигуратор replay, а не дублировать параметры.
6. **Связь с 02:** если `MongoViewRepository` тоже уедет в библиотеку, `ViewStore` для него реализуется там же,
   и demo перестаёт содержать какой-либо код хранения витрин.

## 5. Что нужно, чтобы решить

- Будут ли у реальных проектов на `cqrsx` операции «пересобрать витрину» и «сверить витрину» (ответ «да» почти
  наверняка для rebuild, для verify — вопрос).
- Готовность владельца библиотеки держать публичный `ViewStore` и интеграционные тесты для Postgres.
- Выбор по п. 2 (scratch). Это единственное место, где API не очевиден.

Рекомендация аудита: **B сейчас** (две строки контракта, которых не хватает Postgres), **A позже**, когда появится
второй потребитель verify.
