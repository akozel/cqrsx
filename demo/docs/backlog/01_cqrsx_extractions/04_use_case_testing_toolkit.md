# 04. Тестовый инструментарий уровня use case

| | |
|---|---|
| Статус | сделано, коммит `7292ecd`: `feat(cqrsx): use-case testing toolkit; adopt in demo` (исполнитель назван `TestCommandExecutor<A, S>`, см. план, решение Р5) |
| Ценность | высокая: решение D7 («use cases тестируются на `MemStore`») становится библиотечным путём для любого проекта |
| Цена | низкая, без новых зависимостей; код живёт в `pub mod testing`, покрытие обязательно |
| cqrsx | `cqrs/testing/mem_executor.rs`, `cqrs/testing/mem_view_repository.rs` (новые), `task/attempt.rs` |
| demo | удалить `application/test_support.rs`, `testing.rs::MemoryReadModelStore`, `application/workflows/test_support.rs::attempt` |
| Зависимости | нет |

## Сейчас в demo

Библиотека даёт given/when/then только на уровне агрегата, поэтому demo написало обвязку само:

```rust
// demo: application/test_support.rs
pub type TestFramework<A, S> = Box<dyn Fn(Vec<NewTask>) -> CqrsFramework<A, S> + Send + Sync>;
pub type TestExecutor<A, S> = CqrsCommandExecutor<A, TestFramework<A, S>>;

#[derive(Clone, Default)]
pub struct BoundTasks(Arc<Mutex<Vec<NewTask>>>);

pub fn executor<A, S>(store: S, services: A::Services, bound: BoundTasks) -> TestExecutor<A, S> where /* … */ {
    let build: TestFramework<A, S> = Box::new(move |tasks| {
        bound.0.lock().unwrap().extend(tasks);            // remember what the use case tried to publish
        CqrsFramework::new(store.clone(), vec![], services.clone())
    });
    CqrsCommandExecutor::new(build, AggregateConflictRetryPolicy::default())
}

pub struct UnreachableStore;                              // every call: DatabaseConnectionError
impl<A: Aggregate> EventStore<A> for UnreachableStore { /* … */ }

// demo: testing.rs
pub struct MemoryReadModelStore<V> { documents: Mutex<BTreeMap<String, Stored<V>>> }

// demo: application/workflows/test_support.rs
pub fn attempt<P: TaskPayload>(correlation_id: Uuid) -> TaskAttempt {
    TaskAttempt::new(Uuid::new_v4(), TaskFormat::of::<P>().unwrap(), json!({}), json!({ "correlation_id": correlation_id }), 1).unwrap()
}
```

## Станет в cqrsx

То же самое в `cqrs::testing`, который по `components.md` и есть «публичный инструментарий для downstream»:

```rust
// cqrsx: cqrs/testing/mem_executor.rs
/// A `CommandExecutor` over `MemStore` that records the tasks bound to every call.
#[derive(Clone)]
pub struct MemCommandExecutor<A: Aggregate> {
    store: MemStore<A>,
    services: A::Services,
    tasks: Arc<Mutex<Vec<NewTask>>>,
}

impl<A: Aggregate> MemCommandExecutor<A> {
    pub fn new(store: MemStore<A>, services: A::Services) -> Self { /* … */ }
    pub fn store(&self) -> &MemStore<A> { &self.store }
    /// Everything handed to the executor so far, in call order; clears the record.
    pub fn take_tasks(&self) -> Vec<NewTask> { /* … */ }
}

impl<A> CommandExecutor<A> for MemCommandExecutor<A> where /* … */ {
    async fn execute_with_metadata(&self, id: &str, command: A::Command, metadata: HashMap<String, String>, tasks: Vec<NewTask>)
        -> Result<(), CommandGatewayError<A::Error>>
    {
        self.tasks.lock().unwrap().extend(tasks.iter().cloned());
        CqrsCommandExecutor::new(|_| CqrsFramework::new(self.store.clone(), vec![], self.services.clone()), AggregateConflictRetryPolicy::default())
            .execute_with_metadata(id, command, metadata, tasks)
            .await
    }
}

/// An event store that is never reachable: drives every "storage unavailable" branch.
pub struct UnavailableStore;

// cqrsx: cqrs/testing/mem_view_repository.rs
/// In-memory `ViewRepository` with the optimistic-version rules of the PostgreSQL one.
pub struct MemViewRepository<V, A> { /* … */ }

// cqrsx: task/attempt.rs
impl TaskAttempt {
    /// First claim of `payload` with `metadata`, for handler tests.
    pub fn first<P: TaskPayload>(payload: &P, metadata: Value) -> Result<Self, TaskValidationError> { /* … */ }
}
```

## Станет в demo

```rust
let executor = MemCommandExecutor::new(MemStore::default(), ConcertServices::default());
let app = ConcertApp::new(CommandService::new(executor.clone(), activity, lock), executor.store().clone(), 6);

app.announce_concert(&concert_id(), announce(), CommandMetadata::ui()).await;
let executed = app.book_seat(&concert_id(), book("a1", "fox", booking_id), CommandMetadata::ui()).await;

assert_eq!(executed.tasks, ["encore_record_ticket v1"]);
assert_eq!(executor.take_tasks()[0].payload()["price"], 120);   // row A is VIP, the price comes from the venue
```

Фикстуры `harness`, `announce`, `book`, `cancel` остаются в demo: они про концерты.

## Зачем, на примере

«Бронь места публикует задачу `encore_record_ticket` с ценой зала» это самый важный тест приложения, и сейчас его
нельзя написать без 75 строк своей обвязки. Второй проект напишет их снова, с другими багами.

## Критерии приёмки

- [x] `MemCommandExecutor`, `UnavailableStore`, `MemViewRepository`, `TaskAttempt::first` публичны и задокументированы.
- [x] Тесты: записанные задачи совпадают с переданными; отклонённая команда задач не публикует; `UnavailableStore` даёт `Unavailable`; `MemViewRepository` возвращает `OptimisticLockError` при устаревшей версии.
- [x] `components.md`: строка «Public testing tools» дополнена.
- [x] demo: три файла тестовой обвязки удалены, тесты use cases и workflow-шагов переведены на библиотечные типы.
