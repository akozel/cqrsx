# 01. Один тип адаптера задач вместо enum

| | |
|---|---|
| Статус | сделано: `feat(cqrsx): boxed task adapter; adopt in demo` |
| Ценность | высокая: каждое приложение с двумя и более типами задач пишет этот enum |
| Цена | низкая: один новый модуль в `cqrsx`, без новых зависимостей |
| cqrsx | `task/adapter/boxed.rs` (новый), `task/runtime/dispatcher_factory.rs` (опционально) |
| demo | удалить `infrastructure/task_runtime/adapter.rs`, упростить `infrastructure/task_runtime/dispatcher.rs` |
| Зависимости | нет |

## Сейчас в demo

`TaskDispatcher<A>` принимает один тип `A`, а трейт `TaskAdapter` нельзя положить в `Box<dyn>`, потому что его
`invoke` возвращает `impl Future`. Поэтому приложение пишет enum на столько вариантов, сколько у него типов задач,
и дублирует `match` в двух методах:

```rust
// demo: infrastructure/task_runtime/adapter.rs
pub enum EncoreTaskAdapter<E, M> {
    RecordTicket(JsonTaskAdapter<RecordTicketV1, RecordTicketHandler<E>>),
    RecordCancellation(JsonTaskAdapter<RecordCancellationV1, RecordCancellationHandler<E>>),
    SendETicket(JsonTaskAdapter<SendETicketV1, SendETicketHandler<M>>),
    IssueRefund(JsonTaskAdapter<IssueRefundV1, IssueRefundHandler<M>>),
}

impl<E, M> TaskAdapter for EncoreTaskAdapter<E, M> where /* … */ {
    fn formats(&self) -> Vec<TaskFormat> {
        match self {
            Self::RecordTicket(a) => a.formats(),
            Self::RecordCancellation(a) => a.formats(),
            Self::SendETicket(a) => a.formats(),
            Self::IssueRefund(a) => a.formats(),
        }
    }
    async fn invoke(&mut self, attempt: &TaskAttempt) -> Result<(), TaskHandlerError> {
        match self { /* the same four arms again */ }
    }
}

// demo: infrastructure/task_runtime/dispatcher.rs
let invalid = TaskDispatcherError::InvalidFormat;
let adapters = vec![
    EncoreTaskAdapter::RecordTicket(JsonTaskAdapter::new(RecordTicketHandler { /* … */ }).map_err(invalid)?),
    EncoreTaskAdapter::SendETicket(JsonTaskAdapter::new(SendETicketHandler { mailer: mail_server() }).map_err(invalid)?),
    // …
];
TaskDispatcher::try_new(adapters)
```

## Станет в cqrsx

Теневой трейт с боксированным future и обёртка, которая сама реализует `TaskAdapter`:

```rust
// cqrsx: task/adapter/boxed.rs
trait DynTaskAdapter: Send {
    fn formats(&self) -> Vec<TaskFormat>;
    fn invoke<'a>(&'a mut self, attempt: &'a TaskAttempt)
        -> Pin<Box<dyn Future<Output = Result<(), TaskHandlerError>> + Send + 'a>>;
}

impl<T: TaskAdapter> DynTaskAdapter for T {
    fn formats(&self) -> Vec<TaskFormat> { TaskAdapter::formats(self) }
    fn invoke<'a>(&'a mut self, attempt: &'a TaskAttempt) -> Pin<Box<dyn Future<Output = _> + Send + 'a>> {
        Box::pin(TaskAdapter::invoke(self, attempt))
    }
}

/// Any adapter behind one type, so one dispatcher can hold different handlers.
pub struct BoxedTaskAdapter(Box<dyn DynTaskAdapter>);

impl BoxedTaskAdapter {
    pub fn new(adapter: impl TaskAdapter + 'static) -> Self { Self(Box::new(adapter)) }
    pub fn json<P, H>(handler: H) -> Result<Self, TaskFormatError>
    where P: TaskPayload + DeserializeOwned + Send + 'static, H: TaskHandler<P> + 'static {
        Ok(Self::new(JsonTaskAdapter::<P, H>::new(handler)?))
    }
}

impl TaskAdapter for BoxedTaskAdapter {
    fn formats(&self) -> Vec<TaskFormat> { self.0.formats() }
    async fn invoke(&mut self, attempt: &TaskAttempt) -> Result<(), TaskHandlerError> { self.0.invoke(attempt).await }
}
```

Опционально: тип фабрики диспетчера становится `Fn(usize) -> …`, то есть получает номер воркера. Сейчас
`build_checked_dispatcher` знает индекс, но не передаёт его, поэтому demo держит счётчик `AtomicU64` для сидов хаоса.

## Станет в demo

Файл `adapter.rs` удаляется, регистрация становится списком:

```rust
TaskDispatcher::try_new(vec![
    BoxedTaskAdapter::json::<RecordTicketV1, _>(RecordTicketHandler { fans: deps.fans.clone(), follow_up_max_attempts: deps.task_max_attempts })?,
    BoxedTaskAdapter::json::<RecordCancellationV1, _>(RecordCancellationHandler { /* … */ })?,
    BoxedTaskAdapter::json::<SendETicketV1, _>(SendETicketHandler { mailer: mail_server() })?,
    BoxedTaskAdapter::json::<IssueRefundV1, _>(IssueRefundHandler { mailer: mail_server() })?,
])
```

Чеклист «добавить новую задачу» (ARCHITECTURE_OPTIONS §11.3, пункт 4) сокращается до одной строки регистрации.

## Зачем, на примере

Добавляете пятую задачу «напомнить о концерте за день». Сейчас это новый вариант enum, две новые ветки `match` и
строка регистрации. После: только строка регистрации. Цена: одна аллокация на вызов задачи, на фоне запроса к базе
её не видно.

## Критерии приёмки

- [x] `BoxedTaskAdapter` реализует `TaskAdapter`; `TaskDispatcher<BoxedTaskAdapter>` принимает адаптеры разных типов.
- [x] Тест: два `JsonTaskAdapter` с разными payload в одном диспетчере, маршрутизация по формату, дубликат формата отвергается.
- [x] Тест: `Permanent` и `Retry` из обработчика проходят через обёртку без изменений.
- [x] `components.md`: строка «Wire adaptation» упоминает `BoxedTaskAdapter`.
- [x] demo: `adapter.rs` удалён, `EncoreAdapter` заменён на `BoxedTaskAdapter`, `cargo test` зелёный.
