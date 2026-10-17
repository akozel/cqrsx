# 03. PostgreSQL-исполнитель с services и обработчиками событий

| | |
|---|---|
| Статус | сделано: `feat(cqrsx): postgres executor with services and event handlers; adopt in demo` |
| Ценность | высокая: библиотечный конструктор сейчас не годится ни для одного агрегата с проекцией |
| Цена | низкая |
| cqrsx | `postgres/command.rs`, `cqrs/event_handler.rs` (`impl EventHandler for Arc<dyn EventHandler>`) |
| demo | `infrastructure/postgres.rs` (удалить `Executors`, `BuildFramework`, `PostgresExecutor`, `OutboxStore`), `bootstrap.rs`, `tests/write_side.rs` |
| Зависимости | тикет 02 желателен (имена обработчиков), но не обязателен |

## Сейчас в demo

В cqrsx есть готовый `PostgresCommandExecutor`, но он требует `Services = ()` и не регистрирует ни одного обработчика
(зафиксировано в API_MAPPING §4 как D2). У обоих агрегатов demo есть проекции, у концерта ещё и лимит билетов в
services. Поэтому сборка переписана вручную:

```rust
// demo: infrastructure/postgres.rs
pub type OutboxStore<A> = PersistedEventStore<PostgresEventRepositoryWithOutbox, A>;
pub type BuildFramework<A> = Box<dyn Fn(Vec<NewTask>) -> CqrsFramework<A, OutboxStore<A>> + Send + Sync>;
pub type PostgresExecutor<A> = CqrsCommandExecutor<A, BuildFramework<A>>;

impl Executors {
    pub fn executor<A>(&self, handlers: Vec<NamedHandler<A>>, services: A::Services) -> (PostgresExecutor<A>, Vec<&'static str>)
    where A: Aggregate + 'static, A::Services: Clone + 'static,
    {
        let (names, factories): (Vec<_>, Vec<_>) = handlers.into_iter().map(|h| (h.name, h.build)).unzip();
        let pool = self.pool.clone();
        let snapshot_every = self.snapshot_every;
        let build: BuildFramework<A> = Box::new(move |tasks| {
            let repository = PostgresEventRepositoryWithOutbox::new(pool.clone()).with_outbox(tasks);
            CqrsFramework::new(
                PersistedEventStore::new_snapshot_store(repository, snapshot_every),
                factories.iter().map(|build| build()).collect(),   // fresh boxes for every command
                services.clone(),
            )
        });
        (CqrsCommandExecutor::new(build, self.retry), names)
    }
}
```

Фабрики обработчиков нужны потому, что `CqrsFramework::new` забирает `Vec<Box<dyn EventHandler>>` по значению, а
фреймворк строится заново на каждую команду.

## Станет в cqrsx

Обработчики разделяются через `Arc`, services клонируются:

```rust
// cqrsx: cqrs/event_handler.rs
#[async_trait]
impl<A: Aggregate> EventHandler<A> for Arc<dyn EventHandler<A>> {
    fn name(&self) -> String { (**self).name() }
    async fn handle(&self, id: &str, events: &[EventEnvelope<A>]) -> Result<(), PersistenceError> {
        (**self).handle(id, events).await
    }
}

// cqrsx: postgres/command.rs
pub struct PostgresCommandExecutor<A: Aggregate> {
    pool: PgPool,
    config: PostgresCommandConfig,
    services: A::Services,
    event_handlers: Vec<Arc<dyn EventHandler<A>>>,
}

impl<A: Aggregate> PostgresCommandExecutor<A> where A::Services: Clone {
    pub fn new(pool: PgPool, config: PostgresCommandConfig, services: A::Services) -> Self { /* … */ }
    pub fn with_event_handler(mut self, handler: Arc<dyn EventHandler<A>>) -> Self { /* … */ }

    pub async fn execute_with_metadata(&self, id: &str, command: A::Command, metadata: HashMap<String, String>, tasks: Vec<NewTask>)
        -> Result<(), CommandGatewayError<A::Error>>
    {
        let executor = CqrsCommandExecutor::<A, _>::new(
            |tasks| {
                let repository = PostgresEventRepositoryWithOutbox::new(self.pool.clone()).with_outbox(tasks);
                let store = PersistedEventStore::new_snapshot_store(repository, self.config.snapshot_interval());
                let handlers = self.event_handlers.iter().cloned().map(|h| Box::new(h) as Box<dyn EventHandler<A>>).collect();
                CqrsFramework::new(store, handlers, self.services.clone())
            },
            self.config.conflict_retry(),
        );
        executor.execute_with_metadata(id, command, metadata, tasks).await
    }
}
```

Функция `command_executor(pool, config)` остаётся как краткая форма для `Services = ()` без обработчиков.

## Станет в demo

```rust
// bootstrap.rs
let concerts = PostgresCommandExecutor::<Concert>::new(pool.clone(), command_config.clone(), ConcertServices { ticket_limit: config.ticket_limit })
    .with_event_handler(Arc::new(NotifyingHandler::new(
        ViewProjection::new(read.halls.clone()).with_name(CONCERT_HALLS),
        activity.clone(),
    )));
```

`Executors`, три алиаса и `HandlerFactory` удаляются. В `infrastructure/postgres.rs` остаются только помощники
чтения истории (`history`, `event_history`). Интеграционный тест `write_side.rs` зовёт библиотечный конструктор.

## Зачем, на примере

Это кофемашина с единственной кнопкой «эспрессо без чашки»: технически работает, но каждый собирает свою обвязку,
чтобы получить обычный кофе. Любое приложение с хотя бы одной проекцией сейчас обязано повторить эти 45 строк.

## Критерии приёмки

- [x] `PostgresCommandExecutor::new(pool, config, services)` и `with_event_handler(Arc<dyn EventHandler<A>>)`.
- [x] `impl EventHandler<A> for Arc<dyn EventHandler<A>>`.
- [x] Интеграционный тест в `cqrsx/tests/postgres/command.rs`: обработчик вызывается после коммита, services доходят до `handle`, падение обработчика даёт `CommittedWithEventHandlerFailure`.
- [x] `components.md`: строка «Backend assembly» обновлена.
- [x] demo: `Executors` удалён, `bootstrap` и `write_side.rs` используют библиотечный конструктор.
