# 02. Имя обработчика в ошибке после коммита

| | |
|---|---|
| Статус | сделано, коммит `445f52e`: `feat(cqrsx): handler names in post-commit failures; adopt in demo` |
| Ценность | высокая: убирает параллельный список имён, который легко рассинхронизировать |
| Цена | низкая; меняет публичные типы ошибок (крейт не публикуется) |
| cqrsx | `cqrs/event_handler.rs`, `cqrs/projection.rs`, `cqrs.rs`, `cqrs/command.rs`, `cqrs/command/executor.rs` |
| demo | `application/command_service.rs` (удалить `NamedHandler`, поле `projections`), `bootstrap.rs` |
| Зависимости | нет; упрощает тикет 03 |

## Сейчас в demo

Когда событие записано, а проекция упала, библиотека возвращает номер обработчика. Приложение обязано хранить список
имён в порядке регистрации:

```rust
// demo: application/command_service.rs
pub struct NamedHandler<A> {
    pub name: &'static str,
    pub build: HandlerFactory<A>,   // exists only to keep the name and the handler together
}

pub struct CommandService<A, E> {
    executor: E,
    /// Projection names in handler registration order (see [`NamedHandler`]).
    projections: Vec<&'static str>,
    // …
}

Err(CommandGatewayError::CommittedWithEventHandlerFailure { handler_indices, .. }) => CommandOutcome::Accepted {
    warnings: handler_indices
        .into_iter()
        .map(|index| Warning {
            code: "projection_failed",
            projection: projections.get(index).copied().unwrap_or("unknown").to_owned(),
        })
        .collect(),
},
```

## Станет в cqrsx

У обработчика появляется имя, и ошибка несёт его:

```rust
// cqrsx: cqrs/event_handler.rs
#[async_trait]
pub trait EventHandler<A: Aggregate>: Send + Sync {
    /// Reported in post-commit failures; defaults to the type name.
    fn name(&self) -> String { std::any::type_name::<Self>().to_owned() }
    async fn handle(&self, aggregate_id: &str, events: &[EventEnvelope<A>]) -> Result<(), PersistenceError>;
}

// cqrsx: cqrs/projection.rs
impl<R, V, A> ViewProjection<R, V, A> {
    pub fn with_name(mut self, name: impl Into<String>) -> Self { self.name = Some(name.into()); self }
}

// cqrsx: cqrs.rs
pub struct EventHandlerFailure { pub handler_index: usize, pub handler_name: String, pub error: PersistenceError }

// cqrsx: cqrs/command.rs
CommittedWithEventHandlerFailure { last_sequence: usize, handlers: Vec<FailedHandler> }
pub struct FailedHandler { pub index: usize, pub name: String }
```

Декораторы (в demo это `NotifyingHandler`) делегируют `name()` внутреннему обработчику.

## Станет в demo

`NamedHandler` и поле `projections` исчезают:

```rust
// bootstrap.rs
let halls = ViewProjection::new(read.halls.clone()).with_name(CONCERT_HALLS);

// command_service.rs
Err(CommandGatewayError::CommittedWithEventHandlerFailure { handlers, .. }) => CommandOutcome::Accepted {
    warnings: handlers.into_iter().map(|h| Warning { code: "projection_failed", projection: h.name }).collect(),
},
```

## Зачем, на примере

Сегодня библиотека сообщает «упал обработчик номер 1», а приложение переводит: «номер 1 это fan_wallets». Если
кто-то поменяет местами две строки регистрации, перевод станет неверным, и ни компилятор, ни тесты этого не заметят.
После: библиотека сразу говорит «упал fan_wallets».

## Критерии приёмки

- [x] `EventHandler::name()` с дефолтом; `ViewProjection::with_name`.
- [x] `EventHandlerFailure` и `CommandGatewayError::CommittedWithEventHandlerFailure` несут имя и индекс.
- [x] Тест: два обработчика, падает второй, ошибка содержит его имя; дефолтное имя равно имени типа.
- [x] `components.md`: строка «Post-commit failures» обновлена.
- [x] demo: `NamedHandler` удалён, `CommandService::new` без списка имён, активити-лог по-прежнему пишет `projection.failed` с именем коллекции.
