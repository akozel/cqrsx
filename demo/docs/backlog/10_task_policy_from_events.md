# 10. Задачи вычисляются из принятого события, а не угадываются до команды

| | |
|---|---|
| Статус | предложено, **архитектурное решение** (ARCHITECTURE_OPTIONS §9.1) |
| Ценность | высокая: убирает двойное чтение агрегата и окно рассинхронизации между задачей и событием |
| Цена | высокая: меняется контракт `EventStore::commit`, путь записи outbox и правило неизменности задач между conflict-retry |
| cqrsx | `cqrs.rs`, `cqrs/store.rs`, `cqrs/store/persisted.rs`, `cqrs/store/repository.rs`, `postgres/outbox.rs`, `cqrs/command/executor.rs`, `cqrs/testing/mem_store.rs` |
| demo | `application/concert/{book_seat,cancel_booking,tasks}.rs`, `application/concert.rs` (поле `history`), `application/command_service.rs` (`Executed::tasks_unavailable`), `application/workflows.rs` |
| Зависимости | тикет 05 желателен (метаданные причинности внутри политики) |

## Сейчас в demo

Библиотека принимает список задач вместе с командой, до того как агрегат принял решение. Поэтому use case должен
заранее угадать, какое событие получится. Для отмены брони это значит прочитать концерт, найти бронь и цену, собрать
задачу возврата, и только потом исполнитель читает концерт второй раз и решает по-настоящему:

```rust
// demo: application/concert/cancel_booking.rs
pub async fn cancel_booking(&self, concert_id: &ConcertId, command: CancelBooking, metadata: CommandMetadata) -> Executed<ConcertError> {
    if self.service.is_locked() { return Executed::locked(); }
    let tasks = match self.refund_tasks(concert_id, &command, &metadata).await {   // first read of the aggregate
        Ok(tasks) => tasks,
        Err(error) => return Executed::tasks_unavailable(error),
    };
    self.execute(concert_id, command.into(), metadata, tasks).await               // the executor reads it again
}

async fn refund_tasks(&self, concert_id: &ConcertId, command: &CancelBooking, metadata: &CommandMetadata) -> Result<Vec<NewTask>, String> {
    let Some(seat) = VENUE.seat(&command.seat) else { return Ok(vec![]) };
    let mut context = self.history.load_aggregate(concert_id.as_str()).await.map_err(|e| e.to_string())?;
    let Some(booking) = context.aggregate().bookings().get(&seat.id).filter(|b| b.fan_id == command.fan_id) else { return Ok(vec![]) };
    let task = tasks::record_cancellation(concert_id, command, seat, booking, metadata, self.chain_max_attempts).map_err(|e| e.to_string())?;
    Ok(vec![task])
}
```

## Станет в cqrsx

Политика задач вызывается между `handle` и `commit`, и задачи едут в коммит:

```rust
// cqrsx: cqrs.rs
pub type TaskPolicy<A> = Arc<dyn Fn(&str, &<A as Aggregate>::Event, &HashMap<String, String>) -> Result<Vec<NewTask>, TaskValidationError> + Send + Sync>;

impl<A, ES> CqrsFramework<A, ES> {
    pub fn with_task_policy(mut self, policy: TaskPolicy<A>) -> Self { /* … */ }

    pub async fn execute_with_metadata(&self, aggregate_id: &str, command: A::Command, metadata: HashMap<String, String>) -> Result<(), AggregateError<A::Error>> {
        let mut context = self.store.load_aggregate(aggregate_id).await?;
        let mut aggregate = context.aggregate().clone();
        let sink = EventSink::default();
        aggregate.handle(command, &self.service, &sink).await.map_err(AggregateError::Domain)?;
        let events = sink.collect().await;
        let tasks = self.tasks_for(aggregate_id, &events, &metadata)?;           // the policy, after the decision; TaskValidationError → UnexpectedError
        let committed = self.store.commit(events, context, metadata, tasks).await?;   // tasks are a commit parameter now
        // …
    }
}

// cqrsx: cqrs/store.rs
fn commit(&self, events: Vec<A::Event>, context: Self::AC, metadata: HashMap<String, String>, tasks: Vec<NewTask>) -> impl Future<Output = …> + Send;
```

Привязка задач к репозиторию при его создании через `with_outbox(tasks)` становится не нужна: задачи приходят в
`persist` вместе с событием. Явный параметр `tasks` у `CommandExecutor` можно оставить для задач, не выводимых из
события, или убрать; это часть решения.

## Станет в demo

Правило записано один раз, рядом с фактами, и вычисляется из уже принятого события:

```rust
// demo: application/concert/tasks.rs
pub fn tasks_for(concert_id: &str, event: &ConcertEvent, metadata: &HashMap<String, String>) -> Result<Vec<NewTask>, TaskValidationError> {
    Ok(match event {
        ConcertEvent::SeatBooked(e) => vec![record_ticket(concert_id, e, metadata)?],
        ConcertEvent::BookingCancelled(e) => vec![record_cancellation(concert_id, e, metadata)?],   // the event already carries booking_id and refund
        ConcertEvent::ConcertAnnounced(_) => vec![],
    })
}

// demo: application/concert/cancel_booking.rs
pub async fn cancel_booking(&self, concert_id: &ConcertId, command: CancelBooking, metadata: CommandMetadata) -> Executed<ConcertError> {
    self.execute(concert_id, command.into(), metadata).await
}
```

Исчезают: поле `history` у `ConcertApp`, второе чтение, ветка `tasks_unavailable`, повторная проверка места в
`book_seat` и три оговорки в документации про «задача привязана до решения». Шаги workflow (`run_chain_step`) тоже
перестают строить follow-up задачу вручную: её порождает политика `Fan` из `TicketRecorded` / `TicketCancelled`.

## Зачем, на примере

Между двумя чтениями бронь могла смениться: фанат отменил и тут же снова купил то же место. Тогда задача понесёт
старый `booking_id`, а событие новый, и кошелёк не узнает о второй отмене. С политикой такого окна нет.

## Что надо решить явно

1. Задачи пересчитываются на каждой conflict-попытке; пишет только победившая. Сейчас документация и тест
   `executor_builds_once_and_retries_the_unchanged_command_and_tasks` закрепляют обратное: список задач и их id
   неизменны между попытками. Новое правило: id задачи стабилен внутри одной попытки записи, этого достаточно.
2. Сигнатура политики: `(&str, &A::Event, &metadata)` или `&EventEnvelope<A>` (последовательность известна только
   после коммита, поэтому envelope недоступен).
3. Судьба явного параметра `tasks` в `CommandExecutor::execute_with_metadata`.

## Критерии приёмки

- [ ] Дизайн-записка с ответами на три вопроса выше согласована.
- [ ] `EventStore::commit` принимает задачи; `MemStore` хранит их для тестов; `PersistedEventStore` передаёт в `persist`; `PostgresEventRepositoryWithOutbox` пишет их той же CTE.
- [ ] Тесты: отклонённая команда не публикует задач; политика с ошибкой валидации не пишет событие; конфликт-retry публикует задачи только победившей попытки.
- [ ] `components.md`: строки «Command orchestration», «Event store», «Atomic publication» обновлены.
- [ ] demo: `cancel_booking` без предварительного чтения, `tests/write_side.rs` зелёный, JSON задач в outbox не меняется.
