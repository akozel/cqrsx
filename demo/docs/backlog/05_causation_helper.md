# 05. Помощник причинности: команда, событие, задача, следующая команда

| | |
|---|---|
| Статус | предложено |
| Ценность | средняя: это соглашение об именах ключей, а не механизм; но без него цепочки между проектами не склеятся |
| Цена | низкая |
| cqrsx | `cqrs/command/causation.rs` (новый), `task.rs` (`NewTask::caused_by`) |
| demo | `application/command_service.rs::CommandMetadata`, `application/tasks.rs::new_task`, `application/workflows.rs::correlation_of` |
| Зависимости | нет; нужен тикету 10 |

## Сейчас в demo

Идентификаторы корреляции протаскиваются через три файла, с ручным парсингом строки обратно в `Uuid`:

```rust
// demo: application/command_service.rs
impl CommandMetadata {
    pub fn from_task(correlation_id: Uuid, task_id: Uuid, task_type: &str) -> Self {
        Self { command_id: Uuid::now_v7(), correlation_id, causation_id: task_id, recorded_at: Self::now(), source: format!("task:{task_type}") }
    }
    pub fn to_map(&self) -> HashMap<String, String> { /* five inserts */ }
}

// demo: application/tasks.rs
pub fn new_task<T: TaskPayload>(payload: T, metadata: &CommandMetadata, concert_id: &ConcertId, fan_id: &FanId, max_attempts: i32) -> Result<NewTask, TaskValidationError> {
    NewTask::new(payload)?
        .with_metadata(json!({
            "command_id": metadata.command_id, "correlation_id": metadata.correlation_id, "causation_id": metadata.causation_id,
            "concert_id": concert_id, "fan_id": fan_id,
        }))?
        .with_max_attempts(max_attempts)
}

// demo: application/workflows.rs
pub fn correlation_of(metadata: &serde_json::Value, task_id: Uuid) -> Uuid {
    metadata.get("correlation_id").and_then(|v| v.as_str()).and_then(|s| Uuid::parse_str(s).ok()).unwrap_or(task_id)
}
let metadata = CommandMetadata::from_task(correlation_of(attempt.metadata(), attempt.task_id()), attempt.task_id(), P::TASK_TYPE);
```

## Станет в cqrsx

```rust
// cqrsx: cqrs/command/causation.rs
/// Who caused a command; the same three ids travel with its events and its tasks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Causation { pub command_id: Uuid, pub correlation_id: Uuid, pub causation_id: Uuid, pub source: String }

impl Causation {
    /// A new chain: all three ids are the command id.
    pub fn root(source: impl Into<String>) -> Self { /* … */ }
    /// A command issued while handling a task: inherits the task's correlation id,
    /// is caused by the task and is sourced `task:<type>`. Falls back to the task id.
    pub fn from_task(attempt: &TaskAttempt) -> Self { /* … */ }
    /// Keys for `execute_with_metadata`.
    pub fn event_metadata(&self) -> HashMap<String, String> { /* … */ }
}

impl NewTask {
    /// Adds the causation keys to the task metadata, keeping the keys already there.
    pub fn caused_by(self, causation: &Causation) -> Result<Self, TaskValidationError> { /* … */ }
}
```

## Станет в demo

```rust
// a task published by a use case
let task = NewTask::new(payload)?
    .with_metadata(json!({ "concert_id": concert_id, "fan_id": fan_id }))?   // app keys stay app keys
    .caused_by(&metadata.causation)?
    .with_max_attempts(max_attempts)?;

// a command issued by a task handler
let causation = Causation::from_task(attempt);                            // was: three calls across two files
```

Поле `recorded_at` остаётся в demo: это прикладное поле (им пользуются витрины и sidecar).

## Зачем, на примере

Фанат жмёт «купить», команда получает id X. Событие `SeatBooked` ложится в журнал с correlation X. Задача
`encore_record_ticket` несёт X. Её обработчик создаёт команду `RecordTicket`: correlation снова X, causation равен
id задачи. В рентгене фильтр по X показывает всю цепочку. Соглашение стандартное, но имена ключей сейчас живут в
demo, и второй проект назовёт ключ `correlationId`. Тогда цепочки между сервисами не склеятся.

## Критерии приёмки

- [ ] `Causation::{root, from_task, event_metadata}`, `NewTask::caused_by` (слияние с уже заданными ключами).
- [ ] Тесты: `from_task` наследует корреляцию и ставит причину в id задачи; без ключа в метаданных падает на id задачи; `caused_by` не затирает чужие ключи.
- [ ] Ключи документированы в `components.md` рядом с «Command boundary».
- [ ] demo: `correlation_of` удалён, `CommandMetadata::from_task` делегирует библиотеке, JSON в `events.metadata` и `transactional_outbox.metadata` не меняется (проверить wire-тестами).
