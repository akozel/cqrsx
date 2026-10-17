# 06. Прогресс и темп replay

| | |
|---|---|
| Статус | сделано, коммит `e54cc20`: `feat(cqrsx): replay progress observer and pace; adopt in demo` |
| Ценность | средняя: любой инструмент пересборки хочет прогресс-бар и ограничение скорости |
| Цена | низкая |
| cqrsx | `cqrs/event_handler/replay.rs` |
| demo | удалить `application/projections/progress.rs`, упростить `application/projections/maintenance.rs::replay_into` |
| Зависимости | нет; используется тикетом 07 |

## Сейчас в demo

У библиотечного `EventReplay` есть наблюдатель ошибок, но нет наблюдателя успеха. Для полосы «воспроизведено 57 из
121» demo оборачивает проекцию декоратором:

```rust
// demo: application/projections/progress.rs
pub type ProgressCallback = dyn Fn(&str, usize, &str) + Send + Sync;

#[async_trait]
impl<A, H> EventHandler<A> for ProgressHandler<H> where A: Aggregate, H: EventHandler<A> {
    async fn handle(&self, aggregate_id: &str, events: &[EventEnvelope<A>]) -> Result<(), PersistenceError> {
        if let Some(delay) = self.delay { tokio::time::sleep(delay).await; }   // slow motion
        self.inner.handle(aggregate_id, events).await?;
        for event in events { (self.on_event)(aggregate_id, event.sequence, &event.payload.event_type()); }
        Ok(())
    }
}

// demo: application/projections/maintenance.rs
EventReplay::new((self.history)(), ProgressHandler::new(ViewProjection::new(repository), delay, on_event)).replay_all().await
```

## Станет в cqrsx

```rust
// cqrsx: cqrs/event_handler/replay.rs
pub struct ReplayProgress<'a> { pub aggregate_id: &'a str, pub sequence: usize, pub event_type: String, pub replayed: u64 }
pub type ReplayProgressObserver = dyn Fn(&ReplayProgress<'_>) + Send + Sync;

impl<R, H, A> EventReplay<R, H, A> {
    pub fn set_progress_observer(&mut self, observer: Box<ReplayProgressObserver>) { /* … */ }
    /// Minimum pause before each event; limits the write rate of a rebuild.
    pub fn with_pace(self, pace: Duration) -> Self { /* … */ }
}

// inside run_stream, after a successful handle:
replayed += 1;
if let Some(observer) = &self.progress_observer {
    observer(&ReplayProgress { aggregate_id: &event.aggregate_id, sequence: event.sequence, event_type: event.payload.event_type(), replayed });
}
```

## Станет в demo

Файл `progress.rs` удаляется:

```rust
let mut replay = EventReplay::new((self.history)(), ViewProjection::new(repository)).with_pace(delay.unwrap_or_default());
replay.set_progress_observer(Box::new(move |p| on_event(p.aggregate_id, p.sequence, &p.event_type)));
replay.replay_all().await
```

## Зачем, на примере

Любой инструмент пересборки, от админки до CLI с прогресс-баром, напишет такой же декоратор. Темп тоже не только
для демо: пересборка на полной скорости может положить базу витрин, и ограничить её удобно в одном месте.

## Критерии приёмки

- [x] `set_progress_observer` и `with_pace` на `EventReplay`; наблюдатель вызывается после успешного `handle`, не вызывается для упавшего события.
- [x] Тест с `start_paused` на темп; тест на счётчик `replayed` при `replay` и `replay_all`.
- [x] `components.md`: строка «Event replay» обновлена.
- [x] demo: `progress.rs` удалён, активити-лог `rebuild.progress` работает как прежде (slow/fast режимы).
