# 05. Убрать обработчики исполнителя и старый replay

| | |
|---|---|
| Статус | предложено |
| Ценность | средняя: один путь событий в проекции; API cqrsx без второго механизма |
| Цена | средняя: удаление, переписывание тестов, документация |
| cqrsx | `cqrs/command/executor.rs`, `cqrs/command.rs`, `cqrs/event_handler/delivery.rs` (остаётся как очередь процессора), `cqrs/event_handler/replay.rs` (удалить), `cqrs/store/stream.rs`, `postgres/event_repository/stream.rs`, `postgres/command.rs`, `cqrs/testing/command.rs`, `README.md`, `doc/components.md`; тесты |
| demo | только импорты, если что-то осталось после [04](04_demo_on_processor.md) |
| Зависимости | [04](04_demo_on_processor.md) |
| Где выигрыш | читаемость и размер API cqrsx |

## Что удаляется

- `CommandExecutorCore::with_event_handler`, `backlog`, `RegisteredHandlers`, резерв места и квитанции в пути
  команды; `CommandGatewayError::CommittedWithEventHandlerFailure`, `FailedHandler`; то же у
  `PostgresCommandExecutor` и `TestCommandExecutor`. Путь команды: блокировка агрегата → фреймворк → коммит с
  повторами → `wake()` → ответ. Дедлайн `max_wait` покрывает только блокировку.
- `EventReplay`, `ReplayProgress`, `ReplayProgressObserver`; `PersistedEventRepository::stream_events` и
  `stream_all_events`, `ReplayStream`, `ReplayFeed`, модуль `postgres/event_repository/stream.rs`,
  `with_streaming_channel_size`, `DEFAULT_STREAMING_CHANNEL_SIZE`.
- `DeliveryBacklog` наружу (процессор ждёт свои квитанции сам); `DeliveryConfig` остаётся в `ProcessorConfig`,
  `Done` и `Rebuilder` — у `OrderedDelivery`.
- Модель `ordered_delivery_matches_a_fifo_batch_model` переименовать с префиксом `property_`, чтобы CI гонял её
  с сидом коммита (заодно с переписыванием тестов доставки).

## Что остаётся

- `OrderedDelivery` — очередь процессора на ключ; в `components.md` Delivery становится частью Processor.
- `EventHandler` — контракт обработчика, который вызывает процессор; правило о командах из обработчика остаётся.
- `log_delivery_*` в `diagnostics` — под именами процессора.

## Тесты

- Тесты исполнителя с обработчиками переписываются на процессор над `MemEventRepository` (основная часть уже в
  [03](03_projection_processor.md)).
- Тесты replay — на `request_rebuild`, `request_rebuild_key` и самолечение процессора.
- Quick start в `cqrsx/README.md` — через процессор; doc-тесты без `EventReplay`.

## Критерии приёмки

- [ ] Перечисленное удалено; `cargo doc` без битых ссылок; матрица фич зелёная; покрытие ≥ 97 %.
- [ ] `components.md`: инвентарь, дерево исходников, путь команды и путь проекции, «Upstream attribution»
      (replay utility удалён).
- [ ] 02/14 помечен закрытым через [02](02_event_position.md) (обсуждения 03 и 04 удалены в [04](04_demo_on_processor.md)).
