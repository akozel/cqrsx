# 05. Убрать обработчики исполнителя и старый replay

| | |
|---|---|
| Статус | сделано 2026-10-25 |
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

- [x] Перечисленное удалено; `cargo doc` без битых ссылок; матрица фич зелёная; покрытие ≥ 97 %.
- [x] `components.md`: инвентарь, дерево исходников, путь команды и путь проекции, «Upstream attribution»
      (replay utility удалён).
- [x] 02/14 помечен закрытым через [02](02_event_position.md) (обсуждения 03 и 04 удалены в [04](04_demo_on_processor.md));
      с удалением `all_events` сортирующего чтения журнала больше нет.

## Отклонения

- **Доставка ушла внутрь процессора.** `event_handler/delivery.rs` и `ordered.rs` стали приватным
  `projection/processor/delivery.rs`; в `event_handler.rs` остался только трейт `EventHandler`.
- **`DeliveryConfig` → `DeliveryLimits`** в `processor/config.rs`, с `MIN_DELIVERY_CAPACITY` (2): проверка «не меньше
  двух» переехала из `ProcessorConfig::new` в `DeliveryLimits::new` (`CapacityTooSmall { min }`),
  `ProcessorConfigError::DeliveryCapacityTooSmall` удалён. Поле и геттер — `delivery_limits`, как `lock_limits`.
  Константы — `usize`. В `components.md` записано правило суффиксов `Config` / `Policy` / `Limits` с известными
  отклонениями.
- **`DeliveryBacklog` удалён целиком**, а не только скрыт: процессор ждёт свои квитанции сам. Тесты доставки
  проверяют занятое место ключа (`held`) и пустой реестр ключей. Ребилдер обязателен (`OrderedDelivery::new(name,
  handler, rebuilder, limits)`), ветки «доставка без ребилдера» больше нет.
- **Логи пачек под именами процессора:** `log_projection_batch_failure`, `log_projection_batch_panic`
  (`component="projection"`, `projection` = имя процессора, `jobs`); `log_delivery_refused` удалён.
- **`TestCommandExecutor::over_journal(MemEventRepository, services)`**: исполнитель use case над журналом, который
  читают процессоры; для этого `PersistedEventStore` клонируется (апкастеры общие, `Arc<[…]>`). Quick start
  README — на нём и процессоре.
- **У `CommandExecutorCore` нет поля обработчиков**, тип агрегата держит `PhantomData`.
- **Модель замков** тоже получила префикс `property_`; шаг CI называется «Properties with the commit seed».
- **Новые тесты на пробелы**, которые оставили удалённые: сорок параллельных команд к одному ключу на PostgreSQL
  через `PostgresCommandExecutor` с пробуждением (время пробуждения там не измерить: горизонт снимка общий на
  кластер, и соседний тест с открытой транзакцией задерживает ленту до опроса), метаданные события в проекции, апкастеры
  на пути пересборки, повтор пересборки после сбоя чтения истории, ядро исполнителя с сервисами без `Clone`.
  `tests/postgres/event_store/streams.rs` → `history.rs`: остались чтения истории ключа.
