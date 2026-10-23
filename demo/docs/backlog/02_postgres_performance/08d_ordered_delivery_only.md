# 08d. Только упорядоченная доставка

| | |
|---|---|
| Статус | сделано 2026-10-21 |
| Ценность | одна модель доставки событий в обработчики, без переключателя и без скрытого inline на нижнем уровне |
| Цена | средняя: смена API `CqrsFramework` и регистрации обработчиков, переделка тестов cqrsx |
| cqrsx | `cqrs.rs`, `cqrs/event_handler.rs`, `cqrs/event_handler/delivery.rs`, `cqrs/command/executor.rs`, `cqrs/testing/command.rs`, `postgres/command.rs`, `doc/components.md`, тесты (`tests/cqrs/execution.rs`, `tests/postgres/{command,projection}.rs`, `tests/postgres/projection/committed.rs` и механически остальные) |
| demo | `back-end/src/config.rs`, `bootstrap.rs`, `testing.rs`, комментарий `application/lock.rs`; `tests/write_side.rs`; `.env.example`, `README.md`; стенд `demo/load` (`run.sh`, `compose.yaml`, `tools/collect.mjs`, `tools/report-html.mjs`, `README.md`), `perf/README.md` |
| Зависимости | [08c](08c_aggregate_lock_only.md); общий план — [08](08_per_aggregate_queues.md) |
| Где выигрыш | API cqrsx и конфигурация demo |

> **Имена после [08e](08e_executor_api.md).** `OrderedDeliveryConfig` → `DeliveryConfig`; `with_handlers` удалён:
> `PostgresCommandExecutor` и `TestCommandExecutor` держат один `CqrsCommandExecutor`, а не собирают его на
> каждый вызов. `OrderedDelivery` и её спутники стали внутренними. Ниже — имена 08d.

## Зачем

После 08c у обработчиков событий оставалось два способа доставки:
- `Inline` — команда сама вызывает обработчик, пока держит блокировку своего агрегата;
- `Ordered` — команда передаёт события в буфер ключа, отпускает блокировку, а обработчик ключа применяет их по
  порядку.

Кроме того, у нижнего уровня `CqrsFramework` был свой список обработчиков, и они всегда вызывались inline.

По замеру [08b](08b_ordered_projection_delivery.md) inline держит блокировку и на время записи в MongoDB:
потолок горячего концерта ~500 принятых команд/с против ~750 у `ordered`. При одном клиенте `ordered` не медленнее
inline без блокировки (~310/с). Быстрее был только вариант «блокировка + inline» (419/с), причину не выяснили, а
выигрыш — 0.4 мс на запрос. Ради этого держать второй режим, переменную и ветку кода не стоит.

## Решения

1. **`CqrsFramework` только загружает, решает и записывает.**
   - Конструктор — `new(store, services)`.
   - `execute` и `execute_with_metadata` возвращают закоммиченные события.
   - Удалены:
     - `append_event_handler`, `decide_and_commit`, `notify`;
     - вариант `AggregateError::CommittedWithEventHandlerFailure`;
     - типы `CommittedWithEventHandlerFailure` и `EventHandlerFailure`.
   - Кто вызывает фреймворк напрямую, получает события и сам решает, что с ними делать.
2. **Обработчики есть только у исполнителя команд, и все упорядоченные.**
   - `with_event_handler(handler, OrderedDeliveryConfig)` — одинаково у `CqrsCommandExecutor`,
     `PostgresCommandExecutor` и `TestCommandExecutor`.
   - `HandlerDelivery` и `with_event_handler_via` удалены.
   - Конфигурация передаётся явно, как `LockLimits`.
3. **Номер обработчика в `CommittedWithEventHandlerFailure`** — его позиция регистрации у исполнителя. Исходную
   ошибку обработчика пишет в лог рабочая задача, один раз на пачку; до шлюза доходят номер и имя.
4. **demo.**
   - `ENCORE_PROJECTION_DELIVERY` удалена.
   - `ENCORE_PROJECTION_BUFFER` и `ENCORE_PROJECTION_MAX_BATCH` остаются: каждое значение конфигурации — env с
     константой по умолчанию.
   - `Config::projection_delivery` — `OrderedDeliveryConfig`.
5. **Нового замера нет.** Путь demo по умолчанию с 08b — `ordered`, он замерен. Изменились API и тестовая обвязка.

## Пограничные случаи

| # | Случай | Поведение | Проверка |
|---|---|---|---|
| 1 | Обработчик падает | `CommittedWithEventHandlerFailure` с позицией регистрации и именем; исходная ошибка — в логе рабочей задачи, один раз на пачку | `handlers_report_their_registration_indices_and_each_sees_every_event_in_order`, `committed_event_handler_failures_retain_indices_and_invoke_every_handler`, `projection_failures_preserve_one_command_commit_and_outbox_and_invoke_other_handlers` |
| 2 | Паника обработчика | та же ошибка шлюза, команда не паникует (inline-паника роняла задачу команды) | `a_panic_fails_the_batch_and_the_key_buffer_keeps_working` |
| 3 | Несколько обработчиков одной команды | у каждого свой буфер и рабочая задача: работают одновременно, а не по порядку регистрации; внутри обработчика — порядок коммитов | тесты из п. 1, doc `EventHandler`, `components.md` |
| 4 | Команда без событий, отказ, конфликт, недоступное хранилище | обработчикам ничего не передаётся | `empty_commands_and_all_failure_boundaries_do_not_dispatch`, `a_command_without_events_hands_nothing_over` |
| 5 | Обработчик синхронно выполняет команду | вечное ожидание собственной квитанции (inline-случай раньше кончался 503); правило — задача outbox | `components.md`, doc `EventHandler` и `CqrsCommandExecutor` |
| 6 | Пачка из нескольких команд | обработчик получает склейку событий, ошибка достаётся всей пачке; `max_batch = 1` даёт изоляцию | тесты 08b |
| 7 | `CqrsFramework` напрямую | события возвращаются, обработчиков нет | `execute_commits_and_returns_the_events_in_commit_order`, `framework_test`, `load_handle_commit_preserves_services_and_metadata_and_returns_the_events` |
| 8 | `TestCommandExecutor` | обработчики упорядоченные, как в бою; нужен рантайм Tokio, команда возвращается после квитанций | `committed_handler_failures_publish_and_report_the_handler`, doctest |
| 9 | Остановка и Encore! | `backlog().drained()`, как в 08b | тесты 08b |

## Что сделано

**cqrsx:**
- `CqrsFramework` по решению 1.
- `RegisteredHandlers` хранит только упорядоченные доставки.
- Исполнитель:
  - берёт блокировку и резервирует место у обработчиков;
  - записывает с повторами;
  - передаёт события, отпускает блокировку и ждёт квитанции.
- `with_handlers` снова без условия фичи: им пользуется `TestCommandExecutor`.
- Документация:
  - `components.md`: путь команды, строки инвентаря, правила имён (Delivery — только через буфер ключа), правило
    о командах из обработчиков, «Upstream attribution»;
  - doc-комментарии модулей и типов.
- Тесты:
  - проверки фреймворка через обработчики переписаны на проверку возвращённых событий;
  - проверки обработчиков перенесены на исполнитель;
  - в остальных местах `CqrsFramework::new(store, vec![], services)` заменено на `new(store, services)`.

**demo:**
- `config.rs`, `bootstrap.rs`, `testing.rs` — по решению 4. Тестовая конфигурация теперь тоже упорядоченная (была
  `Inline`).
- `.env.example` и `README.md` без `ENCORE_PROJECTION_DELIVERY`.
- Стенд: параметр `DELIVERY=` удалён; в старых `result.json` остаётся `meta.projection_delivery`.

## Риски

- **Команда из обработчика теперь всегда зависает**, а не получает 503. Обнаружения нет, только правило. Метка
  задачи Tokio — возможное продолжение.
- **Обработчики одной команды больше не идут по очереди.** Если обработчик полагался на то, что другой уже отработал,
  он должен работать через outbox или объединиться с ним в один обработчик.

## Критерии приёмки

- [x] В cqrsx нет `HandlerDelivery`, `with_event_handler_via`, `append_event_handler`, `notify`, обработчиков
      `CqrsFramework`, `AggregateError::CommittedWithEventHandlerFailure`.
- [x] demo и стенд без `ENCORE_PROJECTION_DELIVERY` и `DELIVERY=`; буфер и пачка настраиваются через env.
- [x] `components.md`, тикеты 08, 08b, 08c, README эпика, `PROGRESS.md`.
- [x] Полный набор проверок из корневого `CLAUDE.md` и проверки demo зелёные.

Проверки: полный набор из корневого `CLAUDE.md` зелёный, покрытие 97.80 % строк, сборки по отдельным фичам без
предупреждений; doc-тестов 34 (удалён пример `append_event_handler`). demo — fmt, clippy и тесты: 111 модульных,
архитектура, `write_side` на PostgreSQL, sidecar.
