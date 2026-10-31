# 08c. Только блокировка агрегата

| | |
|---|---|
| Статус | сделано 2026-10-21 |
| Ценность | простота: один способ выполнять команды, меньше переключателей и сочетаний; понятное имя механизма |
| Цена | низкая: удаление режима и переименование, без нового замера |
| cqrsx | `cqrs/command/locks.rs` (было `turns.rs`), `cqrs/command.rs`, `cqrs/command/executor.rs`, `cqrs/event_handler.rs`, `cqrs/event_handler/delivery.rs`, `delivery/ordered.rs` (комментарии), `cqrs/testing/command.rs`, `postgres/command.rs`, `diagnostics.rs`, `doc/components.md`, тесты |
| demo | `back-end/src/config.rs`, `bootstrap.rs`, `testing.rs`, комментарии в `application/lock.rs`, `operations/rebuild.rs`, `command_service.rs`; `tests/write_side.rs`; `.env.example`, `README.md`; стенд `demo/load` (`run.sh`, `compose.yaml`, `tools/collect.mjs`, `tools/report-html.mjs`, `k6/hot-key.js`, `README.md`), `perf/README.md` |
| Зависимости | [08b](08b_ordered_projection_delivery.md) (замер); общий план — [08](08_per_aggregate_queues.md) |
| Где выигрыш | конфигурация demo и API cqrsx |

> **Имена после [08e](08e_executor_api.md).** `max_waiting` → `max_waiters` (`DEFAULT_LOCK_MAX_WAITERS`,
> `ENCORE_COMMAND_LOCK_MAX_WAITERS`, `LOCK_MAX_WAITERS=`), `TooManyWaiting` → `TooManyWaiters`. Реестр
> `AggregateLocks` стал внутренним: `CqrsCommandExecutor::new(factory, retry_policy, lock_limits)` принимает
> `LockLimits`, а общие блокировки — это один исполнитель в `Arc`. `with_handlers` удалён. Ниже — имена 08c.

## Зачем

**Режим `concurrent` больше не нужен.** По замеру 08b на одной ноде блокировка агрегата с доставкой `ordered`:
- быстрее `concurrent` при 5–40 клиентах на горячем ключе;
- не хуже при одном клиенте и при нагрузке на много концертов;
- вдвое дешевле по CPU на команду и обходится без 409.

`concurrent` выиграл только в одной конфигурации: две ноды за nginx, nginx раздаёт запросы по очереди, все запросы
идут на один концерт. Там он провёл 710 успешных броней в секунду против 554, но двое из трёх клиентов получили 409,
а CPU ушло вдвое больше. Это лечит раздача запросов по концерту ([15](15_sharding.md)).

**Довод про синхронные команды из обработчиков снят.** Такие команды без блокировки работали, с блокировкой ждали
сами себя. Теперь `components.md` запрещает их в любом случае: следующая команда — это задача outbox.

**Слово «ход» (turn) оказалось непонятным.** По сути это мьютекс на ключ агрегата внутри процесса, поэтому
механизм теперь называется «блокировка агрегата» (lock). Имя `lock` в cqrsx свободно; в demo есть замок кассы
`BoxOfficeLock`, но он про другое — пересборка закрывает кассу целиком.

## Что сделано

**cqrsx:**
- Модуль `cqrs/command/turns.rs` переименован в `locks.rs`:

  | Было | Стало |
  |---|---|
  | `AggregateTurns` | `AggregateLocks`, плюс `Default` — реестр с лимитами по умолчанию |
  | `AggregateTurn` | `AggregateLock` |
  | `enter` | `acquire` |
  | `TurnLimits`, `TurnLimitsError` | `LockLimits`, `LockLimitsError` |
  | `TurnRefused` | `LockRefused` |
  | `DEFAULT_TURN_MAX_WAIT`, `DEFAULT_TURN_MAX_WAITING` | `DEFAULT_LOCK_MAX_WAIT`, `DEFAULT_LOCK_MAX_WAITING` |

  Отказ пишется в лог с `operation="lock"`.
- `CommandOrdering` удалён, вместе с ним `with_turns`.
  - `CqrsCommandExecutor::new(build_framework, retry_policy, locks)`: реестр — обязательный аргумент.
  - `PostgresCommandConfig::new(snapshot_interval, conflict_retry, lock_limits)`, геттер — `lock_limits()`.
- `OrderedDeliveryWithoutTurns` удалён: `with_event_handler_via` возвращает исполнитель, а не `Result`.
- `TestCommandExecutor` держит один реестр на все свои клоны, как боевые исполнители.
- Тесты, которые сравнивали путь с блокировкой и без неё, теперь сравнивают общий реестр с отдельными реестрами,
  как у отдельных процессов. Контрольный случай по-прежнему показывает гонку. Конфликтный тест на PostgreSQL идёт
  через два исполнителя — «две ноды».

**demo:**
- `ENCORE_COMMAND_ORDERING` удалён.
- `ENCORE_COMMAND_TURN_MAX_WAIT_MS` и `_MAX_WAITING` переименованы в `ENCORE_COMMAND_LOCK_MAX_WAIT_MS` и
  `_MAX_WAITING` (поле `Config::command_lock_limits`). Проверка «ожидание короче таймаута обработчика outbox»
  осталась.
- `ENCORE_PROJECTION_DELIVERY` без значения — `ordered`. `inline` пока остаётся.
- `.env.example` прописывает все пять значений явно; умолчания — из констант cqrsx.
- Стенд: параметр `ORDERING=` удалён, `TURN_MAX_*` переименованы в `LOCK_MAX_*`. В старых `result.json` остаётся
  `meta.command_ordering`.

**Документы:**
- `components.md`: строка «Aggregate locks»; в правилах имён Lock вместо Turn.
- Тикеты 08, 08a (файл переименован в `08a_aggregate_locks.md`) и 08b: «ход» заменён на «блокировку агрегата».
  В начале каждого — соответствие старых имён новым. Код, тесты и варианты замера в них остались в именах своего
  шага: на них ссылаются результаты.

## Замер

Новый не нужен. Путь «блокировка + `ordered`» замерен в 08b и не изменился: поменялись имена, и удалена ветка без
блокировки.

## Открыто

Нужен ли режим `inline` для проекций. Решено в [08d](08d_ordered_delivery_only.md): не нужен, удалён.

## Критерии приёмки

- [x] В cqrsx нет `CommandOrdering`, `with_turns`, `OrderedDeliveryWithoutTurns`; модуль и типы называются lock.
- [x] demo: нет `ENCORE_COMMAND_ORDERING`; лимиты — `ENCORE_COMMAND_LOCK_*`; в `.env.example` все значения
      прописаны явно.
- [x] Стенд: нет `ORDERING=`; `LOCK_MAX_WAIT_MS=`, `LOCK_MAX_WAITING=`.
- [x] `components.md`, тикеты 08, 08a, 08b, README эпика, `PROGRESS.md`.
- [x] Полный набор проверок из корневого `CLAUDE.md` и проверки demo зелёные.

Проверки: полный набор из корневого `CLAUDE.md` зелёный, покрытие 97.79 % строк (`locks.rs` — 99.58 %); сборки без
фич больше не предупреждают о `with_handlers` (метод нужен только с `postgres`). demo — fmt, clippy и тесты: 111
модульных, архитектура, `write_side` на PostgreSQL, sidecar.
