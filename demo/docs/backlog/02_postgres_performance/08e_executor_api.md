# 08e. Одно ядро исполнителя, закрытые механизмы, имена

| | |
|---|---|
| Статус | сделано 2026-10-23 |
| Ценность | понятный API cqrsx: один исполнитель и две фабрики, наружу только настройки и backlog, имена совпадают с поведением |
| Цена | низкая–средняя: перестройка обёрток исполнителя без смены поведения, переименования, правка тестов |
| cqrsx | `cqrs/command/executor.rs` (`FrameworkFactory`), `cqrs/command/locks.rs`, `cqrs/command.rs`, `cqrs/event_handler.rs`, `cqrs/event_handler/delivery.rs`, `delivery/ordered.rs`, `cqrs/testing/command.rs`, `postgres/command.rs`, `diagnostics.rs`, `cqrs.rs` (комментарий), `doc/components.md`, тесты |
| demo | `back-end/src/config.rs`, `bootstrap.rs`, `testing.rs`, `application/operations/rebuild.rs`, `domain/fan.rs` (видимость тестовых помощников), `tests/write_side.rs`; `.env.example`, `README.md`; стенд `demo/load` (`run.sh`, `compose.yaml`, `README.md`), `perf/README.md` |
| Зависимости | [08d](08d_ordered_delivery_only.md); общий план — [08](08_per_aggregate_queues.md) |
| Где выигрыш | API cqrsx и читаемость движка; поведение и замеры не меняются |

## Зачем

Обзор кода после 08d показал три слабых места, не связанных с самой идеей (блокировка агрегата, резерв места до
записи, передача событий под блокировкой, квитанции после неё):

1. **Три исполнителя дублировали состояние.** `PostgresCommandExecutor` и `TestCommandExecutor` собирали
   `CqrsCommandExecutor` на каждый вызов и подсовывали ему обработчики через внутренний `with_handlers`.
   Блокировки задавались тремя способами: реестром в `CqrsCommandExecutor::new`, лимитами в
   `PostgresCommandConfig`, умолчанием в тестовом исполнителе. Два исполнителя с разными реестрами молча теряли
   порядок.
2. **Механизмы были публичными без пользователей.** `AggregateLocks`, `OrderedDelivery`, `DeliverySlot`,
   `DeliveryReceipt` снаружи использовали только два теста demo, а вызов резерва и передачи без блокировки ломает
   порядок, и ничто это не запрещало.
3. **Имена.** `max_wait` рядом с `max_waiting` путались; `OrderedDeliveryConfig` выбивался из ряда
   `DeliveryBacklog` и `DEFAULT_DELIVERY_*`; `Lane`/`LaneRef` означали разное в двух модулях; помощник
   std-мьютекса назывался `lock`. Лог сбоя обработчика не называл обработчик, так что с несколькими обработчиками
   оператор не видел, какой упал.

## Решения

1. **Одно ядро, две фабрики.** Фабрика строит фреймворк на каждую команду, как и раньше; сам исполнитель живёт
   долго и владеет реестром блокировок и обработчиками.

   ```rust
   pub trait FrameworkFactory<A: Aggregate>: Send + Sync {
       type Store: EventStore<A>;
       /// One framework per command, bound to the command's outbox tasks.
       fn framework(&self, tasks: Vec<NewTask>) -> CqrsFramework<A, Self::Store>;
   }
   // every `Fn(Vec<NewTask>) -> CqrsFramework<A, S>` is a factory too

   CqrsCommandExecutor::new(factory, retry_policy, lock_limits)
       .with_event_handler(handler, DeliveryConfig)   // before the executor is shared
       .backlog()
   ```

   - `PostgresCommandExecutor::new(pool, config, services)` — обёртка над одним `CqrsCommandExecutor` с фабрикой
     PostgreSQL (репозиторий с задачами команды, хранилище со снапшотами, клон сервисов); политика повторов и
     лимиты блокировки берутся из `PostgresCommandConfig`.
   - `TestCommandExecutor::new(store, services)` — такая же обёртка в `Arc`: клоны делят один исполнитель;
     обработчики регистрируются до клонирования, иначе паника с понятным текстом.
   - Команды у обёрток идут только через трейт `CommandExecutor`; собственных `execute` у них нет.
   - Удалены `with_handlers`, `command_executor()` и `with_retry_policy`.
   - Правило для пользователя: один исполнитель на тип агрегата в процессе, делить через `Arc`. Два исполнителя
     одного типа — как два процесса: их рассудит оптимистическая проверка хранилища.
2. **Наружу только настройки и наблюдение.** Публичны `LockLimits`, `LockLimitsError`, `DeliveryConfig`,
   `DeliveryConfigError`, `DeliveryBacklog` и константы умолчаний. `AggregateLocks`, `AggregateLock`,
   `LockRefused`, `OrderedDelivery`, `DeliverySlot`, `DeliveryReceipt`, `DeliveryError` — `pub(crate)`, модуль
   `ordered` закрыт.
3. **Имена.**
   - `OrderedDeliveryConfig` → `DeliveryConfig`, `OrderedDeliveryConfigError` → `DeliveryConfigError`: доставка
     одна, слово «ordered» остаётся у внутреннего механизма, который гарантирует порядок.
   - `max_waiting` → `max_waiters` везде: поле и геттер, `DEFAULT_LOCK_MAX_WAITERS`, `ZeroMaxWaiters`,
     `TooManyWaiters` (в логе `reason="too_many_waiters"`), `ENCORE_COMMAND_LOCK_MAX_WAITERS`, на стенде
     `LOCK_MAX_WAITERS=`.
   - Внутри: в блокировках `Lane`/`LaneRef` → `Line`/`LineRef` (документация и так говорила «line»); в доставке
     `Lane`/`LaneRef`/`Lanes` → `KeyBuffer`/`BufferRef`/`Buffers`, помощник `lock()` → `guarded()`, в комментариях
     про std-мьютекс — «mutex».
4. **Логи.** Сбой и паника доставки пишутся с полем `handler` — именем обработчика (имя типа или коллекции витрины).
5. **Тексты.** `command.rs` описывает модуль целиком; у `CommandGatewayError::Unavailable` перечислены все причины
   (хранилище, отказ блокировки, нет места в буфере); у `LockLimits::max_wait` и в `.env.example` сказано, что срок
   покрывает и ожидание места в буфере обработчика; из `delivery.rs` убрано упоминание Encore!; комментарии про
   «per-command framework» у `Arc<H>` и в `components.md` заменены.
6. **Известное ограничение записано в `components.md`.** Future команды не защищена от отмены на участке
   «запись ушла в базу — передача обработчику»: при обрыве клиента axum отменяет future обработчика HTTP, событие и
   задачи outbox могут записаться, а витрина узнает о них только после Encore!. Так было и до эпика; защита —
   отдельный тикет.

## Пограничные случаи

| # | Случай | Поведение | Проверка |
|---|---|---|---|
| 1 | Регистрация обработчика у тестового исполнителя после клонирования | паника «register event handlers before cloning the executor» | `a_handler_registered_after_a_clone_is_refused` |
| 2 | Два исполнителя одного типа агрегата | как два процесса: конфликты решает хранилище | `locks_accept_twenty_parallel_commands_with_a_single_attempt_where_separate_executors_conflict`, `a_conflict_retries_inside_the_lock_before_the_next_command_starts` |
| 3 | Отказ блокировки | как раньше, без доступа к реестру: держатель ждёт в загрузке, следующий ждёт, третий получает отказ сразу | `a_refused_lock_is_unavailable_logged_without_id_and_builds_nothing` |
| 4 | Сбой обработчика с несколькими обработчиками | в логе `handler="<имя>"` | `delivery_diagnostics_log_one_safe_event_per_batch_with_the_storage_category` |
| 5 | Остановка demo с невыполненной передачей | как в 08b, через `TestCommandExecutor<Fan>` с удерживающей проекцией | `draining_waits_for_the_projections_and_reports_what_is_left_at_the_limit`, `a_rebuild_wipes_nothing_until_the_projections_have_applied_what_was_handed_over` |

## Что сделано

**cqrsx:** по решениям 1–5; `cqrsx/tests` и `demo` переведены на `PostgresCommandExecutor::new(pool, config, ())`
вместо `command_executor` и на `LockLimits::default()` вместо `AggregateLocks::default()`.

**demo:** `Config::command_lock_limits` читает `ENCORE_COMMAND_LOCK_MAX_WAITERS`; `Config::projection_delivery` —
`DeliveryConfig`; тесты `bootstrap.rs` и `rebuild.rs` строят backlog через `TestCommandExecutor<Fan>` с командой в
отдельной задаче (помощники `domain::fan::test_support` видны крейту в тестах); `tests/write_side.rs` без лишнего
импорта. Стенд: `LOCK_MAX_WAITERS=`.

**Документы:** `components.md` (строки инвентаря, раскладка исходников, правило об отмене); заметки с новыми
именами в 08, 08a, 08b, 08c, 08d; README эпика и backlog; `PROGRESS.md`.

## Потом

- Защита участка «запись и передача» от отмены вызывающей future: выполнять его в отдельной задаче и ждать её
  `JoinHandle`.
- Таймаут обработчика и проверка «команда из обработчика» через task-local: сейчас оба случая заканчиваются
  зависанием.
- Разделение `Unavailable` на «хранилище» и «занято», если HTTP-слою понадобится различать коды.
- `cqrsx/README.md` со сквозным примером: агрегат, исполнитель с обработчиком, задача outbox, остановка через
  backlog.

## Критерии приёмки

- [x] В cqrsx один `CqrsCommandExecutor` с `FrameworkFactory`; `PostgresCommandExecutor` и `TestCommandExecutor` —
      обёртки без собственных `execute`; нет `with_handlers`, `command_executor`, `with_retry_policy`.
- [x] Публичны только `LockLimits`, `DeliveryConfig`, `DeliveryBacklog`, ошибки конфигурации и константы;
      механизмы блокировок и доставки — `pub(crate)`.
- [x] `max_waiters` и `DeliveryConfig` во всех именах, включая env, стенд и документы; в логе сбоя обработчика —
      его имя.
- [x] `components.md`, тикеты 08–08d, README эпика, `backlog/README.md`, `PROGRESS.md`.
- [x] Полный набор проверок из корневого `CLAUDE.md` и проверки demo зелёные.

Проверки: полный набор из корневого `CLAUDE.md` зелёный, покрытие 97.84 % строк, сборки по отдельным фичам без
предупреждений, doc-тестов 34 и 7 compile-fail. demo — fmt, clippy и тесты: 111 модульных, архитектура, `write_side`
на PostgreSQL, sidecar.
