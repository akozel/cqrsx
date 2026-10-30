# 02. Риск: падения и неверное поведение

| | |
|---|---|
| Статус | предложено |
| Ценность | высокая: каждое место — падение процесса или неверное поведение уже сейчас |
| Цена | низкая–средняя: правки местные; одна (panic обработчика задач) меняет поведение и тест |
| cqrsx | `cqrs/store/persisted.rs`, `postgres/command.rs`, `engine.rs`, `task/executor/default_executor.rs`, `task/runtime/supervisor.rs`, `postgres/view_repository.rs`, `task.rs`, `engine/serve.rs`, `cqrs/projection/processor/delivery.rs`, `task/queue/settlement.rs`, `postgres/outbox/write.rs`, `engine/build.rs`, `cqrs/projection.rs`; общий модуль для `unwinding` |
| demo | вызовы `new_snapshot_store`/`PostgresCommandConfig::new`, если есть |
| Зависимости | 01 |
| Правила | 6.1, 7, 8, 11, 21, 34 |

## Сейчас и станет

Места — по HEAD `e995a53`, подробности в [аудите](../../cqrsx-style-audit.html#risk).

1. **Деление на ноль в хранилище событий** (правило 6.1). `PersistedEventStore::new_snapshot_store(repo, 0)`
   принимается, после чего каждый commit делит на ноль (`persisted.rs:31`, `current_sequence % 0`). Оба рабочих вызова
   (`engine.rs:237`, `postgres/command.rs:85`) держат `NonZeroUsize` и отбрасывают его через `.get()`.
   **Станет:** `SnapshotPolicy::Every(NonZeroUsize)`, конструктор принимает `NonZeroUsize`;
   `PostgresCommandConfig::new` тоже принимает `NonZeroUsize` и перестаёт возвращать `Result` — уходят
   `PostgresCommandConfigError` и `expect` в `engine.rs:270`. Переименование в `new`/`with_snapshots` — задача 05.
2. **panic обработчика задачи не перехватывается** (правила 8, 11). `default_executor.rs:106`: panic уходит через
   `execute_and_settle`, worker погибает, supervisor (`supervisor.rs:83`) запускает новый, задача остаётся без
   settlement до истечения lease. **Станет:** `dispatch` обёрнут тем же `unwinding`, что у проекций
   (`delivery.rs:426`, переезжает в общий модуль); `error!` без payload и lease token; settlement
   `Retry` с постоянной причиной (`"handler panicked"`) и обычной паузой политики. Перезапуск worker остаётся
   последней защитой. Тест `tests/task/runtime/supervision.rs:183`
   (`panic_restarts_without_poll_tick_with_the_shared_dispatcher_and_a_new_task`) переписывается: panic → повтор задачи,
   worker жив; перезапуск проверяется отдельным путём (отмена или panic вне обработчика).
3. **`assert!` в supervisor задач** (правило 7). `supervisor.rs:124`, `:128` проверяют контракт `TaskQueue`
   (не больше `limit` задач, `try_send` проходит). `TaskQueue` реализует пользователь библиотеки. **Станет:** `error!`
   с названием нарушения; лишняя задача отбрасывается и вернётся через восстановление lease; цикл продолжается.
4. **`Row::get` в PostgreSQL-хранилище витрин** (правила 6, 7). `postgres/view_repository.rs:187`, `:202–204`,
   `:261–264` паникуют, если колонки нет или тип не совпал. **Станет:** `try_get` с переводом в
   `DeserializationError`, как `row_decode_error` в `event_repository/read.rs`.
5. **`NewTask::with_metadata` теряет labels** (вне правил, ошибка). `task.rs:92` заменяет metadata целиком и
   выбрасывает labels, которые `NewTask::new` перенёс из payload; обработчик потом не декодирует задачу. **Станет:**
   labels сохраняются (новые ключи дописываются поверх, labels задачи не теряются) или `Err`, если вызывающий
   перезаписал label; поведение описано в документации. Выбор варианта — в плане задачи.
6. **`_` по `BuildError`** (правило 21). `engine/serve.rs:46–47`: `DatabaseUrl`, `MongoUri`, `ViewsWithoutJournal` —
   тоже неверная конфигурация, но сообщение без префикса `invalid configuration:`, который обещает документация
   `built()`. **Станет:** варианты перечислены; текст ошибки для этих трёх получает префикс.
7. **`_` в `TaskSchedule::validate`** (правило 21). `task.rs:44`: новый вариант расписания прошёл бы без проверки.
   **Станет:** `Self::After(_) | Self::Now | Self::At(_) => Ok(())` после ветки проверки `After`.
8. **`_` по `Work` в доставке проекций** (правило 21). `delivery.rs:327–335`, `:362–373`: с новым вариантом
   `take_while` насчитает 0 и worker будет раздавать пустые пакеты, при объединении задание пропадёт. **Станет:**
   полное сопоставление `Work`.
9. **`_` в `SettlementResult`** (правило 21). `task/queue/settlement.rs:48` (`!matches!(self,
   Self::OwnershipLost)`), `:53` (`_ => None`). **Станет:** варианты перечислены.
10. **`_` по `SnapshotWrite` и `Views`** (правило 21). `postgres/outbox/write.rs:72`, `:118–121`;
    `engine/build.rs:264–272` (`else { "MongoDB" }`). **Станет:** `Insert { .. } | Update { .. }`;
    `match views { Memory => …, Postgres => …, MongoDb => … }`.
11. **Неверная документация `set_error_observer`** (правило 34). `cqrs/projection.rs:211–219` говорит, что ошибки
    «simply ignored»; на деле они возвращаются как `Err` из `load`, `handle`, `rebuild`. **Станет:** документация
    описывает, когда вызывается наблюдатель и что ошибка всё равно возвращается.

## Этапы

1. План с выбором варианта для п. 5 и списком тестов (каждое исправление — тест, который падает на `e995a53`).
2. Пункты 1–11, тесты, правки demo.
3. `CLAUDE.md`-проверки; коммит.

## Риски

- **п. 2 меняет поведение:** обработчик, который паникует всегда, теперь крутится повторами до лимита попыток, а не
  перезапускает worker. Это и есть цель (задача уходит в `failed` по политике); проверить, что лимит попыток и пауза
  берутся из политики задачи.
- **п. 4:** перевод `Row::get` в ошибку не должен изменить тексты SQL; набор операторов на стенде (задача 08)
  сверяется с 01.

## Как проверить

- Новый тест на каждый пункт; `tests/task/runtime/supervision.rs` с новым ожиданием.
- `CLAUDE.md`: fmt, clippy, тесты всех features, оба прогона покрытия ≥ 97 %.

## Критерии приёмки

- [ ] 11 пунктов исправлены, у каждого тест.
- [ ] Изменения поведения (п. 2, 3, 5, 6) перечислены в сообщении коммита и в README эпика.
- [ ] Тексты SQL и `GET /api/config` demo не изменились.
