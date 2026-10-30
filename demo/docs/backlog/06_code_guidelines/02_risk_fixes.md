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

- [x] 11 пунктов исправлены, у каждого тест.
- [ ] Изменения поведения (п. 2, 3, 5, 6) перечислены в сообщении коммита и в README эпика. README — да; сообщение
  коммита — при коммите.
- [x] Тексты SQL и `GET /api/config` demo не изменились.

## Результаты

Сделано 2026-10-30 на HEAD `a270a06` поверх правок задачи 01 в рабочем дереве (коммита ещё нет). Тексты SQL не
менялись; `GET /api/config` demo тот же (`snapshot_every` по-прежнему число).

| п. | Что сделано | Тест |
|---|---|---|
| 1 | `SnapshotPolicy::Every(NonZeroUsize)`; `new_snapshot_store(repo, NonZeroUsize)`; `PostgresCommandConfig::new(NonZeroUsize, …) -> Self`, геттер отдаёт `NonZeroUsize`; `PostgresCommandConfigError` и `expect` в `engine.rs` удалены | `postgres::command::tests::config_keeps_its_values_and_its_interval_is_never_zero`; ноль больше не компилируется |
| 2 | `unwinding` переехал в общий `src/unwinding.rs`; `DefaultTaskExecutor` оборачивает им `dispatch`: `error!` без сообщения panic, payload и lease token, `Retry` с причиной `HANDLER_PANICKED` (`"handler panicked"`) и паузой политики | `default_executor::tests::a_handler_panic_is_a_retry_after_the_pause_of_its_policy_logged_without_payload_or_lease_token`; `engine::memory::tests::a_handler_that_always_panics_spends_the_budget_of_its_policy_and_its_task_fails` (паузы 1, 2, 4, 8, 8 с, шесть попыток, `Failed`); `tests/task/runtime/supervision.rs`: `a_handler_panic_retries_its_task_after_the_policy_pause_and_the_worker_lives_on` (вместо `panic_restarts_…`), `supervisor_survives_a_handler_panic_storage_errors_and_lost_acknowledgements` |
| 2 | перезапуск worker проверяется смертью вне обработчика: свой `TaskExecutor`, который паникует один раз | `a_worker_that_dies_restarts_without_poll_tick_with_the_shared_dispatcher_and_a_new_task`, `cancellation_before_a_restart_does_not_start_a_replacement_worker` |
| 3 | `assert!` в supervisor заменены функцией `admit`: лишние задачи сверх `limit` — `error!` с названием контракта `TaskQueue::claim`, берётся `limit`; задача, не вошедшая в канал, — `error!`; цикл продолжается | `supervisor::tests::a_queue_that_claims_beyond_the_limit_is_logged_and_the_runtime_goes_on`, `a_task_that_does_not_fit_the_channel_is_logged_and_dropped` |
| 4 | `try_get` + `row_decode_error` (переехал в `event_repository.rs`, общий с `read.rs`) во всех чтениях `PostgresViewRepository` | `tests/postgres/projection.rs::columns_of_other_types_are_deserialization_errors_not_panics` (`version integer`, `payload text`) |
| 5 | вариант «labels сохраняются»: `NewTask` помнит `T::LABELS`, `with_metadata` заменяет metadata, но возвращает в неё labels; ключ вызывающего с именем label получает значение label (иначе payload не декодируется). Описано в документации метода | `task::tests::metadata_of_the_caller_keeps_the_labels_and_a_label_keeps_its_value` |
| 6 | `built()` перечисляет все варианты `BuildError`; `DatabaseUrl`, `MongoUri`, `ViewsWithoutJournal` — с префиксом `invalid configuration:`. Эталонов demo с этим текстом нет | `engine::serve::tests::startup_errors_read_as_the_process_wrote_them`, `an_unparsable_store_address_is_an_invalid_configuration` |
| 7 | `Self::After(_) \| Self::Now \| Self::At(_) => Ok(())` | `task::tests::construction_validates_format_objects_attempts_and_schedule` (+ `Now`) |
| 8 | `next_batch` сопоставляет `Work` полностью; объединение пакета — через `Work::events()` с полным `match` | тесты `delivery.rs` о пакетах и rebuild |
| 9 | `is_applied` и `failure` перечисляют варианты | `settlement::tests::confirmed_results_and_recovery_counts_are_unambiguous` (+ `Failed` применён) |
| 10 | `SnapshotWrite`: `Insert { .. } \| Update { .. }`; `Views`: `match` с тремя вариантами | `views_outside_memory_need_the_journal_in_postgres`, тесты outbox со снимком и без |
| 11 | документация `set_error_observer`: наблюдатель видит ошибки `load`, `handle`, `rebuild`, ошибка всё равно возвращается; пример логирует, а не паникует | `a_gap_fails_without_writing`, `failures_are_reported_once_and_never_save_partial_views` |

Тесты пп. 1–6 падают на прежнем коде (п. 1 — не компилируется). Пункты 7–11 поведения не меняют: защита в том, что
новый вариант enum ломает сборку, поэтому тест, который падал бы на прежнем коде, невозможен; тесты закрепляют
нынешнее поведение для каждого варианта.

**Изменения поведения:** пп. 2, 3, 5, 6 — перечислены в [README эпика](README.md#изменения-поведения).

**Вне задачи (разрешено):** тест `task::context::tests::a_context_tells_the_handler_its_task_its_attempt_and_the_pause_of_its_retry`
проверял, что Debug не содержит `c-1`, а случайный UUID может его содержать; идентификатор корреляции заменён на
`corr-q` (нешестнадцатеричные буквы).

**Проверки** (все с кодом 0): build, fmt, clippy `-D warnings`, полные тесты workspace (6 543 прошли, 0 упало),
`-p cqrsx --no-default-features` без features и с каждой из `postgres`, `worker`, `host`, `logging`, `mongodb`,
`http`; покрытие строк — workspace 98,02 %, `cqrsx` 97,92 % (после задачи 01 было 98,06 % и 97,97 %); demo — fmt,
build, clippy `-D warnings`, тесты без feature `load` и с ней. Новые тесты supervisor и исполнителя прогнаны ещё 10 раз
подряд — стабильны.

**Документация:** `cqrsx/doc/components.md` — интервал снимков `NonZeroUsize`, без `PostgresCommandConfigError`;
panic обработчика у исполнителя; перезапуск и нарушение контракта `TaskQueue` у supervisor; `with_metadata` и labels;
префикс `invalid configuration:`; ошибки типов колонок у `PostgresViewRepository`.
