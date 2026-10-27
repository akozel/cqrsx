# 11. Модуль и движок в памяти: `Module`, `Cqrs::in_memory()`, `run_tasks()`

| | |
|---|---|
| Статус | сделано 2026-10-27 |
| Ценность | высокая: модуль-плагин описывает, что подключает; тест сценария «забронировал, отменил, прогнал задачи, проверил письма» без баз |
| Цена | высокая: новый слой сборки движка над существующими компонентами; бэкенд в памяти; очередь задач в памяти как публичный инструмент |
| cqrsx | новые `src/engine.rs` и `src/engine/{build,module,setup,outcome,views,memory}.rs` (`Cqrs`, `Builder`, `BuildError`, `Registrations`, `Module`, `TaskEntry`, `Setup`, `Settings`, `Source`, `Outcome`, `ViewQuery`, `Views`; бэкенд в памяти — закрытый `MemoryBackend`, без открытого трейта `Backend`), `src/task/testing{,/mem_task_queue}.rs` (`MemTaskQueue`), `src/cqrs/testing/mem_event_repository.rs` (столбцы доставки outbox, `count_after`, `hold_loads`), `src/cqrs/command/observer.rs` (`AnyCommandObserver`), `src/task/{context,dependencies,handlers}.rs` (доступ движка), `tests/engine{,/scenario}.rs`, `doc/components.md`, `README.md` |
| demo | `concert.rs`, `fan.rs`, `operations.rs` (корни модулей), `shared/application/{mail,policies,test_support}.rs`, `shared/infrastructure/http_mailer.rs` (хаб ленты — зависимость обработчиков писем), `fan/application/{send_e_ticket,issue_refund}.rs`, `operations/application/activity_observer.rs` (`AnyCommandObserver`), `testing.rs` (`test_app()`), `concert/application/tests.rs` (сценарии и гонки на движке в памяти), `concert/application/{reactions,test_support}.rs`, `bootstrap{.rs,/aggregates*,/projections*,/tasks.rs,/read_models.rs}` (строится из `Registrations`, до 12), `config.rs` (формула таймаута почты), `tests/{contracts,architecture}.rs` (помощники и тексты) |
| Зависимости | 07, 09, 10 |
| Гарантии | G-01, G-05, G-09, G-11, G-12, G-14, G-15, G-23, G-26 |

## Сейчас

Сборка — руками в `bootstrap/`: исполнитель на агрегат (`PostgresCommandConfig`, `AggregateConflictRetryPolicy`,
`LockLimits`, планировщик, пробуждение), процессор на витрину (репозиторий, закладки, наблюдатель, владелец),
диспетчер задач, маршруты. Тест сценария целиком (команда → задача → команда → письмо) невозможен без PostgreSQL:
обработчики тестируются по одному с `TaskAttempt::first`.

## Станет

Дизайн [07](design/07_bootstrap.md) (модуль, тесты) и [04](design/04_application.md) («Как тестировать»).

**cqrsx, `engine`**
- `Module::new(name)`: `.aggregate::<A>(rules)` (требует `A: Reactions`, `A::Command: CommandInfo`, `A::Error:
  Rejection`), `.view::<V>()`, `.task(handler).policy(name)` (политика типа задачи; без `.policy` — умолчания
  движка), `.provide(dep)`, `.policy(name, retry, max_attempts)` (значения политики),
  `.observe(Arc<dyn CommandObserver<A>>)` / `.observe_projections(Arc<dyn ProjectionObserver>)` (для `operations`);
  `.routes(..)` — в 12 под фичей `http`.
- `Setup` — то, что модуль получает при сборке: `env(name, default)`, `millis(..)`, `engine()` (действующие настройки
  движка только для чтения, например таймаут обработчика для почтового запроса). Общих ресурсов нет (решение
  пользователя 2026-10-27, вопрос 6): HTTP-клиент почты создаёт `shared/infrastructure`, cqrsx не зависит от
  `reqwest`.
- `Cqrs::in_memory()` → `Builder` с `MemoryBackend`: `.module(f).provide(x).build() -> Result<Cqrs, BuildError>`.
  `build` проверяет: тип агрегата зарегистрирован один раз (G-11), у каждого обработчика есть зависимости, формат
  задачи зарегистрирован один раз, имя политики, к которому привязан тип задачи, объявлено, имя витрины — один раз.
- `Backend` — что создаёт хранилища: журнал и outbox (`EventStore` с задачами), очередь задач, закладки, репозиторий
  витрины по `V::NAME`. Память: `MemEventRepository` (журнал + outbox + позиции), `MemTaskQueue` (новая, публичная:
  claim по готовности, аренда и settle по правилам PostgreSQL по смыслу, время — часы Tokio), `MemCheckpointStore`,
  `MemViewRepository`. Остальное общее для всех бэкендов: `CommandExecutorCore` на агрегат с `ReactionsPlanner`,
  `CommitWakeup` на тип, процессор на витрину, диспетчер с общими обработчиками, `Executors`, `Policies`.
- `Cqrs`: `execute::<A>(&A::Id, command) -> Outcome<A>` (принято с задачами, отказ, конфликт, недоступно);
  причинность: `command_id` из `#[command_id]` или новый v7, корень с источником, который задаёт точка входа
  (`let ui = cqrs.from("ui"); ui.execute::<Concert>(..)` — решение пользователя 2026-10-27, вопрос 8),
  `recorded_at` фиксированной ширины (G-12);
  `view::<V>()` → `get(&id)`, `list()`, `range(lower, upper)` (10); `views()` → `status()`, `rebuild(name, pace)`,
  `rebuild_key(name, id)`, `verify(name)`, `lag(name)` (над `CheckpointStore`, `count_after`, `verify`);
  `dependency::<T>()`; `settings()`.
- Тестовые помощники движка в памяти: `run_tasks()` — прогнать очередь до конца (все задачи, которые готовы сейчас,
  включая те, что порождают новые; отложенные и повторы с паузой остаются до сдвига часов Tokio); `catch_up()` —
  запустить процессоры (один раз) и дождаться, пока закладки дойдут до головы журнала; `outbox()`, `journal()`.
- Один исполнитель на тип агрегата — движок, второй `.aggregate::<A>()` — ошибка сборки (закрывает пробел G-11).

**demo**
- Корни модулей, как в дизайне 07: `concert::module(setup)` — `ConcertRules { ticket_limit: setup.env("TICKET_LIMIT",
  4)? }`, `.view::<ConcertHallView>()`; `fan::module` — `.provide(mail)`, `.policy(STEP, ..)`, `.policy(MAIL, ..)`,
  `.task(record_ticket).policy(STEP)`, `.task(record_cancellation).policy(STEP)`, `.task(send_e_ticket).policy(MAIL)`,
  `.task(issue_refund).policy(MAIL)`, `.view::<FanWalletView>()`; `operations::module` — `ActivityHub`, наблюдатели,
  перестройка (`RebuildService` над `cqrs.views()`), сверка.
- `testing::test_app()` → `Cqrs::in_memory().module(concert::module).module(fan::module).module(operations::module)
  .provide(RecordingMailer)` — тестовый `.provide` побеждает модульный.
- Сценарии: «отмена возвращает деньги» (`run_tasks()` → письма `e_ticket`, `refund`), «повторная доставка не шлёт
  второе письмо», «отмена, обогнавшая билет, повторяется и доходит после билета», «витрины догоняют журнал»
  (`catch_up()` → `cqrs.view::<ConcertHallView>().get(..)`); гонки отмен переезжают на `test_app()`.
- `bootstrap` до 12 строит PostgreSQL-проводку из тех же `Module` (временное дублирование сборки).

## Этапы

1. `MemTaskQueue` с тестами по правилам очереди (claim готовых, аренда, settle, восстановление, `After`).
2. `engine`: `Module`, `Setup`, `Backend`, `MemoryBackend`, `Cqrs`, `build`-проверки, `Outcome`, `views()`,
   помощники; тесты модуля: каждая проверка сборки; один исполнитель на тип; `execute` — четыре исхода и
   метаданные (G-12); `run_tasks` до конца цепочки на двух агрегатах; `catch_up`; `provide` тестовый побеждает.
3. Интеграционный тест cqrsx `tests/engine/scenario.rs`: касса и книжка на движке в памяти — тот же путь, что в
   `tests/cqrs/planning.rs`, но через модули.
4. Демо: корни модулей, `test_app()`, сценарии; `bootstrap` из модулей.
5. `components.md`: раздел «Engine»: `Cqrs`, `Module`, `Setup`, `Backend`, `MemoryBackend`, `MemTaskQueue`;
   словарь: модуль, реакция, движок, flow вместо workflow.

## Зачем, на примере

```rust
#[tokio::test]
async fn a_cancellation_refunds_the_fan() {
    let (cqrs, mail) = test_app();
    let ui = cqrs.from("ui");
    ui.execute::<Concert>(&radiohead(), announce()).await.unwrap();
    ui.execute::<Concert>(&radiohead(), book("A1", "fox")).await.unwrap();
    ui.execute::<Concert>(&radiohead(), cancel("A1", "fox")).await.unwrap();
    cqrs.run_tasks().await;
    assert_eq!(mail.kinds(), ["e_ticket", "refund"]);
}
```

Сегодня такой тест невозможен: нет очереди в памяти и нет сборки, общей для теста и продакшена.

## Риски

- **Сложность переезжает в движок.** `engine` собирает только существующие компоненты; новых механизмов нет (кроме
  `MemTaskQueue`, зеркала PostgreSQL-очереди по смыслу). Бюджет: модуль `engine` без тестов — не больше, чем
  удаляемая проводка демо (`bootstrap/` ≈ 500 строк).
- **Сборка в памяти ≠ сборка на PostgreSQL.** Общее — всё, кроме `Backend`; 12 добавляет второй бэкенд без изменения
  `Cqrs`. Тест: один и тот же список модулей собирается на обоих.
- **`run_tasks` и часы.** Повтор с паузой 1 с не выполняется в том же `run_tasks`; тест двигает часы Tokio
  (`start_paused`) — как у `MemCheckpointStore`.
- **Временное дублирование** сборки в `bootstrap` до 12 — один коммит.

## Как проверить

- Проверки `CLAUDE.md`; матрица: `engine` в памяти компилируется с `--no-default-features`.
- Покрытие: `engine` и `MemTaskQueue` целиком.
- Демо: сценарии на `test_app()`, остальные тесты.

## Поведение не меняется: чем доказано

- Путь команды — тот же `CommandExecutorCore` с теми же политиками по умолчанию (G-01, G-05): движок не содержит
  своей логики блокировок и повторов; тест сравнивает настройки ядра, собранного движком, с константами.
- Пробуждение — тот же `CommitWakeup` на тип, общий для исполнителя и процессоров (G-09): тест в памяти.
- Причинность и `recorded_at` — те же ключи и формат (G-12, G-14): тесты переезжают из демо в `engine`.
- Процессоры, закладки, перестройка — те же компоненты (G-23, G-26).
- Один исполнитель на тип — теперь проверяет сборка (G-11).

## Критерии приёмки

- [x] `Module`, `Setup`, `Cqrs::in_memory()`, `build`-проверки, `execute`, `view`, `views()`, `run_tasks`,
      `catch_up`; `MemTaskQueue` публичный.
- [x] Интеграционный сценарий cqrsx на движке в памяти; сценарии демо на `test_app()`.
- [x] Корни модулей демо по дизайну 07; `bootstrap` строится из модулей.
- [x] `components.md` (раздел «Engine», словарь), guarantees.md (G-11 закрыт).

## Результат

**Результат (2026-10-27).** В cqrsx появился движок `engine`.

- Модуль (`Module`) регистрирует агрегат с правилами и реакциями, витрины, задачи с политиками
  (`.task(h).policy(P)`), зависимости и наблюдателей всех команд и процессоров.
- `Cqrs::in_memory().module(..).provide(..).build()` собирает существующие компоненты над хранилищами в памяти:
  `CommandExecutorCore` с `ReactionsPlanner`, `CommitWakeup` на тип, процессор на витрину, общий диспетчер,
  `Executors`, `Policies`.
- Сборка отказывает второй регистрации агрегата (G-11 закрыт), витрине дважды, политике без объявления, обработчику
  без зависимости и т. д.
- `cqrs.from("ui").execute::<A>(..)` ставит причинность из `#[command_id]` или нового v7.
- `view::<V>()` и `views()` дают запросы и операции витрин.
- В памяти `run_tasks()` прогоняет цепочки задач до конца, а `catch_up()` догоняет витрины.
- `MemTaskQueue` — очередь задач в памяти по правилам PostgreSQL над outbox `MemEventRepository`, с часами Tokio и
  хуком `lose_settlements`.

Демо: корни модулей `concert`, `fan`, `operations`.
- `test_app()` и сценарии без баз: отмена возвращает деньги, повторная доставка после потерянного подтверждения, отмена
  раньше билета, витрины догоняют журнал; гонки отмен переехали на движок без ослабления.
- Бутстрап до 12 строит PostgreSQL-проводку из регистраций модулей.
- Хаб ленты — зависимость обработчиков писем, проверяется при сборке.

Эталоны 02, SQL, HTTP не менялись. Сверка утверждений: 907 / 5 / 1 / 0 / 0 / 45. Покрытие — 98,00 % (workspace) и
97,88 % (cqrsx). Бюджет строк `engine` превышен: 1073 против 655.
