# 07. Реакции: `Reactions`, `Decided`, `then.task(..)`, политики по имени

| | |
|---|---|
| Статус | сделано 2026-10-27; решение пользователя 2026-10-27 (README эпика, вопрос 2): политика привязана к типу задачи при регистрации, реакция её не называет |
| Ценность | высокая: главный файл дизайна (04) — «случилось X — дальше Y» одним `match`, компилятор проверяет полноту |
| Цена | средняя: новый компонент поверх `TaskPlanner`, политики задач, перевод планировщиков демо |
| cqrsx | новый `src/cqrs/reactions.rs` (`Reactions`, `Decided`, `Then`, `ReactionsPlanner`), новый `src/task/policy.rs` (`PolicyName`, `Retry`, `TaskPolicy`, `Policies`, `PolicyError`), `src/task.rs` (`DEFAULT_MAX_ATTEMPTS`, метки), `src/task/{format,attempt}.rs`, `src/task/adapter/json.rs` (метки, В-1 плана), `cqrsx-macros` (`#[label]` в `#[derive(Task)]`), `src/cqrs/command/planner.rs` (без изменений контракта), `doc/components.md`, `README.md` |
| demo | `concert/application/reactions.rs` (`impl Reactions for Concert`), `fan/application/reactions.rs`, `concert/application/fan_ledger.rs`, `fan/application/{send_e_ticket,issue_refund}.rs` (конструкторы `::of`), `shared/application/policies.rs` (новый: `STEP`, `MAIL`), `shared/application/tasks.rs` (удаляется, тесты — в `bootstrap/tasks.rs`), `bootstrap/tasks.rs` (`policies`), `bootstrap.rs`, `bootstrap/aggregates/*` (`ReactionsPlanner` и `Policies`), `testing.rs`, тестовые помощники модулей, `tests/{write_side,contracts,architecture}.rs` |
| Зависимости | 06 |
| Гарантии | G-04, G-05, G-06, G-08, G-13, G-15, G-19 (бюджет попыток), G-33 (метаданные задач: только id причинности) |

## Сейчас

Задачи планирует `TaskPlanner<A>` (04/20): `plan(&Decision) -> Result<Vec<NewTask>, TaskPlanError>` с циклом по
`decision.events`, ручной сборкой `NewTask::new(payload)?.with_metadata(..)?.with_max_attempts(n)?` и кортежами
планировщиков. После 03 планировщики лежат в `concert/application/reactions.rs` и `fan/application/reactions.rs`, но
пишутся по-прежнему как `TaskPlanner`. Пауза перед повтором считается в обработчике (`retry_after`), бюджет попыток
передаётся числом в каждый сборщик задачи.

## Станет

Дизайн [04](design/04_application.md), [05](design/05_tasks.md) (политики).

**cqrsx**
- `cqrs::reactions::Reactions: Aggregate` — `fn react(decided: &Decided<'_, Self>, then: &mut Then)`.
- `Decided<'a, A> { id: &'a A::Id, state: &'a A, command: &'a A::Command, event: &'a A::Event }` и `metadata(&self)
  -> &HashMap<String, String>` только для чтения. `state` — состояние после события (ссылка на рабочую копию под
  блокировкой).
- `Then`: `task(payload: impl TaskPayload)` (метаданные из полей `#[label]` задачи), `task_after(delay: Duration,
  payload)` (`TaskSchedule::After`); первая задача, которую не удалось построить, отказывает всему плану;
  `into_tasks()` и `Decided::new(..)` — чтобы проверить реакцию отдельно. Политику реакция не называет: она у типа
  задачи (решение пользователя 2026-10-27).
- `ReactionsPlanner<A: Reactions>` — `impl TaskPlanner<A>`: строит `Decided` из `Decision` (D-02: ровно одно
  событие; решение, собранное руками с другим числом событий, — `TaskPlanError::Application`), зовёт `A::react`,
  превращает `Then` в `Vec<NewTask>`: `max_attempts` — из политики типа задачи (реестр `Policies` по формату; формат
  без политики — умолчание движка `task::DEFAULT_MAX_ATTEMPTS` = 10, бывший литерал `NewTask::from_payload`), ошибка
  сериализации → `TaskPlanError::Task` (отказ «недоступно», G-08). Метаданные задачи — только поля `#[label]`; штамп
  причинности и id — ядро, как сейчас (G-13: порядковый номер — по порядку `then.task(..)`).
- `task::policy`: `PolicyName` (строковая константа), `Retry::{fixed(d), backoff(base, max)}` с
  `pause(attempt) -> Duration` = `min(base · 2^(n−1), max)` — формула `MailClientSettings::retry_after` дословно, без
  разброса; `TaskPolicy::new(retry, max_attempts)` (бюджет положителен); реестр `Policies` (`Arc`): значения по имени
  (`with_policy`) и привязка формата задачи к объявленному имени (`with_task::<T>(name)`), чтение — `policy`,
  `name_of`, `of`, `max_attempts`; отказы — `PolicyError`. Им делятся планировщик (здесь) и исполнитель задач (09).
  Привязка: в 11 — `.task(h).policy(MAIL)` в корне модуля, до того — `bootstrap/tasks.rs::policies`.
- `#[derive(Task)]` понимает `#[label]`: такие поля — `TaskPayload::LABELS`; `NewTask::new` переносит их из тела в
  метаданные (значения JSON, без дублирования — дизайн 05), `JsonTaskAdapter` возвращает их в копию тела перед
  разбором, `TaskAttempt::first` переносит так же. Derive отказывает, где serde меняет или убирает ключ поля. Без меток
  путь задачи прежний. В демо полей с `#[label]` нет; derive тестируется в cqrsx.
- `TaskPlanner`, `Decision`, кортежи остаются строительным блоком (задача 01: «блоки остаются для своих хранилищ и
  тестов cqrsx»); в README их больше нет (14).

**demo**
- `concert/application/reactions.rs`:

  ```rust
  impl Reactions for Concert {
      fn react(decided: &Decided<'_, Self>, then: &mut Then) {
          match decided.event {
              ConcertEvent::ConcertAnnounced(_) => {}
              ConcertEvent::SeatBooked(booked) => then.task(fan_ledger::record_ticket(decided.id, booked)),
              ConcertEvent::BookingCancelled(cancelled) => then.task(fan_ledger::record_cancellation(decided.id, cancelled)),
          }
      }
  }
  ```

  `fan/application/reactions.rs` — `TicketRecorded → then.task(SendETicketV1::of(..))`, `TicketCancelled →
  then.task(IssueRefundV1::of(..))`. Имена контрактов `…V1` остаются (В-2 плана): без суффикса `RecordTicket` и
  `RecordCancellation` совпали бы с командами книжки в тех же файлах.
- `shared/application/policies.rs`: `pub const STEP: PolicyName`, `pub const MAIL: PolicyName`. Значения и привязка
  типов — в `bootstrap` (до 11): `STEP` — `Retry::fixed(1 с)` для `encore_record_ticket` и
  `encore_record_cancellation`, `MAIL` — `Retry::backoff(ENCORE_TASK_RETRY_BASE_MS, ENCORE_TASK_RETRY_MAX_MS)` для
  `encore_send_e_ticket` и `encore_issue_refund`, обе с `ENCORE_TASK_MAX_ATTEMPTS`.
- `shared/application/tasks.rs::new_task` и ключи `concert_id`/`fan_id` в метаданных задач удаляются (исключение
  01); тесты файла переезжают в `bootstrap/tasks.rs`; `impl TaskPlanner` в демо удаляются; `bootstrap/aggregates/*`:
  `.with_task_planner(ReactionsPlanner::<Concert>::new(policies.clone()))`, `policies` строит `wire()` один раз.
- В этой задаче обработчики по-прежнему возвращают `retry_after` сами (почта — своя формула, шаги — 1 с): паузу по
  политике исполнитель начнёт считать в 09. Так коммит 07 не меняет поведения.

## Этапы

1. `task::policy` с тестами: `pause` по попыткам против эталона `retry_pauses.txt` (02) для `fixed(1 с)` и
   `backoff(1 с, 8 с)`; привязка формата к имени; формат без политики — умолчание.
2. `reactions.rs`: `Decided`, `Then`, `ReactionsPlanner`, тесты: ровно одно событие; `task_after` даёт `After`;
   порядок задач = порядок вызовов; `#[label]` → метаданные; политика типа → `max_attempts`; ошибки.
   Интеграционный тест в памяти: реакция на двух агрегатах через `TestCommandExecutor` (как `tests/cqrs/planning.rs`,
   но через `Reactions`).
3. Демо: реакции, конструкторы задач, `fan_ledger.rs`, политики, bootstrap; тесты реакций — точный JSON задач по
   событию (те же утверждения, что у планировщиков), без `TaskPlanError::Application`.
4. `components.md`: строки «Reactions» и «Task policies»; «Task planning» — что `ReactionsPlanner` поверх него.

## Зачем, на примере

Отмена брони читается в одной ветке: `BookingCancelled(cancelled) => then.task(fan_ledger::record_cancellation(
decided.id, cancelled))`. Добавить событие `SeatReserved` без реакции нельзя — `match` неполный.

## Риски

- **Политика у типа задачи, а не при постановке** (решение пользователя 2026-10-27 вместо согласованного ранее
  `then.task(x).policy(MAIL)`): в строках outbox ничего нового, привязка проверяется при сборке (11), реакция короче.
  Цена — одна политика на тип задачи; если понадобится политика для отдельной постановки, это тема 11 списка
  пересмотра.
- **`Decided.state` — состояние после события.** Данные, которых после события уже нет (снятая бронь), берутся из
  события, как требует дизайн 02; `before` из `Decision` реакции не видят. Если понадобится — тема для пересмотра.

## Как проверить

- Проверки `CLAUDE.md`, покрытие (реакции, политики, derive `#[label]`).
- Демо: тесты реакций, `cancel_booking.rs` (гонки), `write_side.rs`.
- Эталон `retry_pauses.txt`: политики дают те же паузы.

## Поведение не меняется: чем доказано

- Планирование по-прежнему под блокировкой, на каждой попытке, задачи коммитятся с событием попытки (G-04, G-05,
  G-06, G-15): `ReactionsPlanner` — `TaskPlanner`, тесты `executor.rs`, `tests/postgres/planning.rs` и гонки
  `cancel_booking.rs` не меняются.
- Id задач и штамп причинности — тот же код ядра (G-13); порядок задач в `Then` совпадает с порядком кортежа
  планировщиков (одна задача на событие в демо).
- `max_attempts` = 6 из политики типа (G-19); тело задач — тот же JSON (G-33); метаданные — три id причинности,
  как сейчас, без `concert_id`/`fan_id` (исключение 01).
- Паузы повторов в этом коммите по-прежнему считают обработчики (G-19).

## Критерии приёмки

- [x] `Reactions`, `Decided`, `Then`, `task_after`, политики в cqrsx с тестами.
- [x] Демо: реакции обоих агрегатов, конструкторы задач, `STEP`/`MAIL`; `TaskPlanner` в демо не используется;
      `new_task` удалён.
- [x] Эталон пауз проходит на политиках; JSON и метаданные задач не изменились (кроме исключения 01).
- [x] Типы задач привязаны к политикам (`bootstrap`, до 11); формат без политики получает умолчания движка.
- [x] `components.md`, guarantees.md.

## Результат

Сделано 2026-10-27.
- **cqrsx:**
  - `cqrs::reactions`: `Reactions::react(&Decided, &mut Then)` — полный `match` по событиям; `Decided { id, state
    (после события), command, event, metadata() }`, `Decided::new`; `Then::{task, task_after, into_tasks}` — первая
    задача, которую не удалось построить, отказывает плану; `ReactionsPlanner<A>` — `TaskPlanner<A>`: решение не с
    одним событием — `TaskPlanError::Application`, бюджет — из политики типа задачи;
  - `task::policy`: `PolicyName`, `Retry::{fixed, backoff}` (формула почты дословно, без разброса), `TaskPolicy::new`,
    реестр `Policies` (`Arc`, `with_policy`, `with_task::<T>`, `of`, `max_attempts`), `PolicyError`;
    `task::DEFAULT_MAX_ATTEMPTS` = 10 вместо литерала;
  - `#[label]` в `#[derive(Task)]` → `TaskPayload::LABELS`: поле хранится в метаданных, не в теле (`NewTask::new`,
    `TaskAttempt::first`), `JsonTaskAdapter` возвращает его перед разбором; derive отказывает, где serde меняет ключ.
    Без меток путь задачи прежний.
- **Демо:** реакции кассы и книжки — по одной ветке на событие; `fan_ledger` и `SendETicketV1::of`,
  `IssueRefundV1::of` строят контракты без бюджета; `STEP`/`MAIL` в `shared/application/policies.rs`, значения и
  привязка — `bootstrap/tasks.rs::policies`; `shared/application/tasks.rs` и `new_task` удалены, ключи `concert_id`,
  `fan_id` в метаданных задач больше не пишутся (исключение 01).
- **Неизменность:** тест строк четырёх задач написан и пройден до замены, после неё меняются только метаданные (два
  ключа); политики дают паузы эталона `retry_pauses.txt`; эталоны 02 без изменений.
- **Сверка утверждений:** 814 / 7 / 1 / 1 / 0 / 31.
- **Тесты и покрытие (первый полный прогон):** cqrsx lib 349, интеграционные 245, doctest'ы 44 + 19, macros 38; демо 159 + 14; покрытие — workspace 97,77 %, cqrsx 97,68 %. Повторный прогон после правки покрытия — журнал, «Окончательные проверки».
