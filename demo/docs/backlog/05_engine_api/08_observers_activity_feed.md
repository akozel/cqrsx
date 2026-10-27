# 08. Наблюдатели движка и лента активности в `operations`

| | |
|---|---|
| Статус | предложено 2026-10-27 |
| Ценность | высокая: уходят прослойки `CommandService`, `ConcertApp`, `FanApp` и сценарии в одну строку; лента — одна реализация наблюдателя на все команды |
| Цена | средняя: наблюдатель команд в ядре, перевод HTTP и обработчиков на порт исполнителя напрямую |
| cqrsx | новый `src/cqrs/command/observer.rs` (`CommandObserver`, `CommandReport`, `Outcome`), `src/cqrs/command/executor.rs` (`with_observer`), `src/postgres/command.rs`, `src/cqrs/testing/command.rs`, `doc/components.md` |
| demo | `shared/application/command_service.rs` (удаляется), `concert/application/{app,announce_concert,book_seat,cancel_booking}.rs` (удаляются), `fan/application/app.rs` (удаляется), `shared/application/metadata.rs` (новый, временный), `operations/application/activity_observer.rs` (команды и витрины), `concert/presentation/execute_command.rs`, `shared/application/chain.rs`, `bootstrap/*`, `tests/write_side.rs`, `tests/contracts.rs` |
| Зависимости | 05 (`CommandInfo`, `Rejection`), 07 |
| Гарантии | G-12, G-35, G-36 |

## Сейчас

Каждая команда идёт через `CommandService::run(label, command, metadata)`: оно зовёт исполнитель, переводит
`CommandGatewayError` в `CommandOutcome`, публикует запись ленты (`command.accepted/rejected/failed`) и возвращает
`Executed { outcome, tasks }`. `ConcertApp` и `FanApp` держат по сервису и строят `CommandLabel` (тип команды,
`fan_id`); сценарии `announce_concert`, `book_seat`, `cancel_booking` — по строке делегирования. Записи
`projection.*` даёт `ActivityObserver` (наблюдатель процессора, уже движок), `rebuild.*` — `RebuildService`,
`task.attempt.*` — почтовый клиент.

## Станет

Дизайн [04](design/04_application.md): «ленту активности ведёт модуль `operations`, подписываясь на наблюдателей
движка; модули для ленты ничего не пишут».

**cqrsx**
- `cqrs::command::observer::CommandObserver<A>`: `fn command(&self, report: &CommandReport<'_, A>)`.
  `CommandReport { aggregate_id: &A::Id, command: &A::Command, metadata: &HashMap<String, String>, outcome:
  Outcome<'_, A> }`, `Outcome::{Accepted { tasks: &[NewTask] }, Rejected(&A::Error), Conflict, Unavailable}`.
  Наблюдатель видит команду целиком: имя и подписи берёт через `CommandInfo`, код и параметры отказа — через
  `Rejection`; ядру эти границы не нужны.
- `CommandExecutorCore::with_observer(Arc<dyn CommandObserver<A>>)`; вызов — после попыток, когда блокировка уже
  отпущена и пробуждение отправлено, до ответа. Отказ в блокировке — тоже `Unavailable` в отчёте. Паника наблюдателя
  — паника команды (как у наблюдателя процессора: наблюдатель не паникует). `PostgresCommandExecutor` и
  `TestCommandExecutor` — `with_observer`.
- Это та же точка, куда эпик 04/14 («удержание блокировки по фазам») добавит длительности фаз; здесь отчёт их не несёт.

**demo**
- `operations/application/activity_observer.rs`: `impl<A> CommandObserver<A> for ActivityObserver where A::Command:
  CommandInfo, A::Error: Rejection` — публикует те же записи с теми же полями (`aggregate_type`, `aggregate_id`,
  `command_id`, `correlation_id`, `source`, `command_type`, `fan_id`, `tasks` | `code`, `params` | `outcome`,
  `message`): `fan_id` — подпись `fan_id` из `labels()` (у `AnnounceConcert` — `null`, как сейчас); id — из
  метаданных отчёта.
- HTTP `execute_command.rs` зовёт исполнитель напрямую: `state.concerts.execute_with_metadata(&concert_id, command,
  metadata)` и переводит `Result<CommitReport, CommandGatewayError>` в ответ (200 с `tasks` из отчёта, 422 с
  `Rejection`, 409, 503). `CommandOutcome`, `Executed`, `map_gateway_result`, `CommandLabel`, `CommandService`,
  `ConcertApp`, `FanApp`, сценарии — удаляются; `write_side.rs` и `chain.rs` работают с исполнителем.
- `CommandMetadata` (причинность + `recorded_at`) временно — `shared/application/metadata.rs`; в 11 её ставит движок.
- `AppState.concerts: Arc<PostgresCommandExecutor<Concert>>`, `fans` — для обработчиков задач.
- Записи `task.attempt.*` остаются за почтовым клиентом: они несут `inbox_id`, `duplicate`, `reason_code` — то, что
  знает только провайдер. Наблюдатель задач в движке в этом эпике не вводится (решение пользователя 2026-10-27,
  вопрос 4); вариант с заметками обработчика (`ctx.note(..)`) в отчёте наблюдателя — тема 16 списка пересмотра.

## Этапы

1. `CommandObserver` в ядре, пресеты, тесты: отчёт на каждый из четырёх исходов и на отказ в блокировке; вызов после
   освобождения блокировки (следующая команда ключа не ждёт наблюдателя); метаданные и задачи в отчёте — те, что
   закоммичены.
2. Демо: наблюдатель, HTTP, удаление прослоек; тесты сценариев (`book_seat.rs`, `cancel_booking.rs` — гонки,
   `announce_concert.rs`) переезжают в `concert/application/reactions.rs` и `concert/presentation/execute_command.rs`
   как тесты реакций и обработчика; `tests/contracts.rs` — эталон ленты строится через наблюдатель.
3. `components.md`: строки «Command observer» и «Command implementation».

## Зачем, на примере

Вместо `ConcertApp::cancel_booking → CommandService::run → publish_outcome` HTTP-обработчик делает один вызов
движка, а лента — одна реализация на оба агрегата. Файлы `application/concert/{announce_concert,book_seat,
cancel_booking}.rs`, `command_service.rs`, `concert.rs`, `fan.rs` исчезают: минус шесть файлов на пути «отмена →
возврат».

## Риски

- **Формат ленты.** Поля записей `command.*` собираются из другого источника (`CommandInfo`, `Rejection`,
  метаданные отчёта); эталон `activity_feed.txt` (02) и тесты наблюдателя держат совпадение по полям и значениям.
- **Момент публикации.** Сейчас запись публикуется после ответа исполнителя; наблюдатель зовётся в том же месте
  (после блокировки), порядок записей на одном ключе — порядок коммитов.
- **Паника наблюдателя** роняет команду. Наблюдатель демо только публикует в канал — он не паникует; правило
  документируется.

## Как проверить

- Проверки `CLAUDE.md`, покрытие (наблюдатель).
- Демо: тесты; `tests/contracts.rs` (лента); `write_side.rs`; HTTP-тесты `operations/presentation/tests.rs`.

## Поведение не меняется: чем доказано

- Записи ленты `command.*` — те же имена и поля (G-36: эталон `activity_feed.txt` не меняется; тесты наблюдателя
  проверяют значения на четырёх исходах).
- HTTP-ответы — те же коды и формы (G-35: тесты `execute_command.rs` и `operations/presentation/tests.rs`).
- Метаданные событий — те же ключи (G-12: `metadata.rs` с прежним тестом `ui_metadata_uses_one_id_…`).
- Путь команды не менялся: наблюдатель вызывается вне блокировки, тесты G-01–G-05 без правок.

## Критерии приёмки

- [ ] `CommandObserver` в ядре и пресетах, с тестами на все исходы и на вызов вне блокировки.
- [ ] Демо без `CommandService`, `ConcertApp`, `FanApp`, сценариев; HTTP зовёт исполнитель; лента — наблюдатель в
      `operations`.
- [ ] Эталон ленты и HTTP-тесты проходят без изменений.
- [ ] `components.md`, guarantees.md.
