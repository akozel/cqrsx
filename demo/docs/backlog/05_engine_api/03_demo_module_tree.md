# 03. Дерево демо: модули-плагины `concert`, `fan`, `operations`, `shared`

| | |
|---|---|
| Статус | предложено 2026-10-27 |
| Ценность | высокая: все следующие правки демо ложатся сразу на место; дерево «на пробу» (дизайн 01) проверяется на знакомом коде |
| Цена | средняя: около 95 файлов переезжают, `tests/architecture.rs` переписывается; логика не меняется |
| cqrsx | ничего |
| demo | `back-end/src/**` (перемещения), `back-end/tests/architecture.rs`, `back-end/src/lib.rs`, `PLAN.md` §4 (структура каталога), `README.md` демо (таблица слоёв) |
| Зависимости | 02 |
| Гарантии | G-22, G-35; остальные не затрагиваются (перемещения) |

## Сейчас

Слои — каталоги верхнего уровня: `domain/{concert,fan,shared}`, `application/{concert,fan,workflows,operations,
projections,…}`, `presentation/http/*`, `infrastructure/*`. Путь «отмена → возврат» проходит через 22 файла в четырёх
каталогах, а процессы `TicketDelivery` и `Refund` планируют задачи для обоих агрегатов из одной папки
`workflows/`. `tests/architecture.rs` проверяет слои как каталоги верхнего уровня и запрещает `presentation` любой путь
`cqrsx`.

## Станет

Дерево из дизайна [01](design/01_overview.md): модуль — каталог со слоями внутри, корень модуля — файл рядом.
Только перемещения и правка путей `use`; ни одна функция не меняет тело. Карта переезда:

| Откуда | Куда | Примечание |
|---|---|---|
| `domain/concert/aggregate.rs` | `concert/domain/concert.rs` | состояние и `apply`; остальное `domain/concert/*` → `concert/domain/*` как есть |
| `domain/fan/*` | `fan/domain/*` | |
| `domain/shared/{ids,venue}.rs` | `shared/domain/{ids,venue}.rs` | |
| `application/concert/{announce_concert,book_seat,cancel_booking,test_support}.rs`, `application/concert.rs` (`ConcertApp`) | `concert/application/{…}.rs`, `concert/application/app.rs` | удаляются в 08 |
| `application/concert/views/concert_hall.rs` | `concert/application/views/concert_hall.rs` | |
| `application/operations/time_travel.rs` | `concert/application/time_travel.rs` | операция над одним концертом |
| `application/fan.rs` (`FanApp`), `application/fan/wallet.rs`, `application/fan/views/fan_wallet.rs` | `fan/application/{app,wallet,views/fan_wallet}.rs` | |
| `application/workflows/ticket_delivery/{record_ticket,send_e_ticket}.rs`, `refund/{record_cancellation,issue_refund}.rs` и контракты из `*/tasks.rs` | `fan/application/{record_ticket,send_e_ticket,record_cancellation,issue_refund}.rs` | задача и обработчик в одном файле (дизайн 05); контракт — в файле своего обработчика |
| `application/workflows/{ticket_delivery,refund}.rs` (планировщики) | `concert/application/reactions.rs` (`impl TaskPlanner<Concert>`: обе реакции кассы), `fan/application/reactions.rs` (`impl TaskPlanner<Fan>`) | типы `TicketDelivery` и `Refund` исчезают: их `impl` раскладываются по агрегатам; сборщики задач книжки из фактов кассы — `concert/application/fan_ledger.rs` |
| `application/workflows.rs` (`ChainStep`, `run_chain_step`, `map_fan_outcome`), `application/tasks.rs` (`new_task`), `application/command_service.rs` | `shared/application/{chain,tasks,command_service}.rs` | временно; удаляются в 08–09 |
| `application/ports.rs` | `shared/application/mail.rs` (`Mail`, `Mailer`), `shared/application/read_model.rs` (`ReadModelStore`, `Stored`; до 10), `operations/application/ledger.rs` (`EventLedgerStats`, `LedgerTotals`) | |
| `application/activity.rs` | `shared/application/activity.rs` | `ActivityHub` нужен и `operations`, и почтовому клиенту |
| `application/projections/{activity_observer,verification}.rs`, `application/operations/{rebuild,verify}.rs` | `operations/application/{activity_observer,verification,rebuild,verify}.rs` | |
| `application/read_models.rs` | `bootstrap/read_models.rs` | проводка; уходит в 11 |
| `presentation/http/concerts/*` | `concert/presentation/*` | `execute_command`, `get`, `list`, `rebuild`, `timeline`, `state_at` |
| `presentation/http/fans/wallet.rs` | `fan/presentation/wallet.rs` | |
| `presentation/http/{health,config,activity/stream,projections/*}.rs`, `presentation/http/tests.rs` | `operations/presentation/*` | |
| `presentation/http.rs` (`parse_*_id`, `stored_document`), `presentation/http/error.rs` | `shared/presentation/{mod,error}.rs` | `ApiError` нужен всем модулям |
| `infrastructure/mail/http_mailer.rs` | `shared/infrastructure/http_mailer.rs` | |
| `infrastructure/mongo/read_model.rs` | `shared/infrastructure/mongo_read_model.rs` | до 10 |
| `infrastructure/mongo/read_side.rs` (имена коллекций) | `pub const NAME` в файлах витрин | в 10 — `View::NAME` |
| `infrastructure/postgres.rs` | `shared/infrastructure/postgres.rs` | `HistoryStore`, `event_history`, `PostgresLedger` |
| `bootstrap.rs`, `bootstrap/*`, `process.rs`, `config.rs`, `main.rs`, `lib.rs`, `testing.rs` | на месте | композиция; удаляются в 12 |

Корни модулей `concert.rs`, `fan.rs`, `operations.rs`, `shared.rs` пока только объявляют `pub mod`; их содержание —
задача 11.

**`tests/architecture.rs`** переписывается под правила «Кто кого видит» ([01](design/01_overview.md)):
- в модуле `M`: `M/domain` видит только `crate::shared::domain` и `cqrsx::cqrs::{aggregate, event, event_sink,
  testing}` (список растёт в 04 и 05); `M/application` видит `M/domain`, `crate::shared::{domain, application}` и
  `cqrsx` без `postgres`, `mongodb`, `task::{adapter, dispatcher, runtime}`, `host`, `logging`; `M/presentation` видит
  `M/{application, domain}`, `crate::shared::{domain, application, presentation}` и ничего из `cqrsx`;
- между модулями: `concert/application` может использовать контракты задач `crate::fan::application::{record_ticket,
  record_cancellation}` и ничего больше из `fan`; `fan` не называет `crate::concert`; `operations` не называет
  `concert` и `fan` (витрины приходят через проводку); `shared` не называет модули;
- `shared/infrastructure` видит `shared/{domain, application}`; `bootstrap`, `process`, `config`, `main`, `lib`,
  `testing` — исключения, как сейчас;
- сторожевое число проверенных файлов (`checked > 50`) пересчитывается.

## Этапы

1. `git mv` по карте одним коммитом; `pub mod` в корнях; `use`-пути.
2. Разложить `TicketDelivery`/`Refund` по `reactions.rs` двух модулей и `fan_ledger.rs` (перенос тел функций,
   тесты планировщиков переезжают вместе с ними).
3. `tests/architecture.rs`: новые правила и их тест на самопроверку (`the_checker_sees_grouped_imports…`).
4. `lib.rs`, `PLAN.md` §4, `README.md` демо (таблица «back-end/»).

## Зачем, на примере

Чтобы прочитать отмену сейчас, открывают `presentation/http/concerts/execute_command.rs`, затем
`application/concert/cancel_booking.rs`, `domain/concert/commands/cancel_booking.rs`, `application/workflows/refund.rs`,
`application/workflows/refund/tasks.rs`, `…/record_cancellation.rs`, `domain/fan/commands/record_cancellation.rs`,
`…/issue_refund.rs`, `infrastructure/mail/http_mailer.rs` — девять каталогов. После переезда путь — `concert/presentation`
→ `concert/domain` → `concert/application/{reactions,fan_ledger}` → `fan/application/record_cancellation` →
`fan/domain` → `fan/application/{reactions,issue_refund}` → `shared/infrastructure/http_mailer`: два модуля, читается
сверху вниз.

## Риски

- **Большой diff.** Перемещения с `git mv` сохраняют историю (`git log --follow`); в коммите нет правок логики, что
  проверяет `git diff -M --stat`: только переименования и строки `use`.
- **Раскладка может оказаться неудобной** (дизайн 01: «на пробу»). Оценка — тема 15 [списка пересмотра](revisit.md);
  запасной вариант — слои сверху, модули внутри.
- **Процессы разложены по агрегатам.** `TicketDelivery` как тип исчезает; связь шагов держится на типах задач
  (`RecordTicket` → `SendETicket`). Риск из задачи 01: проверить на пути «отмена → возврат», что он читается.

## Как проверить

- `cd demo && cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test`
  (включая `tests/architecture.rs` и `tests/write_side.rs`).
- `git diff -M90% --stat HEAD` показывает переименования; `grep -c "fn " ` по модулям до и после совпадает.
- Покрытие не считается: чистый перенос.

## Поведение не меняется: чем доказано

Перемещения не меняют код; все 132 теста демо проходят на новых путях. G-22 (граница модулей) теперь держит новый
`tests/architecture.rs`; G-35 (HTTP) — тесты `operations/presentation/tests.rs` и маршруты в `bootstrap/routes.rs`
без изменений.

## Критерии приёмки

- [ ] Дерево совпадает с картой; `application` ни одного модуля не содержит `domain`.
- [ ] `tests/architecture.rs` проверяет правила модулей и ловит нарушение (тест на самопроверку с примером
      `crate::fan::domain` из `concert/application`).
- [ ] Тесты демо проходят; diff состоит из переименований и путей.
- [ ] `PLAN.md` §4 и `README.md` демо описывают новое дерево; тема 15 добавлена в `revisit.md`.
