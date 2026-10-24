# 04. Демо на процессоре

| | |
|---|---|
| Статус | предложено |
| Ценность | высокая: касса открыта во время Encore!; сцены сбоев (пауза MongoDB, убитая нода) показывают гарантии вживую |
| Цена | средняя |
| cqrsx | нет |
| demo | `config.rs`, `.env.example`, `process.rs`, `bootstrap/{projections,projections/*,aggregates/*}.rs`, `application/{command_service,lock,workflows}.rs`, `application/operations/{rebuild,verify}.rs`, `application/projections/{maintenance,notifying}.rs`, `presentation/http/{projections/*,concerts/execute_command,error}.rs`, front-end (`commands.ts`, состояния, тексты), `infra/` (гранты ролей), `sidecar` (X-ray, reset), `compose.yaml` (PostgreSQL `idle_in_transaction_session_timeout`), `README.md`, `PLAN.md`, `load/` (хаос-скрипты и сбор отставания) |
| Зависимости | [03](03_projection_processor.md), [04a](04a_mongodb_tools.md) |
| Где выигрыш | сцены демо; замер 01 и 02b; обсуждения 03 и 04 |

## Что меняется

| Было | Станет |
|---|---|
| Исполнитель регистрирует `ViewProjection` обработчиком; команда ждёт квитанцию | Исполнитель получает `CommitWakeup`; процессор на каждую витрину (`concert_halls`/`Concert`, `fan_wallets`/`Fan`) в `ProjectionRuntime` рядом с task runtime |
| Ответ команды несёт `warnings: projection_failed` | «Принято» сразу после коммита (D-17); состояние витрины — SSE и `/api/projections/status` |
| `NotifyingHandler` публикует `projection.updated` / `projection.failed` | `ActivityObserver: ProjectionObserver` публикует `projection.updated` (live), `projection.failed`, `rebuild.started/progress/finished/failed` с прежними полями; `projection.stalled { reason }` при повторных сбоях |
| Encore!: `BoxOfficeLock`, 423, `wait_idle`, `drained`, wipe, два `EventReplay::replay_all` | `RebuildService::start(pace)` → `request_rebuild` обеим строкам; владелец проходит ключи журнала, документы заменяются на месте, wipe не нужен; статус из строк закладок; касса открыта (D-16). `POST /api/concerts/{id}/rebuild` пересобирает один зал (`request_rebuild_key`) |
| Verify: scratch-коллекция и `EventReplay` в неё | свёртка истории каждого ключа в памяти (`get_events` + `View::update`) против документа; без scratch-коллекций и строк закладок |
| `speed: slow \| fast` | `pace_ms` (D-18); UI шлёт `ENCORE_REPLAY_SLOW_DELAY_MS` или 0 |
| `drain_projections` при остановке | процессоры дорабатывают и отпускают аренду сами по `shutdown` |
| `incomplete: true` после 🧨 wipe | wipe сносит коллекции витрин, затем коллекцию их закладок (`MongoCheckpointStore`, 04a; в обратном порядке пересборка, прошедшая между ними, потеряла бы документы): процессоры регистрируют закладки заново и перечитывают журнал с начала — все залы возвращаются сами, Encore! не нужен (D-15 соблюдена: документ строится с начала истории). Документ, удалённый поодиночке, чинит первая бронь по нему (03 F23) |

### Конфигурация

Каждое значение — константа cqrsx плюс переменная демо; раздел 3 `.env.example`:

| Переменная | Умолчание | Что |
|---|---|---|
| `ENCORE_PROJECTION_POLL_MS` | 250 | опрос журнала, когда пробуждения нет (событие записала другая нода) |
| `ENCORE_PROJECTION_PAGE` | 1000 | событий на страницу |
| `ENCORE_PROJECTION_LEASE_MS` | 10000 | аренда закладки; смена владельца после `kill -9` занимает до этого срока |
| `ENCORE_PROJECTION_RETRY_BASE_MS` / `ENCORE_PROJECTION_RETRY_MAX_MS` | 500 / 10000 | пауза после сбоя: `min(base * 2^(n-1), max)` |
| `ENCORE_PROJECTION_CONCURRENCY` | 16 | ключей в работе одновременно |
| `ENCORE_PROJECTION_BUFFER` / `ENCORE_PROJECTION_MAX_BATCH` | 64 / 32 | `DeliveryConfig` процессора: буфер ключа = глубина окна страниц + 1 (`capacity − 1` страниц в полёте), склейка страниц одного ключа в один вызов обработчика |
| `ENCORE_PROJECTION_REBUILD_PAGE` | 100 | ключей на страницу пересборки между живыми страницами |

Уходят: `ENCORE_CHAIN_MAX_ATTEMPTS` (цепочка больше не ждёт lock, бюджет общий — `ENCORE_TASK_MAX_ATTEMPTS`) и
упоминание буфера в описании `ENCORE_COMMAND_LOCK_MAX_WAIT_MS`. PostgreSQL в `compose.yaml` получает
`-c idle_in_transaction_session_timeout=30s`.

### Сборка

- `bootstrap/projections.rs`: модуль витрины отдаёт процессор: `ProjectionProcessor::new(name,
  PostgresEventRepository на главном пуле, MongoCheckpointStore::new(read_db) из cqrsx (04a: закладки живут
  рядом с витринами и исчезают вместе с ними),
  ViewProjection::new(repo).with_name(name), ProcessorConfig::new(poll, page, lease, delivery)?.with_retry(..)?
  .with_rebuild_page(..)?.with_concurrency(..)?)` с `.with_observer(ActivityObserver)`, `.with_wakeup(wakeup)` и
  `.with_owner(имя ноды)`; `ReadSideWiring` держит `CommitWakeup::new()` на тип агрегата (общий у исполнителя и
  процессоров) и `ProjectionRuntime::new().with_processor(..)`.
- `bootstrap/aggregates/*`: исполнитель без `with_event_handler`, с `with_commit_wakeup(wakeup)`; `CommandService`
  без `BoxOfficeLock`.
- `process.rs`: `ProjectionRuntime::run(shutdown)` рядом с task runtime: `lifecycle::run(inbound,
  try_join(task_runtime, projection_runtime), ..)`; `PROJECTION_DRAIN_LIMIT` и `drain_projections` удалены.
  Страницы журнала короткие и идут по индексу, поэтому главный пул со `StatementTimeout`; история ключа при
  пересборке читается там же (`get_events`); `history_pool` остаётся для `totals` и машины времени.
- `application/operations/rebuild.rs`: `start(pace)` и `rebuild_concert(id)` через `CheckpointStore::request_rebuild`
  / `request_rebuild_key`; `status_json` из `CheckpointStore::status` — `state`, `position`, `applied`,
  `rebuild_cursor`, `rebuilt`, `owner`, `locked_until`, `failures`, `last_error`, `waiting`, отставание как число
  событий выше позиции (`count(*)` по индексу через `PostgresEventRepository`); `rebuild.*` из observer владельца;
  `rebuild.finished` — когда обе строки закрыли курсор; `rebuild.progress` несёт ключи и события; `total` для
  `rebuild.started` — `totals()` как сейчас.
- `application/operations/verify.rs`: по описанию выше, ключи — `aggregate_ids_after` страницами; отказ
  `409 rebuild_in_progress` по строкам закладок.
- `application/lock.rs`, `CommandPass`, `wait_for_unlock`, исход `Locked`, `423` в HTTP и UI — удалены.
- `presentation/http/projections/status.rs`: статус обеих проекций в одном ответе.
- PostgreSQL-схема `003-projection-checkpoints.sql` демо не нужна: закладки в Mongo; compose демо и стенда не меняются.
- sidecar: X-ray показывает документы `encore_read.projection_checkpoints`; 🧨 wipe и ⟲ reset сносят эту коллекцию
  после витрин (`READ_MODELS`, затем закладки; reset — после `TRUNCATE` журнала), так что после wipe процессоры
  начинают с начала журнала. Имя коллекции — `cqrsx::mongodb::projection_checkpoints::PROJECTION_CHECKPOINTS_COLLECTION`
  (sidecar от cqrsx не зависит: константа повторяется со ссылкой).
- front-end: убрать `states.locked` и обработку 423; индикатор отставания и состояние `stalled` с причиной;
  кнопка Encore! не блокирует кассу; тексты — приложение A PLAN.md.

## Сцены демо

1. **Бронь → витрина.** Как сейчас; задержка — пробуждение процессора на той же ноде.
2. **Пауза MongoDB** (`docker pause`): брони принимаются, лента показывает `projection.failed` и растущее
   отставание, после `unpause` зал догоняет одной страницей. Урок: write side не зависит от read side.
3. **Убитая нода** (две ноды за nginx, `kill -9` владельца): вторая нода берёт закладку по истечении аренды, ни
   одной потерянной брони; `kill -TERM` — передача сразу.
4. **Encore! при открытой кассе**: залы пересобираются один за другим, места загораются по истории, брони во
   время пересборки видны сразу или в конце; отдельный зал — одной кнопкой.
5. **🧨 wipe**: витрины и их закладки исчезли, процессоры регистрируют закладки заново и перечитывают журнал —
   все залы возвращаются сами за секунды, Encore! не нужен; документ никогда не «врёт». Удалённый поодиночке
   документ (X-ray) чинит первая бронь по нему.

## Тесты

- **Unit demo**: сборка с процессорами и пробуждениями; `RebuildService` против `MemCheckpointStore`; статус;
  observer публикует прежние поля; парсер новых переменных (`ENCORE_PROJECTION_*` → `ProcessorConfig`); verify.
- **`write_side.rs`** (PostgreSQL): 20 параллельных броней → `wait_caught_up` → зал равен свёртке журнала, у
  документа `last_sequence`; бронь при остановленном процессоре применяется после его запуска; Encore! под
  параллельными бронями даёт свёртку; удалённый документ одного концерта чинится первой бронью по нему;
  пересборка одного зала не трогает соседний; wipe всех коллекций → витрины восстанавливаются без Encore!.
- **E2E и хаос на стенде** (`demo/load`, скрипты в `load/chaos/`); критерий везде — verify `ok` и равенство числа
  принятых броней числу занятых мест:
  - `01-hot-key` 40 VU и `docker pause` MongoDB на 10 с посреди окна;
  - две ноды, `kill -9` владельца на 10-й секунде; то же с `kill -TERM`;
  - перезапуск PostgreSQL посреди окна: команды отвечают 503 на время, процессор восстанавливается;
  - Encore! с `pace_ms = 0` и `pace_ms = 80` посреди всплеска, 3 раунда;
  - 🧨 wipe посреди всплеска → затронутые концерты чинятся сами, Encore! для остальных → verify `ok`;
  - soak 1 час (02/10) с процессором: отставание ограничено, `projection_checkpoints` не растёт, дырок нет.
- Сбор метрик стенда: `collect.mjs` снимает `/api/projections/status` раз в секунду → отставание p50/p99 и время
  догона после паузы в `result.json` и в отчёте.

## Как измерить

Против `perf-08b-ord32-*` эпика 02, префикс `perf-03-`, те же правила (образ «до» и «после», прогоны вперемешку,
`REPEAT=3`, `POOL=20`):

| Точка | Профиль | Что смотрим |
|---|---|---|
| `01-hot-key` 10 и 40 VU | `app`, `db` | принятых/с (ожидание: не ниже — блокировка короче), p99, CPU PG на команду (в пределах шума плюс индекс), CPU MongoDB на команду (ниже за счёт страниц), отставание p99 |
| `02b-spread-vus50` | `db` | нет `≠` по принятым/с и CPU на команду |
| две ноды, `BALANCE=key` и `round_robin`, `01-hot-key-vus40` | `app` | принятых/с, отставание, одна активная нода на проекцию |

## Риски

- После «принято» клиент может прочитать витрину до её обновления: задержка — одно пробуждение, на другой ноде —
  опрос; UI слушает SSE.
- Сцена 🧨 wipe меняет смысл: не «пусто до Encore!», а полная самопересборка (D-15 соблюдена: с начала истории);
  wipe под нагрузкой на нескольких нодах не делать (03 «Риски»: удалённый документ начинает версии заново).
- Verify посреди живой нагрузки честно показывает `differs` для документов, обновившихся между scratch и live; так
  и сейчас.

## Критерии приёмки

- [ ] Демо собирается и проходит тесты без `BoxOfficeLock`, 423, `NotifyingHandler`, `EventReplay`,
      `drain_projections`; D-15..D-18 отражены в PLAN.md §2.8, §2.9, §3.2, §3.4, §3.5, §15.
- [ ] Сцены 1–5 воспроизводятся на стенде; E2E-скрипты в `load/chaos/` с verify `ok`.
- [ ] Замер записан в тикет и `PROGRESS.md`.
- [ ] Обсуждения 03 и 04 закрыты ссылкой сюда; README демо (Architecture, Demo script) обновлён.
