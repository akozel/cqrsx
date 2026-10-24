# 04a. MongoDB-инструменты read side в cqrsx

| | |
|---|---|
| Статус | сделано 2026-10-24 (см. «Отклонения») |
| Ценность | средняя–высокая: витрины и закладки проекций в MongoDB получают строгость библиотечного кода; закладка живёт рядом с витриной, как у Axon/Marten/Akka; тикет 04 худеет до проводки демо |
| Цена | средняя: фича, два типа, контейнерные тесты, матрица фич, документы |
| cqrsx | фича `mongodb` (`Cargo.toml`: драйвер 3.9.1 без умолчаний, `compat-3-0-0` + `rustls-tls`), новые `src/mongodb.rs`, `src/mongodb/{view_repository,projection_checkpoints}.rs`; правки `cqrs/projection/processor.rs` (регистрация строки перед каждым захватом), `cqrs/testing/mem_checkpoint_store.rs` (`clear`), `diagnostics.rs` (`log_mongodb_failure`), `lib.rs`, `doc/components.md`, `README.md`; корневые `CLAUDE.md`, `README.md`, `.github/workflows/ci.yml`; тесты `tests/mongo.rs`, `tests/mongo/*`, фикстуры `tests/support/{port_lease,postgres,mongo}.rs` |
| demo | `infrastructure/mongo/view_repository.rs` удалён; `infrastructure/mongo/read_model.rs` (`MongoReadModel`: запись через библиотечный `MongoViewRepository`, запросы и wipe свои); `read_side.rs`, `bootstrap/projections/{concert_halls,fan_wallets}.rs`; `tests/architecture.rs` (`cqrsx::mongodb` запрещён в `application`); `Cargo.toml` (фича `mongodb` у cqrsx) |
| Зависимости | [03](03_projection_processor.md) |
| Где выигрыш | [04](04_demo_on_processor.md): `MongoCheckpointStore` и сцена wipe; любое приложение «журнал в PostgreSQL, витрины в MongoDB» |

## Было

Витрины демо лежали в MongoDB через собственный `MongoViewRepository` демо (`{ _id, version, last_sequence, view }`,
около 300 строк с тестами), а хранилище закладок у cqrsx было только для PostgreSQL ([03](03_projection_processor.md)).
Закладка в PostgreSQL рядом с журналом работает, но расходится с каноном (Axon `TokenStore`, Marten
`mt_event_progression`, Akka Projections offset store держат смещение в базе read model) и даёт сцене 🧨 wipe
неудобный смысл: витрины исчезли, закладка осталась, нетронутые концерты пусты до Encore!.

## Стало

**Граница.** MongoDB в cqrsx — только read side: репозиторий витрин и закладки проекций. Журнал, outbox и очередь
задач остаются в PostgreSQL. Закладка хранит позицию PostgreSQL-журнала (`xid8` как `Int64`), поэтому базы
сбрасываются только вместе: `make reset` (`down -v`) и ⟲ reset sidecar так и делают. Read side отдельно сносится
так: сначала витрины, потом закладки — процессоры регистрируют строки заново и пересобирают всё с начала журнала (в
обратном порядке пересборка, успевшая пройти между двумя сносами, потеряла бы документы). Правила записаны в
`components.md` и README cqrsx.

### API

```rust
// cqrsx, фича `mongodb`; помощников подключения нет — приложение строит `mongodb::Client` и отдаёт `Database`
pub struct MongoViewRepository<V, A>;   // mongodb::view_repository; new(database: &Database, collection: &str); impl ViewRepository<V, A>
pub struct MongoCheckpointStore;        // mongodb::projection_checkpoints; new(database: &Database); impl CheckpointStore
pub const PROJECTION_CHECKPOINTS_COLLECTION: &str = "projection_checkpoints";
```

Документ витрины — как было в демо: `{ _id: view_id, version: Int64, last_sequence: Int64, view: BSON(V) }`;
`version == 0` → `insert_one` (дубль ключа — `OptimisticLockError`), иначе `update_one({ _id, version }, $set { view,
version + 1, last_sequence })` и `matched_count == 0` — конфликт. Не декодируется, нет поля или отрицательный
`last_sequence` — `DeserializationError`; вид, который BSON не держит (`u64` за `Int64`), и переполнение версии —
`UnknownError`; сервер вне досягаемости (`Io`, `ServerSelection`, `ConnectionPoolCleared`) — `ConnectionError`, прочие
ошибки драйвера — `UnknownError`.

Документ закладки:

```json
{ "_id": { "projection": "concert_halls", "aggregate_type": "Concert" },
  "position": 0, "applied": 0,
  "rebuild_requested": false, "rebuild_pace_ms": 0, "rebuild_cursor": null, "rebuilt": 0,
  "owner": null, "lock_token": null, "locked_until": null,
  "failures": 0, "last_error": null, "rebuild_keys": ["c-17", "c-3"], "updated_at": ISODate }
```

Числа — `Int64`, `lock_token` — строка UUID, `locked_until`/`updated_at` — даты по часам сервера. Каждая операция —
один атомарный оператор над одним документом; строки попадают в конвейер через `$literal` (идентификатор может
начинаться с `$`):

| Операция | Как |
|---|---|
| `register` | `update_one({ _id }, [$set { поле: $ifNull(["$поле", умолчание]) … }]).upsert(true)`: новый документ получает все поля, существующий не меняется; дубль ключа при гонке двух нод — успех |
| `claim` | `find_one_and_update({ _id, $or: [locked_until == null, $expr: locked_until < $$NOW] }, [$set owner, lock_token, locked_until = $$NOW + lease])`, документ после; нет документа — `None` |
| `extend` / `record_failure` / `begin_rebuild` / `finish_rebuild` / `release` | `update_one({ _id, lock_token }, [$set …])`, `matched_count == 1` |
| `advance` | `update_one({ _id, lock_token, position: { $lte: to } }, [$set applied += (position < to ? n : 0), position = to, failures = 0, last_error = null])` — идемпотентно, как в PostgreSQL |
| `advance_rebuild` | `update_one({ _id, lock_token, rebuild_cursor: { $ne: null } }, [$set rebuilt += (rebuild_cursor != cursor ? n : 0), rebuild_cursor = cursor])` — повтор того же курсора считается один раз; порядок курсоров не проверяется (см. «Отклонения») |
| `request_rebuild` | `find_one_and_update({ _id }, [$set rebuild_requested ∨= cursor == null, pace cond])`, документ до → `Accepted` / `AlreadyRunning`; нет документа — `Invalid` |
| `request_rebuild_key` | `update_one({ _id }, { $addToSet: { rebuild_keys: id } })` (порядок вставки = порядок подачи); `matched_count == 0` — `Invalid` |
| `take_requests` | `find_one_and_update({ _id, lock_token }, [$set rebuild_keys = $size > limit ? $slice(rebuild_keys, limit, size − limit) : []])`, документ до: флаг, темп и первые `limit` ключей; нет документа — `None` |
| `status` | `find({ "_id.projection"? }).sort({ _id: 1 })`, курсор без `futures` |

Позиция и счётчики `u64` → `i64` (`Invalid` за пределами, `Corrupt` при отрицательном чтении); темп и лимит — в
диапазоне `i32`, как у PostgreSQL (`$slice` берёт 32-битные операнды). Сбой драйвера — `CheckpointError::Storage` и
одна строка `error` (`log_mongodb_failure`: `component = "mongodb"`, операция, фиксированная категория `io`,
`server_selection`, `command`, `write`, `bson`, `other` и код сервера; текста ошибки нет).

### Процессор

Процессор вызывает `register` перед каждым `claim`, пока не владеет строкой: удалённая коллекция закладок
регистрируется заново любой нодой (и той, что строкой ещё не владела), и проекция перечитывает журнал с начала.
Отдельного стартового цикла регистрации больше нет; при недоступном хранилище ожидающая нода логирует сбой раз в
`poll_interval`, как и сбой `claim`. `MemCheckpointStore::clear()` моделирует снос коллекции.

### Демо

- `bootstrap/projections/{concert_halls,fan_wallets}.rs` и `read_side.rs` берут `MongoReadModel<V, A>`
  (`infrastructure/mongo/read_model.rs`): `ViewRepository` — делегирование библиотечному `MongoViewRepository`,
  `ReadModelStore` (`load`/`list`/`list_range`/`wipe`) — свои запросы над той же коллекцией по документированной
  форме. `ProjectionMaintenance` до [04](04_demo_on_processor.md) требует один тип с обоими трейтами — отсюда обёртка.
- `infrastructure/mongo/view_repository.rs` удалён; `testing::read_model` (`MemViewRepository`) не меняется.
- `tests/architecture.rs`: `cqrsx::mongodb` запрещён в `application`, как `cqrsx::postgres`.
- Версия драйвера в workspace демо = версия cqrsx (3.9.1, один `bson` 2.15).

## Тесты

- Unit (без сервера): маппинг ошибок драйвера и коды сервера (ошибки собираются из `ErrorKind` и десериализуемых
  `WriteError`/`CommandError`), категории лога без текста ошибки, числа и декодирование документа закладки (все
  `Corrupt`/`Invalid`), «недоступный сервер» (`127.0.0.1:1`, выбор сервера 100 мс): все 13 операций — `Storage` с
  логом, репозиторий — `ConnectionError`; `MemCheckpointStore::clear`;
  `a_wiped_checkpoint_row_is_registered_again_and_the_journal_is_read_from_the_start` в `tests/cqrs/projection/lease.rs`.
- `tests/mongo.rs` (фича `mongodb`, testcontainers `docker.io/library/mongo:8.2`, без replica set):
  - `view_repository`: вставка/обновление/контекст и сырая форма документа, дубль/устаревшая версия/переполнение,
    отсутствующие и битые документы, `stress_concurrent_writers_of_one_mongo_document_converge` (писатели вложенных
    префиксов без внешней блокировки, итог — свёртка, `last_sequence` = длина истории);
  - `checkpoints`: зеркало PostgreSQL-тестов (аренда по часам сервера, fencing, идемпотентный `advance`, порядок
    ключей, `Invalid`, `status`), форма документа и значения, похожие на выражения (`$owner`, `$key`, `$cursor`),
    снесённая коллекция и повторная регистрация;
  - `processor` (журнал в памяти, витрины и закладки в Mongo): сорок команд по пяти ключам, пересборка под коммитами,
    **wipe обеих коллекций → `Lost`, повторный захват, все документы восстановлены без запроса**;
  - `journal_in_postgres` (фича `postgres`): журнал в PostgreSQL, read side в Mongo, сорок команд, закладка = `head()`
    (номер транзакции), затем wipe → самопересборка.
- Фикстуры контейнеров — по файлу на фикстуру: `tests/support/port_lease.rs` (перенесён из `postgres/support/`),
  `tests/support/postgres.rs` (`Postgres`; `Database` в PostgreSQL-наборе — его алиас), `tests/support/mongo.rs`.
- Матрица фич: `cargo test --no-default-features --features mongodb`; корневой `CLAUDE.md` и `ci.yml` получили фичу,
  шаг stress — `--test mongo stress_`.

## Отклонения

- **`advance_rebuild` идемпотентен по равенству курсора, а не по порядку.** Порядок ключей задаёт журнал
  (`ORDER BY aggregate_id` под collation PostgreSQL), документ его не знает: байтовое сравнение могло бы отвергнуть
  законный шаг, владелец принял бы `false` за потерю аренды и зациклился на той же странице. Цена: страница, повторённая
  после потерянного ответа, в которую попал новый меньший ключ, посчитается снова (`rebuilt` — счётчик статуса);
  PostgreSQL в том же случае ответит `false` и владелец потеряет аренду. События не теряются ни там, ни там.
- **Регистрация перед каждым захватом**, а не только после потери аренды: нода, никогда не владевшая строкой, иначе не
  восстановила бы её после wipe (`claim` не отличает «занято» от «нет строки»).
- **Модуль `projection_checkpoints`** (зеркало `postgres::projection_checkpoints`), а не `checkpoints`;
  `PROJECTION_CHECKPOINTS_COLLECTION` — константа имени коллекции.
- **Адаптер демо `read_model.rs`/`MongoReadModel`**, а не `read_model_store.rs`: один тип реализует оба трейта, как
  требует `ProjectionMaintenance`.
- **Процессорные тесты Mongo на журнале в памяти** и в реальном времени (пауза времени не сочетается с I/O);
  PostgreSQL-журнал — в одном кросс-тесте.
- **Фикстуры по файлу на базу** вместо одного `containers.rs`: общий файл с `cfg`-частями давал бы `dead_code` и
  `unused_imports` под `clippy --all-targets -D warnings` при частичных фичах.
- **Без `dns-resolver` и без `futures`**: SRV-адреса включает приложение (фичи драйвера объединяются), курсор статуса
  читается `advance`/`deserialize_current`.
- **Замера нет**: путь записи и операции над витриной не изменились (тот же код переехал в библиотеку).

## Риски

- Крейт `mongodb` тяжёлый: `--all-features` в CI дорожает на минуту-две сборки; ещё четыре задания CI тянут образ
  `mongo:8.2` без кэша.
- `$$NOW` в `$expr` фильтра и конвейерные обновления требуют MongoDB ≥ 4.2; стенд, демо и тесты на 8.2.
- Один документ закладки с массивом ключей: безграничный спам `request_rebuild_key` раздует документ (16 МБ) — тот же
  риск, что таблица в PostgreSQL, без индекса.
- Сброс PostgreSQL без Mongo оставит закладку выше головы нового журнала, и события пропадут: правило «вместе»
  обязательно; sidecar ⟲ reset и `make reset` ему следуют, ручной `TRUNCATE events` — нет.

## Критерии приёмки

- [x] Фича `mongodb`: `MongoViewRepository`, `MongoCheckpointStore`; все операции закладок одним атомарным
      оператором с fencing по `lock_token` и часами сервера; процессор регистрирует строку перед каждым захватом.
- [x] `tests/mongo.rs` зелёный, матрица фич с `mongodb`, покрытие ≥ 97 %.
- [x] Демо собирается на библиотечном репозитории; собственный `MongoViewRepository` удалён; запросы — свой адаптер.
- [x] `components.md` (граница read side, совместный сброс, порядок сноса), README cqrsx, `CLAUDE.md`, `ci.yml` обновлены.
