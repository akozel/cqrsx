# 12. Движок на PostgreSQL и MongoDB: `Cqrs::from_env("ENCORE")`, `.serve()`; `config.rs`, `process.rs`, `bootstrap/` удаляются

| | |
|---|---|
| Статус | предложено 2026-10-27; решение пользователя 2026-10-27 (README эпика, вопрос 1): HTTP — фича `http` cqrsx |
| Ценность | высокая: `main.rs` в десять строк; движок читает свои настройки сам; требования 2–4 пользователя |
| Цена | высокая: настройки (39 переменных), пулы, рантаймы, жизненный цикл и HTTP переезжают в cqrsx; новая фича `http` |
| cqrsx | `src/engine/{settings,postgres,mongodb,serve,http}.rs` (новые), `src/host/lifecycle.rs` (процессоры рядом с рантаймом), `src/postgres/pool.rs`, `Cargo.toml` (фича `http`: axum 0.8), `tests/engine/{postgres,settings}.rs`, `doc/components.md`, `README.md`, корневой `CLAUDE.md` (матрица фич) |
| demo | `main.rs`, `concert.rs`, `fan.rs`, `operations.rs` (`.routes`), `operations/presentation/config.rs`, `shared/infrastructure/http_mailer.rs`, `testing.rs`, `tests/write_side.rs`, `tests/contracts.rs`; удаляются `config.rs`, `process.rs`, `bootstrap.rs`, `bootstrap/*`; `.env.example` (без изменений имён), `README.md` демо |
| Зависимости | 11 |
| Гарантии | G-02, G-07, G-09, G-10, G-11, G-17, G-18, G-27, G-29, G-30, G-31, G-35, G-37 |

## Сейчас

`Process::start` (`.env`, логирование, `Config::from_env`, три пула PostgreSQL, клиент MongoDB), `bootstrap::wire`
(исполнители, процессоры, диспетчер, операции, маршруты), `Process::serve` (слушатель, `prepare_task_runtime`,
`lifecycle::run` с процессорами рядом, закрытие пулов). `config.rs` — 580 строк, 32 переменные `ENCORE_*` (и 7
`OUTBOX_*` библиотеки), почти все настраивают механизмы движка; умолчания демо для `OUTBOX_*` — `outbox_default`.

## Станет

Дизайн [07](design/07_bootstrap.md).

**cqrsx**
- `engine::settings::Settings` — все настройки движка с умолчаниями-константами; `Cqrs::from_env(prefix)` →
  `from_lookup(prefix, lookup)`; имена — нынешние имена демо после приставки (`ENCORE_DATABASE_URL`,
  `ENCORE_SNAPSHOT_EVERY`, `ENCORE_COMMAND_LOCK_*`, `ENCORE_PROJECTION_*`, `ENCORE_DATABASE_*_POOL_SIZE`,
  `ENCORE_DATABASE_STATEMENT_TIMEOUT_MS`, `ENCORE_MONGODB_URI`, `ENCORE_MONGODB_POOL_SIZE`, `ENCORE_READ_DB`,
  `ENCORE_HTTP_ADDR`, `ENCORE_NODE_NAME`, `ENCORE_PROJECTION_LAG_CAP`, `ENCORE_PROJECTION_STATUS_TIMEOUT_MS`);
  `OUTBOX_*` без приставки, как сейчас; `.defaults(Defaults)` — значения приложения, действующие только когда
  переменной нет (демо: воркеры 2, буфер 2, опрос 300 мс, обработчик 5 с, аренда 30 с, хранилище 3 с, settle 10 мс,
  пул MongoDB 10). Проверки те же и с теми же текстами: `max_wait` меньше таймаута обработчика, `retry_base ≤
  retry_max`, `settle ≤ poll`, бюджет аренды, положительные значения; ошибка называет переменную с приставкой.
  Ключи модулей (`ENCORE_TICKET_LIMIT`, `ENCORE_MAIL_SERVICE_URL`, `ENCORE_TASK_*`, `ENCORE_REPLAY_*`) читают модули
  через `Setup` с той же приставкой.
- `engine::postgres::Postgres` + `engine::mongodb::MongoDb` — бэкенды: три пула PostgreSQL по плану G-29 (главный:
  команды, очередь, закладки PostgreSQL; истории: без таймаута, `min 0`, idle 30 с; проекций: страницы журнала и
  истории ключей), клиент MongoDB (таймауты 3 с, имя приложения, пул); `.event_store(Postgres)`,
  `.projections(MongoDb)` или `.projections(Postgres)`. `.build().await` открывает пулы лениво, как сейчас.
- `settings().describe()` — построчное описание действующих настроек и плана ресурсов (эталон 02).
- `serve()` (фича `host`): слушатель `ENCORE_HTTP_ADDR`, логирование `RUST_LOG` (`LoggingConfig`, один раз),
  `prepare_task_runtime`, процессоры рядом с `lifecycle::run` (перенос `run_components` из `process.rs` в
  `host::lifecycle`: порядок остановки HTTP → рантайм → процессоры → пулы; паника процессора — ошибка процесса),
  имя ноды — владелец закладок.
- Фича `http` (axum 0.8, в обоих `Cargo.lock`): `Module::routes(axum::Router<Arc<Cqrs>>)`, `serve` объединяет
  маршруты модулей под общим состоянием `Arc<Cqrs>` и `TraceLayer`, как сейчас. Обработчики берут зависимости
  модуля через `cqrs.dependency::<T>()` (например, `ActivityHub` для SSE). Матрица проверок `CLAUDE.md` получает
  строку `--features http`.
- `Cqrs::with_settings(settings)` — для тестов на контейнере (`write_side.rs`).

**demo**
- `main.rs` — набросок дизайна 07 плюс `dotenvy::dotenv().ok()` первой строкой и `DEMO_DEFAULTS`.
- Корни модулей читают свои ключи: `concert` — `TICKET_LIMIT`; `fan` — `MAIL_SERVICE_URL`, `TASK_MAX_ATTEMPTS`,
  `TASK_RETRY_BASE_MS`, `TASK_RETRY_MAX_MS`, таймаут почтового запроса — `setup.engine().handler_timeout() − 1 с`
  (не меньше 1 с, как сейчас); `operations` — `REPLAY_SLOW_DELAY_MS`, `REPLAY_MAX_PACE_MS`, маршруты служебные;
  `/api/config` — из `ConcertRules` и `cqrs.settings()`.
- `config.rs`, `process.rs`, `bootstrap.rs`, `bootstrap/*` удаляются; `testing.rs` — движки в памяти и
  `with_settings` над ленивыми пулами для тестов, которые отвечают до обращения к базе.
- `.env.example`: имена и умолчания не меняются; меняются только комментарии про место настройки.

## Этапы

1. `Settings` + `from_lookup` + `describe` + проверки; тесты каждой переменной и каждой проверки (перенос тестов
   `config.rs` демо в cqrsx), в изолированном процессе для `from_env` (как `TaskRuntimeConfig`).
2. Бэкенды PostgreSQL и MongoDB; `pool_plan`-эквивалент в движке и тест против эталона; интеграционные тесты
   `tests/engine/postgres.rs`: модули на контейнерах PostgreSQL и MongoDB — команда, задача, команда, витрина.
3. `serve` и перенос `run_components` в `host::lifecycle` с его тестами; фича `http`; тест `serve` на свободном
   порту с одним модулем.
4. Демо: `main.rs`, корни модулей, HTTP через `Arc<Cqrs>`, удаление проводки; `write_side.rs` через
   `with_settings`; `tests/contracts.rs`: эталон настроек из `describe()`.
5. `components.md`: «Backend assembly» переписывается под `engine`; «Process lifecycle»; новая «Settings»;
   `README.md` cqrsx — сборка на PostgreSQL = `main.rs` демо; `CLAUDE.md` — матрица.

## Зачем, на примере

```rust
#[tokio::main]
async fn main() -> Result<(), StartupError> {
    dotenvy::dotenv().ok();
    Cqrs::from_env("ENCORE")
        .defaults(DEMO_DEFAULTS)
        .event_store(Postgres)
        .projections(MongoDb)
        .module(concert::module)
        .module(fan::module)
        .module(operations::module)
        .serve()
        .await
}
```

Вместо `Process` (342 строки), `Config` (580) и `bootstrap/` (≈ 500): движок знает свои ключи, модули — свои.

## Риски

- **Незаметная смена умолчания или пула.** Эталон `effective_config.txt` (02) должен совпасть байт в байт: все 39
  переменных (32 `ENCORE_*` и 7 `OUTBOX_*`), производные и план пулов. Это главная защита задачи.
- **Фича `http` тянет axum в cqrsx.** axum уже в дереве зависимостей демо и в офлайн-кэше; фича необязательная,
  `--no-default-features` её не включает. Решение пользователя 2026-10-27: фича `http`; вариант `serve_with(inbound)`
  без axum отклонён.
- **Логирование в `serve`.** `try_init` один раз на процесс, как сейчас в `Process::start`; тесты не зовут `serve`.
- **Остановка.** Порядок и поведение при панике процессора переносятся вместе с тестами `process.rs` (G-30).
- **Две базы — два бэкенда.** `.projections(Postgres)` даёт витрины в PostgreSQL (`PostgresViewRepository`,
  `PostgresCheckpointStore`); демо использует MongoDB, как сейчас.

## Как проверить

- Проверки `CLAUDE.md` с новой строкой матрицы (`http`); покрытие (`settings`, бэкенды, `serve`).
- `tests/engine/postgres.rs` на контейнерах; `write_side.rs` демо.
- `tests/contracts.rs`: `effective_config.txt` без изменений; `cargo run` демо на dev-стеке пользователя не
  используется — проверка на стенде в 13.

## Поведение не меняется: чем доказано

- Действующая конфигурация и план пулов — эталон 02 байт в байт (G-02, G-07, G-17, G-18, G-29, G-37).
- Порядок остановки и реакция на панику процессора — перенесённые тесты `process.rs` в `host::lifecycle` (G-30).
- Пробуждение, один исполнитель на тип, закладки в MongoDB — те же компоненты, собранные движком (G-09, G-11, G-27);
  интеграционный тест на контейнерах повторяет `write_side.rs`.
- HTTP: те же маршруты и ответы (G-35: `operations/presentation/tests.rs` на `Arc<Cqrs>` в памяти).
- Логирование — тот же `LoggingConfig` (G-31).

## Критерии приёмки

- [ ] `Cqrs::from_env`, `Settings`, `Defaults`, бэкенды PostgreSQL и MongoDB, `serve`, фича `http`.
- [ ] Демо: `main.rs` по дизайну; `config.rs`, `process.rs`, `bootstrap/` удалены; корни модулей читают свои ключи.
- [ ] Эталоны настроек, ленты и пауз проходят без изменений; `.env.example` — те же имена и значения.
- [ ] `components.md`, README cqrsx, `CLAUDE.md` (матрица), guarantees.md.
