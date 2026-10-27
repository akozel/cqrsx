# 07. Сборка приложения и модуля

Статус: согласовано, 2026-10-27.

## Назначение

Как приложение собирается в пару строк и как модуль подключает свои части и свою настройку.

## Как это выглядит

```rust
// src/main.rs
#[tokio::main]
async fn main() -> Result<(), StartupError> {
    Cqrs::from_env("ENCORE")    // ENCORE_SNAPSHOT_EVERY, ENCORE_DATABASE_URL, …; the library's OUTBOX_* as they are
        .defaults(DEMO_DEFAULTS) // the demo's values where they differ from the library's, used when a variable is absent
        .event_store(Postgres)   // the journal, snapshots and the outbox
        .projections(MongoDb)    // views and their checkpoints
        .module(concert::module)
        .module(fan::module)
        .module(operations::module) // health, settings, activity, operations over every view
        .serve() // the modules' HTTP next to the task runtime and the processors, until a signal
        .await
}
```

```rust
// src/concert.rs: the module root
//! The box office: concerts, seats, bookings.
pub mod application;
pub mod domain;
pub mod presentation;

pub fn module(setup: &Setup) -> Result<Module, SetupError> {
    Ok(Module::new("concert")
        .aggregate::<Concert>(ConcertRules { ticket_limit: setup.env("TICKET_LIMIT", 4)? }) // ENCORE_TICKET_LIMIT
        .view::<ConcertHall>()
        .routes(presentation::routes))
}
```

```rust
// src/fan.rs: the module root
//! A fan's ledger: tickets and refunds.
pub mod application;
pub mod domain;
pub mod presentation;

pub fn module(setup: &Setup) -> Result<Module, SetupError> {
    // One mailer for every worker, with an HTTP client of its own; a request gives up a second before the handler timeout.
    let mail = shared::infrastructure::HttpMailer::new(setup.env("MAIL_SERVICE_URL", "http://mail:8025")?, setup.engine().handler_timeout())?;
    let attempts = setup.env("TASK_MAX_ATTEMPTS", 6)?;
    let mail_pause = Retry::backoff(setup.millis("TASK_RETRY_BASE_MS", 1000)?, setup.millis("TASK_RETRY_MAX_MS", 8000)?);
    Ok(Module::new("fan")
        .aggregate::<Fan>(())
        .provide(mail) // the handlers' `&Mailer`
        // The policies of the task types, as today: chain steps wait 1 s, mails back off from 1 s to 8 s.
        .policy(STEP, Retry::fixed(Duration::from_secs(1)), attempts)
        .policy(MAIL, mail_pause, attempts)
        .task(application::record_ticket).policy(STEP)
        .task(application::record_cancellation).policy(STEP)
        .task(application::send_e_ticket).policy(MAIL)
        .task(application::issue_refund).policy(MAIL)
        .view::<FanWallet>()
        .routes(presentation::routes))
}
```

- **Реакции агрегата подключаются вместе с ним:** `.aggregate::<Concert>(..)` требует `impl Reactions for Concert`
  (04).
- **Политики задачи.** Корень модуля объявляет значения политик из окружения и привязывает к ним типы задач:
  `.task(issue_refund).policy(MAIL)` (05). Реакция политику не называет (04).
- **Обработчики задач — функции (05).** Их зависимости корень модуля подключает через `.provide(..)`, а при сборке
  движок проверяет, что зависимостей хватает.

## Что делает движок

- **Пулы:**
  - PostgreSQL — для команд и очереди, для истории, для проекций;
  - MongoDB;
  - их размеры, таймаут оператора и проверка соединения при выдаче.
- **Исполнитель на каждый агрегат:** блокировки, повторы при конфликте, снапшоты; реакции агрегата идут вместе с ним.
- **Рантайм задач** с обработчиками всех модулей, **процессоры витрин**, пробуждения.
- **Жизненный цикл:** HTTP, задачи, процессоры; остановка по сигналу в прежнем порядке.
- **Имя ноды** — владелец аренд у закладок витрин; его видно в статусе процессоров. Модули о нодах не знают.
- **Журнал приложения** (`RUST_LOG`).

## Настройки

- **Движок читает свои ключи сам,** с умолчаниями-константами.
  - Приставку задаёт приложение: `Cqrs::from_env("ENCORE")`.
  - Нынешние имена демо после приставки становятся именами движка: `ENCORE_SNAPSHOT_EVERY`, `ENCORE_DATABASE_URL`,
    `ENCORE_PROJECTION_*`, `ENCORE_COMMAND_LOCK_*`, `ENCORE_NODE_NAME` и другие.
  - Библиотечные `OUTBOX_*` остаются как есть.
- **Модуль читает только свои ключи** через ту же приставку: `setup.env("TICKET_LIMIT")` читает `ENCORE_TICKET_LIMIT`.
- **Умолчания демо** там, где они отличаются от библиотечных, задаются декларативно в `main.rs` — `.defaults(..)`.
  Действуют они, только когда переменной нет, как сейчас `outbox_default` в `config.rs`:

  | Параметр | Демо | Библиотека |
  |---|---|---|
  | воркеры | 2 | 128 |
  | буфер | 2 | 128 |
  | опрос | 300 мс | 1 с |
  | таймаут обработчика | 5 с | 15 с |
  | аренда | 30 с | 60 с |
  | таймаут хранилища | 3 с | 6 с |

- **Модулю `Setup` даёт** окружение (через приставку) и действующие настройки движка только для чтения, например
  таймаут обработчика. Общие ресурсы вроде HTTP-клиента создаёт `shared/infrastructure`: cqrsx не зависит от
  `reqwest`.

## Как тестировать

Тот же список модулей, без баз. Тест подменяет зависимость модуля по типу, и тестовый `.provide` побеждает:

```rust
let mail = RecordingMailer::new(); // records the letters instead of sending them
let cqrs = Cqrs::in_memory()
    .module(concert::module)
    .module(fan::module)
    .provide(mail.clone())
    .build()?;
```

## Решено

- **2026-10-26: корень модуля** — файл рядом с каталогом модуля, `main.rs` только перечисляет модули (README дизайна).
- **2026-10-26: маршруты** каждого модуля — из его `presentation/` через `.routes(..)`. Служебные маршруты, лента
  активности и операции над витринами — модуль `operations` (01).
- **2026-10-26: почта:** клиент — `shared/infrastructure`, порт — `shared/application` (01).
- **2026-10-27: имена переменных** — приставка от приложения, `Cqrs::from_env("ENCORE")`. Движок берёт нынешние имена
  демо после приставки, модули читают свои ключи через неё же, `OUTBOX_*` не меняются.
- **2026-10-27: умолчания демо** — в коде демо, декларативно в `main.rs` (`.defaults(..)`). Действуют, только когда
  переменной нет.
- **2026-10-27: зависимости модуля.**
  - Модуль сам создаёт их в своём корне и подключает через `.provide(..)`.
  - Тест подменяет зависимость по типу.
  - `Setup` даёт окружение и общие ресурсы.
- **2026-10-27: имя ноды — настройка движка** (`ENCORE_NODE_NAME` через приставку): владелец аренд, модули о нодах не
  знают.
- **2026-10-27 (бэклог, вопрос 1): HTTP — необязательная фича `http` cqrsx** (axum 0.8). Маршруты модуля —
  `Router<Arc<Cqrs>>`, `serve()` объединяет их и слушает адрес из настроек.
- **2026-10-27 (бэклог, вопрос 2): политика привязана к типу задачи при регистрации** (`.task(h).policy(MAIL)`).
- **2026-10-27 (бэклог, вопрос 6): `Setup` без общих ресурсов** — только окружение и настройки движка.

## Открытые вопросы

Нет.
