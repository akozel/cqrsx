# Аудит границы ответственности: back-end ↔ sidecar (↔ cqrsx)

Дата: 2026-10-17. Ветка `feat/cqrsx-extractions`, коммит `7342ebc` (бэклог 01–06 реализован).
Объект: `demo/back-end` (crate `encore-backend`, 8 тыс. строк) и `demo/sidecar` (crate `encore-sidecar`).

## 1. Критерий и шкала

**Назначение сервисов (как договорились).** Back-end показывает работу `cqrsx` и должен выглядеть как
production-сервис кассы. Sidecar — вспомогательный процесс: окно в базы данных плюс операции CQRS/ES,
которых в повседневной эксплуатации нет (удаление данных, сброс демо). Ограничения из PLAN.md §3.5
(«sidecar без `cqrsx`», «только read-only роль») **сняты**: sidecar может иметь любые зависимости.

**Критерий.** Для каждого модуля и эндпоинта back-end один вопрос:
*был бы он в реальном сервисе кассы, построенном на `cqrsx`?* Если нет, куда он должен уехать:

- **→ sidecar** — демо-операция над данными или демо-справочник;
- **→ cqrsx** — обобщённый код, которому место в библиотеке (пересекается с тикетами 07–09 бэклога);
- **→ внешний мир** — имитация внешней системы, которую production-сервис вызывал бы по сети;
- **остаётся** — production-поведение, пусть и с демо-подачей.

**Шкала обоснованности переноса.**

| Оценка | Смысл |
|---|---|
| ●●● обоснован | граница становится заметно чище, разумной альтернативы нет |
| ●●○ скорее обоснован | польза ясна, но есть разумная альтернатива или ощутимая цена |
| ●○○ спорно | аргументы в обе стороны, решение за владельцем демо |
| — | не переносить |

## 2. Сводная таблица

| # | Что | Где было | Куда | Оценка | Цена | Меняет UI | Связано | Статус (2026-10-17) |
|---|---|---|---|---|---|---|---|---|
| S-1 | `POST /api/projections/wipe` — стереть витрины без пересборки | `presentation/http/projections/wipe.rs`, `RebuildService::wipe`, `WipeError` | sidecar `POST /inspect/projections/wipe` | ●●● | низкая | да (1 вызов) | §3.1 | ✅ сделано: `sidecar/src/{demo,http}.rs`, UI `sidecar.wipe()`; проверка «wipe во время rebuild» не добавлялась (решение владельца) |
| S-2 | `GET /api/fans/{fan_id}/inbox` — чтение ящика внешней почты | `presentation/http/fans/inbox.rs`, `ports::{Inbox, DeliveredMail}`, `MongoInbox::of_fan`, `ReadModels.inbox` | sidecar `GET /inspect/mail/{fan_id}` | ●●● | низкая | да (1 вызов) | §3.2, S-4 | ✅ сделано: `MongoInspector::inbox_of`, UI `sidecar.inbox()` (`_id` → `inbox_id` в `wallet.ts`) |
| S-3 | `/api/config`: персонажи демо и неиспользуемые демо-параметры | `presentation/http/config.rs::PERSONAS`, поля `task_failure_rate`, `task_max_attempts` | персонажи → sidecar `GET /inspect/demo`; два поля удалить | ●●○ | низкая | да (1 поле) | §3.3 | ✅ сделано: `PERSONAS` в `sidecar/src/demo.rs`, UI `session.demo.personas`; reset работает как прежде |
| S-4 | Имитация почтового сервиса: кубик, задержка, причины, запись в `mail_service.inbox` | `infrastructure/mail/{chaos, flaky_mail_server}.rs`, `infrastructure/mongo/inbox.rs`, `ENCORE_TASK_FAILURE_RATE`, `_LATENCY_*`, `_CHAOS_SEED`, `ENCORE_MAIL_DB` | внешний мир: модуль sidecar (решение владельца); в back-end остаётся `HttpMailer` | ●●○ | высокая | нет | §3.4 | ✅ сделано: `sidecar/src/mail.rs` + `mail/{chaos, inbox}.rs` (`POST /mail`, `SIDECAR_MAIL_*`); back-end `infrastructure/mail/http_mailer.rs`, `ENCORE_MAIL_SERVICE_URL`; к базе `mail_service` back-end не подключается |
| C-1 | Счётчики журнала raw SQL по таблице библиотеки | `infrastructure/postgres.rs::PostgresLedger`, `ports::EventLedgerStats` | cqrsx (тикет 08) — вариант A по решению владельца | ●●● | низкая | нет | §3.5, тикет 08 | ✅ сделано: `PostgresEventRepository::totals::<A>()`, `LedgerTotals`, тест `event_store::totals`, `components.md`; `PostgresLedger` без SQL |
| C-2 | Обобщённая часть wipe/rebuild/verify | `application/projections/maintenance.rs`, `ports::ReadModelStore::{list, wipe}` | cqrsx (тикет 07) | ●●○ | средняя | нет | тикет 07 | ✖ закрыто 2026-10-23: не переносить, пока нет проекта, которому это нужно (§3.6) |
| C-3 | Репозиторий витрин для MongoDB | `infrastructure/mongo/view_repository.rs` | cqrsx (тикет 09), условно | ●○○ | высокая | нет | тикет 09 | ✖ закрыто 2026-10-23: не переносить, пока нет проекта, которому это нужно (§3.6) |
| K-1 | Rebuild-lock: касса закрыта на время пересборки | `application/lock.rs` + 12 файлов, где он проверяется; `ENCORE_CHAIN_MAX_ATTEMPTS` | не переносить: либо убрать («живой» rebuild, PLAN P2), либо оставить как осознанное упрощение | ●○○ | средняя | да | §4.1 | ✅ снято: касса открыта во время Encore! (D-16 эпика 03, реализовано 2026-10-25) |
| K-2 | Замедленный replay `speed: slow` | `operations/rebuild.rs::Speed`, `ENCORE_REPLAY_SLOW_DELAY_MS` | остаётся как `pace` (библиотечный `with_pace`) | — | — | нет | §4.2 | ✅ `pace_ms` вместо `speed` (D-18 эпика 03, реализовано 2026-10-25) |
| K-3 | Неиспользуемые зависимости `sha2`, `hex` | `back-end/Cargo.toml` | удалить | ●●● | нулевая | нет | §4.3 | ✅ сделано (вместе с `rand`, который ушёл с имитацией почты) |

Остальное — production-поведение и остаётся в back-end (§5). Обратная проверка sidecar — §6.

**Проверка реализации (2026-10-17/18; коммиты `3c1b771` cqrsx, `790ec18` demo).** `cqrsx`: fmt, clippy `-D warnings`, тесты со всеми
фичами (88 интеграционных + unit) и doctests, матрица фич (`--no-default-features`, `postgres`, `worker`,
`host`, `logging`), `llvm-cov --fail-under-lines 97` для workspace и для `cqrsx` отдельно (строки 97.7 %).
Demo: fmt, clippy, `cargo test --workspace` (back-end 92 unit + 2 architecture + 1 `write_side` на
testcontainers, sidecar 14), front-end typecheck, lint, 52 vitest, production build. Решения по ходу
реализации записаны в PLAN.md D-13; журнал — `PROGRESS.md`.

## 3. Карточки переносов

### 3.1 S-1. `POST /api/projections/wipe` → sidecar ●●●

**Сейчас.** Back-end публикует эндпоинт, который дропает обе коллекции витрин и ничего не восстанавливает.
Он отказывает `423`, пока идёт rebuild, и пишет `projections.wiped` в activity.

```rust
// back-end: presentation/http/projections/wipe.rs
pub async fn handle(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    match state.rebuild.wipe().await {
        Ok(collections) => Ok(Json(json!({ "outcome": "wiped", "collections": collections }))),
        Err(WipeError::Locked) => Err(ApiError::locked()),
        Err(WipeError::Storage(message)) => Err(ApiError::unavailable(message)),
    }
}

// back-end: application/operations/rebuild.rs
/// 🧨 Drop every read model (the inbox is not a read model and stays).
pub async fn wipe(&self) -> Result<Vec<&'static str>, WipeError> { /* lock check, wipe each, activity */ }
```

Sidecar при этом **уже** дропает те же коллекции внутри `reset` (`demo.rs::DROPPED`), под флагом
`SIDECAR_RESET_ENABLED` и с собственным Mongo-клиентом.

**Станет.**

```rust
// sidecar: demo.rs
/// The read models (not the inbox): what 🧨 drops and what a reset drops before the inbox.
pub const READ_MODELS: [(&str, &str); 2] = [("encore_read", "concert_halls"), ("encore_read", "fan_wallets")];

impl DemoAdmin {
    /// 🧨 Drops the read models and nothing else: the ledger and the inbox stay.
    pub async fn wipe_read_models(&self) -> Result<Vec<String>, DemoError> {
        let mut dropped = Vec::new();
        for (database, collection) in READ_MODELS {
            self.mongo.database(&self.database_name(database)).collection::<Document>(collection).drop().await?;
            dropped.push(format!("{database}.{collection}"));
        }
        Ok(dropped)
    }
}

// sidecar: http.rs — 404 when the demo admin is disabled, like /inspect/reset
.route("/inspect/projections/wipe", post(wipe))
```

Back-end: файл `wipe.rs`, маршрут, `WipeError` и публичный `RebuildService::wipe` удаляются;
внутренний сброс перед replay («always from scratch») остаётся приватным шагом `run`. `ReadModelStore::wipe`
остаётся — он нужен rebuild. UI: `backend.wipe()` → `sidecar.wipe()`; `concerts.markWiped()` на `200`
уже вызывается локально, X-ray покажет пустые коллекции через ≤ 400 мс. Запись `projections.wiped`
в activity исчезает (её источник был back-end); ветка `rebuild.onActivity('projections.wiped')` в UI
становится мёртвой и удаляется.

**Зачем, на примере.** Представьте, что back-end уезжает в production без изменений. Первый эндпоинт,
который попросит убрать security review, — `POST /api/projections/wipe`: удаление данных без восстановления,
без аутентификации, и единственная его цель — показать «витрина без истории врёт». Это и есть
«удаление данных из базы», которое мы отнесли к sidecar. Он уже умеет ровно это (`reset`), у него есть
клиент и выключатель; wipe встаёт рядом с reset под тот же флаг.

**Спорные места.** (1) Пропадает защита «wipe во время rebuild → 423». Кнопка в UI и так заблокирована
при `running`; при желании sidecar перед drop спрашивает `GET /api/projections/status` у back-end
(`SIDECAR_BACKEND_URL` у него уже есть) и отвечает `409`. (2) Сценарий демо (README, шаг 7) не меняется:
кнопка 🧨 остаётся в панели Encore, меняется только адресат запроса. Обновить PLAN.md §2.9 и §6.4.

### 3.2 S-2. `GET /api/fans/{fan_id}/inbox` → sidecar ●●●

**Сейчас.** Касса читает почтовый ящик фаната из базы внешнего почтового сервиса ради надписи в кошельке
«📬 доставлено (попытка 2)». Для этого в back-end живут порт, адаптер, поле в `ReadModels` и эндпоинт.

```rust
// back-end: application/ports.rs
pub struct DeliveredMail { /* 12 полей документа inbox */ }
#[async_trait]
pub trait Inbox: Send + Sync {
    async fn of_fan(&self, fan_id: &FanId) -> Result<Vec<DeliveredMail>, StorageError>;
}

// back-end: infrastructure/mongo/inbox.rs
impl Inbox for MongoInbox {
    async fn of_fan(&self, fan_id: &FanId) -> Result<Vec<DeliveredMail>, StorageError> {
        self.collection.find(doc! { "to_fan_id": fan_id.as_str() }).sort(doc! { "first_delivered_at": 1 }) /* … */
    }
}
```

Sidecar уже показывает `mail_service.inbox` целиком (`/inspect/mongo/mail_service/inbox`); не хватает только
фильтра по адресату и порядка по времени доставки.

**Станет.**

```rust
// sidecar: mongo.rs
pub async fn inbox_of(&self, fan_id: &str) -> Result<CollectionView, mongodb::error::Error> {
    let coll = self.client.database(&self.mail_db).collection::<Document>("inbox");
    let docs = coll.find(doc! { "to_fan_id": fan_id }).sort(doc! { "first_delivered_at": 1 }).await?.try_collect().await?;
    Ok(CollectionView { source: "mongodb", database: "mail_service".into(), collection: "inbox".into(),
        query: format!("db.getSiblingDB('mail_service').inbox.find({{ to_fan_id: '{fan_id}' }}).sort({{ first_delivered_at: 1 }})"),
        total: docs.len() as u64, docs: docs.into_iter().map(|d| Bson::Document(d).into_relaxed_extjson()).collect() })
}

// sidecar: http.rs
.route("/inspect/mail/{fan_id}", get(inbox))
```

Back-end: удаляются `fans/inbox.rs`, `ports::{Inbox, DeliveredMail}`, `ReadModels.inbox`; `MongoInbox`
оставляет только `MailStore::deliver` (запись имитации почты, уходит целиком в S-4). UI: `wallet.ts` берёт
письма из `sidecar.inbox(fanId)`; документы приходят как relaxed Extended JSON с `_id` вместо `inbox_id`
(переименование в sidecar или в `types/xray.ts`).

**Зачем, на примере.** Касса не читает почту фаната — её читает фанат в своём почтовом клиенте. В production
у сервиса нет доступа к базе почтового провайдера, и уж точно нет публичного эндпоинта «покажи чужие письма».
Sidecar же буквально «окно в базы»: он показывает `inbox` в X-ray уже сейчас. После переноса в back-end не
остаётся ни одного **чтения** из `mail_service`, а после S-4 — и записи.

**Спорные места.** Ответ sidecar — «сырые документы», а не DTO: это его стиль (`CollectionView` с текстом
запроса), и это даже лучше для демо («вот что лежит в базе почты»). Кошелёк в UI показывает одно поле
(`delivered_on_attempt`, `deliveries`), типизация остаётся на стороне UI.

### 3.3 S-3. `/api/config`: справочник сервиса vs реквизит демо ●●○

**Сейчас.** Один документ `GET /api/config` (он же `hello` в SSE) смешивает справочник кассы и реквизит демо:

| Поле | Что это | Используется UI |
|---|---|---|
| `venue` | зал, зоны, цены — справочник кассы | да |
| `ticket_limit` | правило R6 — конфигурация сервиса | да (`RuleBreaker`, подписи) |
| `snapshot_every` | конфигурация event store | да (маркер снапшота в X-ray) |
| `personas` | Fox, Panda, Unicorn, Arbitrage Bot — актёры демо | да (`PersonaPicker`, аватары) |
| `task_failure_rate` | параметр кубика имитации почты | **нет** (только тип и дефолт) |
| `task_max_attempts` | бюджет попыток почтовых задач | **нет** (только тип и дефолт) |

```rust
// back-end: presentation/http/config.rs
pub const PERSONAS: [Persona; 4] = [ Persona { fan_id: "fox", name: "Fox", avatar: "🦊" }, /* … */ ];
```

**Станет.** Back-end: `GET /api/config { venue, ticket_limit, snapshot_every }`. Sidecar:
`GET /inspect/demo { default_concerts, personas, reset_enabled }`. UI: `session.config.personas` →
`session.demo.personas` (fallback `DEFAULT_PERSONAS` уже есть). Поля `task_failure_rate` и
`task_max_attempts` удаляются из контракта и из `types/domain.ts`.

**Зачем, на примере.** Три концерта по умолчанию уже переехали в sidecar как «реквизит демо» (PROGRESS,
2026-10-17); четыре персонажа — тот же реквизит, оставшийся на старом месте. В production `FanId`
приходит из аутентификации, а не из справочника кассы. А `task_failure_rate` в ответе кассы читается как
«сервис признаётся, что сам роняет 20 % писем» — после S-4 этого параметра у back-end просто не будет.

**Почему не ●●●.** Персонажей можно отдать и целиком UI (константа уже есть), серверу они не нужны вовсе;
выбор «sidecar или UI» — дело вкуса. Главное, что им не место в back-end. `snapshot_every` остаётся: это
честная настройка сервиса, как `ticket_limit`.

### 3.4 S-4. Имитация почтового сервиса → внешний мир ●●○ (архитектурное решение)

**Сейчас.** Внешняя почта имитируется *внутри* back-end. `FlakyMailServer` реализует порт `Mailer`:
спит случайную «сетевую» задержку, бросает кубик (20 %), выбирает смешную причину, делает идемпотентный
upsert в `mail_service.inbox` и публикует `task.attempt.*` в activity. Через `DispatcherDeps` в каждый воркер
прокидываются `failure_rate`, `chaos_seed`, `MailSettings`; `Config::from_env` проверяет
`latency_max < handler_timeout`; крейт тянет `rand` ради кубика.

```rust
// back-end: infrastructure/mail/flaky_mail_server.rs
impl<S: MailStore, C: ChaosPolicy> Mailer for FlakyMailServer<S, C> {
    async fn send(&mut self, attempt: &TaskAttempt, mail: Mail) -> Result<(), TaskHandlerError> {
        tokio::time::sleep(latency).await;
        // DEMO ONLY: the dice roll.
        if self.chaos.should_fail() {
            let reason = REASONS[self.chaos.pick_reason(REASONS.len())];
            return Err(TaskHandlerError::Retry { reason: reason.to_owned(), retry_after: Some(self.retry_after(number)) });
        }
        match self.store.deliver(mail, attempt.task_id(), number).await { /* upsert into mail_service.inbox */ }
    }
}
```

Объём: `chaos.rs` 78 + `flaky_mail_server.rs` 234 + `inbox.rs` 86 строк и 5 переменных окружения.

**Станет.** Почтовый сервис — отдельный процесс (`demo/mail-service`, axum) или дополнительный роутер
sidecar. Он владеет кубиком, задержкой, причинами и базой `mail_service`:

```rust
// demo/mail-service (или sidecar): POST /mail — the moody provider
async fn send(State(s): State<MailState>, headers: HeaderMap, Json(mail): Json<MailDto>) -> Response {
    tokio::time::sleep(s.latency.sample()).await;                       // simulated network
    if s.chaos.lock().should_fail() {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({ "reason_code": s.chaos.lock().reason() }))).into_response();
    }
    let key = headers.get("Idempotency-Key");                           // e_ticket:<booking_id> / refund:<cancellation_id>
    let delivery = s.inbox.deliver(key, mail).await?;                   // upsert; a repeat only bumps `deliveries`
    (StatusCode::OK, Json(json!({ "inbox_id": delivery.inbox_id, "duplicate": delivery.duplicate }))).into_response()
}
```

В back-end остаётся один адаптер без единой строчки про кубик:

```rust
// back-end: infrastructure/mail/http_mailer.rs — the whole mail code of the box office
impl Mailer for HttpMailer {
    async fn send(&mut self, attempt: &TaskAttempt, mail: Mail) -> Result<(), TaskHandlerError> {
        self.activity.publish("task.attempt.started", /* as now */);
        let response = self.client.post(format!("{}/mail", self.base_url))
            .header("Idempotency-Key", &mail.id).json(&mail).send().await;
        let outcome = match response {
            Ok(r) if r.status().is_success() => Ok(()),
            Ok(r) if r.status() == StatusCode::SERVICE_UNAVAILABLE => Err(TaskHandlerError::Retry {
                reason: r.json::<Rejected>().await.map(|b| b.reason_code).unwrap_or_else(|_| "mail_unavailable".into()),
                retry_after: Some(self.backoff.after(attempt.number())),   // 1, 2, 4, 8, 8 s — the client's own policy
            }),
            Ok(r) => Err(TaskHandlerError::Permanent { reason: format!("mail service answered {}", r.status()) }),
            Err(_) => Err(TaskHandlerError::Retry { reason: "mail_unreachable".into(), retry_after: Some(self.backoff.after(attempt.number())) }),
        };
        self.activity.publish(/* task.attempt.succeeded / failed, as now */);
        outcome
    }
}
```

Что меняется в back-end: уходят `infrastructure/mail/{chaos, flaky_mail_server}.rs`,
`infrastructure/mongo/inbox.rs`, зависимость `rand`, переменные `ENCORE_TASK_FAILURE_RATE`,
`ENCORE_TASK_LATENCY_MIN_MS/MAX_MS`, `ENCORE_CHAOS_SEED`, `ENCORE_MAIL_DB`, проверка `latency_max <
handler_timeout`; `DispatcherDeps` теряет `failure_rate`, `chaos_seed`, `mail`. Остаются `Mailer`, `Mail`,
`ENCORE_TASK_RETRY_BASE_MS/MAX_MS` (политика повторов — решение клиента, так и в production),
`ENCORE_TASK_MAX_ATTEMPTS`; приходят `ENCORE_MAIL_SERVICE_URL` и `reqwest`. События `task.attempt.*`
публикует `HttpMailer`, UI их не замечает. Compose: новый сервис (или порт sidecar), nginx без изменений.

**Зачем, на примере.** Откройте `flaky_mail_server.rs` глазами человека, который пришёл посмотреть
«как пишут сервис на cqrsx». Он читает: касса сама себе роняет письма с вероятностью 20 % и сама пишет в базу
почтового провайдера. Ни то, ни другое в production невозможно. После переноса тот же человек видит
`HttpMailer`: POST во внешний API с `Idempotency-Key`, `503` → `Retry` с backoff, `4xx` → `Permanent` —
и это ровно тот код, который он скопирует в свой проект. Второй эффект: back-end перестаёт знать о базе
`mail_service` *вообще*, и тезис «replay не трогает внешние эффекты» становится физическим фактом: у кассы
нет доступа к ящику. Третий: диаграмма в UI уже рисует flaky mail server отдельным блоком — код начнёт
совпадать с картинкой.

**Спорные места и цена.** (1) Появляется сетевой переход и новый отказ: почтовый сервис недоступен → все
почтовые задачи уходят в `Retry { mail_unreachable }`. Это реалистично, но это ещё один сценарий, который
придётся объяснять в README. (2) Детерминизм через `ENCORE_CHAOS_SEED` переезжает к провайдеру; тесты
`HttpMailer` в back-end нужны с заглушкой-сервером (axum на случайном порту, десяток строк).
(3) Где хостить: в sidecar дёшево (ещё один роутер и уже имеющийся Mongo-клиент), но sidecar перестаёт
быть «инспектором» и становится «инспектор + внешний мир». Отдельный `demo/mail-service` чище по смыслу,
но это третий бинарь, Dockerfile, сервис compose и порт. Рекомендация: отдельный крейт, если демо хочет
показывать «внешний мир» отдельной коробкой (диаграмма уже так делает); sidecar — если важна цена.
(4) Это самая дорогая находка аудита; её можно отложить, S-1–S-3 и C-1 от неё не зависят.

**Решение и реализация (2026-10-17).** Хостить в sidecar: владелец демо считает нормальным, что sidecar занят
не только наблюдением, но и «грязной работой». Реализовано как `sidecar/src/mail.rs` (`MailService`, роутер
`POST /mail`, `Idempotency-Key` с запасным ключом из тела, `503 { reason_code }`), `mail/chaos.rs` (кубик) и
`mail/inbox.rs` (upsert в `mail_service.inbox`); настройки `SIDECAR_MAIL_FAILURE_RATE`, `SIDECAR_MAIL_LATENCY_MIN_MS/
MAX_MS`, `SIDECAR_MAIL_CHAOS_SEED`. В back-end — `infrastructure/mail/http_mailer.rs` с тестами против
заглушки-сервера на axum; таймаут запроса выводится из `OUTBOX_HANDLER_TIMEOUT_SECS` минус секунда
(`Config::mail_request_timeout`), отдельной переменной нет.

### 3.5 C-1. Счётчики журнала: raw SQL по таблице библиотеки ●●●

**Сейчас.** Ради `total` в `rebuild.started` и `aggregates` в итоге back-end считает строки в таблице,
которой владеет `cqrsx`:

```rust
// back-end: infrastructure/postgres.rs
let events: i64 = sqlx::query_scalar("SELECT count(*) FROM events").fetch_one(&self.pool).await?;
let aggregates: i64 = sqlx::query_scalar("SELECT count(DISTINCT (aggregate_type, aggregate_id)) FROM events").fetch_one(&self.pool).await?;
```

Sidecar при этом каждые 400 мс считает `events_total` и `events_by_aggregate`, а `EncorePanel.vue` **уже**
берёт `total = rebuild.total || xray.eventsTotal`.

**Станет — вариант A (тикет 08).** `PostgresEventRepository::totals::<A>()` в библиотеке, `PostgresLedger`
сводится к двум вызовам. Прогресс остаётся точным с первого события.

**Станет — вариант B (убрать).** `EventLedgerStats`, `PostgresLedger` и `LedgerTotals` удаляются; число
агрегатов считается в наблюдателе replay (`ReplayProgress.aggregate_id` → `BTreeSet`), `rebuild.started`
не несёт `total`, прогресс-бар берёт знаменатель из X-ray (fallback уже есть).

**Зачем, на примере.** «Кто владеет таблицей, тот и считает в ней строки» (тикет 08): переименование или
партиционирование `events` в библиотеке сломает демо в рантайме при первом Encore!. Плюс в демо сейчас два
разных процесса делают `SELECT count(*) FROM events` для одного и того же числа на экране.

**Рекомендация.** A, если нужен точный знаменатель до первого события replay; иначе B — около 50 строк
и один порт меньше.

**Решение и реализация (2026-10-17).** Вариант A: владелец считает, что подсчёт событий и агрегатов пригодится
и другим проектам. В `cqrsx`: `LedgerTotals` и `PostgresEventRepository::totals::<A>()` (один запрос,
`count(*)` + `count(DISTINCT aggregate_id)` по `aggregate_type`), интеграционный тест
`tests/postgres/event_store/totals.rs`, строка в `components.md`. В demo `PostgresLedger` складывает итоги
`Concert` и `Fan`; порт `EventLedgerStats` остался.

### 3.6 C-2, C-3. Обобщённый код витрин → cqrsx (тикеты 07, 09)

Закрыто 2026-10-23 вместе с эпиком 01 бэклога: потребителя нет, код остаётся в demo. Вернуться, когда появится
проект с витринами в PostgreSQL (у `PostgresViewRepository` нет `list`/`clear`; хватит двух методов контракта,
`verify` можно оставить в приложении) или в MongoDB. Суть: `ProjectionMaintenance::verify` и `ReadModelStore::{list, wipe}`
не знают о концертах и повторятся в любом проекте (тикет 07, ●●○); `MongoViewRepository` — условно,
если следующие проекты тоже хранят витрины в MongoDB (тикет 09, ●○○). После S-1 у `ReadModelStore::wipe`
остаётся один потребитель — rebuild, что упрощает перенос в `ViewStore::clear`.

## 4. Упрощения демо в production-пути (не для sidecar)

Это не утечки в сторону sidecar — перенести их некуда. Но они нарушают критерий «похоже на production»,
поэтому перечислены отдельно. Оба пункта решены эпиком 03 (D-16 и D-18, 2026-10-24; реализованы 2026-10-25);
ниже — состояние на момент аудита.

### 4.1 K-1. Rebuild-lock ●○○

Пока идёт пересборка витрин, касса закрыта: команды UI получают `423 rebuild_in_progress`, `cancel_booking`
проверяет lock до чтения журнала, шаги цепочки ждут до 3 с (`wait_for_unlock`) и просят повтор через 2 с,
а `ENCORE_CHAIN_MAX_ATTEMPTS = 60` существует только потому, что задачи тратят попытки на ожидание lock.
Lock и его следствия встречаются в 12 файлах помимо `lock.rs` (`command_service`, `concert`, `book_seat`,
`cancel_booking`, `workflows`, `rebuild`, `execute_command`, `verify`, `wipe`, `bootstrap`, `config`, `testing`).

В production write side не зависит от read side — об этом говорит сам `lock.rs`: «the lock is only what
keeps a demo rebuild simple and deterministic». LWW-витрины делают «живую» пересборку корректной без lock
(PLAN.md §2.9, вариант P2).

| | С lock (сейчас) | Без lock («живой» rebuild) |
|---|---|---|
| Поведение кассы во время Encore! | `423`, кнопки заблокированы | команды идут, витрина догоняет |
| Детерминизм демо | полный | мерцание возможно, но LWW сходится |
| Что демонстрирует | `423` + повтор задач по `retry_after` | независимость write side от read side |
| Код в production-пути | lock + ожидание в 12 файлах, `ENCORE_CHAIN_MAX_ATTEMPTS` | нет |

**Вердикт: спорно.** Lock даёт детерминированную сцену и показывает честный `423` с повтором задач, но
это самый заметный «не production» паттерн в пути записи. Варианты: оставить и явно назвать в README
упрощением демо (сейчас это сказано только в doc-комментарии `lock.rs`), либо запланировать P2.
Решение владельца демо; к границе с sidecar не относится.

### 4.2 K-2. Замедленный replay —

`speed: slow` и `ENCORE_REPLAY_SLOW_DELAY_MS` — демо-театр («места загораются по одному»), но реализован он
через библиотечный `EventReplay::with_pace`, а темп пересборки — честная production-настройка (не
перегружать read DB). Оставить; при желании переименовать `speed: slow|fast` в `pace_ms` в контракте.

### 4.3 K-3. Неиспользуемые зависимости ●●●

`sha2` и `hex` объявлены в `back-end/Cargo.toml`, но в `src` и `tests` не используются (остаток плана
verify через хэш канонического JSON). Удалить.

## 5. Что остаётся в back-end и почему

| Компонент | Почему это production |
|---|---|
| `POST /api/concerts/{id}/commands`, `GET /api/concerts[/{id}]`, `GET /api/fans/{id}/wallet` | команды и запросы кассы |
| `GET /api/concerts/{id}/timeline`, `state-at/{sequence}` (`operations/time_travel`) | временной запрос по журналу — штатная возможность ES (аудит, разбор инцидентов); нужны `Concert::apply` и `EventStore`, т. е. домен |
| `POST /api/projections/rebuild`, `GET /api/projections/status` (`operations/rebuild`) | пересборка витрин из журнала — штатная эксплуатационная операция; нужны те же `View`/`ViewProjection`, что в live |
| `GET /api/projections/verify` (`operations/verify`) | проверка дрейфа витрин — эксплуатационная диагностика; обобщённая часть остаётся в demo (C-2 закрыт) |
| `GET /api/activity` (`ActivityHub`), `NotifyingHandler` | живой поток событий сервиса для UI/операторов; ничего не хранит |
| `application::{command_service, concert, fan, workflows, tasks}` | ядро: причинность, outbox-цепочки, маппинг исходов |
| `infrastructure::{postgres, mongo::{view_repository, read_side}, task_runtime}` | адаптеры production-хранилищ и wiring runtime |
| `bootstrap`, `config` (кроме перечисленного в S-3/S-4) | composition root и настройки сервиса |

Мелочь по пути: `time_travel::summary()` формирует строки с эмодзи («{} ✕ (refund {} €)») в слое application;
это текст для UI, место ему в presentation или в i18n фронта.

## 6. Обратная проверка: что в sidecar могло бы принадлежать back-end

Ничего переносить обратно не нужно. `seed` идёт через публичный API back-end — правильно. `reset` делает
`TRUNCATE` по таблицам библиотеки под отдельной ролью — осознанное решение демо. Единственная связка:
sidecar знает ключ метаданных `recorded_at` и опирается на его фиксированную ширину
(`ORDER BY metadata->>'recorded_at'`); для инспектора это приемлемая зависимость от контракта данных.

## 7. Границы до и после

| | Back-end до аудита | Back-end после (реализовано) | Sidecar после (реализовано) |
|---|---|---|---|
| Эндпоинты | 14: health, config, 5 × concerts, wallet, **inbox**, **wipe**, rebuild, status, verify, activity | 12: без `inbox` и `wipe`; `config` без персонажей и параметров почты | +`POST /inspect/projections/wipe`, +`GET /inspect/mail/{fan_id}`, `demo` с персонажами, +`POST /mail` (почтовый сервис) |
| Базы, к которым есть доступ | `encore` (rw), `encore_read` (rw), `mail_service` (rw) | `encore` (rw), `encore_read` (rw) | `encore` (ro + TRUNCATE), `encore_read` (ro + drop), `mail_service` (rw: пишет почтовый сервис, читает X-ray) |
| Переменные `ENCORE_*` | 16 | 12: без `TASK_FAILURE_RATE`, `TASK_LATENCY_MIN_MS/MAX_MS`, `CHAOS_SEED`, `MAIL_DB`; +`MAIL_SERVICE_URL` | `ENCORE_MAIL_DB` теперь только у sidecar; +`SIDECAR_MAIL_FAILURE_RATE`, `SIDECAR_MAIL_LATENCY_MIN_MS/MAX_MS`, `SIDECAR_MAIL_CHAOS_SEED` |
| Зависимости | `rand`, `sha2`, `hex`, `mongodb`, … | без `rand`, `sha2`, `hex`; +`reqwest` | +`rand` |
| Код с пометкой DEMO ONLY | `infrastructure/mail/*`, `config.task_failure_rate` | нет | `demo.rs`, `mail.rs`, `mail/*` |

## 8. Что сделано и что осталось

Сделано 2026-10-17 (коммиты `3c1b771` cqrsx, `790ec18` demo): **S-1, S-2, S-3, S-4, C-1, K-3**; документация — PLAN.md (§2.7, §2.9, §3.5,
§5.4, §6.2, §6.4, §7, D-13), README, `.env.example`, `compose.yaml`, бэклог (08 закрыт), PROGRESS.md.

C-2 и C-3 закрыты 2026-10-23 без переноса (§3.6). Осталось решить (каталог [`discussions/`](discussions/README.md)):
**K-1** (rebuild-lock), **K-2** (`speed` в контракте rebuild).
