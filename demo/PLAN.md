

# Encore 🎸 — демо-приложение для `cqrsx`

**План реализации для ИИ-агента.** Положите файл в репозиторий как `demo/PLAN.md` и поставьте агенту задачу:
«Реализуй демо по `demo/PLAN.md`. Начни с Фазы 0 (§10), соблюдай правила из §12».

Версия плана: 1.1 · 17.10.2026 · язык UI: RU + EN · идентификаторы, комментарии в коде, коммиты — на английском.

> **2026-10-31.** Это история плана: фазы выполнены. Документы, на которые он ссылается (`API_MAPPING.md`,
> `BOUNDARY_AUDIT.md`, `PHASE_0_5_REVIEW.md`, `PROGRESS.md`), удалены; они в git по тегу `history/epics-02-07`.
> Живая часть плана переедет в `docs/DESIGN.md`.

> **Версия 1.1 — после Фазы 0.** Реальный API `cqrsx` сверен в `demo/docs/API_MAPPING.md`; принятые решения
> и action items собраны в **§15** и уже внесены в разделы ниже. Главные изменения: два агрегата (`Concert` и
> `Fan`), связанные цепочкой outbox-задач; правило «принятая команда = ровно одно событие»; событие
> `ConcertSoldOut` снято; метаданные команд проходят через `execute_with_metadata`, который добавляется в `cqrsx`
> отдельной Фазой 0.5. При расхождении этого текста с `API_MAPPING.md` побеждает `API_MAPPING.md`.

---

## 0. TL;DR

**Encore** — онлайн-касса концертного зала. Пользователь выбирает персонажа (🦊 Fox, 🐼 Panda, 🦄 Unicorn,
🤖 Arbitrage Bot), кликает по местам неоновой схемы зала и видит, как каждое действие проходит через архитектуру:

```
клик → команда → правила агрегата Concert → событие + outbox-задача (одна транзакция PostgreSQL)
     → проекция «схема зала» в MongoDB → экран
     → воркер шлёт команду агрегату Fan → событие + outbox-задача (вторая транзакция)
     → проекция «кошелёк фаната» → экран
     → воркер «отправляет e-ticket» через капризный почтовый сервер (20 % отказов → retry)
```

Пять «ага-моментов» — по одному на каждую обязательную тему:

| # | Тема | Что делает пользователь | Что он видит |
|---|---|---|---|
| 1 | Event Store / Event Sourcing | бронирует место; двигает ползунок «⏳ машина времени» | в `events` дописывается строка; зал «откатывается» в прошлое, хотя «состояния» не хранит ни одна таблица |
| 2 | Commands и инварианты | кликает по занятому месту, запускает 🤖 перекупщика, жмёт «🧪 Нарушить правило» | красный тост с названием правила и `Δ events = 0` — в журнал не попало ничего |
| 3 | Transactional Outbox | бронирует место | строка события и строка задачи появляются одновременно; задача шлёт команду агрегату `Fan`, тот даёт событие и задачу «отправить e-ticket»; та проходит `queued → in progress → 🎲 retry → delivered`; в «почтовом ящике» появляется e-ticket |
| 4 | Projections | смотрит на MongoDB | один клик → два потока событий (`Concert`, `Fan`) → два документа: «схема зала» и «кошелёк фаната»; кошелёк отстаёт от зала на один шаг цепочки |
| 5 | Replay | 🧨 стирает проекции, жмёт ⏪ «Encore!» | зал гаснет, но правила продолжают работать; затем места «загораются» обратно одно за другим |

Название — каламбур: *encore* («на бис») — это и есть replay: сыграть всю историю ещё раз.

**Стек:** Rust (axum + `cqrsx`) · PostgreSQL 18 (event store + outbox) · MongoDB (проекции) ·
Vue 3 + Vite 8 + Tailwind 4 (UI) · sidecar на Rust (read-only инспектор обеих БД).

### 0.1 Покрытие исходных требований

| Требование | Где в плане |
|---|---|
| EventStore / EventSourcing | §2.3–2.6, §2.10 (машина времени), панели `events` и `snapshots` (§8.3) |
| Commands и инварианты | §2.4–2.6 (правила R1–R8), `RuleBreaker`, 🤖 перекупщик (§8.3) |
| Transactional Outbox | §2.7, §3.3, `OutboxPanel`, `MailboxPanel` |
| Projections | §2.8, `MongoPanel`, `SeatMap` и `WalletPanel` строятся из проекций |
| Replay: очистить проекции и воссоздать из event store | §2.9, §3.4, §6.8, `EncorePanel` |
| Код в `/demo`: `back-end`, `sidecar`, `front-end` | §4, §6, §7, §8 |
| PostgreSQL как event store | §5.2–5.3 (схемы `cqrsx`, отдельная БД `encore`) |
| MongoDB для проекций | §2.8, §6.7 |
| Последний Vue, яркие интерактивные элементы, состояние event store / outbox / проекций, пояснения | §8 (стек, макет, компоненты, анимации), Приложение A (тексты) |
| Outbox-обработчик с 20 % retry (только демо) | §2.7 (`FlakyMailServer`, `ChaosPolicy`) |
| Sidecar с API для визуализации таблиц | §7 |

---

## 1. Цели, аудитория, принципы

**Аудитория.**
1. «Обыватель» (менеджер, аналитик, начинающий разработчик) — понимает идеи по UI и пояснениям, не читая Rust.
2. Разработчик — видит эталонную интеграцию `cqrsx`: код демо должен быть образцом использования библиотеки.

**Принципы.**
- **Всё видно.** Каждое действие отражается в «рентгене» (X-ray): сырые строки PostgreSQL и документы MongoDB,
  рядом — человеческое пояснение и запрос, которым данные получены.
- **UI не проверяет бизнес-правила.** Клик по занятому месту всё равно отправляет команду — судья только агрегат.
  Так инварианты становятся наглядными.
- **Честность.** В рантайме никаких моков: настоящие PostgreSQL и MongoDB, настоящий task runtime `cqrsx`.
  Хаос (20 %) живёт только в демо-обработчике и явно помечен `// DEMO ONLY`.
- **Лаконичность.** Два агрегата (`Concert` — места, `Fan` — билеты зрителя), пять команд, пять событий,
  четыре outbox-задачи, две проекции. Все бизнес-правила проверяются синхронно в `Concert`; `Fan` идёт следом
  по цепочке outbox и хранит только идемпотентные правила.
- **Одна команда — одно событие.** Принятая команда даёт ровно одно событие (с ним атомарно пишутся её
  outbox-задачи); отклонённая не оставляет следа. Это контракт `PostgresEventRepositoryWithOutbox`, он закрепляется
  тестом (§6.9).
- **Изоляция.** Демо не меняет библиотеку `cqrsx` и не влияет на корневой CI (сборка `--locked`, покрытие 97 %).

**Приоритеты.** `[P0]` — обязательно (MVP, закрывает исходные требования); `[P1]` — сильно рекомендуется
(заметно повышает наглядность); `[P2]` — по желанию. Всё, что не помечено, — `[P0]`.

---

## 2. Предметная область «Encore»

### 2.1 Карта соответствия «концепция ↔ домен ↔ где видно»

| Концепция | В домене Encore | Где видно в UI |
|---|---|---|
| Aggregate | `Concert` — один концерт = один поток событий (места); `Fan` — журнал зрителя по одному концерту = один поток (его билеты на этот концерт, D-14) | правила при бронировании; кошелёк |
| Межагрегатная цепочка | `SeatBooked` → задача → `Fan.RecordTicket` → `TicketRecorded` → задача → письмо | ⚙ outbox → второй агрегат на PipelineStrip, общий `correlation_id` |
| Command | «Забронировать B4 для Fox» (`BookSeat`) | клик по месту |
| Invariant | место существует, свободно, ≤ 4 билетов в одни руки… | тост с ошибкой, `Δ events = 0` |
| Domain event | `SeatBooked`, `BookingCancelled`, … | строка в 🐘 `events` |
| Event store | «кассовая книга»: записи только дописываются | 🐘 `events` |
| Snapshot | «закладка» каждые 10 событий | 🐘 `snapshots` |
| Optimistic concurrency | два фаната жмут одно место одновременно | ⚡ «Гонка» `[P1]` |
| Outbox task | «записать билет фанату», «отправить e-ticket», «записать отмену», «вернуть деньги» | 🐘 outbox-таблица |
| TaskHandler / worker | почтальон, чей сервер капризничает (20 %) | статусы, попытки, backoff, 📬 inbox |
| Projection / View | «схема зала» и «кошелёк фаната» | 🍃 `encore_read.*` → экран |
| Replay | «Encore!» — проиграть историю заново | 🧨 / ⏪ кнопки, прогресс-бар |
| Temporal query | «машина времени» | ползунок ⏳ |

### 2.2 Справочные данные (константы в коде, не события)

**Зал** — один для всех концертов: ряды `A`–`E`, по 8 мест, вместимость 40.

| Зона | Ряды | Цена | Цвет |
|---|---|---|---|
| `vip` | A | 120 € | золото `#fbbf24` |
| `standard` | B, C | 80 € | циан `#22d3ee` |
| `balcony` | D, E | 50 € | фиолетовый `#a78bfa` |

Идентификатор места соответствует `^[A-E][1-8]$`, например `B4`.

**Персонажи.**

| `fan_id` | Имя | Аватар | Роль в демо |
|---|---|---|---|
| `fox` | Fox | 🦊 | основной персонаж |
| `panda` | Panda | 🐼 | соперник в «гонке» |
| `unicorn` | Unicorn | 🦄 | ещё один фанат |
| `arbitrage-bot` | Arbitrage Bot | 🤖 | пытается скупить всё и упирается в лимит |
| `crowd-<n>` | случайное имя из списка | эмодзи по хэшу `fan_id` | «🌊 Толпа» |

Аватар для `crowd-*` фронт вычисляет детерминированно (хэш `fan_id` → индекс в списке эмодзи).

**Концерты по умолчанию** — создаются **sidecar-ом** при его старте **обычными командами** `AnnounceConcert`
через публичный API back-end (`POST /api/concerts/{id}/commands`, с повторами, пока back-end не поднялся);
back-end не содержит ни демо-данных, ни демо-переключателей — он образец production-сервиса:

| `concert_id` | Артист | Город | Дата |
|---|---|---|---|
| `radiohead-2026-11-21` | Radiohead | Minsk | 2026-11-21 |
| `rammstein-2026-12-05` | Rammstein | Tokyo | 2026-12-05 |
| `muse-2027-01-16` | Muse | Tbilisi | 2027-01-16 |

Seed идемпотентен «по-бизнесовому»: повторный `AnnounceConcert` отклоняется правилом R2 (`already_announced`),
и sidecar просто считает это успехом — не нужен ни флаг, ни отдельный SQL. Там же живёт **reset демо**
(`POST /inspect/reset`, кнопка «⟲ Сбросить демо» в панели Encore): роль `encore_demo_admin` с правом только
`TRUNCATE` на `events`, `snapshots`, `transactional_outbox`, drop коллекций `concert_halls`, `fan_wallets`,
`mail_service.inbox`, затем тот же seed. Список концертов по умолчанию отдаёт `GET /inspect/demo`.

### 2.3 Агрегат `Concert`

`TYPE = "Concert"`, `aggregate_id = concert_id`.

```rust
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Concert {
    info: Option<ConcertInfo>,            // None ⇒ концерт не анонсирован
    bookings: BTreeMap<SeatId, Booking>,  // только занятые места
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConcertInfo { artist: String, city: String, date: NaiveDate }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Booking { fan_id: FanId, fan_name: String, booking_id: Uuid, price: u32 }

// производные методы: is_sold_out(), tickets_of(&FanId), revenue(), free_seats()
```

`SeatId` и `FanId` — newtype с валидацией (`SeatId` сериализуется строкой: он ключ `BTreeMap` и попадает в
снапшот). Справочник зала — `static VENUE` (модуль `domain::venue`).

**Агрегат `Fan`** — `TYPE = "Fan"`, `aggregate_id = <fan_id>:<concert_id>` (журнал зрителя по одному концерту,
D-14). Downstream-агрегат: получает команды только от outbox-задач, никогда от UI. Хранит билеты зрителя на этот
концерт; состояние не превышает `ticket_limit` билетов, поэтому ни снапшот, ни документ кошелька не растут с
историей зрителя.

```rust
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Fan {
    name: Option<String>,                 // из первого RecordTicket
    tickets: BTreeMap<Uuid, Ticket>,      // booking_id -> билет (активный или отменённый)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ticket { concert_id: String, seat: SeatId, zone: Zone, price: u32, state: TicketState } // Active | Cancelled

// производные методы: active_count(), total_spent()
```

`Concert` — источник истины по местам и всем бизнес-правилам R1–R8. `Fan` — источник истины по тому, что зритель
получил; его правила (§2.5, R9–R11) только защищают от повторной и переставленной доставки задач.

### 2.4 Команды → события → outbox-задачи

| Агрегат | Команда (JSON `type` для UI / внутренняя) | Поля | Событие при успехе (ровно одно) | Outbox-задачи (в той же транзакции) |
|---|---|---|---|---|
| `Concert` | `AnnounceConcert` (UI) | `artist`, `city`, `date` | `ConcertAnnounced { artist, city, date, capacity }` | — |
| `Concert` | `BookSeat` (UI) | `seat`, `fan_id`, `fan_name`; `booking_id` генерирует back-end | `SeatBooked { seat, zone, price, fan_id, fan_name, booking_id }` | `encore_record_ticket` v1 |
| `Concert` | `CancelBooking` (UI) | `seat`, `fan_id`; `cancellation_id` генерирует back-end | `BookingCancelled { seat, fan_id, booking_id, cancellation_id, refund }` | `encore_record_cancellation` v1 |
| `Fan` | `RecordTicket` (внутренняя, из задачи) | `booking_id`, `concert_id`, `seat`, `zone`, `price`, `fan_name` | `TicketRecorded { booking_id, concert_id, seat, zone, price, fan_name }` | `encore_send_e_ticket` v1 |
| `Fan` | `RecordCancellation` (внутренняя, из задачи) | `booking_id`, `cancellation_id`, `refund` | `TicketCancelled { booking_id, cancellation_id, concert_id, seat, refund }` | `encore_issue_refund` v1 |

Цепочка бронирования: `BookSeat` → `SeatBooked` + задача → `Fan.RecordTicket` → `TicketRecorded` + задача →
письмо «🎫 e-ticket». Цепочка отмены зеркальна и заканчивается письмом «💸 Refund». Каждый шаг — отдельная
атомарная транзакция «событие + его задачи».

Заметки:
- Событие хранит **факт** (`price`, который заплатили), а не формулу. Сумма возврата `refund` берётся из
  состояния `Concert` — «сколько заплатили тогда» — в решение агрегата (`BookingCancelled`) и из него
  передаётся дальше по цепочке в payload задачи.
- **Задачи планируют реакции агрегата** (D-26). `Decide` возвращает событие или отказ; `Reactions` чисто
  выбирает следующие задачи из события. Движок вызывает реакцию после решения каждой попытки и атомарно
  записывает событие и задачи. Бизнес-цепочка читается по типам задач в `concert/application` и `fan/application`.
- **Ровно одно событие на принятую команду** (D-02) — это контракт `Decide`. Отказ не пишет событие и задачи.
- `ConcertSoldOut` как событие **снято** (D-03): `sold_out` — производное поле витрины и агрегата
  (`free_seats() == 0`); SOLD OUT в UI срабатывает по переходу `sold_out: false → true` в витрине (§8.8).
- `event_type` = имя варианта enum; `event_version` = `"1.0"` (`NumericVersion` принимает `1`, `1.0`, `1.0.0`;
  при чтении требуется точное строковое равенство — U14).
- Метаданные каждого события — `HashMap<String, String>`: `command_id`, `correlation_id`, `causation_id`,
  `recorded_at` (RFC 3339 фиксированной ширины: `to_rfc3339_opts(SecondsFormat::Micros, true)`), `source`.
  Для UI-команд `correlation_id = command_id`, `causation_id = command_id`, `source = "ui"`. Для внутренних
  команд `correlation_id` наследуется из метаданных задачи, `causation_id = task_id`, `source = "task:<формат>"`.
  Метаданные создаёт движок: `cqrs.source("ui")` для входа и `TaskContext` для внутренних команд.
  Ключи и правило наследования задаёт `cqrsx::cqrs::command::causation::Causation` (`root`, `from_task`,
  `event_metadata`, `stamp_task`); `recorded_at` добавляет `CommandMetadata` в cqrsx.

### 2.5 Инварианты (бизнес-правила)

Проверяются в `Decide<Concert>` соответствующих команд в указанном порядке. Ошибка домена — enum `ConcertError` → в API `{ code, params }`;
тексты сообщений живут в i18n фронта (EN/RU, Приложение A.4).

| # | Код | Команды | Правило | Текст (RU) |
|---|---|---|---|---|
| R1 | `concert_not_found` | BookSeat, CancelBooking | концерт должен быть анонсирован | «Такого концерта нет — его ещё не анонсировали» |
| R2 | `already_announced` | AnnounceConcert | анонсировать можно один раз | «Этот концерт уже анонсирован» |
| R3 | `unknown_seat` | BookSeat, CancelBooking | место существует в зале | «Места Z9 нет: ряды A–E, места 1–8» |
| R4 | `sold_out` | BookSeat | есть свободные места | «Sold out! Билеты закончились» |
| R5 | `seat_taken` | BookSeat | место свободно | «Место B4 уже занято (🦊 Fox)» |
| R6 | `ticket_limit` | BookSeat | у фаната меньше 4 билетов на этот концерт | «Правило против перекупщиков: не больше 4 билетов в одни руки» |
| R7 | `seat_not_booked` | CancelBooking | место забронировано | «Это место никто не бронировал» |
| R8 | `not_your_ticket` | CancelBooking | отменяет только владелец | «Это не ваш билет — отменить может только владелец» |

Лимит (4) передаётся через `Services` агрегата (`ENCORE_TICKET_LIMIT`): правила могут зависеть от конфигурации,
а история — нет (replay не вызывает `handle`, только `apply`).

Правила агрегата `Fan` — технические, они защищают от повторной (at-least-once) и переставленной доставки задач.
Обработчик задачи трактует R9 и R11 как «уже сделано», а R10 — как «ещё рано, повторить» (§2.7):

| # | Код | Команда | Правило | Реакция обработчика задачи |
|---|---|---|---|---|
| R9 | `already_recorded` | RecordTicket | `booking_id` ещё не записан | задача выполнена (повторная доставка) |
| R10 | `unknown_ticket` | RecordCancellation | билет с таким `booking_id` есть | `Retry` через 1 с: задача отмены обогнала задачу записи |
| R11 | `already_cancelled` | RecordCancellation | билет ещё активен | задача выполнена (повторная доставка) |

Эти коды в UI не показываются как тосты (команды внутренние), но попадают в activity с `source: "task:…"`.

### 2.6 Агрегат и команды

Актуальный код — `back-end/src/concert/domain/aggregate.rs`, `domain/commands/` и `domain/events/`.
`Aggregate::apply` применяет событие к состоянию; каждый тип команды реализует `Decide<Concert>` и
возвращает одно событие или доменный отказ. `type Rules = ConcertRules` задаёт лимит билетов.
Сквозной компилируемый пример — [быстрый старт cqrsx](../cqrsx/README.md#quick-start).

### 2.7 Outbox: задачи и обработчики

| Событие | Задача | Обработчик и эффект |
|---|---|---|
| `SeatBooked` | `encore_record_ticket` v1 | `record_ticket`: команда `Fan.RecordTicket` |
| `TicketRecorded` | `encore_send_e_ticket` v1 | `send_e_ticket`: письмо через `Mailer` |
| `BookingCancelled` | `encore_record_cancellation` v1 | `record_cancellation`: команда `Fan.RecordCancellation` |
| `TicketCancelled` | `encore_issue_refund` v1 | `issue_refund`: письмо о возврате |

`Concert` и `Fan` реализуют `Reactions`: задача строится из события и записывается с ним одним оператором.
В outbox хранятся формат, payload и метаданные причинности. `concert_id` и `fan_id` содержатся в payload;
дополнительные одноимённые метки метаданных после эпика 05 не пишутся. Идентификатор задачи выводит ядро
из причинности и формата; повторная доставка использует тот же ID.

Обработчики — функции с `#[task_handler]`, общие для воркеров. Зависимости выдаются по типу, следующая команда
выполняется через `ctx.execute`. Повторная запись или отмена уже обработанного билета — done; `UnknownTicket`
при обогнавшей билет отмене — retry; конфликт и недоступность — retry, прочие доменные отказы — fail.
Политика `STEP` задаёт паузу 1 с; `MAIL` — 1, 2, 4, 8 с с ограничением попыток `ENCORE_TASK_MAX_ATTEMPTS`.
Её привязывает корень `fan.rs` к типу обработчика; обработчик сообщает только done/retry/fail.

Почтовый провайдер — sidecar `POST /mail`, клиент — `shared/infrastructure/http_mailer.rs`. Повтор
безопасен: ключ письма строится из `booking_id` или `cancellation_id`. Демонстрационные настройки рантайма:
2 воркера, буфер 2, poll 300 мс, таймаут обработчика 5 с, lease 30 с, storage timeout 3 с.

### 2.8 Проекции (MongoDB, база `encore_read`)

Обе проекции строятся **только** из событий и обновляются **процессорами проекций** (эпик 03): процессор идёт по
журналу по своей закладке (`projection_checkpoints` в MongoDB рядом с витринами), просыпается от каждого коммита
(`CommitWakeup`) и читает новые события страницами — ничто не теряется при сбое, повтор вместо потери. Ответ
команды — «принято» сразу после коммита (D-17); витрина догоняет мгновением позже. Обе — обычные `ViewProjection` (D-06): `concert_halls` над потоком
`Concert` (`view_id = concert_id`), `fan_wallets` над потоком `Fan` (`view_id = <fan_id>:<concert_id>`, D-14). Собственный обработчик с
группировкой по `fan_id` не нужен: `ViewProjection` передаёт в `View::apply` события одного агрегата, а
`view_id` всегда равен `aggregate_id`. В UI кошелёк отстаёт от зала на один шаг цепочки — это показывается, а не
прячется.

**Правило обновления (обязательно).** Движок применяет события агрегата к документу строго по порядку:
документ хранит `last_sequence`, повтор пропускается, а дыра даёт `SequenceGap` без записи (D-15). Свёртки
не содержат дополнительной проверки LWW (эпик 05/10). Записи мест и билетов сохраняют `sequence` и время;
отмена оставляет состояние `free` или `cancelled`. Производные поля пересчитываются из карты.

**1. `concert_halls`** — `ViewProjection` + `MongoViewRepository<ConcertHallView>`, `_id = concert_id`:

```json
{
  "_id": "radiohead-2026-11-21",
  "version": 14,
  "last_sequence": 14,
  "view": {
    "concert": { "artist": "Radiohead", "city": "Tbilisi", "date": "2026-11-21" },
    "capacity": 40,
    "seats": {
      "B4": { "state": "booked", "fan_id": "fox", "fan_name": "Fox", "zone": "standard", "price": 80,
              "booking_id": "0192f0c4-…", "sequence": 7, "changed_at": "2026-10-17T09:12:03Z" },
      "A1": { "state": "free", "sequence": 9, "changed_at": "2026-10-17T09:13:40Z" }
    },
    "sold": 12, "revenue": 960, "sold_out": false,
    "last_sequence": 14
  }
}
```

- `sold_out = (sold == capacity)` — производное поле (событие `ConcertSoldOut` снято, D-03).
- `version` — версия документа для оптимистической блокировки самого view (`ViewMeta`); не путать с `sequence`.
- `last_sequence` конверта — последнее применённое событие агрегата (`ViewMeta::last_sequence`, эпик 03/01):
  документ принимает только следующее, повтор пропускает, дыру отвергает; документ без истории не создаётся
  (D-15). `last_sequence` внутри `view` — бегущий максимум витрины для UI.
- `changed_at` берётся из метаданных события (`recorded_at`), а не из `now()`: пересборка даёт тот же документ.

**2. `fan_wallets`** — `ViewProjection` + `MongoViewRepository<FanWalletView>` над агрегатом `Fan`,
`_id = <fan_id>:<concert_id>` — один документ на журнал (D-14). Строится из `TicketRecorded` / `TicketCancelled`
(U4 закрыт решением D-06, собственный обработчик не нужен). Кошелёк зрителя в API собирается из всех его документов
одним диапазоном `_id` `[fox:, fox;)` (`application/fan/wallet.rs`).

```json
{
  "_id": "fox:radiohead-2026-11-21",
  "version": 5,
  "view": {
    "fan_id": "fox", "fan_name": "Fox",
    "tickets": {
      "0192f0c4-…": { "concert_id": "radiohead-2026-11-21", "seat": "B4", "price": 80,
                      "state": "active", "sequence": 7, "changed_at": "2026-10-17T09:12:03Z" }
    },
    "active_count": 1, "total_spent": 80
  }
}
```

Ключ билета — `booking_id`; `sequence` — номер события в потоке журнала. Кошелёк в API собран из нескольких журналов, их `sequence` несравнимы — UI сортирует билеты по
`changed_at`. `changed_at` — `recorded_at` события `Fan`.

**Не проекция: `mail_service.inbox`** — «внешний почтовый сервис» (симуляция) в отдельной базе. Пишется только
outbox-обработчиками; wipe и replay его **не трогают**. Ключевой урок: «пересборка витрин не рассылает письма заново».

```json
{ "_id": "e_ticket:0192f0c4-…", "kind": "e_ticket", "to_fan_id": "fox", "to_name": "Fox",
  "subject": "🎫 Your e-ticket: radiohead-2026-11-21, seat B4", "concert_id": "radiohead-2026-11-21", "seat": "B4",
  "task_id": "…", "delivered_on_attempt": 2, "deliveries": 1, "first_delivered_at": "2026-10-17T09:12:07Z" }
```

### 2.9 Replay — «Encore!»

- **🧨 Стереть витрины** (sidecar, `POST /inspect/projections/wipe`; у back-end такого эндпоинта нет, удаление
  данных без восстановления — демо-операция): drop коллекций `concert_halls` и `fan_wallets`, затем их закладок
  `projection_checkpoints` (inbox не трогаем; в обратном порядке пересборка, прошедшая между сносами, потеряла бы
  документы). Write-side продолжает работать: бронирование занятого места по-прежнему отклоняется, потому что
  агрегат восстанавливается из PostgreSQL, а не из MongoDB. Процессоры регистрируют закладки заново и перечитывают
  журнал с начала: все залы возвращаются сами, Encore! для этого не нужен (D-15: документ строится только с начала
  истории). Документ, удалённый поодиночке, чинит первая бронь по нему (самолечение процессора).
- **⏪ Encore!** (`POST /api/projections/rebuild`, тело `{ "pace_ms": 80 }` или `{}`; `202`, если принята хотя бы
  одна строка закладки, `409 rebuild_in_progress`, если обе пересборки уже идут, `400`, если `pace_ms` больше
  `ENCORE_REPLAY_MAX_PACE_MS`, `503` при отказе хранилища закладок): `RebuildService` просит обе закладки
  (`request_rebuild`), владелец каждой на любой ноде идёт по ключам журнала и **заменяет документы на месте**
  свёрткой истории агрегата через те же рабочие потоки ключей, что и живые события. Витрины не стираются, касса
  открыта (D-16): команды принимаются, а бронь во время пересборки видна сразу или в конце, когда пересборка дойдёт
  до её зала. `pace_ms` — пауза между событиями одного ключа (D-18; UI шлёт `ENCORE_REPLAY_SLOW_DELAY_MS = 80` в
  медленном режиме или 0). Наблюдатель сервиса раз в 250 мс читает строки закладок: `rebuild.started { rebuild_id,
  pace_ms, keys_total, events_total }`, `rebuild.progress { keys }`, `rebuild.finished { keys, duration_ms }`;
  каждый заменённый документ даёт `projection.rebuilt { projection, view_id, events, reason }`.
- **⟲ Один зал** (`POST /api/concerts/{id}/rebuild`, `202`): владелец закладки залов заменяет один документ
  (`request_rebuild_key`), остальное не трогается.
- **Статус** (`GET /api/projections/status`): по каждой проекции закладка, отставание в событиях (`count_after` с
  потолком `ENCORE_PROJECTION_LAG_CAP`), владелец, сбои и состояние `stalled` > `unclaimed` > `starting` >
  `rebuilding` > `live`; при недоступном хранилище закладок — последние известные строки с `stale: true`.
- **🔍 Проверить витрину** `[P1]` (`GET /api/projections/verify`, разрешён всегда, и во время Encore!): каждый документ
  сравнивается со свёрткой **применённой им части** истории (`last_sequence`, D-20). Отстающий или отсутствующий
  документ — не ошибка, он считается в `behind { documents, events }`; расхождением (`differs`, `unexpected`)
  становится только неверный. Ответ `{ ok, checked, behind, differences }`: «✅ N верны, M отстают на K событий» или
  «❌ N расхождений». Документы читаются раньше истории, поэтому коммит между чтениями даёт `behind`, а не ложное
  расхождение.

### 2.10 Машина времени

`GET /api/concerts/{id}/state-at/{sequence}`: back-end читает поток событий одного концерта
(`JournalEventStore::<PostgresEventRepository, Concert>::new(repo).load_events(id)` →
`Vec<EventEnvelope<Concert>>`; снапшоты **не** используются), применяет `Concert::apply` к пустому агрегату до
нужного `sequence` и возвращает состояние агрегата и само событие. В UI — ползунок ⏳ под схемой зала: двигаем — зал перерисовывается
«в прошлом» (сепия, баннер «Это состояние нигде не хранится — оно вычислено из событий 1…7»).

---

## 3. Архитектура

### 3.1 Компоненты

UI обращается к HTTP-маршрутам `concert`, `fan` и `operations`. `Cqrs` строит исполнители агрегатов,
процессоры витрин и общий диспетчер задач. PostgreSQL хранит журнал, снапшоты и outbox; MongoDB — витрины
и закладки. Sidecar независимо читает обе базы и предоставляет демонстрационный почтовый сервис.

### 3.2 Поток команды `BookSeat`

Маршрут передаёт команду через `cqrs.source("ui")`; движок загружает `Concert`, вызывает `Decide`, затем его
`Reactions` и атомарно пишет `SeatBooked` с `encore_record_ticket`. Воркер выполняет задачу, командует `Fan`
через `TaskContext` и записывает `TicketRecorded` с `encore_send_e_ticket`. Процессоры асинхронно обновляют
зал и кошелёк; письмо отправляется отдельным обработчиком. Отмена зеркальна: `BookingCancelled` →
`record_cancellation` → `TicketCancelled` → `issue_refund`.

### 3.3 Жизненный цикл outbox-задачи

Claim под арендой → общий обработчик → fenced settlement done, retry с паузой политики или fail. Просроченная
аренда восстанавливается; повторные внешние эффекты должны быть идемпотентны.

### 3.4 Поток replay

Процессор владеет закладкой и перестраивает документы по журналу; доставка повторов пропускается по
`last_sequence`, разрыв останавливает запись. Verify сравнивает документ со свёрткой его применённого префикса.

### 3.5 Ключевые решения

Команда решает одно событие, реакции планируют задачи, каждое внешнее действие идемпотентно. Один модуль
объявляет агрегат, обработчики, политики и витрины; движок собирает инфраструктуру.

## 4. Структура каталога `/demo`

```text
back-end/src/
  main.rs, lib.rs            сборка Cqrs и DEMO_DEFAULTS
  concert.rs, concert/       domain/, application/, presentation/
  fan.rs, fan/               domain/, application/ (задачи и витрины), presentation/
  operations.rs, operations/ health, activity, rebuild, verify
  shared/                    общие типы, порты, инфраструктура
sidecar/src/                 X-ray, demo lifecycle, mail service
front-end/src/               интерфейс Vue
load/                        изолированный стенд k6
```

Границы слоёв проверяются `back-end/tests/architecture.rs`; он же не даёт файлу или каталогу повторить имя,
которое уже есть в пути: состояние агрегата — `concert/domain/aggregate.rs`, а не `concert/domain/concert.rs`.

## 5. Инфраструктура

### 5.1 Изоляция cargo-workspace

- `demo/Cargo.toml` — **виртуальный манифест своего workspace** (`[workspace] resolver = "3"`,
  `members = ["back-end", "sidecar"]`, `[workspace.package] edition = "2024"`, `publish = false`,
  `[workspace.dependencies]`). Cargo, поднимаясь от `demo/back-end`, первым найдёт `demo/Cargo.toml` и остановится.
- В корневой `Cargo.toml` добавить `exclude = ["demo"]` в секцию `[workspace]` (защита от glob-members и явное
  намерение). Это одна из двух разрешённых правок вне `demo/`; вторая — при необходимости корневой `.dockerignore`
  (§5.2).
- Зависимость на библиотеку: `cqrsx = { path = "../cqrsx", features = ["host", "logging"] }` в
  `[workspace.dependencies]` (путь относительно `demo/`). Без `host` нет `cqrsx::host`, без `logging` нет
  `cqrsx::logging`. `cqrsx/Cargo.toml` не использует `workspace = true`, наследовать нечего.
- Корневой `rust-toolchain.toml` (1.98.1) действует и для `demo/` — rustup ищет его вверх по дереву.
- `demo/Cargo.lock` коммитится; корневой CI (`cargo build --locked --workspace --all-features`, `cargo llvm-cov …
  --fail-under-lines 97`) демо не видит.
- `sqlx` в демо — **той же версии, что в `cqrsx`**: `0.8` (в `Cargo.lock` библиотеки 0.8.6), иначе типы пула не
  совпадут. Без compile-time макросов (`query!`) — только runtime-запросы, чтобы не требовать `DATABASE_URL` при
  сборке.

### 5.2 `demo/compose.yaml` (набросок)

```yaml
name: encore
services:
  postgres:
    image: docker.io/library/postgres:18.6-alpine   # ТОТ ЖЕ образ, что в корневом compose (U9)
    environment: { POSTGRES_USER: cqrsx, POSTGRES_PASSWORD: cqrsx, POSTGRES_DB: encore }
    ports: ["127.0.0.1:${ENCORE_POSTGRES_PORT:-5434}:5432"]   # 5433 занят корневым compose
    volumes:
      - encore-pg:/var/lib/postgresql                # путь тома как в корневом compose (PG 18+, U9)
      - ./infra/postgres/00-inspector-role.sql:/docker-entrypoint-initdb.d/00-inspector-role.sql:ro,z
      - ../cqrsx/sql/001-event-store.sql:/docker-entrypoint-initdb.d/10-event-store.sql:ro,z          # U9
      - ../cqrsx/sql/002-transactional-outbox.sql:/docker-entrypoint-initdb.d/20-outbox.sql:ro,z     # U9
      - ./infra/postgres/30-grants.sql:/docker-entrypoint-initdb.d/30-grants.sql:ro,z
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U cqrsx -d encore"]
      interval: 2s
      retries: 30
  mongo:
    image: docker.io/library/mongo:8.0               # можно 9.0; драйвер 3.9 требует сервер ≥ 4.4
    ports: ["127.0.0.1:${ENCORE_MONGO_PORT:-27018}:27017"]
    volumes: ["encore-mongo:/data/db"]
    healthcheck:
      test: ["CMD", "mongosh", "--quiet", "--eval", "db.adminCommand('ping').ok"]
      interval: 2s
      retries: 30
  # [P1] профиль "app": полный стек одной командой (env с адресами сервисов — см. ниже)
  backend:  { profiles: ["app"], build: { context: .., dockerfile: demo/back-end/Dockerfile }, ports: ["127.0.0.1:8080:8080"] }
  sidecar:  { profiles: ["app"], build: { context: .., dockerfile: demo/sidecar/Dockerfile },  ports: ["127.0.0.1:8090:8090"] }
  frontend: { profiles: ["app"], build: { context: ./front-end }, ports: ["127.0.0.1:8000:80"] }
volumes:
  encore-pg: {}
  encore-mongo: {}
```

- **Схемы event store и outbox не копировать** — монтировать те же SQL-файлы `cqrsx/sql/001-event-store.sql` и
  `cqrsx/sql/002-transactional-outbox.sql`, что использует корневой compose (единый источник правды; константы
  `EVENT_STORE_SCHEMA` / `TRANSACTIONAL_OUTBOX_SCHEMA` — `include_str!` тех же файлов). Обе схемы живут в `public`
  и идемпотентны (`CREATE TABLE IF NOT EXISTS`).
- Флаги SELinux: для двух SQL-файлов, общих с корневым compose, — строчная `z` (shared); `Z` перемаркировала бы
  их эксклюзивно. Для собственных файлов демо — тоже `z`.
- Профиль `app`: `depends_on` с `condition: service_healthy`; внутри сети compose — адреса сервисов, а не
  `localhost`: `ENCORE_HTTP_ADDR=0.0.0.0:8080`, `ENCORE_DATABASE_URL=postgresql://cqrsx:cqrsx@postgres:5432/encore`,
  `ENCORE_MONGODB_URI=mongodb://mongo:27017`, `SIDECAR_HTTP_ADDR=0.0.0.0:8090`,
  `SIDECAR_DATABASE_URL=postgresql://encore_inspector:inspector@postgres:5432/encore`,
  `SIDECAR_MONGODB_URI=mongodb://mongo:27017`; nginx фронта проксирует на `backend:8080` и `sidecar:8090`.
- Контекст сборки образов — корень репозитория (back-end нужен `cqrsx/`; workspace демо целиком тоже). В
  Dockerfile — явные `COPY` только нужных путей. Если контекст раздут каталогами `target/`, разрешено добавить в
  корень `.dockerignore` с `**/target` и `**/node_modules` (вторая и последняя допустимая правка вне `demo/`).

### 5.3 Роль только для чтения (sidecar)

`infra/postgres/00-inspector-role.sql`:

```sql
CREATE ROLE encore_inspector LOGIN PASSWORD 'inspector';
ALTER ROLE encore_inspector SET default_transaction_read_only = on;   -- вторая линия защиты
GRANT CONNECT ON DATABASE encore TO encore_inspector;
```

`infra/postgres/30-grants.sql` (после схем):

```sql
GRANT USAGE ON SCHEMA public TO encore_inspector;                -- если схемы cqrsx создают свою schema — и на неё
GRANT SELECT ON ALL TABLES IN SCHEMA public TO encore_inspector;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO encore_inspector;
```

MongoDB для демо — без аутентификации; порты PostgreSQL и MongoDB публикуются только на `127.0.0.1`.

### 5.4 Порты и переменные окружения

| Сервис | Порт (host) | Переопределение |
|---|---|---|
| PostgreSQL | 5434 | `ENCORE_POSTGRES_PORT` |
| MongoDB | 27018 | `ENCORE_MONGO_PORT` |
| back-end | 8080 | `ENCORE_HTTP_ADDR` |
| sidecar | 8090 | `SIDECAR_HTTP_ADDR` |
| front-end (dev, Vite) | 5173 | `vite.config.ts` |
| front-end (профиль app, nginx) | 8000 | compose |

| Переменная | Кто читает | По умолчанию | Назначение |
|---|---|---|---|
| `ENCORE_DATABASE_URL` | back-end | `postgresql://cqrsx:cqrsx@localhost:5434/encore` | event store + outbox |
| `ENCORE_MONGODB_URI` | back-end | `mongodb://localhost:27018` | проекции |
| `ENCORE_READ_DB` | back-end, sidecar | `encore_read` | база витрин |
| `ENCORE_MAIL_DB` | sidecar | `mail_service` | база почтового сервиса (пишет его имитация в sidecar, показывает X-ray; back-end к ней не подключается) |
| `ENCORE_MAIL_SERVICE_URL` | back-end | `http://127.0.0.1:8090` | внешний почтовый сервис (`POST /mail`); в демо его хостит sidecar |
| `ENCORE_HTTP_ADDR` | back-end | `127.0.0.1:8080` | |
| `SIDECAR_MAIL_FAILURE_RATE` | sidecar | `0.2` | DEMO ONLY: шанс, что почтовый сервис ответит `503` и задача уйдёт в retry (до 2026-10-17 — `ENCORE_TASK_FAILURE_RATE` back-end) |
| `SIDECAR_MAIL_LATENCY_MIN_MS` / `_MAX_MS` | sidecar | `600` / `1800` | имитация сети почтового сервиса; max должен быть меньше таймаута запроса back-end (H − 1 с) |
| `SIDECAR_MAIL_CHAOS_SEED` | sidecar | пусто | детерминированный кубик (тесты, отладка) |
| `ENCORE_TASK_MAX_ATTEMPTS` | back-end | `6` | claim-attempt limit задачи (`NewTask::with_max_attempts`) |
| `ENCORE_TASK_RETRY_BASE_MS` / `ENCORE_TASK_RETRY_MAX_MS` | back-end | `1000` / `8000` | backoff почтовых задач через `retry_after`: 1, 2, 4, 8, 8 с |
| `ENCORE_TICKET_LIMIT` | back-end | `4` | правило R6 |
| `ENCORE_SNAPSHOT_EVERY` | back-end | `10` | снапшот каждые N событий (U12) |
| `ENCORE_REPLAY_SLOW_DELAY_MS` | back-end | `80` | slow-motion replay |
| `SIDECAR_SEED` | sidecar | `true` | анонс трёх концертов по умолчанию при старте (через API back-end) |
| `SIDECAR_RESET_ENABLED` | sidecar | `true` | `POST /inspect/reset` (кнопка ⟲ в UI) |
| `SIDECAR_BACKEND_URL` | sidecar | `http://127.0.0.1:8080` | куда слать `AnnounceConcert` |
| `SIDECAR_ADMIN_DATABASE_URL` | sidecar | `postgresql://encore_demo_admin:…` | роль только для `TRUNCATE` трёх таблиц |
| `SIDECAR_DATABASE_URL` | sidecar | `postgresql://encore_inspector:inspector@localhost:5434/encore` | только чтение |
| `SIDECAR_MONGODB_URI` | sidecar | `mongodb://localhost:27018` | |
| `SIDECAR_HTTP_ADDR` | sidecar | `127.0.0.1:8090` | |
| `SIDECAR_POLL_MS` | sidecar | `400` | период опроса БД |
| `RUST_LOG` | оба | `info` | **один уровень** (`off/error/warn/info/debug/trace`): `cqrsx::logging::LoggingConfig::from_env` директивы вида `info,crate=debug` отвергает; sidecar читает так же |
| `OUTBOX_WORKER_CONCURRENCY` (C) | back-end | `2` (библиотека: 128) | воркеры task runtime |
| `OUTBOX_QUEUE_CAPACITY` (Q) | back-end | `2` (128) | локальный буфер claim-ов (`buffer_capacity`) |
| `OUTBOX_QUEUE_REFILL_PERCENT` | back-end | `25` (25) | claim, когда в буфере меньше этой доли Q; тогда буфер заполняется (`buffer_refill_percent`) |
| `OUTBOX_POLL_INTERVAL_MS` | back-end | `300` (1000) | период опроса outbox и recovery просроченных lease |
| `OUTBOX_HANDLER_TIMEOUT_SECS` (H) | back-end | `5` (15) | запрос к почтовому сервису сдаётся на секунду раньше (`Config::mail_request_timeout`), чтобы попытка закончилась своим retry, а не таймаутом обработчика |
| `OUTBOX_LEASE_SECS` (L) | back-end | `30` (60) | валидируется библиотекой: `L > 2·S + H + 5 с + ceil(Q/C)·(H+S)` = 24 с |
| `OUTBOX_STORAGE_TIMEOUT_MS` (S) | back-end | `3000` (6000) | бюджет claim/settle; с дефолтом 6000 lease 30 с не пройдёт |
| `OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS` | back-end | `4000` (4000) | statement_timeout транзакций очереди |

Ключи `OUTBOX_*` — это собственные имена `cqrsx`; back-end читает их через
`TaskRuntimeConfig::from_lookup(|name| env::var(name).ok().or_else(|| demo_default(name)))`, то есть библиотечные
имена с демо-дефолтами, работающими и без `.env`. Все значения — в `demo/.env.example`; оба бинарника читают
`.env` через `dotenvy` (если файл есть).

### 5.5 `Makefile` и скрипты

```make
COMPOSE ?= podman compose          # или: make up COMPOSE="docker compose"
.PHONY: up down reset backend sidecar frontend dev check full

up:       ; $(COMPOSE) up -d postgres mongo && ./scripts/wait-db.sh   # ждёт healthcheck-и
down:     ; $(COMPOSE) down
reset:    ; $(COMPOSE) down -v      # стирает ВСЁ, включая журнал событий (только для dev)
backend:  ; cargo run -p encore-backend
sidecar:  ; cargo run -p encore-sidecar
frontend: ; cd front-end && npm run dev
dev:      ; ./scripts/dev.sh        # up + backend + sidecar + frontend, общий Ctrl+C
check:    ; cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings \
            && cargo test --workspace && cd front-end && npm run typecheck && npm run lint && npm test
full:     ; $(COMPOSE) --profile app up -d --build      # [P1]
```

`scripts/dev.sh` — bash: `make up`, затем три процесса в фоне с префиксами логов, `trap` на `SIGINT/SIGTERM` гасит
всех; ждёт `GET /api/health` и `GET /inspect/health` и печатает «Open http://localhost:5173».

---

## 6. `demo/back-end` (crate `encore-backend`)

### 6.1 Зависимости

| Crate | Версия (на 10.2026) | Зачем |
|---|---|---|
| `cqrsx` | path `../cqrsx`, `features = ["host", "logging"]` (default `postgres` + `worker` остаются; U11) | всё ES/CQRS/outbox |
| `async-trait` | 0.1 | `EventHandler` объявлен через `#[async_trait]`; остальные трейты `cqrsx` — native `async fn` |
| `tokio` | 1.x, `full` | рантайм |
| `axum` | 0.8.x (последняя 0.8.9) | HTTP, SSE (`axum::response::sse`) |
| `tower-http` | 0.6.x, `trace` | логирование запросов |
| `serde`, `serde_json` | 1.x | DTO |
| `uuid` | 1.x, `v7`, `serde` | `command_id` (сортируемые по времени) |
| `chrono` | 0.4 с `serde` (как в `cqrsx`) | даты концертов, `recorded_at` |
| `mongodb` | 3.9.x (BSON через `mongodb::bson`); версия не проверялась в Фазе 0 | проекции, inbox |
| `sqlx` | **`0.8`, та же, что в `cqrsx`** (0.8.6); `connect_pool` принимает `PgConnectOptions` | пул, requeue `[P2]` |
| `tokio-util` | 0.7 (как в `cqrsx`) | `CancellationToken` |
| `tokio-stream`, `futures` | latest | SSE-стримы (`BroadcastStream`) |
| `rand` | 0.10 (как в `cqrsx`: `rand::rng()`, `Rng::random_bool`, `StdRng::seed_from_u64`) | хаос 20 % |
| `thiserror` | 2.x | ошибки |
| `tracing` | 0.1 | логи (инициализация — через `cqrsx::logging::LoggingConfig`) |
| `sha2`, `hex` | latest | `[P1]` verify |
| `dotenvy` | latest | `.env` |
| dev: `testcontainers` | как в корне | `[P1]` интеграционные тесты |

### 6.2 Модули

`concert` владеет кассой и залом, `fan` — книжкой, задачами и кошельком, `operations` — activity,
перестройкой и сверкой. `shared` содержит типы, политики и почтовый порт. Корни `concert.rs`, `fan.rs`,
`operations.rs` регистрируют эти компоненты в движке.

### 6.3 Сборка компонентов (`main.rs`)

`Cqrs::from_env("ENCORE").defaults(DEMO_DEFAULTS).event_store(Postgres).projections(Mongo)` подключает
три корня модулей и вызывает `serve().await`. Движок строит пулы, исполнители, проекции, общий диспетчер,
HTTP и управляет остановкой. Полный актуальный код — `back-end/src/main.rs`.

### 6.4 HTTP API

| Метод и путь | Сторона | Назначение | Коды |
|---|---|---|---|
| `GET /api/health` | — | liveness | 200 |
| `GET /api/config` | справочник | зал, персонажи, параметры демо | 200 |
| `GET /inspect/demo` (sidecar) | демо | концерты по умолчанию, персонажи, `reset_enabled` | 200 |
| `POST /inspect/projections/wipe` (sidecar) | демо | 🧨 стереть витрины и их закладки (журнал цел, процессоры перечитают его с начала) | 200, 503 |
| `POST /inspect/reset` (sidecar) | демо | стереть всё и заново анонсировать концерты по умолчанию | 200, 404 (выключен), 503 |
| `GET /inspect/mail/{fan_id}` (sidecar) | read: `mail_service.inbox` | письма фаната, как их сохранил почтовый сервис | 200, 503 |
| `POST /mail` (sidecar, почтовый сервис) | внешний мир | принять письмо от back-end: `200 { inbox_id, duplicate }` или `503 { reason_code }` | 200, 400, 503 |
| `POST /api/concerts/{concert_id}/commands` | write | выполнить команду | 200, 400, 409, 422, 503 |
| `GET /api/concerts` | read: `concert_halls` | сводка по всем концертам | 200 (`[]`, если витрина пуста) |
| `GET /api/concerts/{concert_id}` | read: `concert_halls` | документ схемы зала | 200, 404 `projection_missing` |
| `GET /api/fans/{fan_id}/wallet` | read: `fan_wallets`, диапазон `_id` журналов фаната | кошелёк фаната, собранный из его журналов: `{ fan_id, ledgers: [{ concert_id, version, last_sequence }], view }` (D-14) | 200, 404 |
| `GET /api/concerts/{concert_id}/timeline` | event store | события концерта для ползунка | 200 |
| `GET /api/concerts/{concert_id}/state-at/{sequence}` | event store | машина времени | 200, 404 |
| `POST /api/projections/rebuild` | admin | Encore! `{ pace_ms }` при открытой кассе | 202, 400, 409, 503 |
| `POST /api/concerts/{concert_id}/rebuild` | admin | пересобрать один зал по его истории | 202, 400, 503 |
| `GET /api/projections/status` | admin | по проекции: закладка, отставание, владелец, состояние; Encore! этой ноды | 200, 503 |
| `GET /api/projections/verify` `[P1]` | admin | сверка документов с применённой частью истории, `behind` отдельно | 200, 503 |
| `GET /api/activity` | SSE | живой лог | `text/event-stream` |
| `POST /api/outbox/{task_id}/requeue` `[P2]` | admin | `REQUEUE_FAILED_OUTBOX_TASK` | 200, 404 |

**Команда.** Тело — JSON с полем `type`:

```json
{ "type": "AnnounceConcert", "artist": "Radiohead", "city": "Tbilisi", "date": "2026-11-21" }
{ "type": "BookSeat", "seat": "B4", "fan_id": "fox", "fan_name": "Fox" }
{ "type": "CancelBooking", "seat": "B4", "fan_id": "fox" }
```

Алгоритм команды (`presentation::http::concerts::execute_command` → use case → `application::command_service`):
1. Десериализация (ошибка → `400 { outcome: "invalid" }`).
2. `command_id = Uuid::now_v7()`; `BookSeat` получает `booking_id = command_id`, `CancelBooking` —
   `cancellation_id = command_id`.
3. `metadata = { command_id, correlation_id: command_id, causation_id: command_id, recorded_at: now, source: "ui" }`
   (`HashMap<String, String>`, `recorded_at` фиксированной ширины — §2.4).
4. Задачи: `BookSeat` → `[NewTask(RecordTicketV1)]`, `CancelBooking` → `[NewTask(RecordCancellationV1)]`,
   `AnnounceConcert` → `[]`. Метаданные задачи = метаданные команды + `concert_id`, `fan_id`;
   `with_max_attempts(ENCORE_TASK_MAX_ATTEMPTS)`.
6. `concert_commands.execute_with_metadata(concert_id, command, metadata, tasks)` (U1, U3, D-04).
7. Исход → HTTP (§6.6) + activity.

Внутренние команды `Fan` идут тем же сервисом (`fan_commands`), но из обработчиков задач: метаданные наследуют
`correlation_id` задачи, `causation_id = task_id`, `source = "task:encore_record_ticket"`; задачи —
`[NewTask(SendETicketV1)]` / `[NewTask(IssueRefundV1)]`; исход маппится в результат задачи по таблице §2.7.

Ответы:

```json
// 200
{ "outcome": "accepted", "command_id": "0192f0c4-…", "concert_id": "radiohead-2026-11-21",
  "tasks": ["encore_record_ticket v1"], "warnings": [] }
// 422
{ "outcome": "rejected", "command_id": "0192f0c5-…", "concert_id": "radiohead-2026-11-21",
  "error": { "code": "seat_taken", "params": { "seat": "B4", "by": "Fox" },
             "message": "Seat B4 is already taken by Fox" } }
```

`committed_sequence` в ответе **нет**: `CommandExecutor` при успехе возвращает `()`, а `sequence` UI видит в X-ray
(решение D-04; наблюдатель с `last_sequence` и счётчиком конфликтов — P1-опция, §15). `message` — английский
fallback, UI берёт текст из i18n по `code` и `params`.

**`GET /api/config`:**

```json
{
  "venue": { "rows": ["A","B","C","D","E"], "seats_per_row": 8,
             "zones": [ { "id": "vip", "rows": ["A"], "price": 120 },
                        { "id": "standard", "rows": ["B","C"], "price": 80 },
                        { "id": "balcony", "rows": ["D","E"], "price": 50 } ] },
  "ticket_limit": 4, "snapshot_every": 10
}
```

Персонажи и концерты по умолчанию — реквизит демо, их отдаёт sidecar (`GET /inspect/demo`); параметры имитации
почты у back-end больше нет (аудит 2026-10-17).

**`GET /api/concerts`** — `[{ concert_id, artist, city, date, sold, capacity, revenue, sold_out, last_sequence, version }]`.
**`GET /api/concerts/{id}`** — `{ concert_id, version, view }` (как в §2.8).
**`GET /api/concerts/{id}/timeline`** — `[{ sequence, event_type, recorded_at, summary }]`, где `summary` — короткая
строка вида `"B4 → Fox"`.
**`GET /api/concerts/{id}/state-at/{n}`** — `{ sequence: n, last_sequence, state: { info, bookings }, event }`.

### 6.5 Activity (SSE `GET /api/activity`)

Каждое сообщение: `event: activity`, `data: { "id": <монотонный u64>, "at": "<RFC 3339>", "type": "…", … }`.
При подключении сначала приходит `hello` (конфиг + до 100 последних записей из буфера), затем живой поток.
Activity хранится только в памяти — это лог, а не состояние (так и подписано в UI).

| `type` | Когда | Поля |
|---|---|---|
| `command.accepted` | команда закоммичена | `aggregate_type`, `aggregate_id`, `command_id`, `correlation_id`, `source`, `command_type`, `fan_id?`, `tasks` (без `sequence`: executor его не возвращает, D-04) |
| `command.rejected` | доменный отказ | `aggregate_type`, `aggregate_id`, `command_id`, `correlation_id`, `source`, `command_type`, `code`, `params` (для `source: "task:…"` UI не показывает тост) |
| `command.failed` | conflict / unavailable | `aggregate_type`, `aggregate_id`, `command_id`, `source`, `outcome`, `message` |
| `projection.updated` | процессор применил события к документу | `projection`, `view_id`, `sequence`, `events` |
| `projection.failed` | раунд процессора не удался (повтор после паузы) | `projection`, `view_id?`, `handler`, `category`, `failures`, `message` |
| `projection.rebuilt` | документ заменён свёрткой своей истории (Encore!, один зал, лечение дыры) | `projection`, `view_id`, `events`, `reason` (`requested` / `healed`) |
| `projection.lease` | нода взяла, потеряла или отдала закладку | `projection`, `lease` (`acquired` / `lost` / `released`), `owner` |
| `task.attempt.started` | обработчик задачи начал попытку | `task_id`, `format`, `attempt`, `correlation_id` |
| `task.attempt.failed` | 🎲 отказ (retry) | `task_id`, `format`, `attempt`, `reason_code`, `retry: true` |
| `task.attempt.succeeded` | письмо доставлено | `task_id`, `format`, `attempt`, `inbox_id`, `duplicate` |
| `projections.wiped` | wipe | `collections` |
| `rebuild.started` / `.progress` / `.finished` | Encore (наблюдатель сервиса на ноде, которая приняла запрос) | `rebuild_id`, `pace_ms`, `keys_total`, `events_total` / `keys` / `keys`, `duration_ms` |

`rebuild.progress`: раз в 250 мс, если изменилось число пересобранных документов в строках закладок (они двигаются
страницами); `projection.stalled` и `rebuild.failed` нет — процессор не падает, а повторяет, «застряла» — состояние статуса.

### 6.6 Маппинг исходов команды

| Исход `cqrsx` | HTTP | `outcome` | Activity |
|---|---|---|---|
| успех | 200 | `accepted` | `command.accepted` |
| `CommandError`: domain (`ConcertError`) | 422 | `rejected` | `command.rejected` |
| conflict (после retry-политики) | 409 | `conflict` | `command.failed` |
| unavailable | 503 | `unavailable` | `command.failed` |
| битый JSON / неизвестный `type` (в том числе внутренние `RecordTicket`/`RecordCancellation` через HTTP) | 400 | `invalid` | — |

Для внутренних команд `Fan` те же исходы маппятся не в HTTP, а в результат `TaskHandler` (§2.7); activity получает
`command.accepted` / `command.rejected` с `source: "task:…"`, UI показывает их приглушённо и без тостов.

Отказ read side — нормальный учебный сценарий: команда принимается, витрина отстаёт и догоняет сама, когда MongoDB
вернётся (демо-сценарий, шаг 9, D-17).

### 6.7 `MongoViewRepository<V>`

- Документ: `{ _id: view_id, revision: i64, last_sequence: i64, payload: <V как BSON> }` (`last_sequence` —
  последнее применённое событие агрегата, поле обязательное: старых документов нет, базы пересоздаются).
- `load(view_id)` → `find_one({ _id })` → десериализовать `payload`.
- `load_stored(view_id)` → `(view, ViewMeta { id, revision, last_sequence })`.
- `update_view(view, ctx)`:
  - `ctx.revision == 0` → `insert_one({ _id, revision: 1, last_sequence, payload })`; duplicate key (E11000) →
    **конфликт**;
  - иначе `update_one({ _id, revision: ctx.revision }, { $set: { payload, revision: ctx.revision + 1, last_sequence } })`;
    `matched_count == 0` → **конфликт**.
- Конфликт маппится в тот вариант ошибки `cqrsx`, на который `ViewProjection` отвечает повтором; прочее — в
  ошибку соединения/неизвестную.
- В view использовать `BTreeMap` (детерминированный порядок ключей) и знаковые целые (в BSON нет беззнаковых;
  `u64` не использовать).
- Запись документов — у процессора (библиотечный `MongoViewRepository`), чтение для API — `MongoReadModel` демо; сброс
  коллекции — 🧨 wipe у sidecar, не у back-end.

### 6.8 `RebuildService`

- `RebuildService<C: CheckpointStore>` за портом `ReadSide` (`status`, `rebuild(pace)`, `rebuild_concert(id)`): только
  просит и смотрит; пересобирают владельцы закладок. Состояние в памяти ноды — запущенный Encore! и `last_result`
  плюс последние прочитанные строки закладок для статуса при недоступном хранилище.
- `rebuild(pace)` → `request_rebuild` обеим строкам (`concert_halls`/`Concert`, `fan_wallets`/`Fan`): принята хотя бы
  одна → `202`; обе уже идут → `409 rebuild_in_progress`; `pace > ENCORE_REPLAY_MAX_PACE_MS` → `400`; отказ хранилища
  или ещё не зарегистрированная строка → `503`. Затем `tokio::spawn` наблюдателя: раз в 250 мс читает строки,
  публикует `rebuild.progress`, и когда ни у одной строки нет запроса и курсора (или строки нет), — `rebuild.finished`.
- `rebuild_concert(id)` → `request_rebuild_key` строки залов.
- `status()` — раздел 2.9: таймаут чтения `ENCORE_PROJECTION_STATUS_TIMEOUT_MS`, отставание
  `PostgresEventRepository::count_after(position, cap)` через порт `EventLedgerStats` (D-24).
- Процессоры (`bootstrap::projections`): по одному на витрину, `ViewProjection::new(MongoReadModel)` под
  `ProjectionProcessor` с общим `MongoCheckpointStore`, журнал читают через отдельный пул
  `ENCORE_DATABASE_PROJECTION_POOL_SIZE`, `ENCORE_PROJECTION_*` → `ProcessorConfig`, `ENCORE_NODE_NAME` — имя владельца.
  Рантайм проекций — отдельная задача на общем токене остановки; её паника останавливает процесс с ошибкой.
- Outbox-воркеры, цепочка `Fan` и почтовый inbox replay не затрагивает: replay вызывает только `View::apply`,
  команды не выполняются, задачи не создаются.

### 6.9 Тесты back-end

- **Домен** (`TestFramework::<A>::with(rules).given(..).when(..).then_expect_event / then_expect_error` из
  `cqrsx`): каждое правило R1–R8 (`Concert`) и R9–R11 (`Fan`) отдельным тестом с узнаваемым именем
  (`r5_rejects_taken_seat`, `r10_rejects_cancellation_of_unknown_ticket`); happy-path для каждой из пяти команд;
  после брони последнего места `is_sold_out()`; лимит из правил; refund = цена из истории; повторный
  `RecordTicket` отклоняется, повторный `RecordCancellation` отклоняется.
- **Инвариант «одно событие на команду»** (D-02): это тип результата `Decide`, в тестах проверяется
  `then_expect_event` или отказ.
- **Обработчики цепочки**: маппинг исходов `CommandError` → `TaskResult` по §2.7
  (`UnknownTicket` → retry, `AlreadyRecorded` → done), с тестовым контекстом.
- **Проекции**: свёртка упорядоченной последовательности, повторная доставка и разрыв обрабатываются движком;
  состояние отменённых записей и производные поля проверяются тестами.
- **Outbox и почта**: тесты контекста и in-memory движка проверяют цепочку и повторную доставку; тесты sidecar
  проверяют отказы сервиса, идемпотентную запись inbox и повторное увеличение `deliveries`.
- `[P1]` **Интеграционные** (testcontainers: PostgreSQL того же образа, что в корневых тестах, + MongoDB):
  принятая команда → строка события + строка задачи с одним `correlation_id`; отклонённая → ни одной строки;
  полная цепочка `BookSeat` → `TicketRecorded` → письмо; `events.metadata` содержит `command_id`/`recorded_at`;
  wipe + rebuild → verify OK; replay не меняет число писем и задач.

---

## 7. `demo/sidecar` (crate `encore-sidecar`)

### 7.1 Ответственность

Две роли. Первая — «окно в базы данных»: показывает сырое состояние таблиц и коллекций **так, как оно лежит в
СУБД**, только чтение, под ролью `encore_inspector` (read-only на двух уровнях); не знает домен — лишь имена
таблиц/коллекций и колонки сортировки. Вторая (с 2026-10-17, `docs/BOUNDARY_AUDIT.md`) — всё demo-only, чтобы
back-end оставался production-подобным: seed концертов по умолчанию, персонажи, 🧨 wipe витрин, ⟲ reset
(`encore_demo_admin`, только `TRUNCATE`), чтение ящика фаната и **имитация внешнего почтового сервиса**
(`POST /mail`, `sidecar/src/mail.rs`), которую back-end вызывает по HTTP как настоящего провайдера. Не зависит
от `cqrsx` по факту, не по правилу.

### 7.2 Эндпоинты

| Метод и путь | Что отдаёт |
|---|---|
| `GET /inspect/health` | liveness + доступность обеих БД |
| `GET /inspect/postgres/events?aggregate_id=&limit=200` | последние события (новые сверху) |
| `GET /inspect/postgres/snapshots?aggregate_id=` | снапшоты |
| `GET /inspect/postgres/outbox?limit=200` | outbox-задачи |
| `GET /inspect/postgres/tables` `[P1]` | таблицы, колонки (`information_schema`), число строк |
| `GET /inspect/mongo/{database}/{collection}?limit=100` | документы (whitelist: `encore_read.concert_halls`, `encore_read.fan_wallets`, `mail_service.inbox`; иначе 404) |
| `GET /inspect/stats` | счётчики: события (всего и по концертам), задачи по статусам, документы по коллекциям |
| `GET /inspect/stream` | SSE: сообщения `state` со всем перечисленным выше |
| `GET /inspect/mail/{fan_id}?limit=100` | письма фаната из `mail_service.inbox`, старые сверху (как их сохранил почтовый сервис) |
| `GET /inspect/demo` | реквизит демо: `default_concerts`, `personas`, `reset_enabled` |
| `POST /inspect/projections/wipe` | DEMO ONLY: 🧨 drop `concert_halls` и `fan_wallets`; `{ outcome: "wiped", dropped }` |
| `POST /inspect/reset` | DEMO ONLY: TRUNCATE трёх таблиц, drop трёх коллекций, seed заново; 404 при `SIDECAR_RESET_ENABLED=false` |
| `POST /mail` | DEMO ONLY: почтовый сервис для back-end — задержка, кубик (`503 { reason_code }`), идемпотентный upsert в `mail_service.inbox` по `Idempotency-Key` (`200 { inbox_id, duplicate }`) |

Формат ответа таблицы — с самим запросом, чтобы UI мог показать «вот что мы спросили у базы»:

```json
{ "source": "postgres", "table": "events",
  "query": "SELECT row_to_json(t) FROM (SELECT * FROM events ORDER BY … DESC LIMIT 200) t",
  "total": 57,
  "rows": [ { "aggregate_type": "Concert", "aggregate_id": "radiohead-2026-11-21", "sequence": 7,
              "event_type": "SeatBooked", "event_version": "1.0", "payload": { }, "metadata": { } } ],
  "db_time": "2026-10-17T09:12:05.123Z" }
```

```json
{ "source": "mongodb", "database": "encore_read", "collection": "concert_halls",
  "query": "db.getSiblingDB('encore_read').concert_halls.find({}).sort({ _id: 1 }).limit(100)",
  "total": 3, "docs": [ ] }
```

### 7.3 Реализация

- Универсальная выборка: `SELECT row_to_json(t) AS row FROM (SELECT * FROM <table> ORDER BY <cols> DESC LIMIT $1) t` —
  sidecar не нужно знать все колонки; UI сам решает, что показывать. Таблицы и порядок (U5, U15 закрыты;
  полный список запросов — `API_MAPPING.md` §6): `events` — `ORDER BY metadata->>'recorded_at' DESC NULLS LAST,
  aggregate_id, sequence DESC` (строка `recorded_at` фиксированной ширины, поэтому сортируется как текст; при
  фильтре по `aggregate_id` — `ORDER BY sequence DESC`); `snapshots` — `ORDER BY aggregate_type, aggregate_id`;
  `transactional_outbox` — `ORDER BY created_at DESC, id`. Фильтр по `aggregate_type` (`Concert` / `Fan`) — bind-параметр.
  **Никакого динамического SQL** из параметров запроса; `aggregate_id` — bind-параметр. `sqlx` — 0.8, как в библиотеке.
- **Все PostgreSQL-запросы одного цикла опроса — в одной транзакции** `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`:
  так `events` и outbox читаются из одного снимка, и UI никогда не покажет событие без его задачи (или задачу без
  события) из-за гонки между двумя `SELECT`. Иначе «атомарность» на экране могла бы мигать — а это главное, что
  демо доказывает про outbox.
- `db_time` = `SELECT now()` в той же транзакции: фронт считает обратные отсчёты по часам БД, а не браузера.
- BSON → relaxed Extended JSON → `serde_json::Value` (даты — ISO-строками).
- Один общий поллер на всех клиентов (`tokio::sync::watch`): каждые `SIDECAR_POLL_MS` собирает `State { events,
  snapshots, outbox, mongo: { concert_halls, fan_wallets, inbox }, stats, db_time, errors }`, считает хэш без
  `db_time`; если изменился — рассылает. Новый подписчик сразу получает текущее состояние; heartbeat-комментарий
  каждые 15 с.
- Недоступная БД не роняет процесс: в `errors` попадает `{ source, message }`, UI показывает «🐘 PostgreSQL недоступен».
- Лимиты: events 200, outbox 200, snapshots 50, документов по 100 на коллекцию.

### 7.4 Тесты sidecar

- Unit: стабильность хэша (одинаковые данные → одинаковый хэш), whitelist коллекций.
- `[P1]` Интеграционный: под ролью `encore_inspector` `INSERT` в `events` завершается ошибкой.

---

## 8. `demo/front-end` (Vue)

### 8.1 Стек

| Пакет | Версия | Комментарий |
|---|---|---|
| Node.js | 24 LTS (`.nvmrc`) | Vite 8 требует Node 20.19+ / 22.12+ |
| `vue` | 3.5.x | последний stable на 17.10.2026 — 3.5.44; ветка 3.6 в статусе RC (3.6.0-rc.11). Если к старту работ выйдет 3.6.0 stable — брать её. Vapor mode не использовать |
| `vite` | 8.x | Rolldown-сборщик |
| `@vitejs/plugin-vue` | latest, совместимый с Vite 8 | |
| `typescript`, `vue-tsc` | latest | `strict: true` |
| `pinia` | 3.x | сторы |
| `vue-i18n` | 11.x | Composition API (`legacy: false`) |
| `@vueuse/core` | latest | `useEventSource`, `useIntervalFn`, `useTimeAgo`, `usePreferredReducedMotion` |
| `tailwindcss`, `@tailwindcss/vite` | 4.3.x | стили |
| `canvas-confetti` | latest | 🎉 SOLD OUT |
| `vitest` | latest, совместимый с Vite 8 | unit-тесты |
| `eslint` + `eslint-plugin-vue` + `typescript-eslint`, `prettier` | latest | |

Без UI-библиотек и без роутера (одна страница). Шрифты — локально через `@fontsource` (Inter, JetBrains Mono),
без внешних CDN. Dev-прокси Vite: `/api` → `http://127.0.0.1:8080`, `/inspect` → `http://127.0.0.1:8090`
(одно происхождение — CORS не нужен; SSE через прокси работает).

### 8.2 Макет экрана (1920×1080, проектор)

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│ 🎸 ENCORE · cqrsx demo    ① Журнал ② Правила ③ Outbox ④ Витрины ⑤ Encore!     RU|EN    ⓘ   │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│ 📨 Команда ─▶ 🛡 Агрегат ─▶ ┏ 🐘 events ┓ ─▶ 🍃 Витрины ─▶ 🖥 Экран                          │
│                            ┃ 🐘 outbox ┃ ─▶ ⚙ Воркер 🎲 ─▶ 📬 Почта                          │
│                            ┗ 1 транзакция ┛                                                 │
├───────────────────────────────────────────┬─────────────────────────────────────────────────┤
│ [Tbilisi 12/40] [Berlin 3/40] [Lisbon]    │ 🐘 events · 57    ⓘ </> │ 🐘 outbox · 14    ⓘ </> │
│ Кто вы: [🦊 Fox 2/4] 🐼 Panda 🦄 Unicorn 🤖   │ #12 SeatBooked B4  ✨   │ 🎫 🎲 retry 2.4 s  2/6  │
│                                           │ #11 SeatBooked A3       │ 🎫 ✅ delivered    1/6  │
│          ╭──── 🎤 STAGE ────╮             │ #10 📸 SeatBooked C1    │ 💸 📨 sending…     1/6  │
│    A  ▢ ▢ 🦊 ▢ ▢ ▢ ▢ ▢   VIP 120 €         ├─────────────────────────┼─────────────────────────┤
│    B  ▢ ▢ ▢ 🐼 ▢ ▢ ▢ ▢   Standard 80 €     │ 🍃 concert_halls │ wallets│ 📬 inbox │ 📜 activity  │
│    C  ▢ ▢ ▢ ▢ ▢ ▢ ▢ ▢                     │ { "_id": "radiohead-2026-11-21",│ 🎫 B4 → Fox (поп. 2)  │
│    D  ▢ ▢ ▢ ▢ ▢ ▢ ▢ ▢   Balcony 50 €      │   "version": 13,        │ 🎲 B5: попытка 1 ✖      │
│    E  ▢ ▢ ▢ ▢ ▢ ▢ ▢ ▢                     │   "view": { "sold": 12, │ ✅ B4 доставлен         │
│ ⏳ ●━━━━━━━━━━━━━━━━━━○  событие 12 из 12   │     … } }               │                         │
│ [🤖 Перекупщик] [🌊 Толпа] [⚡ Гонка]       │                         │                         │
│ 🧪 [Z9] [занятое] [5-й билет] [чужой] […]  │                         │                         │
│ 🎫 Мои билеты: B4 · 80 € · 📬 доставлен     │                         │                         │
│ [🧨 Стереть] [⏪ Encore!] 🐢 ▓▓▓▓▓░░ 62 %    │                         │                         │
└───────────────────────────────────────────┴─────────────────────────────────────────────────┘
```

- Слева (2/3 ширины): блок «Путь команды» — **живая архитектура** (PipelineStrip), путь каждой команды
  подсвечивается в реальном времени, под ним панели `events` и `outbox`; справа (1/3) — касса с компактными
  элементами управления; нижний ряд — остальной рентген (`mongo`, почта, activity, снапшоты).
- Слева — **APP**: то, что видит фанат.
- Справа — **X-RAY**: что лежит в базах (данные sidecar), сетка 2×2 на ширине ≥ 1440 px, вкладки на 1024–1439 px,
  вертикальный стек ниже (на телефонах ничего не должно ломаться, но оптимизация не требуется).
- При первом заходе — баннер «Как читать этот экран» (Приложение A.1), закрывается крестиком (помнится в
  `localStorage`).

### 8.3 Компоненты и поведение

| Компонент | Приоритет | Поведение |
|---|---|---|
| `AppHeader` | P0 | логотип, переключатель языка, ⓘ — открыть баннер «Как читать экран» |
| `PipelineStrip` | P0 | диаграмма архитектуры по `docs/img.png` на холсте **Vue Flow** (`@vue-flow/core`, статичный: без drag/pan/zoom, вписан в блок, zoom ≤ 1) в блоке верхнего ряда (50 % ширины, рядом с панелями events и outbox): три рамки сервисов — `Concert service` (Command → Aggregate → events + outbox → `Outbox worker → Fan`), `Fan service` (Command → Aggregate → events + outbox → `Outbox worker → email`), `projection` (read database → ◇ UI); ромбы ◇ UI (касса, под сервисом Concert) и ◇ email; внизу круг с персонажем (🦊 Fox…), соединённый с обоими ◇ UI и с ◇ email; снаружи — «почтовый сервер 🎲» с обратным ребром retry к events + outbox; сигналы — кружки, бегущие по рёбрам (рёбра ортогональные, узлы на сетке: Command над Aggregate, Outbox worker над events + outbox); подсветка по таблице ниже; на холсте только названия блоков — без подписей и пояснений; **клик по любому блоку или рамке** открывает модальное окно `PipelineExplain` с развёрнутым пояснением (`explain.<id>` в i18n), для агрегатов — таблица правил R1–R8 / R9–R11. Данные графа — `pipeline/graph.ts`, хореография сигналов — `pipeline/choreography.ts` |
| `ConcertTabs` | P0 | концерты из `/api/concerts`; прогресс sold/capacity; бейдж SOLD OUT; если витрина пуста — вкладки из `GET /inspect/demo` (`default_concerts`) с «?» |
| `PersonaPicker` | P0 | четыре больших аватара; у каждого «билетов n/4» на текущем концерте |
| `SeatMap`, `SeatButton` | P0 | сцена со свечением и именем артиста, ряды A–E, легенда зон; состояния — §8.6; клики — ниже |
| `ActionBar` | P0 / P1 | 🤖 Перекупщик, 🌊 Толпа (P0), ⚡ Гонка (P1) |
| `RuleBreaker` | P0 | кнопки-нарушители правил — ниже |
| `WalletPanel` | P0 | билеты текущего персонажа из `fan_wallets`, статус e-ticket, кнопка отмены, письма о возврате; пока бронь есть в зале, но ещё нет в кошельке (задача `encore_record_ticket` в пути) — строка «⏳ записывается в кошелёк…» |
| `EncorePanel` | P0 / P1 | 🧨 Стереть, ⏪ Encore!, 🐢 slow-motion (по умолчанию вкл.), прогресс-бар (`replayed` / `stats.events_total` из sidecar), итог «✅ 128 событий → 3 зала + 9 кошельков за 1.4 с»; 🔍 Проверить (P1) |
| `TimeMachine` | P0 | ползунок по событиям концерта; режим `history` для `SeatMap` |
| `XrayPanel` | P0 | общий каркас панели: бейдж СУБД (🐘 / 🍃), имя таблицы/коллекции, счётчик, ⓘ пояснение (i18n), `</>` — запрос из ответа sidecar, «обновлено N мс назад» |
| `EventStorePanel` | P0 | последние события обоих агрегатов с бейджем `Concert` / `Fan`; фильтры «только этот концерт» и «только этот фанат»; новые строки въезжают с зелёным свечением 2 с; payload/metadata — `JsonView` (свернуто, однострочный превью); 📸-маркер у строк с `sequence % ENCORE_SNAPSHOT_EVERY == 0` (P1) |
| `SnapshotsPanel` | P0 | `aggregate_id`, `last_sequence`, payload (`JsonView`); подпись «закладка: грузим снапшот + хвост событий» |
| `OutboxPanel` | P0 | задачи: короткий id, формат (🔁 `record_*` / 🎫 / 💸), `StatusChip` (§8.5), «попытка n/max» с 🎲 за каждый отказ, `Countdown` до повтора, `last_error`, payload; счётчики по статусам в шапке |
| `MongoPanel` | P0 | вкладки `concert_halls` / `fan_wallets`; документы `JsonView`; изменённые пути подсвечиваются 2 с (`lib/diff.ts`); документ текущего концерта/персонажа — первым и раскрыт |
| `MailboxPanel` | P0 | письма из `mail_service.inbox` (тема, кому, на какой попытке, `deliveries`); бейдж «НЕ проекция — внешний сервис» |
| `ActivityFeed` | P0 | лента activity с иконками, новые сверху (максимум 300); клик → трассировка (P1) |
| `Toasts` | P0 | успех / отказ (с кодом правила) / предупреждение |
| `ChapterBar`, `ChapterCallout` | P1 | главы ①–⑤ (§8.10) |
| `RaceResult` | P1 | карточка «победитель / проигравший» |
| `AnnounceDialog` | P1 | «+ Анонсировать концерт»: артист/город из подсказок, `concert_id = <artist-slug>-<yyyy-mm-dd>` |

**Клики по месту.** Свободное или «?» → `BookSeat` от текущего персонажа. Чужое → **всё равно** `BookSeat` (чтобы
увидеть R5). Своё → поповер «Отменить билет? Возврат 80 €» → `CancelBooking`. Пока команда в полёте — место
пульсирует (`pending`); отказ — «тряска» 400 мс и красная вспышка; успех — «поп» 0.6 → 1.15 → 1.

**ActionBar.**
- 🤖 **Перекупщик**: от `arbitrage-bot` последовательно 6 × `BookSeat` случайных свободных мест (из последней витрины;
  если витрины нет — из схемы зала), пауза 250 мс, каждая следующая команда — после ответа на предыдущую. Итоговый
  тост считается по фактическим ответам, например «🤖 куплено 4 · отклонено 2 правилом R6».
- 🌊 **Толпа**: 10 × `BookSeat` случайными `crowd-<1..60>` на случайные свободные места, пауза 150 мс, тоже
  последовательно; нет свободных мест — остановиться. Именно так достигается SOLD OUT.
- ⚡ **Гонка** `[P1]`: случайное свободное место; `Promise.all` двух `BookSeat` (Fox и Panda) одновременно;
  `RaceResult`: «🦊 Fox выиграла · 🐼 Panda: seat_taken» + пояснение оптимистической блокировки (A.3).

**RuleBreaker.** Каждая кнопка отправляет заведомо неверную команду и показывает: отправленный JSON, ответ
(код правила + локализованный текст) и `events: 57 → 57 (Δ 0)` (по `stats` из следующего сообщения sidecar).

| Кнопка | Команда | Правило |
|---|---|---|
| «Место Z9» | `BookSeat` `Z9` | R3 |
| «Занятое место» | `BookSeat` на занятое (если занятых нет — сначала обычная бронь от Unicorn) | R5 |
| «Пятый билет» | от 🤖: добрать до 4, затем пятый | R6 |
| «Чужой билет» | `CancelBooking` чужого места | R8 |
| «Отменить свободное» | `CancelBooking` свободного места | R7 |
| «Повторный анонс» | `AnnounceConcert` текущего концерта | R2 |
| «Концерт-призрак» | `BookSeat` в `atlantis-2099` | R1 |

**WalletPanel — статус e-ticket** (связка по `booking_id`):
- в inbox есть `e_ticket:<booking_id>` → «📬 доставлен (попытка n)»;
- иначе в outbox есть задача с этим `booking_id` (payload) → статус-чип из §8.5;
- иначе «…».

**TimeMachine.** Ползунок 1…`last_sequence` текущего концерта (из `/timeline`), засечки по событиям (цвет — тип),
подпись события под курсором, кнопки ⏮ ⏭. Не в конце шкалы → `SeatMap` в режиме `history` с данными
`/state-at/{n}`, фильтр сепия, баннер «Это состояние нигде не хранится — оно вычислено из событий 1…n», кнопка
«Вернуться в настоящее». Соответствующая строка в `EventStorePanel` подсвечивается.

**PipelineStrip — когда что светится.**

| Триггер | Анимация |
|---|---|
| UI отправил команду (локально) | кружок ● персонаж → ◇ UI → Command → Aggregate; на Aggregate янтарное кольцо «busy» |
| ответ `accepted` | Aggregate зелёный; кружок Aggregate → events + outbox (бокс вспыхивает розовым: «1 транзакция») → read database → ◇ UI → ● персонаж |
| ответ `rejected` | Агрегат мигает красным, пузырь с кодом правила; стрелка Агрегат → events + outbox перечёркнута, подпись «ничего не записано» |
| `command.*` с `aggregate_type: Fan` (из activity) | кружок events + outbox (Concert) → `Outbox worker → Fan` → Command (Fan) → Агрегат (Fan); `accepted` — дальше events + outbox (Fan) → read database → ◇ UI; `rejected` — как выше, на агрегате Fan |
| `task.attempt.started` | кружок events + outbox (Fan) → `Outbox worker → email` → mail server; на сервере янтарное кольцо «busy» |
| `task.attempt.failed` | 🎲 «бросок», красное кольцо и пузырь с `reason_code` на почтовом сервере; красный кружок бежит по пунктирному ребру «retry» обратно в events + outbox |
| `task.attempt.succeeded` | 🎲 «бросок», зелёное кольцо на сервере; кружок mail server → ◇ email → ● персонаж |
| `projections.wiped` | рамка `projection` и read database гаснут, подпись «пусто» |
| `rebuild.progress` | по обеим стрелкам events + outbox → read database бежит розовый пунктир, подпись «⏪ replay» |

Сцены проигрываются **по очереди** (`choreography.ts`, очередь сцен): каждая сцена идёт заявленное время плюс
пауза 200 мс, следующий сигнал ждёт своей очереди. Сервер отвечает за миллисекунды, поэтому без очереди клик, команда
Fan и письмо накладывались бы друг на друга. Если сцена стартовала бы позже чем через 4 с после реального события
(толпа, перекупщик), диаграмма её пропускает — панели events и outbox показывают всё. Скорость анимации
выбирается кнопками в шапке блока (0.5× · 1× · 2×, `session.pace`, хранится в `localStorage`): множитель
применяется ко всем таймерам сцен, длительности кружков и паузе между сценами.
Сцены «принято» **ветвятся** в events + outbox: очередь держит сцену только до момента, когда кружок доходит
до events + outbox, и следующая сцена (цепочка задач или письмо) стартует ровно в этот момент, пока первая
дорисовывает свою ветку к read database → UI. Так видно, что проекция и outbox-задача рождаются в одной транзакции.

### 8.4 Данные и сторы (Pinia)

| Стор | Содержимое |
|---|---|
| `session` | персонаж, выбранный концерт, язык, reduced-motion, текущая глава |
| `concerts` | список и view по id; `stale` после wipe; fallback-список из `session.demo.default_concerts` (sidecar) |
| `wallet` | кошелёк и inbox текущего персонажа |
| `commands` | `send(concertId, body)` → исход; множество команд в полёте (`concert:seat`); тосты; сигналы для `PipelineStrip` |
| `activity` | SSE `/api/activity` (`useEventSource` с авто-переподключением); записи; маршрутизация событий в другие сторы |
| `xray` | SSE `/inspect/stream`; текущее и предыдущее состояние (для диффов); множества «уже видели» (для вспышек); смещение `db_time` |
| `rebuild` | статус, прогресс, итог |
| `tour` | `[P1]` главы |

Правила обновления данных:
- после ответа на собственную команду → перезапросить зал (концерт), кошелёк (персонаж) и список;
- `projection.updated` (например, из другой вкладки) → перезапрос с debounce 150 мс;
- во время rebuild → перезапрос зала и списка не чаще раза в 250 мс (места «загораются» по одному), по
  `rebuild.finished` — финальный перезапрос;
- `projections.wiped` → сбросить кэш view → места `unknown`;
- страховочный опрос раз в 5 с.

### 8.5 Статусы outbox-задач в UI

```ts
// lib/outboxStatus.ts — чистая функция, покрыта тестами.
// Колонка transactional_outbox.status явная: 'pending' | 'processing' | 'completed' | 'failed' (U5 закрыт).
export type UiTaskStatus = 'queued' | 'in_progress' | 'retry_scheduled' | 'delivered' | 'failed';

export function deriveTaskStatus(row: OutboxRow): UiTaskStatus {
  switch (row.status) {
    case 'completed':  return 'delivered';
    case 'failed':     return 'failed';
    case 'processing': return 'in_progress';   // просроченный lease до recovery тоже in_progress (≤ poll)
    case 'pending':    return row.attempts > 0 ? 'retry_scheduled' : 'queued';
  }
}
// обратный отсчёт: available_at − dbNow; ≤ 0 → «ждёт воркера»; «попытка n/max» = attempts/max_attempts
```

| UI-статус | Чип | Подпись (RU) |
|---|---|---|
| `queued` | серый | «в очереди» |
| `in_progress` | синий, пульсирует | «отправляется… (попытка n)» |
| `retry_scheduled` | янтарный, 🎲, кольцо обратного отсчёта | «сервер капризничает — повтор через 3 с» |
| `delivered` | зелёный | «доставлено» |
| `failed` | красный | «не доставлено: исчерпан лимит попыток» (P2: кнопка Requeue) |

- Обратный отсчёт считается по часам БД: `availableAt − (Date.now() + dbTimeOffset)`.
- Завершённые строки очередь **не удаляет** (`finished_at` заполняется), «призрачные» строки не нужны.
- Причина 🎲 — колонка `last_error` (туда попадает `reason` из `TaskHandlerError::Retry`, то есть `reason_code`);
  activity `task.attempt.failed` нужна только для анимации броска.
- Задачи цепочки (`encore_record_*`) показываются с иконкой 🔁 и подписью «команда агрегату Fan»; почтовые — 🎫 / 💸.

### 8.6 Состояние места

```ts
// lib/seatState.ts — чистая функция, покрыта тестами.
export type SeatUiState = 'free' | 'booked' | 'mine' | 'unknown';

export function seatState(seat: SeatId, ctx: SeatContext): SeatUiState {
  if (ctx.mode === 'history') {                       // машина времени: данные агрегата, «unknown» не бывает
    const b = ctx.historyState.bookings[seat];
    return !b ? 'free' : b.fan_id === ctx.persona ? 'mine' : 'booked';
  }
  if (!ctx.view) return 'unknown';                    // витрина стёрта
  const entry = ctx.view.seats[seat];
  if (!entry) return 'free';
  if (entry.state === 'free') return 'free';
  return entry.fan_id === ctx.persona ? 'mine' : 'booked';
}
// `pending` и анимации — отдельный оверлей поверх базового состояния.
```

### 8.7 Визуальный стиль

- Тёмная «концертная» тема: фон `#0b0b1a`, панели `#14142b`, рамки `#26264a`; неон: розовый `#ff3ea5`
  (основной), циан `#22d3ee`, лайм `#a3e635` (успех), янтарь `#fbbf24` (retry), красный `#f43f5e` (отказ),
  фиолетовый `#a78bfa`. Цвета зон — §2.2.
- Шрифты: Inter (интерфейс), JetBrains Mono (таблицы, JSON).
- Анимации — CSS keyframes и `<Transition>` / `<TransitionGroup>`: «поп» места, «тряска» отказа, пульс `pending`,
  въезд строк со свечением, бросок 🎲, кольцо обратного отсчёта (SVG `stroke-dashoffset`), бегущий градиент по
  стрелкам `PipelineStrip`, конфетти при SOLD OUT, сепия в режиме истории.
- `prefers-reduced-motion`: без конфетти, тряски и бегущих градиентов; смена цветов остаётся.
- Доступность: места — `<button>` с `aria-label` («B4, Standard, 80 €, свободно»), видимый фокус; навигация
  стрелками по залу — `[P1]`.

### 8.8 Конфетти и SOLD OUT

Срабатывает, когда у выбранного концерта `sold_out` в витрине меняется `false → true` (производное поле
`sold == capacity`; отдельного события нет, D-03). Над сценой — неоновая надпись SOLD OUT; на вкладке концерта — бейдж.

### 8.9 i18n

- `vue-i18n` 11, `legacy: false`; локали `en`, `ru`; по умолчанию `ru`, если `navigator.language` начинается с
  `ru`, иначе `en`; переключатель в шапке (запоминается в `localStorage`, доступ обёрнут в `try/catch`).
- Пространства ключей: `rules.<code>` (с параметрами), `chaos.<reason_code>`, `chapters.<id>.{title,body,try}`,
  `panels.<id>.{title,explain}`, `pipeline.*`, `actions.*`, `status.*`, `common.*`.
- Тест `i18nParity.spec.ts`: наборы ключей `en` и `ru` совпадают; есть переводы для всех кодов правил и причин
  отказа.
- Тексты — из Приложения A (можно улучшать, смысл сохранять).

### 8.10 Главы-подсказки `[P1]`

`ChapterBar`: ① Журнал · ② Правила · ③ Outbox · ④ Витрины · ⑤ Encore!. Клик открывает `ChapterCallout`
(плавающая карточка): заголовок, 2–4 предложения (A.2), кнопки «Попробовать» (выполняют действие: забронировать
случайное место, нарушить правило, стереть витрины…), подсветка нужных панелей (остальные приглушены до 40 %),
кнопка «Дальше →».

---

## 9. Демо-сценарий (≈ 7 минут)

| Шаг | Время | Действие ведущего | Что показать |
|---|---|---|---|
| 1. Знакомство | 30 с | открыть UI, прочитать баннер | слева касса, справа рентген, сверху путь команды; в `events` уже 3 строки `ConcertAnnounced` — «даже концерты появились через команды» |
| 2. Event Sourcing | 1 мин | как 🦊 Fox кликнуть B4; подвигать ⏳ | новая строка `SeatBooked` (sequence 2); изменился документ `concert_halls`; ползунок показывает зал в прошлом — «состояние вычисляется из событий» |
| 3. Инварианты | 1 мин | переключиться на 🐼 Panda, кликнуть B4; запустить 🤖; пара кнопок 🧪 | «Место занято», `Δ events = 0`; 🤖: 4 ✅ и 2 ❌ по R6 |
| 4. Outbox | 1,5 мин | забронировать ещё место | вспышка «1 транзакция»: строка `SeatBooked` и строка задачи 🔁 одновременно; задача 🔁 `delivered` за полсекунды, и в `events` появляется `TicketRecorded` агрегата `Fan` вместе с задачей 🎫 (вторая вспышка); задача 🎫 `in progress` → (возможно) `🎲 retry через 2 с` → `delivered`; письмо в 📬; в кошельке «📬 доставлен (попытка 2)» |
| 5. Витрины | 1 мин | 🌊 Толпа × 3–4 (до SOLD OUT) | много задач, часть с 🎲; SOLD OUT 🎉 по витрине (`sold == capacity`); два потока событий (`Concert`, `Fan`) → два документа — зал и кошелёк; кошелёк «догоняет» зал через задачу `encore_record_ticket` |
| 6. Отмена | 30 с | Fox отменяет билет | `BookingCancelled` + задача 🔁 → `TicketCancelled` + задача 💸 → письмо «💸 Refund» |
| 7. Encore! | 1,5 мин | 🧨 Стереть → кликнуть место, которое было занято → забронировать свободное → ⏪ Encore! (slow) | зал серый, коллекции пусты; занятое место всё равно «занято» — правила работают по журналу; новая бронь даёт «⚠️ неполная витрина»; места загораются по одному; 📬 inbox не изменился — письма повторно не ушли; P1: 🔍 Проверить → ✅ |
| 8. Гонка `[P1]` | 30 с | ⚡ Гонка | «Fox выиграла, Panda — seat_taken» + пояснение оптимистической блокировки |
| 9. Отказ MongoDB (по желанию) | 1 мин | `podman compose stop mongo` → забронировать место → `podman compose start mongo` → Encore! | тост «⚠️ событие записано, витрина не обновилась»; после Encore! всё сходится — «событие не потеряно, витрина догнала журнал» |

---

## 10. Этапы работ и критерии приёмки

Каждая фаза — отдельный коммит `demo: phase N — <summary>` с зелёными проверками своей части (Фаза 0.5 —
коммит в `cqrsx`, сообщение без префикса `demo:`).

### Фаза 0 — Discovery (без кода демо) — ✅ выполнена 17.10.2026

Результат: `demo/docs/API_MAPPING.md`; решения — §15 этого плана. Повторять не нужно.

### Фаза 0.5 — Метаданные в командном порту `cqrsx` (правка библиотеки, D-04)

- Единственная разрешённая правка `cqrsx` (исключение к §12.1), отдельным коммитом **до** Фазы 1.
- В `cqrs/command.rs`: метод `execute_with_metadata(&self, aggregate_id: &str, command: A::Command,
  metadata: HashMap<String, String>, tasks: Vec<NewTask>)` в трейте `CommandExecutor`; `execute` получает
  дефолтную реализацию, делегирующую с пустой картой. Реализации в `CommandExecutorCore`
  (`retry_on_aggregate_conflict` передаёт клон карты в `framework.execute_with_metadata` на каждой попытке) и
  в `PostgresCommandExecutor`. Возвращаемый тип не меняется.
- Тесты: unit в `cqrs/command/executor.rs` (метаданные доходят до `commit`, одинаковы между попытками при
  конфликте); интеграционный в `tests/postgres/command.rs` (`events.metadata` равен переданной карте); обновить
  doc-тесты, включая `compile_fail` в `cqrs/command.rs` (добавить новый метод, чтобы он падал по прежней причине);
  строку про `CommandExecutor` в `cqrsx/doc/components.md`.
- **Приёмка:** команды из корневого `CLAUDE.md` зелёные: `cargo fmt --all --check`,
  `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings`,
  `RUST_TEST_THREADS=2 PROPTEST_RNG_SEED=42 cargo test --locked --workspace --all-features`, матрица
  `--no-default-features` (+ по одной `postgres`, `worker`, `host`, `logging`), оба `cargo llvm-cov … --fail-under-lines 97`.
  Корневой `Cargo.lock` не меняется (новых зависимостей нет).

- Прочитать в `cqrsx/src`: `cqrs/aggregate.rs`, `cqrs/event.rs`, `cqrs/event_sink.rs`, `cqrs/command/{executor,retry}.rs`,
  `cqrs/event_handler.rs`, `cqrs/event_handler/replay.rs`, `cqrs/projection.rs`, `cqrs/projection/repository.rs`,
  `cqrs/store/{persisted,stream,upcaster}.rs`, `task.rs`, `task/{format,attempt,handler,adapter/json,dispatcher}.rs`,
  `task/runtime/*`, `postgres/{command,task_runtime,outbox,view_repository}.rs`, `postgres/task_queue/*`,
  `host/lifecycle.rs`, `logging.rs`; SQL-ассеты и корневой `compose.yaml`; **интеграционные тесты в `tests/`** —
  в них есть реальные примеры сборки компонентов.
- Заполнить `demo/docs/API_MAPPING.md`: по каждому пункту U1–U16 (§13) — ответ, точная сигнатура, решение.
- **Приёмка:** все U закрыты; где реальность отличается от плана — записано решение; есть точные имена таблиц,
  колонок и значений статусов. Блокер без fallback → остановиться и описать его.
- **Контрольная точка:** пройдена, решения в §15.

### Фаза 1 — Каркас и инфраструктура

- Workspace `demo/`, `demo/.gitignore`, `exclude` в корне, `compose.yaml`, init-SQL, `.env.example`, `Makefile`,
  `scripts/*`.
- back-end и sidecar с `/health`; front-end (Vite 8 + Vue + TS + Tailwind + Pinia + vue-i18n) с макетом-заглушкой
  §8.2; черновик `README.md`.
- **Приёмка:** `make up` → обе БД healthy; таблицы event store и outbox созданы; `encore_inspector` может `SELECT`
  и не может `INSERT`; `cargo build` в `demo/` проходит; корневой `cargo build --locked --workspace --all-features`
  проходит, корневой `Cargo.lock` не изменился; `npm run dev` открывает страницу; оба `/health` → 200.

### Фаза 2 — Домен

- `venue`, `Concert` и `Fan`, их команды, события, ошибки; тесты §6.9 (домен и инвариант «одно событие на команду»).
- **Приёмка:** `cargo test -p encore-backend domain` зелёный; каждое правило R1–R11 покрыто тестом с узнаваемым
  именем (`r5_rejects_taken_seat`, `r10_rejects_cancellation_of_unknown_ticket` и т. п.); тест инварианта D-02
  присутствует и зелёный.

### Фаза 3 — Write-side

- Сборка двух `CommandExecutorCore` (`Concert`, `Fan`) с `execute_with_metadata` из Фазы 0.5, `POST …/commands`
  (только команды `Concert`), метаданные (§2.4), маппинг исходов, seed, `GET /api/config`, `ActivityHub` и SSE
  (`command.*`), завершение SSE по токену остановки.
- **Приёмка:**

  ```bash
  curl -s localhost:8080/api/concerts/radiohead-2026-11-21/commands -H 'content-type: application/json' \
       -d '{"type":"BookSeat","seat":"B4","fan_id":"fox","fan_name":"Fox"}'   # 200 accepted
  curl -s localhost:8080/api/concerts/radiohead-2026-11-21/commands -H 'content-type: application/json' \
       -d '{"type":"BookSeat","seat":"B4","fan_id":"panda","fan_name":"Panda"}'       # 422 seat_taken
  ```

  В `events` ровно одна новая строка, её `metadata` содержит `command_id`, `correlation_id`, `recorded_at`,
  `source: "ui"`; после перезапуска back-end seed не дублирует события.

### Фаза 4 — Outbox и цепочка агрегатов

- Четыре payload-а и формата задач (§2.7), `NewTask` при командах `Concert` и `Fan`, `ChaosPolicy`,
  `FlakyMailServer`, четыре `TaskHandler` (два почтовых, два «почтальона» цепочки с маппингом исходов §2.7),
  `enum EncoreTaskAdapter`, `TaskDispatcherFactory`, runtime под `host::lifecycle::run`, activity `task.*`, inbox.
- **Приёмка:** после `BookSeat` есть строка `SeatBooked` и строка задачи `encore_record_ticket` с одним
  `correlation_id`; через ≤ 1 с — строка `TicketRecorded` агрегата `Fan` с тем же `correlation_id` и
  `causation_id = task_id` и строка задачи `encore_send_e_ticket`; после отклонённой команды — ни одной строки;
  повторное выполнение задачи `encore_record_ticket` (например, `requeue` вручную) не создаёт второго
  `TicketRecorded`; на 50 бронях у ≈ 20 % **почтовых** задач больше одной попытки (10 ± 5), у задач цепочки —
  ровно одна; в inbox ровно одно письмо на бронь; Ctrl+C — корректный drain без ошибок в логах при открытой
  вкладке с SSE.

### Фаза 5 — Проекции и read API

- `MongoViewRepository`, `ConcertHallView` (`View<Concert>`), `FanWalletView` (`View<Fan>`), обе через
  `ViewProjection` под процессорами проекций, эндпоинты чтения.
- **Приёмка:** документы соответствуют командам; тесты упорядоченной свёртки зелёные; при остановленной MongoDB команда принимается
  (`200 accepted`), а витрина догоняет после её возвращения.

### Фаза 6 — Replay и машина времени

- `RebuildService` (пересборка на месте процессорами, прогресс, slow-motion через `pace_ms`), `timeline`, `state-at`;
  🧨 wipe — у sidecar (Фаза 7).
- **Приёмка:** после wipe коллекций нет, бронь занятого места → всё так же 422; fast-rebuild даёт документы,
  совпадающие с документами до wipe (сравнение JSON без `version` — тестом или `scripts/compare-projections.sh`);
  slow-rebuild 100 событий ≈ 8 с с прогрессом; команды во время rebuild принимаются (D-16), задачи цепочки идут как
  обычно; число писем в inbox и строк outbox не меняется; `state-at` возвращает верное
  историческое состояние.

### Фаза 7 — Sidecar

- Эндпоинты §7.2, `row_to_json`, MongoDB, `stats`, общий поллер + SSE, обработка недоступности БД.
- **Приёмка:** `curl localhost:8090/inspect/postgres/events` отдаёт строки и `query`; SSE шлёт `state` только при
  изменениях (+ heartbeat); остановка PostgreSQL даёт `errors`, процесс жив.

### Фаза 8 — Front-end: ядро `[P0]`

- Каркас, сторы, API-клиенты, SSE, все P0-компоненты §8.3, тосты, конфетти, i18n RU/EN с текстами Приложения A.
- **Приёмка:** шаги 1–7 демо-сценария проходятся вручную без devtools; `npm run typecheck && npm run lint &&
  npm test` зелёные.

### Фаза 9 — Полировка `[P1]`

- Главы, «Гонка», verify, 📸-маркеры, трассировка по `correlation_id` (наведение на событие подсвечивает его задачу и
  документ), `AnnounceDialog`, навигация с клавиатуры, вкладки X-ray на средних экранах, интеграционные тесты.
- **Приёмка:** шаг 8 сценария и полный проход по главам; verify ✅ после rebuild.

### Фаза 10 — Упаковка и документация

- Dockerfile-ы, nginx (`/api` → backend, `/inspect` → sidecar; для SSE `proxy_buffering off`, HTTP/1.1,
  `Connection ''`), профиль `app` в compose, `README.md` (быстрый старт, схема архитектуры, демо-сценарий,
  troubleshooting), по желанию GIF.
- **Приёмка:** на чистой машине `make up && make dev` (или `make full`) поднимает рабочее демо; шаги README
  воспроизводятся без доработок.

---

## 11. Definition of Done

- [ ] Выполнены критерии приёмки фаз 0.5–8 и 10 (P0); Фаза 0 закрыта `docs/API_MAPPING.md`.
- [ ] Для автономного прогона: `docs/PROGRESS.md` с таблицей «фаза → статус → как проверено → что проверить
      вручную»; `docs/PHASE_0_5_REVIEW.md` с описанием правки `cqrsx`.
- [ ] `make check` зелёный: `cargo fmt --check`, `clippy -D warnings`, `cargo test`, `vue-tsc`, `eslint`, `vitest`.
- [ ] Корень репозитория не затронут, кроме `exclude = ["demo"]` (и, при необходимости, `.dockerignore`); корневые
      команды из README (`cargo build --locked …`, тесты, coverage) работают как раньше.
- [ ] Хаос 20 % изолирован в `infrastructure::mail::{chaos, flaky_mail_server}`, помечен `// DEMO ONLY`, настраивается
      переменной, по умолчанию `0.2`.
- [ ] Нет `unwrap()` / `expect()` в рабочих путях (кроме разбора конфигурации при старте — с понятными сообщениями).
- [ ] Все тексты UI — через i18n; тест паритета EN/RU зелёный.
- [ ] В ключевых местах комментарии объясняют **почему**: порядок правил агрегата, порядок применения событий в проекциях,
      идемпотентность outbox-обработчика, безопасность replay. Демо — учебный материал.
- [ ] `README.md` описывает запуск, архитектуру и демо-сценарий.

---

## 12. Правила для агента

1. Не изменять `cqrsx/` и файлы вне `demo/`. Исключения: Фаза 0.5 (`execute_with_metadata`, §10) отдельным
   коммитом; `exclude = ["demo"]` в корневом `Cargo.toml`; при необходимости корневой `.dockerignore` (§5.2).
2. Фаза 0 выполнена: `docs/API_MAPPING.md` и §15 — источник истины по API `cqrsx`. Если при реализации
   обнаружится новое расхождение — следовать реальному API, сохраняя наблюдаемое поведение из плана, и дописать
   отличие в `docs/API_MAPPING.md` §4.
3. Нужной возможности в `cqrsx` нет — применить fallback из §13 / §15; библиотеку не «допиливать» сверх Фазы 0.5.
   Fallback невозможен — остановиться и описать блокер.
4. В рантайме демо никаких моков и in-memory хранилищ (MemoryStore и фейки — только в тестах).
5. Код, комментарии и коммиты — по-английски; тексты UI — через i18n (RU/EN).
6. Простота важнее универсальности: без DI-фреймворков, UI-библиотек и «задела на будущее».
7. Учётные данные — только dev, как в корневом compose; секретов не коммитить.
8. `make reset` (`down -v`) стирает журнал событий — в README это помечено предупреждением.

---

## 13. Неизвестные (закрыты в Фазе 0)

Все шестнадцать пунктов закрыты: факты, сигнатуры и решения — в `demo/docs/API_MAPPING.md` (§2 сводка, §3
карточки). Таблица сохранена как исторический контекст; колонка «Fallback» больше не действует, действуют
решения §15.

Пути — относительно `cqrsx/src/`.

| # | Вопрос | Где смотреть | Fallback |
|---|---|---|---|
| U1 | Как задачи привязываются к команде: prebuilt `NewTask` в executor, через services или выводятся из событий? | `cqrs/command/executor.rs`, `postgres/command.rs`, `postgres/outbox/*`, тесты | prebuilt из данных команды (как в плане) |
| U2 | Допустимо ли несколько событий в одном коммите с outbox («One event, optional checkpoint and explicit tasks»)? | `postgres/outbox/write_plan.rs` | убрать `ConcertSoldOut`; sold-out — производное поле |
| U3 | Передаются ли метаданные событий при выполнении команды? | `cqrs/command/executor.rs`, `EventEnvelope` | `recorded_at` — полем payload событий (`announced_at`, `booked_at`, `cancelled_at`), back-end кладёт его в команду |
| U4 | Умеет ли `ViewProjection` вычислять view-id из события? | `cqrs/projection.rs` | собственный `EventHandler` `FanWalletsProjection` |
| U5 | Схема outbox: имя таблицы, колонки (id, формат, версия, payload, metadata, статус, счётчик попыток, лимит, время доступности, lease, ошибка, метки времени), значения статусов; удаляются ли завершённые строки | `TRANSACTIONAL_OUTBOX_SCHEMA`, `postgres/task_queue/*` | константы sidecar и `outboxStatus.ts`; «призрачные» строки, если удаляются |
| U6 | Настраивается ли backoff (`redelivery_backoff`)? | `task/queue/retry.rs`, `PostgresTaskQueueConfig` | дефолт; UI показывает реальное время по БД; упомянуть в README |
| U7 | API `TaskHandlerError` (retry / permanent), `TaskAttempt` (`task_id`, `number`), требования `TaskDispatcherFactory` | `task/handler.rs`, `task/attempt.rs`, `task/runtime/dispatcher_factory.rs` | — (разобраться обязательно) |
| U8 | API `EventReplay` / `ReplayStream` (все id и один id), поведение при ошибке обработчика. *Снято в 03/05 (2026-10-25): replay и потоки удалены, пересборку ведёт процессор (`request_rebuild`, `request_rebuild_key`).* | `cqrs/event_handler/replay.rs`, `cqrs/store/stream.rs`, `postgres/event_repository/stream.rs` | — |
| U9 | Где SQL-файлы схем для корневого compose; путь тома PG 18; флаги SELinux | корневой `compose.yaml`, `postgres.rs` (`include_str!`) | применять константы при старте back-end (§5.2) |
| U10 | Какие env-ключи читает `cqrsx` (`OUTBOX_QUEUE_CAPACITY` и др.), дефолты, кросс-валидация бюджетов (lease vs задержка) | `task/runtime/config.rs`, `env.rs`, `postgres/task_runtime.rs` | задать значения кодом |
| U11 | Feature-флаги `cqrsx`, версия `sqlx` | `cqrsx/Cargo.toml` | — |
| U12 | Как включить снапшоты (`SnapshotPolicy` приватна) и схема `snapshots` | `cqrs/store/persisted.rs`, `PostgresCommandConfig` | без снапшотов: панель пишет «снапшоты выключены» |
| U13 | Сигнатура `host::lifecycle::run` (как передать axum как inbound) | `host/lifecycle.rs` | — |
| U14 | Формат `event_version` для `NumericVersion` (`"1"` или `"1.0"`) | `cqrs/store/upcaster.rs` | — |
| U15 | Есть ли в `events` глобальная позиция или метка времени для сортировки «последних событий»? | `EVENT_STORE_SCHEMA` | сортировка по `metadata->>'recorded_at'`, затем `sequence` |
| U16 | Наблюдаемы ли конфликт-ретраи (для «Гонки»)? | `cqrs/command/retry.rs` | показывать только исходы, без числа конфликтов |

---

## 14. Вне рамок

Аутентификация и мультиарендность; настоящая отправка почты; продакшен-деплой; саги / process managers;
горизонтальное масштабирование back-end в compose демо (процессоры это умеют — аренда закладки, сцена на стенде `load/chaos/`, D-21 — но лента событий живёт в памяти ноды);
мобильная вёрстка; отдельный CI-job для демо (можно добавить позже).

Идеи для `[P2]`: requeue упавших задач кнопкой (`REQUEUE_FAILED_OUTBOX_TASK`); «потерянный ack» — успех после
эффекта превращается в retry, и видно, как идемпотентность подавляет дубликат (`deliveries = 2`); апкастинг
`SeatBooked` v1 → v2 (`EventUpcaster`, версия `"2.0"`); «живой» rebuild без lock; e2e-тест на Playwright;
сага с компенсацией — лимит билетов переезжает в `Fan`, при отказе задача шлёт `Concert.ReleaseSeat`
(вариант B из обсуждения Фазы 0); наблюдатель `EventStore` для `committed_sequence` и счётчика конфликтов в
ответе команды («Гонка»); таймлайн фаната в машине времени.

---

## 15. Решения по итогам Фазы 0 и action items

Источник фактов — `demo/docs/API_MAPPING.md` (далее AM). Каждое решение уже внесено в разделы выше; здесь —
контрольный список для реализации. Статус: ☐ не сделано.

| ID | Решение | Где в плане | Action items для реализации |
|---|---|---|---|
| **D-01** | **Два агрегата.** `Concert` (места, все бизнес-правила R1–R8 синхронно) и `Fan` (билеты зрителя), связанные цепочкой outbox: `SeatBooked` → `encore_record_ticket` → `Fan.RecordTicket` → `TicketRecorded` → `encore_send_e_ticket` → письмо; отмена зеркально. `Fan` получает команды только из задач. | §1, §2.1, §2.3–2.8, §3.2, §3.5, §4, §6.2–6.4, §6.8, §8.3, §9 | ☐ `domain::fan` с R9–R11 (Фаза 2) ☐ `FanCommands` с `execute_with_metadata` (Фаза 3) ☐ обработчики `record_ticket` / `record_cancellation` с таблицей исходов §2.7 (Фаза 4) ☐ `FanWalletView: View<Fan>` через `ViewProjection` (Фаза 5) ☐ второй проход replay (Фаза 6) ☐ бейдж агрегата и «⏳ записывается в кошелёк» в UI (Фаза 8) |
| **D-02** | **Ровно одно событие на принятую команду;** отклонённая не оставляет следа. Пустой успех не используется. Причина: `PostgresEventRepositoryWithOutbox` коммитит один `event` и его задачи (AM §1 КТ-1). | §1, §2.4, §6.9 | ☐ тест инварианта на обоих агрегатах (Фаза 2) ☐ в `handle` каждой команды ровно один `sink.write` на пути успеха |
| **D-03** | **`ConcertSoldOut` снят.** `sold_out` — производное (`free_seats() == 0` в агрегате, `sold == capacity` в витрине). Межагрегатная цепочка уже демонстрирует «задача → команда», самокоманда `CloseSales` не нужна. | §2.4, §2.6, §2.8, §8.8, §9 | ☐ `ConcertEvent` без `ConcertSoldOut` ☐ конфетти только по витрине |
| **D-04** | **Метаданные через библиотеку:** `CommandExecutor::execute_with_metadata` добавляется в `cqrsx` (Фаза 0.5). Демо держит два долгоживущих `CommandExecutorCore`, без декоратора `EventStore`. Следствие: `committed_sequence` и счётчик конфликтов в ответе команды отсутствуют (P2). | §2.4, §6.3 п.7, §6.4, §10 Фаза 0.5, §12.1 | ☐ коммит в `cqrsx` с тестами и зелёным CI-набором из `CLAUDE.md` ☐ `events.metadata` проверяется в приёмке Фазы 3 |
| **D-05** | **Имена форматов задач** только `[A-Za-z0-9_]`: `encore_record_ticket`, `encore_send_e_ticket`, `encore_record_cancellation`, `encore_issue_refund`, все версии 1. Причина: `diagnostics::safe_type` печатает иные имена как `unknown`. | §2.7, §6.4 | ☐ `TYPE` констант ровно такие |
| **D-06** | **Обе витрины — `ViewProjection`** (`view_id == aggregate_id`); `FanWalletsProjection` и `FanOutHandler` из версии 1.0 удалены. | §2.8, §4, §6.2, §6.3 п.6 | ☑ нет собственных `EventHandler`; витрины ведут процессоры (эпик 03) |
| **D-07** | **Конфигурация runtime:** C=2, Q=2, poll 300 мс, H=5 с, L=30 с, S=3 с через ключи `OUTBOX_*` с демо-дефолтами (`from_lookup`); `RUST_LOG` — один уровень. | §2.7, §5.4, §6.3 | ☐ `Config::from_env` с проверкой `latency_max < H` ☐ `.env.example` |
| **D-08** | **Backoff почтовых задач — через `retry_after`** (1, 2, 4, 8, 8 с); задачи цепочки без хаоса; `reason` = код причины (виден в `last_error`). | §2.7, §5.4, §8.5 | ☐ `ENCORE_TASK_RETRY_BASE_MS` / `_MAX_MS` ☐ `outboxStatus.ts` по колонке `status` |
| **D-09** | **Инфраструктура:** `connect_pool(PgConnectOptions)`; `cqrsx` с `features = ["host", "logging"]` + `async-trait`; `sqlx 0.8`; `rand 0.10`; compose монтирует `../cqrsx/sql/*.sql` с `:ro,z`; `demo/.gitignore`. | §4, §5.1, §5.2, §6.1, §6.3 п.3 | ☐ Фаза 1 |
| **D-10** | **Graceful shutdown:** SSE-потоки завершаются по `CancellationToken`; после `host::lifecycle::run` — `pool.close()`. | §6.3 п.11, §10 Фаза 4 | ☐ приёмка «Ctrl+C при открытой вкладке» |
| **D-11** | **Replay:** два `replay_all` (по типу агрегата), порядок событий по `sequence` поперёк агрегатов; `rebuild.started` несёт `total`; verify P1 — во временные коллекции, без in-memory фейков. *Заменено D-16…D-20 (03/04): пересборку ведёт процессор; `EventReplay` удалён в 03/05.* | §2.9, §6.8 | ☒ заменено |
| **D-12** | **Sidecar:** сортировка `events` по `metadata->>'recorded_at'` (строка фиксированной ширины), outbox по `created_at`, строки outbox не удаляются, `last_error` показывается. Запросы — AM §6. | §7.3, §8.5 | ☐ Фаза 7 |
| **D-13** | **Граница back-end ↔ sidecar (аудит 2026-10-17, `docs/BOUNDARY_AUDIT.md`).** Back-end — production-подобный сервис кассы; всё demo-only и удаляющее данные — у sidecar: 🧨 wipe (`POST /inspect/projections/wipe`), письма фаната (`GET /inspect/mail/{fan_id}`), персонажи (`GET /inspect/demo`), имитация почтового сервиса (`POST /mail`). Back-end зовёт почту по HTTP (`HttpMailer`) и к базе `mail_service` не подключается. Счётчики журнала — в `cqrsx` (`PostgresEventRepository::totals`, тикет 08). Ограничения «sidecar без `cqrsx`» и «только read-only» сняты. Открытые вопросы — `docs/discussions/`. | §2.7, §2.9, §3.5, §5.4, §6.2, §6.4, §7 | ☑ реализовано 2026-10-17 |
| **D-14** | **Журнал `Fan` — на пару «фанат + концерт»:** `aggregate_id = <fan_id>:<concert_id>`, `fan_wallets._id` тот же; домен `Fan`, wire-форматы событий и снапшота, `ViewProjection`, rebuild и verify не меняются. Кошелёк фаната собирается при чтении из диапазона `_id` `[<fan>:, <fan>;)` и отвечает `{ fan_id, ledgers, view }`. Причина: нагрузочный прогон 03b 2026-10-18 — состояние, снапшот и документ кошелька одного фаната росли без границы, стоимость задачи O(n), слив outbox 120 → 12 задач/с, потери проекции (`docs/discussions/06_fan_ledger_growth.md`). Данные демо пересоздаются (`make reset`). | §2.1, §2.3, §2.8, §3.2, §6.4 | ☑ реализовано 2026-10-18 |
| **D-15** | **Документ без истории не создаётся.** Документ витрины хранит `last_sequence` и принимает только следующее событие своего агрегата (`cqrsx`, эпик 03/01: повтор пропускается, дыра — `SequenceGap` без записи); пересборка и лечение дыры заменяют документ свёрткой истории с начала. Флаг `incomplete` удалён из вида, API и UI. Совместимость со старыми базами не поддерживается: проект не выпущен, данные пересоздаются (`make reset`). | §2.8, §2.9, §6.7, §8.6 | ☑ реализовано 2026-10-24 |
| **D-16** | **Касса открыта во время Encore!:** пересборка идёт на месте, команды принимаются, `BoxOfficeLock`, `423` и `ENCORE_CHAIN_MAX_ATTEMPTS` удалены. | §2.9, §3.4, §6.4, §6.8 | ☑ реализовано 2026-10-25 |
| **D-17** | **Ответ команды — «принято» сразу после коммита**, без `projection_failed`; состояние витрины — SSE `projection.updated` и `/api/projections/status`; UI ждёт `projection.updated` своего документа вместо немедленного чтения. | §2.8, §6.6, §8 | ☑ реализовано 2026-10-25 |
| **D-18** | **`pace_ms` вместо `speed`** в Encore!: пауза между событиями одного ключа; верхняя граница `ENCORE_REPLAY_MAX_PACE_MS`. | §2.9, §6.4 | ☑ реализовано 2026-10-25 |
| **D-20** | **Verify сравнивает документ со свёрткой применённой им части истории;** отстающие и отсутствующие считаются в `behind`, расхождение — только неверный документ; стенд проверяет после догона и требует ноль отстающих. | §2.9 | ☑ реализовано 2026-10-25 |
| **D-21** | **Сцена «убитая нода» — только на стенде** (`load/chaos/`); compose демо с одной нодой, потому что лента событий живёт в памяти ноды. | §3.4 | ☑ стенд 2026-10-25 |
| **D-22** | **Нагрузочные и стресс-прогоны не дольше 5 минут;** часовой soak не нужен. | `load/` | ☑ принято 2026-10-24 |
| **D-23** | **Полный замер тикета 04 — 42 прогона** с отставанием проекций p50/p99 в каждой точке. | `load/` | см. `PROGRESS.md` |
| **D-24** | **Отставание считает `PostgresEventRepository::count_after(position, cap)`** через порт `EventLedgerStats`, потолок `ENCORE_PROJECTION_LAG_CAP` (100 000). | §2.9, §6.8 | ☑ реализовано 2026-10-25 |
| **D-25** | **Историческое решение: задачи планируются из решения агрегата** (эпик 04, тикет 20). Агрегат решает только события; процесс — один тип с `TaskPlanner<Concert>` и `TaskPlanner<Fan>`; исполнитель регистрирует кортеж процессов. Метаданные — карта; ядро читает из неё `Causation`, штампует задачи и выводит их id (UUID v5). Отмена без предварительного чтения концерта. | §2.4, §2.7 | ☑ реализовано 2026-10-26 |
| **D-26** | **Реакции по агрегату в модуле; процесс читается от события по типам задач.** Заменяет D-25: `Reactions` определяет задачи из фактов, функции-обработчики и их политики регистрирует модуль; инфраструктуру собирает `Cqrs`. | §2.4, §2.7, §3 | ☑ реализовано в эпике 05 |

Открытых вопросов перед Фазой 0.5 нет.

---

## Приложение A. Тексты для UI (RU / EN)

Тексты можно улучшать, но смысл и аналогии (кассовая книга, витрина, конверт в ящике исходящих, «на бис»)
сохранять — на них держится понятность для неспециалиста.

### A.0 Словарь демо и UI

Одно понятие — одно слово на каждом языке интерфейса (`front-end/src/i18n/{ru,en}.ts`) и одно имя в коде. Правила
имён библиотеки — `doc/components.md`, раздел «Naming and integration rules»; эта таблица — слова демо. Тексты
приложения и i18n приводятся к ней при переименованиях демо (эпик 08, задача 05): до того в них ещё встречаются
«закладка» и «bookmark» для снимка, «ledger» для журнала и «replay» для пересборки.

| Понятие | ru UI | en UI | В коде |
|---|---|---|---|
| журнал событий | журнал | journal — не «ledger» | `events` |
| снимок агрегата | снимок — не «снапшот», не «закладка» | snapshot — не «bookmark» | `snapshots` |
| ревизия снимка или документа | ревизия — не «версия» | revision | `revision` |
| закладка проекции | закладка | checkpoint | `projection_checkpoints` |
| аренда закладки | аренда закладки | checkpoint lease | `projection.lease_changed`, `activity.lease.*` |
| витрина | витрина | read model — не «projection» | `concert_halls`, `fan_wallets` |
| проекция (код, процессор) | проекция | projection | `ProjectionProcessor` |
| пересборка | пересборка, Encore! — не «replay» | rebuild, Encore! — не «replay» | `request_rebuild`, `rebuild_pace` |
| стереть витрины | стереть витрины | wipe read models | sidecar |
| сбросить демо | сбросить демо | reset the demo | sidecar |
| лента записей ноды | лента — только SSE | activity | группа ключей `activity.*` (не `feed.*`) |
| журнал фаната | журнал фаната | fan's ledger | `FanLedgerId` |
| задача, исполнитель | задача, воркер | task, worker | `transactional_outbox` |
| отказ команды | команда отклонена | rejected | `Rejection` |
| конфликт записи | параллельное изменение | conflict | `Conflict` |
| отставание | отстаёт событий | lag | `lag`, `lag_capped` |
| состояния закладки | в строю, пересборка, запускается, без владельца, застряла | live, rebuilding, starting, unclaimed, stalled | `encore.states.*` |
| лечение дыры | лечение дыры | healed | `RebuildReason::Healed` |
| витрина обновлена | витрина обновлена | read model updated | `projection.applied` |
| не витрина | НЕ витрина — внешний сервис | NOT a read model — external service | `inbox` |

### A.1 Баннер «Как читать этот экран»

- **RU:** Слева — касса концертного зала: то, что видит фанат. Справа — рентген: что прямо сейчас лежит в базах
  данных. Сверху — путь, который проходит каждая команда. Кликните по любому месту и проследите, что изменится.
- **EN:** On the left is the box office — what a fan sees. On the right is an X-ray of what the databases hold right
  now. At the top is the path every command takes. Click any seat and watch what changes.

### A.2 Главы

**① Журнал событий / Event store**
- **RU:** Мы ничего не исправляем и не удаляем. Каждое принятое решение — «место B4 продано Fox» — дописывается
  новой строкой в таблицу `events`, как в кассовой книге. Текущее состояние зала — это все записи концерта,
  прочитанные по порядку. Каждые 10 событий движок кладёт закладку (snapshot), чтобы не перечитывать книгу с начала.
  *Попробуйте:* забронируйте место и найдите новую строку в 🐘 events; подвигайте ⏳ машину времени.
- **EN:** Nothing is ever edited or deleted. Every accepted decision — "seat B4 sold to Fox" — is appended to the
  `events` table, like a ledger. The current state of the hall is simply all of the concert's records read in order.
  Every 10 events the engine stores a bookmark (snapshot) so it doesn't have to reread the whole book.
  *Try it:* book a seat and find the new row in 🐘 events; drag the ⏳ time machine.

**② Команды и правила / Commands and rules**
- **RU:** Клик — это команда: просьба, а не факт. Агрегат «Концерт» восстанавливает себя из журнала и проверяет
  правила: место существует, оно свободно, не больше 4 билетов в одни руки. Нарушено правило — команда отклонена,
  и в журнал не попадает ни одной строки. *Попробуйте:* любую кнопку 🧪 или 🤖 перекупщика.
- **EN:** A click is a command — a request, not a fact. The Concert aggregate rebuilds itself from the ledger and
  checks the rules: the seat exists, it is free, no more than 4 tickets per fan. If a rule is broken, the command is
  rejected and not a single row is written. *Try it:* any 🧪 button or the 🤖 scalper.

**③ Transactional Outbox**
- **RU:** После покупки фанат должен получить e-ticket. Мы не отправляем письмо «прямо сейчас»: в той же
  транзакции, что и событие, в таблицу outbox кладётся задача «отправить билет» — как конверт в ящик исходящих.
  Записано либо и то и другое, либо ничего. Фоновый воркер забирает задачи и доставляет их. Наш почтовый сервер
  капризный: в 20 % случаев он «падает» 🎲, и задача повторяется позже. Билет всё равно дойдёт — и ровно один раз.
  *Попробуйте:* забронируйте место и проследите за задачей в 🐘 outbox и письмом в 📬.
- **EN:** After buying a seat, the fan must get an e-ticket. We don't send the email "right now": in the same
  transaction as the event, a "send ticket" task goes into the outbox table — like an envelope dropped into the
  out-tray. Either both are written or neither. A background worker picks tasks up and delivers them. Our mail server
  is moody: it fails 20 % of the time 🎲 and the task is retried later. The ticket still arrives — exactly once.
  *Try it:* book a seat and follow its task in 🐘 outbox and the email in 📬.

**④ Витрины / Projections**
- **RU:** Экрану нужен удобный вид данных, а не журнал. Обработчики событий превращают события в документы MongoDB:
  схему зала из журнала концерта и кошелёк из журнала фаната. Один клик — два потока событий — две разные витрины,
  и кошелёк догоняет зал на шаг позже. MongoDB здесь — витрина, которую не страшно разбить: правда хранится только
  в журнале PostgreSQL.
  *Попробуйте:* запустите 🌊 толпу и сравните документы `concert_halls` и `fan_wallets`.
- **EN:** A screen needs data shaped for it, not a ledger. Event handlers turn events into MongoDB documents: a seat
  map from the concert's ledger and a wallet from the fan's ledger. One click, two event streams, two different read
  models, and the wallet catches up one step later. MongoDB is a shop window that is safe to break: the truth lives
  only in the PostgreSQL ledger.
  *Try it:* run the 🌊 crowd and compare the `concert_halls` and `fan_wallets` documents.

**⑤ Encore! / Replay**
- **RU:** Сотрите витрины — зал погаснет, но правила продолжат работать: решения принимаются по журналу, а не по
  витрине. Нажмите «Encore!» — каждый документ пересоберётся на месте по своей истории, а касса всё время открыта. Вместе с витринами
  пропали и закладки процессоров, поэтому они сами перечитывают журнал с начала. Письма при этом повторно не
  отправляются: внешние действия живут в outbox, а не в витринах.
  *Попробуйте:* 🧨 стереть → кликнуть занятое место → ⏪ Encore! в замедленном режиме и забронировать место.
- **EN:** Wipe the read models — the hall goes dark, but the rules keep working: decisions are made from the ledger,
  not the shop window. The processors lost their checkpoints too, so they read the ledger again from the start.
  Press "Encore!" — every document is rebuilt in place from its history while the box office stays open. No email is
  re-sent: side effects live in the outbox, not in read models.
  *Try it:* 🧨 wipe → click a taken seat → ⏪ Encore! in slow motion and book a seat.

### A.3 Пояснения панелей (ⓘ) и состояний

| Ключ | RU | EN |
|---|---|---|
| `panels.events` | Журнал событий. Только INSERT — никогда UPDATE или DELETE. Ключ — (aggregate_type, aggregate_id, sequence); sequence — номер события внутри одного концерта. Если два запроса пишут одно и то же sequence, побеждает первый — это оптимистическая блокировка. | The event ledger. INSERT only — never UPDATE or DELETE. Key: (aggregate_type, aggregate_id, sequence); sequence is the event number within one concert. If two requests write the same sequence, the first wins — that's optimistic locking. |
| `panels.snapshots` | Закладки: сохранённое состояние агрегата на событии N. Загрузка концерта = снапшот + события после него. | Bookmarks: the aggregate's saved state at event N. Loading a concert = snapshot + the events after it. |
| `panels.outbox` | Задачи, записанные в той же транзакции, что и событие. Воркер берёт задачу в аренду (lease), выполняет и фиксирует результат: готово — или повтор после паузы (backoff). Задача переживёт даже падение сервера. | Tasks written in the same transaction as the event. A worker leases a task, runs it and records the outcome: done, or retry after a pause (backoff). A task survives even a server crash. |
| `panels.concert_halls` | Витрина «схема зала»: один документ на концерт. Строится только из событий; её можно удалить и пересобрать. | Read model "seat map": one document per concert. Built only from events; safe to delete and rebuild. |
| `panels.fan_wallets` | Витрина «кошелёк фаната»: один документ на пару фанат + концерт (журнал фаната по этому концерту, агрегат Fan), который заполняется задачами outbox после каждой брони. Кошелёк на экране собран из всех журналов фаната. | Read model "fan wallet": one document per fan and concert (the fan's ledger for that concert, the Fan aggregate), which outbox tasks fill in after every booking. The wallet on screen is assembled from all of the fan's ledgers. |
| `panels.inbox` | Не витрина! Это «внешний почтовый сервис» (имитация). Replay его не трогает — письма не уходят повторно. | Not a read model! This is the "external mail service" (simulated). Replay never touches it — no email is sent twice. |
| `panels.activity` | Живой лог back-end: что происходит прямо сейчас. Хранится только в памяти. | Live back-end log: what is happening right now. Kept in memory only. |
| `states.race` | Оба запроса прочитали версию N и попытались записать N+1. База приняла только первый; второй повторился на свежем состоянии — и правило «место свободно» его отклонило. | Both requests read version N and tried to write N+1. The database accepted only the first; the second was retried on fresh state — and the "seat must be free" rule rejected it. |
| `states.history` | Это состояние нигде не хранится — оно вычислено из событий 1…{n}. | This state isn't stored anywhere — it was computed from events 1…{n}. |
| `states.wiped` | Витрины пусты. Журнал цел: процессоры перечитывают его с начала, и зал вернётся сам. | Read models are empty. The ledger is intact: the processors read it again from the start, and the hall comes back by itself. |
| `states.replaying` | ⏪ Encore!: витрины пересобираются по журналу на месте. Касса открыта — бронируйте. | ⏪ Encore!: the read models are rebuilt from the ledger in place. The box office stays open — book away. |

### A.4 Сообщения правил

| `code` | RU | EN |
|---|---|---|
| `concert_not_found` | Такого концерта нет — его ещё не анонсировали | This concert hasn't been announced |
| `already_announced` | Этот концерт уже анонсирован | This concert has already been announced |
| `unknown_seat` | Места {seat} нет: ряды A–E, места 1–8 | Seat {seat} doesn't exist: rows A–E, seats 1–8 |
| `sold_out` | Sold out! Билеты закончились | Sold out! No tickets left |
| `seat_taken` | Место {seat} уже занято ({by}) | Seat {seat} is already taken by {by} |
| `ticket_limit` | Правило против перекупщиков: не больше {limit} билетов в одни руки | Anti-scalper rule: max {limit} tickets per fan |
| `seat_not_booked` | Место {seat} никто не бронировал | Nobody has booked seat {seat} |
| `not_your_ticket` | Это не ваш билет — отменить может только владелец | Not your ticket — only the owner can cancel it |
| `rebuild_in_progress` | Encore! уже идёт | Encore! is already running |

### A.5 Причины «капризов» почтового сервера

Суффикс к любой причине — RU: «(имитация, шанс 20 %) — повтор запланирован», EN: "(simulated, 20 % chance) —
retry scheduled".

| `reason_code` | RU | EN |
|---|---|---|
| `smtp_timeout` | 📡 Почтовый сервер не ответил вовремя | 📡 The mail server timed out |
| `server_on_fire` | 🔥 Почтовый сервер перегрелся | 🔥 The mail server is on fire |
| `rate_limited` | 🐌 Провайдер ограничил скорость | 🐌 Rate-limited by the provider |
| `mailbox_full` | 📦 Ящик получателя переполнен | 📦 The recipient's mailbox is full |

### A.6 Подписи `PipelineStrip`

RU: Сервис Concert · Сервис Fan · проекция · имена элементов как в EN (user, Command, Aggregate (rules),
events + outbox, Outbox worker → Fan, Outbox worker → email, mail server, email, read database, UI) ·
ничего не записано · ⏪ replay · пусто · «клик по блоку — подробности»
EN: Concert service · Fan service · projection · user · Command · Aggregate (rules) · events + outbox ·
Outbox worker → Fan · Outbox worker → email · mail server · email · read database · UI · nothing written ·
⏪ replay · empty · "click a block for details"

Пояснения блоков (`explain.<id>`: заголовок, вводная фраза, пункты; у агрегатов — таблица правил) — в
`front-end/src/i18n/explain.{en,ru}.ts`.

---

## Приложение B. Рассмотренные альтернативы домена

| Домен | Плюсы | Почему не выбран |
|---|---|---|
| Банковский счёт | классика ES | скучно; на нём построены примеры upstream `cqrs-es` |
| Тамагочи | очень весело | «время» (голод растёт сам) требует планировщика команд; outbox-сценарий натянут |
| Пицца-трекер | понятный жизненный цикл | больше состояний и шагов; нет наглядной «сетки», которая гаснет и загорается при replay |
| Краудфандинг | естественный outbox (платёж) | меньше интерактивности; прогресс-бар менее выразителен, чем схема зала |
| Кино / авиабилеты | та же механика мест | концерт веселее (персонажи, перекупщик, SOLD OUT) и обходится без тарифов и багажа |

**Почему концерт.** Бронь места знакома всем; правила интуитивны («место занято», «не больше 4 в одни руки»);
e-ticket — естественный побочный эффект, который нельзя потерять (идеальный повод для outbox); схема зала — яркая
визуальная витрина, которую эффектно стереть и восстановить; гонка за одно место сама объясняет оптимистическую
блокировку; а название *Encore* («на бис») само объясняет replay.

---
