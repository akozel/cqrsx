# 02. Таблица гарантий, недостающие тесты и эталоны «до»

| | |
|---|---|
| Статус | сделано 2026-10-27 |
| Ценность | высокая: без неё требование 1 («поведение не меняется») нечем доказать |
| Цена | низкая: тесты и один инструмент стенда, код движка и демо по смыслу не меняется |
| cqrsx | ничего |
| demo | `back-end/tests/contracts.rs` (новый), `back-end/tests/golden/*`, `back-end/src/{config.rs, process.rs}` (чистые функции для эталонов), `docs/backlog/02_postgres_performance/perf/tools/statements.mjs` (новый), `perf/README.md` |
| Зависимости | нет |
| Гарантии | все строки [guarantees.md](guarantees.md); закрывает пробелы G-07, G-11 (частично), G-19, G-29, G-32, G-36, G-37 |

## Сейчас

Гарантии движка держат 566 тестов cqrsx и 132 теста демо (инвентарь — [guarantees.md](guarantees.md)). Но четыре
вещи, которые миграция меняет сильнее всего, тестом не покрыты как контракт:
- **действующая конфигурация демо** — 39 переменных окружения (32 `ENCORE_*`, которые читает бэкенд, и 7
  `OUTBOX_*`), их умолчания, производные значения (лимиты блокировок, настройки процессоров, бюджет аренды) и план
  пулов (`process.rs::postgres_pools`, `mongo_client`) живут в `config.rs` и `process.rs`, которые задача 12 удаляет;
- **лента активности** — 13 видов записей, их поля задают четыре разных места (`command_service.rs`,
  `activity_observer.rs`, `operations/rebuild.rs`, `http_mailer.rs`), а фронтенд читает их по именам;
- **паузы перед повтором задач** — формула почты (`min(base · 2^(n−1), max)`) и константа шагов цепочки (1 с)
  закопаны в `http_mailer.rs` и `workflows.rs`; задачи 07 и 09 переносят их в политики движка;
- **набор текстов SQL** — стенд пишет `pg_statements.tsv`, но ничто не сравнивает наборы двух прогонов.

## Станет

1. **Таблица гарантий** — [guarantees.md](guarantees.md) (первая версия написана вместе с бэклогом). Задача дополняет
   столбец «Тесты» именами новых тестов и проверяет, что у каждой строки есть хотя бы один тест.
2. **Эталоны в демо** — `back-end/tests/contracts.rs`, файлы в `back-end/tests/golden/`:
   - `effective_config.txt` — действующая конфигурация без переменных окружения, построчно `ключ = значение`,
     отсортировано: каждый `ENCORE_*` и `OUTBOX_*` с действующим значением; производные (лимиты блокировок,
     `ProcessorConfig` целиком, бюджет аренды, таймаут почтового запроса); план пулов (три пула PostgreSQL: размер,
     минимум, idle timeout, политика пинга, таймаут оператора; клиент MongoDB: пул, таймауты, имя приложения). Для
     этого `process.rs` получает чистую функцию `pool_plan(&Config) -> PoolPlan` (ею же пользуется `postgres_pools`),
     а `Config` — `describe() -> Vec<(String, String)>`. После задачи 12 то же описание даёт движок:
     `cqrs.settings().describe()` плюс настройки модулей, и файл должен совпасть байт в байт;
   - `activity_feed.txt` — для каждого из 13 видов записи: имя и отсортированный список полей. Записи строятся через
     нынешних производителей: исходы команды (`publish_outcome`), наблюдатель процессора (`ActivityObserver`),
     наблюдатель перестройки (`RebuildService`), почтовый клиент (`HttpMailer` против скриптованного сервера из его
     тестов). Поле `fan_id` у `command.*` — обязательный ключ (после 05 оно приходит из `#[label]`);
   - `retry_pauses.txt` — пауза перед повтором по номеру попытки 1…8 для трёх случаев: письма (1, 2, 4, 8, 8, 8, 8, 8
     с), шаги цепочки (1 с), без политики (границы `redelivery_backoff`: 4–6, 8–12, … с, не больше 300). После 07/09
     то же считают политики `MAIL`, `STEP` и умолчание движка.
3. **Инструмент стенда** `perf/tools/statements.mjs`: нормализованные тексты операторов одного прогона
   (`pg_statements.tsv` и `pg_statements.drain.tsv`) как отсортированное множество; режим сравнения двух партий
   печатает тексты, которые есть только в одной. Строка в `perf/README.md`.
4. **Проверочный прогон** `01-hot-key-vus40`, `db`, `REPEAT=1`, из дерева: подтверждает, что инструмент читает
   прогон; множество операторов записывается в раздел «Результат» этой задачи как точка отсчёта.
5. **Правило для следующих задач:** раздел «Поведение не меняется: чем доказано» каждой задачи ссылается на строки
   guarantees.md и на эталоны; задача, которая переименовывает тест, правит таблицу.

## Этапы

1. Прочитать guarantees.md против кода: каждая строка — существующий тест. Расхождения исправить в таблице.
2. `Config::describe`, `pool_plan`, `mongo_plan` — чистые функции без ввода-вывода, с тестами на две-три строки.
3. `tests/contracts.rs` и три эталона. Эталон пишется один раз руками (не из кода) и проверяется глазами: значения
   должны совпасть с `.env.example`.
4. `statements.mjs`, `node --check`, строка в `perf/README.md`.
5. Проверочный прогон стенда; результат в тикет.

## Зачем, на примере

Задача 12 заменяет `config.rs` (580 строк) на `Cqrs::from_env("ENCORE")`. Если движок прочитает
`ENCORE_PROJECTION_SETTLE_MS` с библиотечным умолчанием 0 вместо демо-умолчания 10 мс, ни один нынешний тест этого
не заметит: все они проверяют разбор переменной, а не действующее значение. Эталон `effective_config.txt` с
строкой `projection.settle = 10 ms` заметит.

## Риски

- **Эталон закрепляет случайное.** В эталон попадают только документированные значения из `.env.example` и
  `components.md`; строки, которые невозможно обосновать, в эталон не входят.
- **Лента: записи почтового клиента** требуют HTTP-сервера в тесте; используется скриптованный сервер из тестов
  `http_mailer.rs` (вынести в `testing.rs`).

## Как проверить

- `cd demo && cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test`.
- `node --check perf/tools/statements.mjs`; сравнение прогона с самим собой — пустая разница.
- Проверочный прогон: стенд `encore-load`, не дольше 5 минут.

## Поведение не меняется: чем доказано

Код движка и демо не меняет поведения: добавляются чистые функции описания (`describe`, `pool_plan`) и тесты.
`postgres_pools` строится из `pool_plan`, поэтому план в эталоне — тот, по которому процесс создаёт пулы (G-29).

## Критерии приёмки

- [x] У каждой строки guarantees.md есть тест; пробелы G-07, G-19, G-29, G-32, G-36, G-37 закрыты эталонами и
      инструментом.
- [x] `tests/contracts.rs` проходит; три эталона лежат в `tests/golden/` и совпадают с `.env.example`.
- [x] `statements.mjs` работает на реальном прогоне; множество операторов записано в тикет.
- [x] guarantees.md ссылается на новые тесты.

## Результат

**Сделано 2026-10-27.** Таблица гарантий сверена с кодом (129 имён тестов, все числа; исправлены G-32, G-36,
G-37), у каждой строки G-01…G-37 есть тест; G-11 закрыт частично (структурный тест сборки демо, отказ второй
регистрации — задача 11). Закрыты пробелы G-07, G-19, G-29, G-32, G-36, G-37.

- **Эталоны** `back-end/tests/golden/`, написаны руками; `tests/contracts.rs` (5 тестов) сравнивает с ними
  нынешних производителей:
  - `effective_config.txt` — 68 строк: 39 переменных (32 `ENCORE_*` + 7 `OUTBOX_*`) под своими именами, производные
    `projection.pages_in_flight`, `outbox.lease_budget`, `mail.request_timeout`, план пулов `postgres.*` (три пула,
    с умолчаниями SQLx из построенных `PgPoolOptions`) и `mongodb.*`; конфигурация из явно пустого окружения;
    отдельные тесты: процесс читает ровно эти 39 переменных, и каждая записана в `.env.example` с тем же значением;
  - `activity_feed.txt` — 18 строк на 13 видов: ключи JSON записи (с `at`, `id`, `type`), варианты по `outcome`,
    `reason`, `lease`, `retry`; `fan_id` — в каждой `command.*`;
  - `retry_pauses.txt` — 24 строки: `mail.N` 1, 2, 4, 8, 8, 8, 8, 8 с, `step.N` 1 с, `redelivery.N` — границы
    `4 s ..= 6 s` … `240 s ..= 300 s` (проверяются выборкой).
- **Чистые функции:** `Config::from_lookup`, `Config::describe`, `process::{pool_plan, mongo_plan}` с `describe`,
  `bootstrap::tasks::mail_settings`; `postgres_pools` и `mongo_client` строят пулы только из плана.
- **Инструмент стенда** `perf/tools/statements.mjs`: множество нормализованных текстов операторов по точкам и разница
  двух партий; правило нормализации, её цена и код выхода — в `perf/README.md`.
- **Проверочный прогон** `e05-02-01-hot-key-vus40-db` (`db`, 2 прогона, образ из дерева): 773 принятых/с,
  7.70 оператора и 4 818 Б WAL на команду, `loads/commit` = 1 — в диапазонах `e20s-`; множество операторов совпало с
  `e20s-` (образ задачи 04/20). Точка отсчёта — 12 операторов (16 сырых текстов, нормализация: `$n` и литералы → `?`):

```text
SELECT aggregate_type, aggregate_id, last_sequence, current_snapshot, payload FROM snapshots WHERE aggregate_type = ? AND aggregate_id = ?
SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata FROM events WHERE aggregate_type = ? AND aggregate_id = ? AND sequence > ? ORDER BY sequence
SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata, tx::text AS position FROM events WHERE aggregate_type = ? AND tx > ?::text::xid8 AND tx < pg_snapshot_xmin(pg_current_snapshot()) ORDER BY tx, aggregate_id, sequence LIMIT ?
SELECT count(*) AS events FROM (SELECT ? FROM events WHERE aggregate_type = ? AND tx > ?::text::xid8 LIMIT ?) AS capped
SELECT tx::text AS position FROM events WHERE aggregate_type = ? ORDER BY tx DESC LIMIT ?
WITH expired AS ( SELECT id FROM transactional_outbox WHERE status = ? AND locked_until <= statement_timestamp() ORDER BY locked_until, id LIMIT ? FOR UPDATE SKIP LOCKED ) UPDATE transactional_outbox AS o SET status = CASE WHEN o.attempts >= o.max_attempts THEN ? ELSE ? END, available_at = statement_timestamp() + LEAST( LEAST(? * power(?, LEAST(GREATEST(o.attempts - ?, ?), ?)), ?) * (? + random() * ?), ?) * interval ?, finished_at = CASE WHEN o.attempts >= o.max_attempts THEN statement_timestamp() ELSE ? END, last_error = ?, locked_by = ?, lock_token = ?, locked_until = ? FROM expired WHERE o.id = expired.id RETURNING o.id, o.task_type, o.task_version, o.attempts, o.max_attempts
WITH owned AS MATERIALIZED ( SELECT id, locked_until FROM transactional_outbox WHERE id = ? AND lock_token = ? FOR UPDATE ) UPDATE transactional_outbox AS o SET status = CASE WHEN ? = ? AND attempts >= max_attempts THEN ? ELSE ? END, available_at = CASE WHEN ? = ? AND attempts < max_attempts THEN statement_timestamp() + ? * interval ? ELSE available_at END, finished_at = CASE WHEN ? <> ? OR attempts >= max_attempts THEN statement_timestamp() ELSE ? END, last_error = ?, locked_by = ?, lock_token = ?, locked_until = ? FROM owned WHERE o.id = owned.id AND owned.locked_until > clock_timestamp() RETURNING o.status
WITH ready AS ( SELECT id FROM transactional_outbox WHERE status = ? AND task_type = ? AND task_version = ? AND available_at <= statement_timestamp() ORDER BY available_at, id LIMIT ? FOR UPDATE SKIP LOCKED ) UPDATE transactional_outbox AS o SET status = ?, attempts = attempts + ?, locked_by = ?, lock_token = gen_random_uuid(), locked_until = clock_timestamp() + ? * interval ? FROM ready WHERE o.id = ready.id RETURNING o.id, o.payload, o.metadata, o.attempts, o.lock_token
WITH written_events AS (INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (aggregate_type, aggregate_id, sequence) DO NOTHING RETURNING sequence) SELECT (SELECT COUNT(*) FROM written_events)::bigint AS written
WITH written_events AS (INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (aggregate_type, aggregate_id, sequence) DO NOTHING RETURNING sequence), written_tasks AS (INSERT INTO transactional_outbox (id, aggregate_type, aggregate_id, aggregate_sequence, task_type, task_version, payload, metadata, available_at, max_attempts) SELECT id, aggregate_type, aggregate_id, aggregate_sequence, task_type, task_version, payload, metadata, COALESCE(scheduled_at, statement_timestamp() + delay_ms * interval ?), max_attempts FROM (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS input(id, aggregate_type, aggregate_id, aggregate_sequence, task_type, task_version, payload, metadata, scheduled_at, delay_ms, max_attempts) WHERE EXISTS (SELECT ? FROM written_events) RETURNING id) SELECT (SELECT COUNT(*) FROM written_events)::bigint AS written
WITH written_snapshot AS (INSERT INTO snapshots (aggregate_type, aggregate_id, last_sequence, current_snapshot, payload) VALUES (?, ?, ?, ?, ?) RETURNING ?), written_events AS (INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) SELECT * FROM (VALUES (?, ?, ?, ?, ?, ?, ?)) AS input(aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) WHERE EXISTS (SELECT ? FROM written_snapshot) RETURNING sequence), written_tasks AS (INSERT INTO transactional_outbox (id, aggregate_type, aggregate_id, aggregate_sequence, task_type, task_version, payload, metadata, available_at, max_attempts) SELECT id, aggregate_type, aggregate_id, aggregate_sequence, task_type, task_version, payload, metadata, COALESCE(scheduled_at, statement_timestamp() + delay_ms * interval ?), max_attempts FROM (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS input(id, aggregate_type, aggregate_id, aggregate_sequence, task_type, task_version, payload, metadata, scheduled_at, delay_ms, max_attempts) WHERE EXISTS (SELECT ? FROM written_events) RETURNING id) SELECT COUNT(*)::bigint AS written FROM written_snapshot
WITH written_snapshot AS (UPDATE snapshots SET last_sequence = ?, current_snapshot = ?, payload = ? WHERE aggregate_type = ? AND aggregate_id = ? AND current_snapshot = ? RETURNING ?), written_events AS (INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) SELECT * FROM (VALUES (?, ?, ?, ?, ?, ?, ?)) AS input(aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) WHERE EXISTS (SELECT ? FROM written_snapshot) RETURNING sequence), written_tasks AS (INSERT INTO transactional_outbox (id, aggregate_type, aggregate_id, aggregate_sequence, task_type, task_version, payload, metadata, available_at, max_attempts) SELECT id, aggregate_type, aggregate_id, aggregate_sequence, task_type, task_version, payload, metadata, COALESCE(scheduled_at, statement_timestamp() + delay_ms * interval ?), max_attempts FROM (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS input(id, aggregate_type, aggregate_id, aggregate_sequence, task_type, task_version, payload, metadata, scheduled_at, delay_ms, max_attempts) WHERE EXISTS (SELECT ? FROM written_events) RETURNING id) SELECT COUNT(*)::bigint AS written FROM written_snapshot
```

Тесты демо: 145 бэкенда (было 132) и 14 сайдкара; переименованных нет. Задачи, которые меняют код из столбца «Код»
guarantees.md, ссылаются на эти эталоны в разделе «Поведение не меняется: чем доказано»; задача, которая
переименовывает тест, правит таблицу.
