# 08. Крейты `cqrsx-postgres`, `cqrsx-mongodb`, `cqrsx-fixtures`; ядро без фич

| | |
|---|---|
| Статус | сделано 2026-10-31 ([результаты](#результаты)) |
| Ценность | высокая: ядро не может зависеть от sqlx и MongoDB — это проверяет компилятор; в ядре ни одного `cfg(feature)` |
| Цена | средняя–высокая: два крейта, общие тестовые фикстуры, перенос половины интеграционных тестов |
| Зависимости | 07 |

## Сейчас

Адаптеры уже чистые: из закрытых частей ядра они берут только функции логирования sqlx и MongoDB (`diagnostics`) и
тестовые помощники (захват логов, фикстуры контейнеров в `tests/support`). Ядро держит фичи `postgres` и `mongodb` ради
них и ради движка.

Исключение одно — ядро само называет `sqlx` (уточнение оркестратора перед задачей): логи ядра `log_command_failure` и
`log_projection_batch_failure` ищут в цепочке источников ошибки `sqlx::Error` и добавляют поля `storage_category` и
`sqlstate` (`tests/log_fallback.rs` проверяет `storage_category="pool_closed"`). Без `sqlx` в ядре эти поля пропали бы,
поэтому нужен способ, которым адаптер передаёт ядру категорию и код ошибки хранилища, не называя `sqlx` в ядре.

## Станет

| Сейчас | Станет |
|---|---|
| `src/postgres.rs` (схемы SQL), `src/postgres/*` | `cqrsx-postgres/src/lib.rs`, `src/*` |
| `cqrsx/sql/*` | `cqrsx-postgres/sql/*` |
| `diagnostics.rs` (`sqlx_kind`, `sqlstate`, `log_sqlx_failure`, `safe_type` — что нужно адаптеру) | `cqrsx-postgres/src/diagnostics.rs`, закрытый |
| `cqrsx/tests/postgres.rs`, `tests/postgres/*` | `cqrsx-postgres/tests/…` |
| `src/mongodb.rs`, `src/mongodb/*` | `cqrsx-mongodb/src/lib.rs`, `src/*` |
| `diagnostics.rs` (`mongodb_kind`, `log_mongodb_failure`) | `cqrsx-mongodb/src/diagnostics.rs`, закрытый |
| `cqrsx/tests/mongo.rs`, `tests/mongo/*` | `cqrsx-mongodb/tests/…`; `journal_in_postgres` — с dev-dependency на `cqrsx-postgres` |
| `tests/support/{postgres,mongo,port_lease}.rs`, `diagnostics::test_support::capture`, `env::test_support` | `cqrsx-fixtures` (`publish = false`, от `cqrsx` не зависит) |

- **Ядро без фич:** `[features]` удаляется, `sqlx`, `mongodb`, `futures` уходят из зависимостей ядра. Ядро и адаптеры
  берут `cqrsx-fixtures` как dev-dependency: он не зависит от `cqrsx`, поэтому вторых копий типов ядра в юнит-тестах нет.
- **Движок:** `postgres = ["dep:cqrsx-postgres", "dep:sqlx"]`, `mongodb = ["postgres", "dep:cqrsx-mongodb",
  "dep:mongodb"]`. Тесты движка на PostgreSQL берут фикстуры из `cqrsx-fixtures`.
- **Лицензия:** код адаптера PostgreSQL производен от cqrs-es — `licenses/Apache-2.0.txt` и строка атрибуции
  копируются в `cqrsx-postgres`.
- **Покрытие по крейтам** (решение 10 эпика): рабочая область ≥ 97 %, `cqrsx`, `cqrsx-postgres`, `cqrsx-mongodb`,
  `cqrsx-engine` — каждый ≥ 97 %. Задача 01 подготовила ядро к прогону без чужих тестов.

## Поведение не меняется: чем доказано

Тексты SQL и схемы байт в байт (файлы переезжают без правок; `tests/postgres/schema.rs` проверяет то же), тесты
адаптеров переезжают без изменений проверок, число тестов прежнее, эталоны демо байт в байт. Прогоны покрытия — по
каждому крейту и по рабочей области.

## Демо и документы

Демо не называет адаптеры напрямую в коде, кроме тестов (`write_side.rs` — `cqrsx_postgres::…` в dev-dependencies);
`tests/architecture.rs`: запреты `cqrsx::postgres`, `cqrsx::mongodb` → `cqrsx_postgres`, `cqrsx_mongodb`. `CLAUDE.md`
(крейты, команды тестов и покрытия, где лежат фикстуры), `components.md`, README.

Схемы SQL переезжают в `cqrsx-postgres/sql`, поэтому меняются пути монтирования в `compose.yaml` (корень),
`demo/compose.yaml`, `demo/load/compose.yaml` и `demo/README.md`. Dockerfile'ы демо (`back-end`, `sidecar`) копируют
корневой `Cargo.toml` и каждый крейт фреймворка. Сейчас они не копируют ни `Cargo.toml`, ни `cqrsx-engine`, и с задачи 07
образы не собираются: `failed to find a workspace root` (находка оркестратора).

## Результаты

2026-10-31, на HEAD `ac7d3fe` плюс правки задачи.

- **Крейты.**
  - `cqrsx-postgres`: журнал и снимки, outbox, очередь задач, контрольные точки, витрины, пулы, `sql/`; копия
    лицензии и строка атрибуции postgres-es.
  - `cqrsx-mongodb`: витрины и контрольные точки.
  - `cqrsx-fixtures` (`publish = false`, от `cqrsx` не зависит): контейнеры PostgreSQL и MongoDB, аренда портов,
    захват логов, тесты в дочернем процессе.

  Всё перенесено `git mv`, SQL — без правок. В ядре нет ни одного `cfg(feature)`, из его зависимостей ушли `sqlx`,
  `mongodb`, `futures`, `testcontainers`.
- **Поля хранилища в логах ядра.** Публичный `cqrsx::diagnostics::StorageFailure` несёт категорию и код отказа;
  `Display`, `Debug` и `source()` у него — как у причины, сама ошибка драйвера доступна через `cause()`. Адаптер
  PostgreSQL оборачивает ошибку драйвера там, где она становится ошибкой ядра (пять мест). Ядро ищет `StorageFailure`
  по цепочке и пишет прежние `storage_category` и `sqlstate`, не называя `sqlx`.
  - `tests/log_fallback.rs` переехал в `cqrsx-postgres` без изменения утверждений.
  - Тесты логов ядра переведены с `sqlx::Error` на `StorageFailure`; добавлены тесты перевода ошибки драйвера в
    категорию и код.
  - Четыре проверки `is::<sqlx::Error>()` смотрят на причину через `cause()`.
- **`cqrsx::diagnostics` публичен** ради `safe_type` и `StorageFailure`: ими пользуются движок и `cqrsx-postgres`. Сами
  строки логов ядра закрыты.
- **Закрытые части ядра, которыми пользовались адаптеры:**
  - `claimed` и `Ledger` заменены малыми значениями в адаптерах;
  - `capture` и `isolated` переехали в фикстуры, прогрев и список переменных передаёт вызывающий;
  - временные `#[path]` и копии `capture`, `isolated`, `safe_type` из задачи 07 удалены.
- **Движок:** `postgres = ["dep:cqrsx-postgres", "dep:sqlx"]`, `mongodb = ["postgres", "dep:cqrsx-mongodb",
  "dep:mongodb"]`, `http` прежний.
- **Демо:**
  - Dockerfile'ы копируют корневой `Cargo.toml` и каждого члена `members`. С задачи 07 образы не собирались, это нашёл
    оркестратор перед задачей. Проверено `cargo metadata --locked --offline` на копиях контекстов сборки.
  - Пути схем исправлены в трёх `compose.yaml` и README.
  - Правило архитектуры называет `cqrsx_postgres` и `cqrsx_mongodb`; исчезнувшие пути `cqrsx::postgres` и
    `cqrsx::mongodb` убраны, синтетические исходники переведены на новые пути (равнозначно).
- **CI:**
  - матрица ядра — одна строка;
  - `property_` гоняется по ядру и обоим адаптерам, `stress_` — по адаптерам;
  - покрытие считается для каждого из четырёх крейтов.
- **Числа.** Тестов по-прежнему 877, ignored 0: два теста аренды портов считались трижды (минус 6), добавлены 6 новых.
  Демо — 175 тестов. Покрытие строк:

  | Рабочая область | `cqrsx` | `cqrsx-postgres` | `cqrsx-mongodb` | `cqrsx-engine` |
  |---|---|---|---|---|
  | 98,14 % | 97,78 % | 98,98 % | 97,88 % | 98,46 % |

  Эталоны демо без изменений.
- **Приёмка, круг 1:** лишняя dev-зависимость движка, фича `env-filter` у фикстур, `cqrsx-fixtures` в исключении
  правила 8, неточные комментарии Dockerfile'ов. Две длинные строки перенёс оркестратор.
