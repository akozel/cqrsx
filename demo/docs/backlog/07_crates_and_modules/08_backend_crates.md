# 08. Крейты `cqrsx-postgres`, `cqrsx-mongodb`, `cqrsx-fixtures`; ядро без фич

| | |
|---|---|
| Статус | предложено |
| Ценность | высокая: ядро не может зависеть от sqlx и MongoDB — это проверяет компилятор; в ядре ни одного `cfg(feature)` |
| Цена | средняя–высокая: два крейта, общие тестовые фикстуры, перенос половины интеграционных тестов |
| Зависимости | 07 |

## Сейчас

Адаптеры уже чистые: из закрытых частей ядра они берут только функции логирования sqlx и MongoDB (`diagnostics`) и
тестовые помощники (захват логов, фикстуры контейнеров в `tests/support`). Ядро держит фичи `postgres` и `mongodb` ради
них и ради движка.

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
