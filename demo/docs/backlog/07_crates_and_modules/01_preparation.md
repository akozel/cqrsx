# 01. Подготовка: покрытие ядра без PostgreSQL и движка, число тестов «до»

| | |
|---|---|
| Статус | сделано 2026-10-31 ([результаты](#результаты)) |
| Ценность | высокая: после задачи 08 ядро меряется по покрытию отдельно, без тестов PostgreSQL, MongoDB и движка |
| Цена | низкая–средняя: тесты в памяти для нескольких файлов и запись чисел «до» |
| Зависимости | — |

## Сейчас

Покрытие `cqrsx` меряется вместе: тесты PostgreSQL (8 272 строки, половина интеграционных тестов) и движка покрывают и
код ядра. Без них ядро покрыто на 97,08 % строк (2026-10-31). Слабые места:

| Файл | Строки | Кто покрывает сейчас |
|---|---|---|
| `cqrs/projection/runtime.rs` | 0 % | только тесты движка и `host` |
| `cqrs/projection/processor.rs` | 80,8 % | тесты PostgreSQL |
| `task/handlers.rs` | 84,1 % | тесты движка |
| `cqrs/aggregate.rs` | 88,0 % | |
| `task/dependencies.rs` | 88,7 % | тесты движка |
| `cqrs/projection/verify.rs` | 92,5 % | тесты движка и PostgreSQL |
| `cqrs/store/persisted.rs` | 92,7 % | тесты PostgreSQL |

В 97,08 % входят функции логирования sqlx и MongoDB из `diagnostics.rs`; в задаче 08 они уходят в адаптеры.

## Станет

1. **Тесты в памяти** для путей ядра из таблицы, которые сегодня проверяют только тесты PostgreSQL, MongoDB, движка или
   `host`. Тест проверяет поведение ядра, а не повторяет чужой тест: `ProjectionRuntime` — запуск, остановка одним
   токеном, паника процессора с его именем в ошибке; процессор — ветки, которые сейчас видит только PostgreSQL; сборка
   обработчиков (`TaskHandlers::build`) — каждая причина отказа. Цель — ядро без чужих тестов ≥ 97 % строк с запасом
   (ориентир ≥ 97,5 %).
2. **Числа «до»** в разделе «Результаты» этой задачи; задачи 02–09 сверяют с ними свои, задача 10 — итог.

| Что | Как |
|---|---|
| Число тестов | `RUST_TEST_THREADS=2 PROPTEST_RNG_SEED=42 cargo test --locked --workspace --all-features` — сумма `passed` по бинарникам и doctest; отдельно — `ignored` (должно быть 0); то же для `demo/back-end` (`cd demo && cargo test`) |
| Покрытие ядра отдельно | команда ниже, итог и файлы < 95 % |
| Прогоны фич | список прогонов из `CLAUDE.md`, каждый зелёный |

Покрытие ядра без тестов PostgreSQL, MongoDB, движка и `host`:

```bash
RUST_TEST_THREADS=2 PROPTEST_RNG_SEED=42 cargo llvm-cov --locked -p cqrsx --all-features \
  --lib --test cqrs --test task --test derives --test logging --test log_fallback \
  --ignore-filename-regex 'cqrsx/src/(postgres|mongodb|engine|host)|cqrsx/src/(postgres|mongodb|host|logging)\.rs|cqrsx-macros' \
  --summary-only -- --skip engine:: --skip postgres:: --skip mongodb:: --skip host::
```

## Поведение не меняется: чем доказано

Код ядра не меняется, добавляются только тесты. Полный набор `CLAUDE.md`, оба прогона покрытия ≥ 97 %.

## Результаты

2026-10-31, на HEAD `55dd126` плюс правки задачи. Код `cqrsx` вне `#[cfg(test)]` не менялся; дефектов тесты не вскрыли.

- **Покрытие ядра** командой тикета: строки **97,08 % → 97,82 %** (327 непокрытых из 14 978), регионы 97,13 % → 97,83 %.
  После задачи рабочая область — 98,26 % строк (97,63 % регионов), `cqrsx` целиком — 98,17 % строк (97,53 % регионов);
  порог `--fail-under-lines 97` — по строкам. (Исправлено при приёмке задачи 05: прежде здесь проценты регионов были
  подписаны как строки.)
- **По файлам:** `projection/runtime.rs` 0 → 100 %, `owner.rs` 93,4 → 96,2 %, `task/dependencies.rs` 86,6 → 98,6 %,
  `store/persisted.rs` 94,3 → 95,3 %, `projection/processor.rs` 83,4 → 88,3 %, `task/handlers.rs` 89,7 → 93,6 %.
- **Не закрыто, с причиной:** заглушки внутри тестовых типов (`aggregate.rs`, `verify.rs`, часть `persisted.rs` и
  `handlers.rs`) — это правка старых тестов; ветки `owner.rs`, достижимые только гонкой или в PostgreSQL (две группы
  ключей в одной транзакции журнала); логирование sqlx и MongoDB в `diagnostics.rs` — уходит в задаче 08.
- **Новые тесты (16):** `tests/cqrs/projection/runtime.rs` — запуск и остановка одним токеном, пустой runtime и `Debug`,
  паника процессора с его именем в ошибке и остановкой остальных, отмена токена при сбросе `run`; `rebuild.rs` — шаги
  перестройки после потери аренды, сбой выборки ключей, ключ, запрошенный во время живой доставки и во время своей
  перестройки, повтор каждой упавшей записи перестройки; `history.rs` — upcaster'ы и клон `PersistedEventStore`;
  юнит-тесты `Operation::as_str`, `Dependencies::{shared, merged}`, `TaskHandlers::assembled`, первый отказ сборки.
- **Числа «до» для задач 02–10** (`cargo test --locked --workspace --all-features`, ignored 0): **867** — `cqrsx` lib 480,
  `tests/cqrs` 76, `derives` 8, `engine` 8, `log_fallback` 1, `logging` 1, `mongo` 17, `postgres` 129, `task` 29,
  `cqrsx-macros` lib 44, doctest `cqrsx` 51 + 23; на HEAD до задачи было 851. Демо (`cd demo && cargo test`): **174**,
  ignored 0.
- Проверки `CLAUDE.md` (fmt, clippy, полный набор, матрица фич, оба `llvm-cov` ≥ 97 %) и демо — зелёные.
