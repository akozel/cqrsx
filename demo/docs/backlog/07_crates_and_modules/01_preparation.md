# 01. Подготовка: покрытие ядра без PostgreSQL и движка, число тестов «до»

| | |
|---|---|
| Статус | предложено |
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

Заполняется при выполнении.
