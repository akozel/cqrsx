# 03. Без panic вне тестов, общий помощник для mutex

| | |
|---|---|
| Статус | предложено |
| Ценность | высокая: библиотека перестаёт ронять процесс на внутренних инвариантах и данных извне; lints не дают вернуть panic |
| Цена | средняя: около 30 мест, новые ветки `Err` требуют тестов (покрытие ≥ 97 %) |
| cqrsx | места ниже; новый модуль помощника блокировок; `Cargo.toml` (`[lints]`), `clippy.toml` (новый, в корне workspace) |
| cqrsx-macros | `src/command.rs`, `src/event.rs`; `Cargo.toml` (`[lints]`) |
| demo | нет |
| Зависимости | 02 |
| Правила | 6–10 |

## Сейчас

После задачи 02 остаются места, где возможен panic (по HEAD `e995a53`, [аудит](../../cqrsx-style-audit.html#panic)),
и 16 мест, где отравленный mutex восстанавливается молча. Хранилища из `cqrs::testing`, на которых работает
`Cqrs::in_memory()`, — рабочий код: исключение правила 8 их не покрывает. Оно покрывает только помощники для проверок
(`TestFramework`, validator) и `MemStore`, который движок не использует.

## Станет

### Места, где возможен panic

| Вид | Места | Как убрать |
|---|---|---|
| `Default` через проверяющий `new(..).expect(..)` с константами | `cqrs/command/retry.rs:73`, `task/queue/storage_config.rs:18`, `task/runtime/config.rs:56`, `task/runtime/worker_pool_config.rs:52`, `cqrs/projection.rs:207` | константы нужного типа (правило 6.2), `Default` собирает структуру из них |
| `unreachable!` после цикла попыток; `debug_assert!` и вычитание с переполнением | `cqrs/command/retry.rs:94`, `:52`; `cqrs/projection.rs:532` | цикл сам возвращает последний результат; число попыток — `NonZeroUsize`; `saturating_sub` |
| ввод пользователя библиотеки | `cqrs/store/upcaster.rs:145` (`NumericVersionUpcaster::new`), `:58`; `task/policy.rs:83` (`PolicyName::new`, `assert!`) | `try_new` → `Result`; разбор через образец; `const fn new(..) -> Option<Self>` |
| внутренние инварианты через `expect` | `processor/delivery.rs:158`, `:262`; `processor/rebuild.rs:20`; `task/runtime/worker_pool.rs:51`; `worker_pool_config.rs:131` | `Err` + `error!`; уровень дозаполнения в `usize` с `min(buffer_capacity)` |
| индексы и срезы | `processor/owner.rs:710`, `:826–830`, `:862`; `task/dispatcher.rs:129`; `task/runtime/poller.rs:41`; `processor/rebuild.rs:100`; `cqrs/projection.rs:561`; `task.rs:161`, `:174` (`Value[..] =`) | `iter_mut().find()`, entry API, `cycle().skip()`, `split_first`, `get(..)`, `as_object_mut()` |
| свой `uuid_v5` со срезом | `cqrs/command/causation.rs:141–149` | feature `v5` у `uuid`, `Uuid::new_v5` (тест уже сверяет `uuid_v5` с эталонами crate `uuid`); прямая зависимость `sha1` уходит, если больше нигде не нужна |
| движок в памяти | `cqrs/testing/mem_event_repository.rs:354–355`; `task/testing/mem_task_queue.rs:168–169`, `:213`, `:294` | как выше |
| макросы | `cqrsx-macros/src/command.rs:82`, `src/event.rs:49` (`unnamed[0]`) | образец по срезу или `first()` |
| помощники для проверок (panic законен) | `cqrs/testing/validator.rs:106–110`, `:204–209`, `:412–418` | раздел `# Panics`, сообщения у `assert_eq!` |

### Помощник блокировок (правило 10)

Одна функция в crate, например `lock(&Mutex<T>, name: &'static str) -> MutexGuard<T>` (и вариант для `get_mut`):
если mutex отравлен — `error!` с именем блокировки, `clear_poison()`, работа продолжается. Ею заменяются все 16 мест:
`cqrs/command/locks.rs:123`; `processor/delivery.rs:103`; `processor/lease.rs:33`, `:103`; `engine.rs:351`;
`engine/memory.rs:129`; `engine/serve.rs:83`; `engine/setup.rs:97`, `:104`; `mem_event_repository.rs:137`, `:177`,
`:228`; `mem_view_repository.rs:83`; `mem_checkpoint_store.rs:127`; `cqrs/testing/command.rs:175`, `:215`.

Под всеми этими блокировками — одна операция с контейнером, без пользовательского кода и `.await`, так что
продолжать работу правильно. Исключение — `mem_view_repository.rs:87–100`: там под блокировкой декодируется
пользовательский тип (`V::deserialize`); значения копируются, блокировка отпускается, потом декодирование.
Комментарий «Nothing panics under this mutex» в `delivery.rs:102` после правок становится правдой.

### Lints

В `[lints]` `cqrsx/Cargo.toml` и `cqrsx-macros/Cargo.toml`: `clippy::unwrap_used`, `expect_used`, `panic`,
`unreachable`, `todo`, `unimplemented`, `indexing_slicing` — `deny`. `clippy.toml` в корне workspace:
`allow-unwrap-in-tests`, `allow-expect-in-tests`, `allow-panic-in-tests`, `allow-indexing-slicing-in-tests`.
`#![allow(..)]` — на модулях помощников для проверок и в корне каждого интеграционного тестового crate.
Clippy не видит `serde_json::Value[..] =` (одно из двух мест в `task.rs` он пропустил) и `Row::get` — их ищем
вручную, как в аудите.

## Этапы

1. План: для каждого внутреннего инварианта — тип ошибки, в который он уходит, и тест на ветку `Err` (если ветка
   недостижима, инвариант выражается типом, а не `Err`).
2. Помощник блокировок и 16 замен.
3. Места panic по видам из таблицы; lints и `clippy.toml`; правило 8 — `# Panics` у помощников для проверок.
4. Обновить skill `rust-style-review`: эти lints убираются из команды шага 1 (они теперь в `[lints]`).
5. `CLAUDE.md`-проверки; коммит.

## Риски

- **Покрытие.** Ветки `Err` для невозможных состояний трудно покрыть тестом. Предпочтение — убрать состояние типом
  (правило 6.1); тест через подстановку отказа — только где это не требует нового API.
- **`indexing_slicing` шумит** на индексах, корректность которых видна из соседней строки. Не ставить `allow` на
  место: переписать на итератор или образец; `allow` с комментарием — только если переписывание делает код хуже, и
  это перечислено в отчёте задачи.
- **Feature `v5` у `uuid`** тянет `sha1_smol`; в эпике 04 его не было в офлайн-кэше. Если не собирается — оставить
  свою функцию, срез заменить на `first_chunk::<16>()` с образцом.

## Как проверить

- `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` с новыми lints.
- `CLAUDE.md`: тесты всех features, оба прогона покрытия ≥ 97 %.

## Критерии приёмки

- [x] Lints группы panic включены, срабатываний ноль; `allow` в рабочем коде перечислены с причиной.
- [x] Все 16 мест идут через помощник блокировок; отравление логируется.
- [x] Тексты SQL и `GET /api/config` demo не изменились.

## Результаты

Сделано 2026-10-30 на HEAD `a270a06` поверх правок задач 01 и 02 в рабочем дереве (коммита ещё нет). Clippy с семью
lints группы panic по `--lib` нашёл 43 места, поиск — ещё три, которых clippy не видит (`debug_assert!` в `retry.rs`,
`assert!` в `PolicyName::new`, второе `Value[..] =` в `task.rs`). Теперь срабатываний ноль, lints включены как `deny`.

| Вид | Мест | Что сделано |
|---|---|---|
| `Default` через проверяющий `new(..).expect(..)` | 5 | `retry.rs`, `storage_config.rs`, `runtime/config.rs`, `worker_pool_config.rs`, `projection.rs`: `Default` собирает структуру из констант (`const …: NonZeroUsize = NonZeroUsize::new(16).unwrap()` — clippy в const не ругается, ошибка — сборка); тесты сверяют `Default` с проверяющим `new` |
| цикл попыток с `unreachable!`, `debug_assert!`, `retry - 1` | 3 | `retry_on_aggregate_conflict` и `ViewProjection::apply_events` — `loop`, который возвращает последний результат; `max_attempts` — `NonZeroUsize`; `saturating_sub(1)` |
| ввод вызывающего | 3 | `NumericVersionUpcaster::try_new -> Result<_, NumericVersionError>`; разбор версии без `unwrap`; `PolicyName::new` — `const fn -> Option<Self>` |
| внутренние инварианты через `expect` | 5 | закрытый семафор буфера и пропавший буфер в `delivery.rs` — `error!` и квитанция `Stopped` (задание не выполняется, владелец идёт обычным путём сбоя); семафор `rebuild.rs` — `error!` и `PersistenceError::UnknownError`; worker без индекса — `error!`, его выход не сообщается; уровень дозаполнения — `try_from` с потолком `buffer_capacity`, без ветки ошибки |
| индексы и срезы | 11 | `owner.rs`: ключ страницы едет в `LiveOutcome`, страница ищется `iter_mut().find()`, группа — `get_mut` (ветка `None` = новый ключ); `dispatcher.rs` — `get`; `poller.rs` — `cycle().skip().take()`; `rebuild.rs` — `skip(1)`; `projection.rs` — `get(..prefix)`; `task.rs` — оба `Value[..] =` через `Value::Object` (не объект — `error!`, ничего не переносится) |
| `uuid_v5` | 1 | см. ниже |
| хранилища движка в памяти | 6 | `mem_event_repository.rs`: страница журнала — цикл без индексов; `mem_task_queue.rs`: `take` возвращает `&mut` строк, а не индексы |
| макросы | 2 | `Variant(Type)` разбирается образцом по `fields.iter()` |
| помощники для проверок | 10 | validator (3): `# Panics`, сообщения у `assert_eq!`; `MemStore` (4) — `allow`; три `expect` `TestCommandExecutor` сведены в один `unshared` с `# Panics` |

**Помощник блокировок.** `src/locking.rs`: `lock(&Mutex<T>, name) -> MutexGuard<T>` и `get_mut(&mut Mutex<T>, name)`; при
отравлении — `diagnostics::log_poisoned_lock` (`error!` с полем `lock`), `clear_poison()`, работа дальше. Через него
идут все 16 мест (у `engine/setup.rs::described` вместо `into_inner` теперь `get_mut`); других `PoisonError` в crate
нет. Тесты отравляют mutex паникой другого потока: значение на месте, отравление снято, `error!` один раз. В
`mem_view_repository.rs` документы копируются, блокировка отпускается, потом `V::deserialize`. Для «Err + error!»
добавлен `diagnostics::log_broken_invariant(component, invariant)`; у каждой такой ветки есть тест.

**Lints.** `[lints.clippy]` в `cqrsx/Cargo.toml` и `cqrsx-macros/Cargo.toml`: `unwrap_used`, `expect_used`, `panic`,
`unreachable`, `todo`, `unimplemented`, `indexing_slicing` — `deny`. `clippy.toml` в корне workspace: четыре
`allow-*-in-tests` (действует и на demo, где тестам это уже разрешено). Clippy считает тестом только `#[test]` и модули
под простым `#[cfg(test)]`, поэтому `#[cfg(all(test, feature = "worker"))]` в `task/runtime.rs` разделён на два
атрибута, а семь `unreachable!` в тестах заменены на `panic!` (настройки для `unreachable` нет). `#![allow(..)]` — в
корне девяти интеграционных тестовых crate `cqrsx/tests` (включая `allocations.rs`; 2026-10-30 подсчёт аллокаций
переехал в `demo/load/epic06/allocations` вместе со своим `#![allow(..)]` и этими lints).

**`allow` в рабочем коде** (все — помощники для проверок, правило 8):
- `cqrs/testing/validator.rs` — `#![allow(clippy::panic)]`: panic и есть проваленная проверка;
- `cqrs/testing/mem_store.rs` — `#![allow(clippy::unwrap_used)]`: `MemStore` движок не использует, `unwrap` отравленного
  `RwLock` роняет тест, который его отравил;
- `cqrs/testing/command.rs`, `TestCommandExecutor::unshared` — `#[allow(clippy::panic, reason = …)]`: регистрация после
  клона — ошибка теста, описана в `# Panics` у `with_task_planner`, `with_commit_wakeup`, `with_observer`.

**`uuid_v5`.** Feature `v5` у `uuid` требует `sha1_smol`, его нет в офлайн-кэше (`cargo generate-lockfile --offline`
не разрешает зависимость). Своя функция осталась, срез `&digest[..16]` заменён неопровержимым образцом по массиву
`[u8; 20]` (`let [bytes @ .., _, _, _, _] = digest`) — длину проверяет компилятор; прямая зависимость `sha1` остаётся.
Тест-эталон против `Uuid::new_v5` не менялся и проходит.

**API:** `NumericVersionUpcaster::try_new` вместо `new`; `PolicyName::new` возвращает `Option` (константы в README,
тестах и demo пишутся `PolicyName::new("…").unwrap()`). demo менялась только в `shared/application/policies.rs`.

**Попутно.** В `owner.rs` строки ключей переехали из `Page.keys` в `LiveOutcome`, и копия ключа на успешном пути
(`owner.rs:826` из задачи 04) исчезла сама: на ключ страницы на одну строку меньше. Это меняет число аллокаций до
замеров задачи 04.

**Проверки** (все с кодом 0, с первого раза): build, fmt, clippy `-D warnings` с новыми lints, полные тесты
workspace, `-p cqrsx --no-default-features` без features и с каждой из `postgres`, `worker`, `host`, `logging`,
`mongodb`, `http`; покрытие строк — workspace 98,04 %, `cqrsx` 97,95 % (после задачи 02 было 98,02 % и 97,92 %); demo —
fmt, build, clippy `-D warnings`, тесты без feature `load` и с ней; `cargo doc` без предупреждений.

### Замер на стенде «до» и «после» 01–03

Серия задачи 01 (`demo/load/epic06/perf/series.plan`): `e06-base` (дерево после 01) против `e06-03` (дерево после 03,
`sha256:e1fb2ddc0743…`), вперемешку, по три повтора. Медиана (min–max); `≠` — диапазоны повторов не пересекаются.

| Точка | Принято/с | CPU бэкенда, мс/1 000 команд | CPU бэкенда, мс/1 000 задач | Операторов SQL на команду |
|---|---|---|---|---|
| `01-hot-key-vus40-app` | 760 → 734 (674–760), −3 % | 888 → 890 (873–901), 0 % | 650 → 658 (620–661), +1 % | 7,68 → 7,64 |
| `03a-drain-w8-db` | 3 614 → 3 636 (3 609–3 663), +1 % | 548 → 545 (531–551), −1 % | 232 → 233 (233–234), 0 % | 3,64 → 3,70 |
| `09-probe-k1-m1` | 8 636 → 8 509 (6 216–8 565), −1 % | 180 → 180 (178–180), 0 % | 48,6 → 48,5 (48,3–48,7), 0 % | 3,28 → 3,26 |
| `09-probe-k4-m1` | 6 037 → 6 061 (5 153–6 085), 0 % | 292 → 291 (290–296), −1 % | 49,3 → 47,8 (47,1–48,1), −3 % | 3,33 → 3,33 |
| `09-probe-k1-m4` | 5 931 → 5 800 (4 049–5 925), −2 % | 198 → 200 (197–203), +1 % | 51,3 → 50,0 (49,1–50,4), −3 % ≠ | 4,06 → 4,08 |
| `09-probe-hot-k4` | 903 → 909 (906–910), +1 % | 414 → 414 (412–417), 0 % | — | 7,61 → 7,60 |

- **Регрессии нет:** CPU бэкенда на команду и на задачу во всех точках в пределах разброса повторов; единственное
  `≠` среди них — в лучшую сторону (−3 % CPU на задачу в `09-probe-k1-m4`, причину не искали). Перехват panic
  обработчика задачи (задача 02) и помощник блокировок на CPU не видны.
- **Наборы текстов SQL совпадают** во всех шести точках (`statements.mjs … e06s-base- e06s-head-`).
- **Подсчёт аллокаций** (`allocations.rs`, теперь `demo/load/epic06/allocations`; после 03 против «до»): число
  аллокаций почти то же (на событие в витрине при K = 1: 84,17 → 84,18 — одна аллокация на догон витрины); байтов на событие в витрине +175 (10 917 → 11 092) — чтение журнала в памяти собирает страницу в
  новый `Vec` вместо прежнего `collect` по месту (только движок в памяти, этот код переписывает F3 задачи 04); байтов
  на команду с задачами +64 (24 831 → 24 895) — `NewTask` хранит labels (задача 02), а вектор задач команды сразу
  выделяет место на четыре.

