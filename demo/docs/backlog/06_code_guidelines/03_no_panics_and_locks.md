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

- [ ] Lints группы panic включены, срабатываний ноль; `allow` в рабочем коде перечислены с причиной.
- [ ] Все 16 мест идут через помощник блокировок; отравление логируется.
- [ ] Тексты SQL и `GET /api/config` demo не изменились.
