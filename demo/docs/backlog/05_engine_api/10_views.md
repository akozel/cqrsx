# 10. Витрины: имя у витрины, три запроса движка, сверка в движке, без LWW

| | |
|---|---|
| Статус | предложено 2026-10-27; решение пользователя 2026-10-27 (README эпика, вопрос 5): сверка переезжает в cqrsx |
| Ценность | средняя–высокая: витрина подключается одной строкой, HTTP читает её через движок, у демо нет своего кода запросов к MongoDB |
| Цена | средняя: `View::NAME`, запросы `list`/`range` в трёх репозиториях, перенос сверки (391 строка) в cqrsx |
| cqrsx | `src/cqrs/projection.rs` (`View::NAME`), `projection/repository.rs` (`list`, `range`, `StoredView`), `src/postgres/view_repository.rs`, `src/mongodb/view_repository.rs`, `src/cqrs/testing/mem_view_repository.rs`, новый `src/cqrs/projection/verify.rs`, `src/cqrs/projection/checkpoint.rs` (`CheckpointKey::of::<V, A>()`), `tests/postgres/projection.rs`, `tests/mongo/view_repository.rs`, `doc/components.md` |
| demo | `concert/application/views/concert_hall.rs`, `fan/application/views/fan_wallet.rs` (без LWW), `fan/application/wallet.rs`, `operations/application/{verification.rs (удаляется), verify.rs}`, `shared/application/read_model.rs` (удаляется), `shared/infrastructure/mongo_read_model.rs` (удаляется), `bootstrap/{projections,read_models}.rs`, `concert/presentation/{get,list}.rs`, `fan/presentation/wallet.rs` |
| Зависимости | 03 |
| Гарантии | G-23, G-24, G-25, G-26, G-28, G-32 |

## Сейчас

- Имя витрины — константа в `infrastructure/mongo/read_side.rs`, повторённая в `ViewProjection::with_name`,
  `MongoViewRepository::new(db, name)` и `CheckpointKey::new(name, A::TYPE)`.
- Запросы HTTP идут через порт демо `ReadModelStore<V>` (`load`, `list`, `list_range`) и адаптер `MongoReadModel`,
  который читает коллекцию cqrsx по её форме (`find().sort(_id)`).
- Сверка (verify, D-20) — `application/projections/verification.rs` демо, обобщённая по `V`, `A`, журналу и
  репозиторию.
- Витрины демо держат проверку «последняя запись побеждает» (`seat_is_older`, `lww_by_booking_id`), введённую до
  эпика 03.

## Станет

Дизайн [06](design/06_projections.md).

**cqrsx**
- `View<A>`: `const NAME: &'static str` — имя коллекции и строки закладки. `ViewProjection::new(repo)` берёт
  `V::NAME` (`with_name` остаётся для двух витрин над одной коллекцией? нет — правило «одна коллекция — одна
  витрина», метод удаляется); `MongoViewRepository::new(db)`, `PostgresViewRepository::new(pool)` — по `V::NAME`;
  `CheckpointKey::of::<V, A>()`.
- `ViewRepository`: `list() -> Vec<StoredView<V>>`, `range(lower, upper)` (`[lower, upper)` по id); `StoredView<V>
  { id, version, last_sequence, view }` — тип cqrsx (сегодня `Stored` демо). Реализации: MongoDB (`find` с
  сортировкой по `_id`), PostgreSQL (`ORDER BY view_id`), память (`list_all` уже есть).
- `cqrs::projection::verify`: `verify::<V, A>(journal, documents) -> Verification` (`checked`, `behind_documents`,
  `behind_events`, `differences`) — перенос `verification.rs` демо как есть, с его шестью тестами.
- Запросы «статус, перестройка, перестройка ключа» уже есть у `CheckpointStore`; типизированный API `cqrs.views()` —
  в 11.

**demo**
- `impl View<Concert> for ConcertHallView { const NAME = "concert_halls"; … }`, `FanWalletView::NAME = "fan_wallets"`.
- LWW убирается: `seat_is_older`, `lww_*`-тесты заменяются тестом «события по порядку дают документ»; поля
  `sequence`/`changed_at` у места остаются (документ не меняется); `fan_wallet.rs` — так же.
- HTTP читает через репозитории cqrsx (до 11 — через `bootstrap/read_models.rs` с `Arc<MongoViewRepository<…>>`):
  `get` → `load_with_context`, `list` → `list`, кошелёк → `range("fox:", "fox;")`. `ReadModelStore`, `Stored`,
  `MongoReadModel` удаляются; `stored_document` в `shared/presentation` принимает `StoredView`.
- `operations/application/verify.rs` — тонкая обёртка над `cqrsx::…::verify` с прежним JSON.

## Этапы

1. `View::NAME` и его использование; `list`/`range`/`StoredView` в трёх репозиториях с тестами (порядок по id,
   границы диапазона, пустая коллекция); PostgreSQL и MongoDB интеграционные тесты.
2. `verify` в cqrsx с перенесёнными тестами.
3. Демо: витрины без LWW, HTTP через cqrsx, удаление адаптера и порта.
4. `components.md`: «Read-model data», «Read-model projection», «MongoDB read side», «Event persistence», новая
   строка «Verification».

## Зачем, на примере

```rust
let hall = halls.load_with_context(concert_id.as_str()).await?;   // GET /concerts/{id}
let wallets = wallets.range("fox:", "fox;").await?;                // кошелёк фаната из его книжек
```

Имя витрины стоит один раз у типа, который её описывает; запросы одинаковы в памяти, в PostgreSQL и в MongoDB, и
тесты HTTP идут на `MemViewRepository` без адаптера.

## Риски

- **LWW защищал перестройку до эпика 03.** Теперь порядок и однократность держит движок (G-24, G-25); документы
  после удаления проверки те же, потому что события ключа приходят по порядку. Перестройка под нагрузкой — тема 14
  списка пересмотра, проверяется в 13 (`VERIFY=1`).
- **Своя реализация витрины под каждую базу** — открытый вопрос 06/1 пользователя; здесь одна `View` на обе базы
  (временно), тема 13 списка пересмотра.
- **Сортировка `_id` в MongoDB и `view_id` в PostgreSQL** должна совпадать с нынешним `find_sorted` (бинарный порядок
  строк) — тест на порядок ключей `fox:…`.

## Как проверить

- Проверки `CLAUDE.md`; матрица (`mongodb`, `postgres` отдельно); покрытие.
- Демо: тесты витрин, HTTP-тесты (`read_models_are_served_in_the_documented_shapes`), `write_side.rs`
  (`the_concert_halls_follow_the_journal_through_their_processor`), сверка.

## Поведение не меняется: чем доказано

- Процессоры, закладки, доставка и перестройка не трогаются (G-23, G-24, G-26): их тесты проходят как есть.
- Документы витрин те же: тест формы документа (`the_document_keeps_its_shape`) не меняется; без LWW свёртка
  упорядоченных событий даёт тот же документ, что и с ней (новый тест: оба варианта на одной истории).
- Сверка — перенесённый код с теми же тестами (G-28).
- Запросы HTTP дают те же документы (G-35: HTTP-тесты без правок утверждений); операции MongoDB на запрос те же
  (`find` с сортировкой), SQL не меняется (G-32).

## Критерии приёмки

- [ ] `View::NAME`; `list`/`range`/`StoredView` в трёх репозиториях; `verify` в cqrsx.
- [ ] Демо: витрины без LWW, HTTP через cqrsx; `ReadModelStore`, `MongoReadModel`, `verification.rs` удалены.
- [ ] Тест «с LWW и без — один документ» и тесты порядка диапазона.
- [ ] `components.md`, guarantees.md.
