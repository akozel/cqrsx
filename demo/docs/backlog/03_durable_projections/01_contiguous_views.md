# 01. Непрерывные документы витрин

| | |
|---|---|
| Статус | предложено |
| Ценность | высокая: любая `ViewProjection` становится идемпотентной; без этого доставка «хотя бы раз» и несколько нод портят витрину |
| Цена | низкая |
| cqrsx | `cqrs/projection.rs`, `cqrs/projection/repository.rs`, `cqrs/store.rs` (вариант ошибки), `postgres/view_repository.rs`, `cqrs/testing/mem_view_repository.rs`, `doc/components.md`; тесты `tests/postgres/projection*.rs`, `tests/cqrs/*`, `tests/postgres/command.rs` (DDL таблиц видов) |
| demo | `infrastructure/mongo/view_repository.rs`, `application/concert/views/concert_hall.rs` (`incomplete`), тексты UI по D-15, `README.md` |
| Зависимости | нет |
| Где выигрыш | [03](03_projection_processor.md); любой `View` вне демо |

## Сейчас

`ViewProjection::apply_events` загружает документ с версией, применяет все переданные события и пишет документ с
проверкой версии (`ViewContext { view_instance_id, version }`). Какие события документ уже видел, библиотека не
знает: одно и то же событие, переданное дважды, применяется дважды. Витрины демо спасает правило LWW по
`sequence` внутри `View::update`; произвольный `View` не спасает ничто. Документ, созданный после 🧨 wipe из
«серединного» события, помечается `incomplete: true` и показывается как «витрина без истории».

## Станет

Документ помнит номер последнего применённого события своего агрегата и принимает только следующее.

```rust
// cqrs/projection/repository.rs
pub struct ViewContext {
    pub view_instance_id: String,
    pub version: i64,          // версия документа для CAS, как сейчас
    pub last_sequence: usize,  // последнее применённое событие агрегата; 0 у нового документа
}
```

```rust
// cqrs/projection.rs, apply_events: правило на каждое событие пачки
match event.sequence.cmp(&(context.last_sequence + 1)) {
    Ordering::Less => continue,                                   // уже применено: пропустить
    Ordering::Equal => { view.update(event); context.last_sequence = event.sequence; }
    Ordering::Greater => return Err(PersistenceError::SequenceGap {
        expected: context.last_sequence + 1, found: event.sequence }),
}
// ничего не применено → документ не пишется, версия не растёт
```

- `PersistenceError::SequenceGap { expected, found }` — новый вариант: процессор ([03](03_projection_processor.md)),
  статус и лог отличают «нет истории» от сбоя хранилища.
- Репозитории хранят `last_sequence` рядом с версией и пишут его тем же CAS, что и документ:
  - `PostgresViewRepository`: колонка `last_sequence bigint NOT NULL DEFAULT 0 CHECK (last_sequence >= 0)` в
    таблице вида; INSERT/UPDATE пишут её, SELECT читает;
  - `MemViewRepository`: третье поле документа; `put` и `list_all` с `last_sequence`;
  - demo `MongoViewRepository`: поле документа `{ _id, version, last_sequence, view }`.
- Повторы при конфликте версии остаются (до 16 попыток): после `OptimisticLockError` документ перечитывается и
  правило применяется к свежему `last_sequence` — так сходятся два писателя одного документа.
- `ViewProjection::load` не меняется.

### Инвариант

Документ витрины в любой момент равен свёртке `View::update` по событиям `1..=last_sequence` своего агрегата,
без дыр. Следствия: двойная доставка безвредна; порядок внутри агрегата нельзя нарушить незаметно; два писателя
(две ноды, перехват аренды, replay рядом с live) приходят к одному документу, потому что `View::update`
детерминирован. `View::update` обязан быть детерминированным и без побочных эффектов — это и сейчас правило.

### D-15: документ без истории

Событие с номером больше `last_sequence + 1` — дыра, и для нового документа тоже (`expected: 1`). Сегодня такой
документ создаётся с `incomplete: true`; по этому тикету проекция возвращает `SequenceGap`, документ не
создаётся, и до Encore! витрина этого агрегата не обновляется. Причина: документ без истории — неверный ответ, а
не «неполный». Альтернатива — флаг «разрешить дыры» на проекцию — отклонена: это переключатель, который узаконивает
неверные данные. Что меняется в демо, описано в [04](04_demo_on_processor.md); до 04 сцена 🧨 wipe показывает
предупреждение `projection_failed` в ответе команды и запись `projection.failed` в ленте.

## Пограничные случаи

| # | Случай | Поведение | Проверка |
|---|---|---|---|
| 1 | Пачка целиком старая | ничего не пишется, `Ok`, версия не растёт | `an_already_applied_batch_writes_nothing` |
| 2 | Пачка начинается до `last_sequence`, продолжается после | префикс пропущен, хвост применён одной записью | `a_batch_overlapping_the_document_applies_only_the_new_tail` |
| 3 | Первое новое событие больше `last_sequence + 1` | `SequenceGap`, записи нет | `a_gap_fails_without_writing` |
| 4 | Дыра внутри пачки | записи нет вовсе, ошибка: пачка одного агрегата приходит из журнала подряд, дыра внутри — признак битого источника | `a_gap_inside_the_batch_writes_nothing` |
| 5 | Новый документ, пачка с 1 | создаётся, `last_sequence` = последний номер пачки | `a_new_document_starts_at_sequence_one` |
| 6 | Новый документ, пачка не с 1 | `SequenceGap { expected: 1 }` | `a_new_document_refuses_a_batch_that_does_not_start_at_one` |
| 7 | Конфликт версии | перечитать, правило к свежему `last_sequence` | `a_version_conflict_rereads_and_skips_what_the_winner_applied` |
| 8 | Два писателя одного документа одновременно, разные пачки одного агрегата | оба приходят к одному документу, каждое событие применено один раз | `two_writers_of_one_document_converge` (PostgreSQL, `tokio::join!`) |
| 9 | Таблица вида без колонки | ошибка SQL при первом обращении; ассет обновления + пересборка | doc, `components.md` |
| 10 | Старые документы: `last_sequence = 0`, вид непустой | первое же событие → `SequenceGap`; после обновления схемы витрины пересобираются | doc, README демо |

## Тесты

- **cqrsx, unit** (`projection.rs` с `MemViewRepository`): таблица выше; property-тест — случайные пачки
  (перекрытия, повторы, дыры) против модели «применённые номера непрерывны от 1», инвариант после каждой операции,
  счётчик применений каждого события равен 1.
- **cqrsx, PostgreSQL** (`tests/postgres/projection*.rs`, `tests/postgres/command.rs`): DDL таблиц видов с
  `last_sequence`; round trip; случаи 7–8; существующие тесты сорока параллельных команд дополнительно проверяют
  `last_sequence` документа.
- **demo**: `MongoViewRepository` round trip с полем; `write_side.rs` — у документа после 20 параллельных броней
  `last_sequence` равен номеру последнего события; 🧨 wipe, затем бронь → `projection.failed` с `SequenceGap`.

## Риски

- Поведение после 🧨 wipe меняется (D-15). Если владелец демо хочет сохранить сцену «витрина без истории», её
  можно показывать по статусу «остановлена: нет истории», а не поддельным документом.
- Любая существующая база нуждается в обновлении таблиц видов и пересборке витрин. Демо пересоздаёт данные
  (`make reset`).

## Критерии приёмки

- [ ] `ViewContext::last_sequence`, правило непрерывности в `ViewProjection`, `PersistenceError::SequenceGap`.
- [ ] Три репозитория хранят и пишут `last_sequence` одним CAS с версией; инструкция обновления таблиц видов в
      `components.md` (имя таблицы задаёт пользователь, поэтому ассет — шаблон `ALTER TABLE <view> ADD COLUMN …`).
- [ ] Тесты таблицы и property-тест; покрытие ≥ 97 %.
- [ ] `components.md`: строки «Read-model data», «Read-model projection», инвариант непрерывности в правилах имён
      и интеграции.
- [ ] demo: поле в MongoDB; `incomplete` удалён из вида и UI (или оставлен по решению владельца с пометкой);
      README «Architecture».
