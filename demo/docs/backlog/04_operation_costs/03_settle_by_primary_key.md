# 03. settle по первичному ключу

| | |
|---|---|
| Статус | предложено |
| Аудит | Q-1, эксперимент в приложении A |
| Ценность | высокая: settle — 73 % времени SQL при сливе и 35 % на горячем ключе |
| Цена | очень низкая: одна строка SQL, комментарий, тест |
| cqrsx | `src/postgres/task_queue/settlement.rs`, `tests/postgres/task_queue/settlement.rs`, `doc/components.md` (строка «PostgreSQL queue») |
| demo | нет |
| Зависимости | 01 (замер по цепочке) |
| Где выигрыш | слив outbox, горячий ключ, долгая работа |

## Сейчас

Settle сначала выбирает и блокирует строку задачи, потом проверяет срок аренды и обновляет её. Выборка — в
`settlement.rs:76`:

```sql
WITH owned AS MATERIALIZED (
    SELECT id, locked_until FROM transactional_outbox
    WHERE id = $1 AND lock_token = $2 AND status = 'processing'
    FOR UPDATE
)
UPDATE transactional_outbox AS o SET … FROM owned
WHERE o.id = owned.id AND owned.locked_until > clock_timestamp()
RETURNING o.status
```

Условие `status = 'processing'` совпадает с предикатом частичного индекса
`transactional_outbox_expired_idx (locked_until, id) WHERE status = 'processing'` (`sql/002-transactional-outbox.sql:33`).
По статистике в нём мало строк, а `id = $1` годится как условие на его второй столбец. Поэтому PostgreSQL 18 ищет
строку по нему, а не по первичному ключу:

```text
->  Index Scan using transactional_outbox_expired_idx on transactional_outbox
      Index Cond: (id = $1)
      Filter: ((lock_token = $2) AND (status = 'processing'::text))
```

Пока идёт слив, в этом индексе лежат записи всех задач, закрытых с прошлого VACUUM, и каждый settle читает их все.

| Где | settle, мс | блоков на вызов | доля времени SQL |
|---|---|---|---|
| стенд, слив `03a`, 8 воркеров | 0.402 | 92 | 73 % |
| стенд, горячий ключ, 40 VU | 0.202 | 42 | 35 % |
| эксперимент, 80 тыс. settle | 0.273 | 122 | — |

## Станет

```sql
WHERE id = $1 AND lock_token = $2
```

Условие на `status` уходит: CHECK таблицы (`sql/002…:23`) требует, чтобы `lock_token` был не `NULL` ровно у строк в
`processing`. Значит, `lock_token = $2` уже означает `processing`, и смысл запроса не меняется. Комментарий в коде
объясняет, почему условия нет: без него его вернут, и план снова уйдёт на частичный индекс.

В эксперименте после правки settle идёт по `transactional_outbox_pkey`: 0.071 мс и 17.5 блока на вызов (−74 %).

## Зачем, на примере

Всплеск броней оставил 40 тыс. задач `encore_record_ticket`. 8 воркеров закрывают их по 2 000 в секунду. Через 10 с в
индексе просроченных аренд 20 тысяч записей закрытых задач, и каждый следующий settle идёт по ним всем, чтобы найти
одну строку по id. После правки settle читает только ветку первичного ключа до этой строки.

## Вариант на потом

Одиночный `UPDATE` без CTE и без `FOR UPDATE`, с условиями `id`, `lock_token` и `locked_until > clock_timestamp()` в
`WHERE`: в эксперименте 0.061 мс против 0.071, ещё −14 %. Комментарий в `settlement.rs` защищает порядок «сначала
блокировка, потом проверка времени» от держателя блокировки, который отпустил строку неизменённой. В этой схеме
строку в `processing` без изменения не блокирует никто: восстановление аренды меняет каждую строку, которую
блокирует. Если пользователь захочет, этот вариант мерится отдельной строкой в той же задаче.

## Риски

- Кто-то снимет CHECK со схемы, и тогда `lock_token = $2` перестанет означать `processing`. Схема принадлежит cqrsx,
  тест схемы (`tests/postgres/schema.rs`) её проверяет.
- План зависит от версии PostgreSQL. Образ закреплён (18.6) и на стенде, и в тестах.

## Как измерить

«До» — HEAD перед задачей, «после» — правка. Прогоны вперемешку, `REPEAT=3`, `POOL=20`:
- `03a-drain-w8`, `db`: скорость слива, время SQL и CPU PostgreSQL на задачу, блоков на settle;
- `01-hot-key-vus40`, `db`: принятых/с, метрики цепочки (01);
- 5-минутная точка (02) на устойчивой частоте: растёт ли она.

## Критерии приёмки

- [ ] Условие на `status` убрано, комментарий объясняет почему; существующие тесты fencing settle проходят.
- [ ] Интеграционный тест: план settle (`EXPLAIN (FORMAT JSON)`) идёт по `transactional_outbox_pkey` на таблице, где
      `transactional_outbox_expired_idx` содержит записи закрытых задач.
- [ ] `components.md`, строка «PostgreSQL queue»: settle ищет строку по ключу.
- [ ] Замер записан в тикет и в `PROGRESS.md`.
