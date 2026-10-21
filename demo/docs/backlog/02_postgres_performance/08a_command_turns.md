# 08a. Ход на ключ агрегата для команд (шаг A)

| | |
|---|---|
| Статус | предложено |
| Ценность | высокая для горячих ключей: внутри процесса нет проигранных гонок, повторов и 409 |
| Цена | средняя–низкая: новый компонент ядра и обёртка вокруг повторов в исполнителе |
| cqrsx | `cqrs/aggregate.rs` (`AggregateKey`), `cqrs/command/turns.rs` (новый), `cqrs/command.rs` (модуль), `cqrs/command/executor.rs`, `postgres/command.rs`, `doc/components.md` |
| demo | `back-end/src/config.rs`, `back-end/src/bootstrap.rs`, `.env.example`; стенд `perf/run.sh` (`ORDERING=`, `TURN_MAX_WAIT_MS=`, `TURN_MAX_WAITING=`), `perf/compose.perf.yaml` |
| Зависимости | 01 для замера, [17](17_two_backend_nodes.md) для замера с двумя нодами; общий план — [08](08_per_aggregate_queues.md) |
| Где выигрыш | 01 (горячий ключ) |

## Что делаем

Команды к одному ключу (`aggregate_type`, `aggregate_id`) внутри процесса выполняются по одной, в порядке прихода.
Ход держится всю команду, включая повторы при конфликте и обработчики: в этом шаге обработчики остаются inline.

```rust
// cqrs/aggregate.rs
/// One aggregate instance, unique across aggregate types.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AggregateKey { aggregate_type: &'static str, aggregate_id: String }
impl AggregateKey {
    pub fn of<A: Aggregate>(aggregate_id: &str) -> Self { … }
}

// cqrs/command/turns.rs
/// How commands to one aggregate run inside this process.
pub enum CommandOrdering {
    /// Concurrently; the store's optimistic check resolves races (the behaviour before turns).
    Concurrent,
    /// One at a time per aggregate key, in arrival order; different keys stay parallel.
    PerAggregate(TurnLimits),
}

/// Checked bounds of waiting for a turn: how long, and how many commands per key.
pub struct TurnLimits { max_wait: Duration, max_waiting: NonZeroUsize }   // new(..) -> Result<_, TurnLimitsError>, Default

/// One FIFO turn per aggregate key. A key's entry lives while its turn is held or awaited.
pub struct AggregateTurns { limits: TurnLimits, lanes: std::sync::Mutex<HashMap<AggregateKey, Arc<Lane>>> }
impl AggregateTurns {
    pub fn new(limits: TurnLimits) -> Self { … }
    /// Waits for the key's turn in arrival order; refuses past the limits.
    pub async fn enter(&self, key: AggregateKey) -> Result<AggregateTurn, TurnRefused> { … }
}

/// Held while a command runs; dropping it hands the turn to the next waiter in order.
pub struct AggregateTurn { … }
pub enum TurnRefused { TimedOut, TooManyWaiting }
```

Как устроено внутри:
- `Lane` — `Arc<tokio::sync::Mutex<()>>` (честный FIFO) и счётчик ждущих.
- Реестр — `std::sync::Mutex<HashMap>`. Блокировка держится только на поиске или вставке, никогда через `await`.
- Страж `AggregateTurn` держит `OwnedMutexGuard`. При уничтожении стража последний владелец удаляет запись ключа.
- Ожидание — `tokio::time::timeout(max_wait, lane.lock_owned())`. Счётчик ждущих уменьшается защитным объектом
  и при отмене.

Исполнитель:

```rust
// cqrs/command/executor.rs, CqrsCommandExecutor::execute_with_metadata
let _turn = match &self.turns {
    Some(turns) => Some(turns.enter(AggregateKey::of::<A>(aggregate_id)).await.map_err(refused)?),
    None => None,
};
retry_on_aggregate_conflict(self.retry_policy, || framework.execute_with_metadata(…)).await   // повторы — внутри хода
```

- `CqrsCommandExecutor::with_turns(Arc<AggregateTurns>)` — для своих хранилищ.
- `PostgresCommandConfig::new(snapshot_interval, conflict_retry, ordering)`: `CommandOrdering` — обязательный
  аргумент, скрытого значения по умолчанию нет. `Default` для конфигурации остаётся с
  `PerAggregate(TurnLimits::default())` или убирается — решить при реализации по тому, где он используется.
- `PostgresCommandExecutor::new` создаёт реестр один раз, из конфигурации. Исполнитель команды, который
  собирается на каждый вызов, получает клон `Arc`.
- `TurnRefused` превращается в `CommandGatewayError::Unavailable`. Причина пишется в лог через `diagnostics`.

Значения по умолчанию — публичные константы `DEFAULT_TURN_MAX_WAIT` (1 с) и `DEFAULT_TURN_MAX_WAITING` (128);
`TurnLimits::default()` собирается из них.

demo, цепочка env → config → cqrsx (таблица — в [08](08_per_aggregate_queues.md), «Параметры»):
- `ENCORE_COMMAND_ORDERING=concurrent|per_aggregate`, `ENCORE_COMMAND_TURN_MAX_WAIT_MS`,
  `ENCORE_COMMAND_TURN_MAX_WAITING` разбираются в `config.rs` с проверкой. Ошибка останавливает старт с именем
  переменной. Значения по умолчанию — константы `cqrsx`, без копий чисел;
- `.env.example` с комментарием к каждой переменной;
- сборка в `bootstrap.rs` для обоих исполнителей (`Concert` и `Fan`);
- стенд: `perf/run.sh ORDERING= TURN_MAX_WAIT_MS= TURN_MAX_WAITING=` → переменные бэкенда через `compose.perf.yaml`.

## Пограничные случаи

| # | Случай | Ожидаемое поведение |
|---|---|---|
| 1 | Команды к разным ключам | параллельно, не ждут друг друга |
| 2 | Тот же `aggregate_id`, другой тип агрегата | разные ключи |
| 3 | Порядок | FIFO в порядке вызова `enter` (гарантия `tokio::sync::Mutex`) |
| 4 | Конфликт при записи (другая нода или исполнитель без общего реестра) | повтор внутри хода, место в очереди не теряется |
| 5 | Ожидание дольше `max_wait` | `Unavailable`; ход не занят, в счётчике ждущих не числится |
| 6 | Ждущих уже `max_waiting` | немедленный `Unavailable`: всплеск не съедает память |
| 7 | Запрос отменён во время ожидания | снят из очереди, порядок остальных не нарушен, счётчик уменьшен |
| 8 | Запрос отменён или паника во время выполнения | ход отпущен при уничтожении стража |
| 9 | Ключ свободен, никто не ждёт | запись удалена из реестра |
| 10 | Обработчик синхронно выполняет команду своего агрегата | ждёт свой же ход до `max_wait` → `Unavailable`; правило в `components.md` |
| 11 | `CommandOrdering::Concurrent` | реестра нет, путь кода прежний (вариант для сравнения на стенде и для тех, кому ход не нужен) |
| 12 | Проекции | работают под ходом: внутри процесса обновления одного ключа уже идут по порядку, но в последовательном участке |
| 13 | Задачи outbox, выполняющие команды (цепочка `Fan` в demo) | тоже берут ход: это тот же исполнитель |
| 14 | Два исполнителя одного типа в процессе | у каждого свой реестр, между ними — проверка в базе, как между нодами. Документировать; общий реестр — через `with_turns` |

## Тесты

**Unit, `cqrs/command/turns.rs`** (остановленные часы, однопоточный рантайм):
- взаимное исключение: счётчик одновременных держателей одного ключа не превышает 1;
- FIFO: N ждущих встают в очередь по одному (`yield_now`), порядок входа совпадает с порядком постановки;
- независимость ключей: держатель ключа `a` не мешает войти в `b`; одинаковый `aggregate_id` у разных типов не
  блокирует;
- `TimedOut` после `max_wait` (продвинуть часы), счётчик ждущих снова 0, следующий входит нормально;
- `TooManyWaiting` на пределе, без ожидания;
- отменённый ждущий (уничтоженный future) не ломает порядок и не оставляет следов в счётчике;
- паника держателя (задача паникует с ходом в руках) отпускает ход, следующий входит;
- в покое реестр пуст (тестовый доступ к числу записей);
- `TurnLimits::new` отвергает нулевые значения;
- модельный тест `proptest`: случайные «войти, выйти, отменить» по трём ключам против простой модели (один
  держатель, FIFO среди неотменённых, пустой реестр в конце).

**Unit, исполнитель** (`cqrs/command/executor.rs`, `MemStore`):
- 20 параллельных команд к одному агрегату при политике повторов в одну попытку: с ходом все приняты;
- контроль без хода — на хранилище, которое уступает управление между загрузкой и записью, там конфликт есть;
- конфликт извне (хранилище отвечает конфликтом на первую запись): повтор идёт внутри хода, вторая команда,
  вставшая в очередь, начинается только после первой (журнал вызовов);
- `TurnRefused` → `Unavailable`; `Concurrent` → прежнее поведение.

**Интеграция на PostgreSQL** (`tests/postgres/command/…`):
- 40 параллельных команд к одному агрегату, повторы в одну попытку, ход включён: 40 приняты, журнал 1..40 без
  дыр, ни одного `Conflict`;
- «две ноды»: два исполнителя со своими реестрами, по 20 команд к одному агрегату, обычная политика повторов:
  журнал без дыр и дублей, все ответы — приняты или `Conflict` (как сейчас между процессами);
- `PostgresCommandConfig::new` с обоими вариантами `CommandOrdering`.

**demo:**
- разбор переменных: значения, ошибки с именем переменной;
- сборка использует конфигурацию;
- E2E smoke (временный скрипт на отдельном стенде) с `per_aggregate`: брони, verify `ok` до и после пересборки,
  машина времени.

**Проверки:** полный набор из корневого `CLAUDE.md`, покрытие ≥ 97 %, `--no-default-features` (компонент в ядре).

## Замер

`01-hot-key` 5 / 10 / 20 / 40 VU, профили `db` и `app`, k6; HEAD против `ORDERING=concurrent` против
`ORDERING=per_aggregate`, вперемешку, 3 прохода. Контроль — `02b-spread-vus50` в профиле `db`. Затем то же при
`NODES=2` (`BALANCE=round_robin` и `BALANCE=key`, задача [17](17_two_backend_nodes.md)).

Ожидание на горячем ключе:
- 409 и откаты → около 0;
- попыток записи на принятую команду ≈ 1 (сейчас ~10);
- CPU PostgreSQL на принятую команду заметно ниже, принятых команд/с больше;
- p99 может вырасти: ожидание в очереди становится видимым.

Потолок ключа ≈ 1 / (загрузка + решение + запись + обработчики). Сколько из этого займут обработчики, покажет
замер. От этой доли зависит, окупится ли шаг B.

## Критерии приёмки

- [ ] `AggregateKey`, `CommandOrdering`, `TurnLimits`, `AggregateTurns`, `AggregateTurn`, `TurnRefused` с
      комментариями и unit-тестами из списка выше, включая модельный тест.
- [ ] Исполнитель: ход вокруг повторов, `with_turns`, отказ → `Unavailable`; `CommandOrdering` в `PostgresCommandConfig::new`.
- [ ] Интеграционные тесты на PostgreSQL: 40 команд без конфликтов, «две ноды».
- [ ] Константы по умолчанию в `cqrsx`; demo: цепочка env → config → cqrsx, `.env.example`, сборка; E2E smoke.
- [ ] `components.md`: строка «Command turns», дерево исходников, правила имён (turn), правило о вложенных командах.
- [ ] Стенд: `ORDERING=`, `TURN_MAX_WAIT_MS=`, `TURN_MAX_WAITING=`; замер (одна и две ноды) записан в тикет.
