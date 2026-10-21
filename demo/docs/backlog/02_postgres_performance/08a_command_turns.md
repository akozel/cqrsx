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
#[derive(Clone)]
pub struct AggregateTurns { limits: TurnLimits, registry: Arc<Registry> }   // Registry — см. «Алгоритм»
impl AggregateTurns {
    pub fn new(limits: TurnLimits) -> Self { … }
    /// Waits for the key's turn in arrival order; refuses past the limits.
    pub async fn enter(&self, key: AggregateKey) -> Result<AggregateTurn, TurnRefused> { … }
}

/// Held while a command runs; dropping it hands the turn to the next waiter in order.
pub struct AggregateTurn { … }
pub enum TurnRefused { TimedOut, TooManyWaiting }
```

Как это устроено внутри — в разделе «Алгоритм» ниже.

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

## Алгоритм

Хранимой связки «HTTP-запрос ↔ агрегат» нет. Каждый HTTP-запрос и так выполняется в своей задаче tokio. Из
запроса берётся ключ агрегата (тип + id из URL), и по ключу задача запроса встаёт в очередь своего агрегата.
Дойдя до головы очереди, она сама выполняет команду, отвечает клиенту и уходит, а очередь будит следующего.
«Связка» существует, только пока запрос стоит в очереди или выполняется.

### Очередь одного ключа и реестр

```rust
// cqrs/command/turns.rs
/// The line of one aggregate key. Tokio's Mutex queues its waiters first in, first out,
/// so the waiter list of `turn` *is* the line of the key.
#[derive(Default)]
struct Lane {
    turn: Arc<tokio::sync::Mutex<()>>,
    waiting: AtomicUsize, // commands in the line, not counting the one holding the turn
}

/// One line per aggregate key that is busy right now. The std mutex guards only the map
/// and is held for a lookup or an insert, never across an `.await`.
type Registry = std::sync::Mutex<HashMap<AggregateKey, Weak<Lane>>>;
```

### Вход в очередь

```rust
impl AggregateTurns {
    /// Waits for the key's turn behind the commands that came before; refuses past the limits.
    pub async fn enter(&self, key: AggregateKey) -> Result<AggregateTurn, TurnRefused> {
        // 1. Find the key's line, or open one if nobody holds or awaits this key.
        let lane = self.lane(key);
        // 2. Stand in the line, unless it is already too long; 3. wait in arrival order, at most max_wait.
        let guard = {
            let _slot = WaitingSlot::join(&lane.waiting, self.limits.max_waiting)
                .ok_or(TurnRefused::TooManyWaiting)?;
            tokio::time::timeout(self.limits.max_wait, lane.turn.clone().lock_owned())
                .await
                .map_err(|_| TurnRefused::TimedOut)?
        }; // `_slot` leaves the count here: this command no longer waits, it holds the turn
        Ok(AggregateTurn { _guard: guard, _lane: lane })
    }

    fn lane(&self, key: AggregateKey) -> LaneRef {
        let mut registry = self.registry.lock().unwrap_or_else(PoisonError::into_inner);
        let lane = match registry.get(&key).and_then(Weak::upgrade) {
            Some(lane) => lane, // someone holds or awaits this key: join their line
            None => {
                let lane = Arc::new(Lane::default()); // the first command for the key opens a line
                registry.insert(key.clone(), Arc::downgrade(&lane));
                lane
            }
        };
        LaneRef { registry: self.registry.clone(), key, lane: Some(lane) }
    }
}
```

### Выход: ход переходит к следующему, пустая очередь исчезает

```rust
/// The key's turn, held while one command runs. Fields drop in declaration order:
/// first the guard (tokio wakes the next command in the line), then the line handle.
pub struct AggregateTurn {
    _guard: tokio::sync::OwnedMutexGuard<()>,
    _lane: LaneRef,
}

/// A reference to a key's line, held by every command that waits in it or holds its turn.
struct LaneRef {
    registry: Arc<Registry>,
    key: AggregateKey,
    lane: Option<Arc<Lane>>,
}

impl Drop for LaneRef {
    fn drop(&mut self) {
        let mut registry = self.registry.lock().unwrap_or_else(PoisonError::into_inner);
        // References are taken and released only under this lock, so "nobody left" is exact.
        drop(self.lane.take());
        if registry.get(&self.key).is_some_and(|lane| lane.strong_count() == 0) {
            registry.remove(&self.key); // nobody holds or awaits the key: close its line
        }
    }
}

/// Counts one command in the line and uncounts it on every exit: turn acquired,
/// timeout, or the request dropped while waiting (the client went away).
struct WaitingSlot<'a>(&'a AtomicUsize);
```

Ссылки на очередь (`Arc<Lane>`) появляются и исчезают только под блокировкой реестра. Поэтому проверка «больше
никого нет» точна: две команды, уходящие одновременно, не оставят в реестре висящую запись. Освободить свою
ссылку *до* проверки нужно именно под блокировкой: иначе обе уходящие команды увидели бы друг друга и ни одна не
удалила бы запись.

### Где ход берётся в пути команды

```rust
// cqrs/command/executor.rs
pub async fn execute_with_metadata(&self, aggregate_id: &str, command: A::Command, …) -> Result<(), CommandGatewayError<…>> {
    // The request's own task stands in the key's line; `_turn` lives until the end of the function.
    let _turn = match &self.turns {
        Some(turns) => Some(turns.enter(AggregateKey::of::<A>(aggregate_id)).await.map_err(turn_refused)?),
        None => None, // CommandOrdering::Concurrent: as before
    };
    let framework = (self.build_framework)(tasks);
    retry_on_aggregate_conflict(self.retry_policy, || {
        framework.execute_with_metadata(aggregate_id, command.clone(), metadata.clone()) // load → decide → write → handlers
    })
    .await
    .map_err(map_aggregate_error)
} // `_turn` is dropped here: the next request for this key starts
```

Путь запроса: HTTP-обработчик axum → сценарий (`ConcertApp`) → `CommandService::run` →
`PostgresCommandExecutor::execute_with_metadata(id, …)` → код выше. HTTP-слою ничего знать не нужно: ход целиком
внутри исполнителя.

### Пример: три запроса

R1 бронирует A1 в `hall-1`, R2 — A2 в `hall-1`, R3 — B1 в `hall-2`:

| Момент | Что происходит | Реестр |
|---|---|---|
| t0 | R1 `enter(concert/hall-1)`: очереди нет, открывает L1, ход свободен — R1 выполняет команду | `hall-1 → L1` |
| t1 | R2 `enter(concert/hall-1)`: L1 есть, R2 встаёт в очередь (`waiting = 1`), его задача спит в списке ждущих L1 | `hall-1 → L1` |
| t2 | R3 `enter(concert/hall-2)`: открывает L2 и сразу выполняет, **параллельно** с R1 | `hall-1 → L1`, `hall-2 → L2` |
| t3 | R1 закончил: страж уничтожен, tokio будит R2 (первый в очереди L1). L1 остаётся: на неё ссылается R2. R1 отвечает 200 | то же |
| t4 | R2 загружает агрегат *уже с событием R1*, конфликта нет, записывает с первой попытки | то же |
| t5 | R3 закончил: на L2 никто не ссылается — запись удалена | `hall-1 → L1` |
| t6 | R2 закончил: L1 удалена | пусто |

Без хода R1 и R2 оба загрузили бы номер N, оба попытались бы записать N+1, один получил бы конфликт и повторил
через 5–50 мс. На горячем ключе с 40 VU так проигрывается ~90 % попыток.

### Что из этого следует

- **Разные ключи не мешают друг другу:** у них разные мьютексы. Общая блокировка реестра держится на поиск в
  `HashMap` и никогда через `.await`.
- **FIFO** гарантирует tokio: «Tokio's Mutex operates on a guaranteed FIFO basis».
- **Клиент ушёл, пока ждал:** axum уничтожает future запроса. Ожидание `lock_owned` снимается из очереди,
  `WaitingSlot` уменьшает счётчик, `LaneRef` при необходимости удаляет запись. Команда не выполнится — для
  команды до коммита это правильно.
- **Паника в команде** разворачивает стек и уничтожает `AggregateTurn`, ход переходит следующему.
- **Память:** запись живёт, только пока ключ кому-то нужен. На горячем ключе с 40 VU очередь — до 39 ждущих, ни
  один предел не срабатывает: 40 × ~2.5 мс ≈ 0.1 с при `max_wait` = 1 с.
- **Несколько нод:** у каждого процесса свой реестр. Команда к `hall-1` на ноде 2 стоит в очереди ноды 2, между
  нодами конфликт по-прежнему ловит база.

Это эскиз: имена и детали уточняются при реализации, но поведение и инварианты — эти.

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
