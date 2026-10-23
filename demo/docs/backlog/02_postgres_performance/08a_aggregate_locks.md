# 08a. Блокировка агрегата для команд (шаг A)

| | |
|---|---|
| Статус | сделано; умолчание demo по замеру 08a — `concurrent`; после [08b](08b_ordered_projection_delivery.md) (проекция вне блокировки) — `per_aggregate`; в [08c](08c_aggregate_lock_only.md) режим `concurrent` удалён |
| Ценность | высокая для горячих ключей: внутри процесса нет проигранных гонок, повторов и 409 |
| Цена | средняя–низкая: новый компонент ядра и обёртка вокруг повторов в исполнителе |
| cqrsx | `cqrs/aggregate.rs` (`AggregateKey`), `cqrs/command/turns.rs` (новый), `cqrs/command.rs` (модуль), `cqrs/command/executor.rs`, `postgres/command.rs`, `diagnostics.rs`, `doc/components.md` |
| demo | `back-end/src/config.rs`, `back-end/src/bootstrap.rs`, `.env.example`, `README.md`; замок кассы (`application/lock.rs`, `command_service.rs`, `operations/rebuild.rs`); тексты 503; стенд `demo/load` (`run.sh` `ORDERING=`, `TURN_MAX_WAIT_MS=`, `TURN_MAX_WAITING=`; `compose.yaml`), `perf/tools/compare.mjs` |
| Зависимости | 01 для замера, [17](17_two_backend_nodes.md) для замера с двумя нодами; общий план — [08](08_per_aggregate_queues.md) |
| Где выигрыш | 01 (горячий ключ) |

> **Имена после [08c](08c_aggregate_lock_only.md).** Механизм в 08a назывался «ход» (turn), теперь — «блокировка
> агрегата» (lock), а режима `concurrent` больше нет. Код, тесты и переменные ниже — в именах 08a:
> `turns.rs` → `locks.rs`, `AggregateTurns` → `AggregateLocks`, `AggregateTurn` → `AggregateLock`, `TurnLimits` →
> `LockLimits`, `TurnRefused` → `LockRefused`, `enter` → `acquire`, `DEFAULT_TURN_*` → `DEFAULT_LOCK_*`,
> `ENCORE_COMMAND_TURN_*` → `ENCORE_COMMAND_LOCK_*`, на стенде `TURN_MAX_*` → `LOCK_MAX_*`; `CommandOrdering`,
> `with_turns`, `ENCORE_COMMAND_ORDERING` и `ORDERING=` удалены. Варианты замера (`conc`, `turn`) и префиксы
> результатов остались прежними. После [08e](08e_executor_api.md): `max_waiting` → `max_waiters`
> (`DEFAULT_LOCK_MAX_WAITERS`, `ENCORE_COMMAND_LOCK_MAX_WAITERS`, на стенде `LOCK_MAX_WAITERS=`); `AggregateLocks`,
> `AggregateLock` и `LockRefused` стали внутренними, `CqrsCommandExecutor::new` принимает `LockLimits`.

## Что делаем

Команды к одному ключу (`aggregate_type`, `aggregate_id`) внутри процесса выполняются по одной, в порядке прихода.
Блокировка держится всю команду, включая повторы при конфликте и обработчики: в этом шаге обработчики остаются inline.

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
retry_on_aggregate_conflict(self.retry_policy, || framework.execute_with_metadata(…)).await   // повторы — под блокировкой
```

- `CqrsCommandExecutor::with_turns(AggregateTurns)` — для своих хранилищ. `AggregateTurns` — дешёвый хэндл,
  клон делит реестр.
- `PostgresCommandConfig::new(snapshot_interval, conflict_retry, ordering)`: `CommandOrdering` — обязательный
  аргумент, скрытого значения по умолчанию нет. `Default` для конфигурации убран: им пользовались два теста.
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
- стенд: `demo/load/run.sh ORDERING= TURN_MAX_WAIT_MS= TURN_MAX_WAITING=` (их принимает и `perf/run.sh`) →
  переменные бэкенда в `demo/load/compose.yaml`. Не в оверлее `perf/`: вторая нода (задача 17) наследует
  `backend` через `extends` только из `compose.yaml`.

## Алгоритм

Хранимой связки «HTTP-запрос ↔ агрегат» нет. Каждый HTTP-запрос и так выполняется в своей задаче tokio. Из
запроса берётся ключ агрегата (тип + id из URL), и по ключу задача запроса встаёт в очередь своего агрегата.
Дойдя до головы очереди, она сама выполняет команду, отвечает клиенту и уходит, а очередь будит следующего.
«Связка» существует, только пока запрос стоит в очереди или выполняется.

### Очередь одного ключа и реестр

Как реализовано (`cqrsx/src/cqrs/command/turns.rs`, сокращённо):

```rust
/// The line of one aggregate key. Tokio's mutex queues its waiters first in, first out and hands a
/// released turn straight to the first of them, so the waiter list of `turn` *is* the line of the key.
#[derive(Default)]
struct Lane {
    turn: Arc<tokio::sync::Mutex<()>>,
    waiting: AtomicUsize, // commands in the line, not counting the one holding the turn
}

struct Entry {
    lane: Arc<Lane>,
    users: usize, // live LaneRefs: commands holding or awaiting the turn; changed only under the lock
}

/// One line per aggregate key that is busy right now. The std mutex is held for a lookup,
/// an insert or a removal, never across an `.await`.
type Registry = std::sync::Mutex<HashMap<AggregateKey, Entry>>;
```

### Вход в очередь

```rust
pub async fn enter(&self, key: AggregateKey) -> Result<AggregateTurn, TurnRefused> {
    let lane = self.lane(key); // under the lock: find the entry or open one, `users += 1`
    // A free turn has nobody waiting for it (a released turn goes straight to the first waiter), so
    // taking it here never overtakes anyone. An idle key pays for no slot and no timer.
    if let Ok(guard) = lane.lane.turn.clone().try_lock_owned() {
        return Ok(AggregateTurn { _guard: guard, lane });
    }
    let guard = {
        let _slot = WaitingSlot::join(&lane.lane.waiting, self.limits.max_waiting).ok_or(TurnRefused::TooManyWaiting)?;
        tokio::time::timeout(self.limits.max_wait, lane.lane.turn.clone().lock_owned())
            .await
            .map_err(|_| TurnRefused::TimedOut)?
    }; // `_slot` leaves the count here: this command no longer waits, it holds the turn
    Ok(AggregateTurn { _guard: guard, lane })
}
```

`WaitingSlot::join` проверяет предел и увеличивает счётчик одним `fetch_update`, поэтому команды, входящие
одновременно, предел не превысят.

### Выход: блокировка переходит к следующему, пустая очередь исчезает

```rust
/// Fields drop in declaration order: first the guard (tokio passes the turn on), then the line reference.
pub struct AggregateTurn {
    _guard: tokio::sync::OwnedMutexGuard<()>,
    lane: LaneRef,
}

impl Drop for LaneRef {
    fn drop(&mut self) {
        let mut registry = lock(&self.registry);
        if let Some(entry) = registry.get_mut(&self.key) {
            entry.users -= 1;
            if entry.users == 0 {
                registry.remove(&self.key); // nobody holds or awaits the key: close its line
            }
        }
    }
}
```

Ссылки на очередь считаются только под блокировкой реестра, поэтому «больше никого нет» определяется точно.
Две команды, уходящие одновременно, не оставят висящую запись. Уход, совпавший со входом, не закроет занятую
очередь. Новая запись ключа появляется только после ухода последней ссылки на старую. Страж не переживает свою
`LaneRef`, поэтому у ключа не бывает двух очередей с занятой блокировкой.

В эскизе была `Weak<Lane>` в реестре и проверка `strong_count`. Явный счётчик `users` проще: его меняют только под
блокировкой, и не нужны `Option` и `take()` в `Drop`.

### Где блокировка берётся в пути команды

```rust
// cqrs/command/executor.rs
pub async fn execute_with_metadata(&self, aggregate_id: &str, command: A::Command, …) -> Result<(), CommandGatewayError<…>> {
    // The request's own task stands in the key's line; `_turn` lives until the end of the function.
    let _turn = match &self.turns {
        Some(turns) => Some(turns.enter(AggregateKey::of::<A>(aggregate_id)).await.map_err(turn_refused::<A::Error>(A::TYPE))?),
        None => None, // CommandOrdering::Concurrent: as before
    };
    let framework = (self.build_framework)(tasks); // after the turn: a refused command builds nothing
    retry_on_aggregate_conflict(self.retry_policy, || {
        framework.execute_with_metadata(aggregate_id, command.clone(), metadata.clone()) // load → decide → write → handlers
    })
    .await
    .map_err(map_aggregate_error)
} // `_turn` is dropped here: the next request for this key starts
```

Путь запроса: HTTP-обработчик axum → сценарий (`ConcertApp`) → `CommandService::run` →
`PostgresCommandExecutor::execute_with_metadata(id, …)` → код выше. HTTP-слою ничего знать не нужно: блокировка
целиком внутри исполнителя.

### Пример: три запроса

R1 бронирует A1 в `hall-1`, R2 — A2 в `hall-1`, R3 — B1 в `hall-2`:

| Момент | Что происходит | Реестр |
|---|---|---|
| t0 | R1 `enter(concert/hall-1)`: очереди нет, открывает L1, блокировка свободна — R1 выполняет команду | `hall-1 → L1` |
| t1 | R2 `enter(concert/hall-1)`: L1 есть, R2 встаёт в очередь (`waiting = 1`), его задача спит в списке ждущих L1 | `hall-1 → L1` |
| t2 | R3 `enter(concert/hall-2)`: открывает L2 и сразу выполняет, **параллельно** с R1 | `hall-1 → L1`, `hall-2 → L2` |
| t3 | R1 закончил: страж уничтожен, tokio будит R2 (первый в очереди L1). L1 остаётся: на неё ссылается R2. R1 отвечает 200 | то же |
| t4 | R2 загружает агрегат *уже с событием R1*, конфликта нет, записывает с первой попытки | то же |
| t5 | R3 закончил: на L2 никто не ссылается — запись удалена | `hall-1 → L1` |
| t6 | R2 закончил: L1 удалена | пусто |

Без блокировки R1 и R2 оба загрузили бы номер N, оба попытались бы записать N+1, один получил бы конфликт и повторил
через 5–50 мс. На горячем ключе с 40 VU так проигрывается ~90 % попыток.

### Что из этого следует

- **Разные ключи не мешают друг другу:** у них разные мьютексы. Общая блокировка реестра держится на поиск в
  `HashMap` и никогда через `.await`.
- **FIFO** гарантирует tokio: «Tokio's Mutex operates on a guaranteed FIFO basis».
- **Клиент ушёл, пока ждал:** axum уничтожает future запроса. Ожидание `lock_owned` снимается из очереди,
  `WaitingSlot` уменьшает счётчик, `LaneRef` при необходимости удаляет запись. Команда не выполнится — для
  команды до коммита это правильно.
- **Паника в команде** разворачивает стек и уничтожает `AggregateTurn`, блокировка переходит следующему.
- **Память:** запись живёт, только пока ключ кому-то нужен. На горячем ключе с 40 VU очередь — до 39 ждущих, ни
  один предел не срабатывает: 40 × ~2.5 мс ≈ 0.1 с при `max_wait` = 1 с.
- **Несколько нод:** у каждого процесса свой реестр. Команда к `hall-1` на ноде 2 стоит в очереди ноды 2, между
  нодами конфликт по-прежнему ловит база.

Это эскиз: имена и детали уточняются при реализации, но поведение и инварианты — эти.

## Пограничные случаи

Тесты `turns.rs` и `executor.rs` — модули `cqrsx::cqrs::command::*::tests`, PostgreSQL —
`cqrsx/tests/postgres/command.rs`, demo — по файлу.

| # | Случай | Поведение | Проверка |
|---|---|---|---|
| 1 | Команды к разным ключам | параллельно; общая блокировка реестра — только на поиск в `HashMap` | `keys_are_independent_also_for_the_same_id_of_another_type` |
| 2 | Тот же `aggregate_id`, другой тип агрегата | разные ключи | то же |
| 3 | Порядок | FIFO по первому опросу `enter`, то есть по приходу в исполнитель (гарантия `tokio::sync::Mutex`) | `one_holder_per_key_and_waiters_enter_in_arrival_order`, модель |
| 4 | Новая команда приходит в момент передачи блокировки | не обгоняет: tokio отдаёт блокировку прямо голове очереди, быстрый путь — только `try_lock` | `a_newcomer_never_overtakes_a_waiter` |
| 5 | Клиент ушёл, пока ждал | снят из очереди, порядок остальных цел, счётчик −1, последняя запись удалена | `a_cancelled_waiter_keeps_the_order_and_leaves_the_count`, `the_last_waiter_cancelled_closes_the_line`, модель |
| 6 | Ждущий уничтожен после того, как ему передали блокировку | блокировка переходит следующему | `a_turn_handed_to_a_dropped_waiter_passes_to_the_next`, модель |
| 7 | Ожидание дольше `max_wait` | `TimedOut` → 503; в счётчике не числится; если передача блокировки и таймер совпали, побеждает блокировка (`timeout` сперва опрашивает внутреннее будущее) | `times_out_after_max_wait_uncounted_and_the_next_enters`, `a_turn_handed_over_at_the_deadline_wins_over_the_timeout`, модель |
| 8 | Ждущих уже `max_waiting` | немедленный 503 без таймера; держатель блокировки не считается | `refuses_past_max_waiting_at_once_holder_not_counted`, модель |
| 9 | Параллельный вход на пределе | ждут ровно `max_waiting` | `exactly_max_waiting_wait_under_parallel_entry` (многопоточный рантайм) |
| 10 | Отмена или паника держателя | блокировка отпущена при уничтожении стража (при `panic = unwind`) | `a_panicking_holder_releases_the_turn_to_the_next_in_line` |
| 11 | Двое уходят разом, уход совпал со входом | запись удаляется точно, висящих записей нет, держатель ключа всегда один | `idle_registry_is_empty_after_parallel_churn_and_holders_never_overlap`, модель |
| 12 | Отравленный мьютекс реестра | `into_inner`: под блокировкой ничего не паникует | ревью |
| 13 | `mem::forget(AggregateTurn)` | ключ занят до перезапуска | `#[must_use]`, doc-комментарий |
| 14 | Память | записи — только занятых ключей; ёмкость `HashMap` держится на пике числа занятых ключей | doc-комментарий |
| 15 | Нулевые или огромные пределы | ноль — ошибка; большой `max_wait` допустим (у tokio дальний дедлайн) | `limits_reject_zero_and_default_uses_the_constants` |
| 16 | Конфликт при записи (другая нода) | повтор под блокировкой; следующая команда ждёт; паузы повторов держат блокировку — так и задумано | `a_conflict_retries_inside_the_turn_before_the_next_command_starts` (с контролем без блокировки) |
| 17 | Отказ в блокировке | `Unavailable`, `warn` без id агрегата; фреймворк не строится, задачи не публикуются | `a_refused_turn_is_unavailable_logged_without_id_and_builds_nothing` |
| 18 | `CommandOrdering::Concurrent` | реестра нет, путь прежний | прежние тесты, замер против образа до 08a |
| 19 | Обработчик синхронно командует своим агрегатом (или чужим, тот — его) | ждёт блокировки до `max_wait` → `CommittedWithEventHandlerFailure`; правило в `components.md`: такие команды — через outbox | `a_handler_waiting_for_its_own_aggregate_times_out_instead_of_hanging` |
| 20 | Будущее команды держит блокировку через `.await` | остаётся `Send` | `the_command_future_with_a_turn_is_send` |
| 21 | Два исполнителя одного типа (или две ноды) | свои реестры, между ними — проверка в базе и повтор | `two_executors_with_their_own_turns_meet_only_in_the_database` |
| 22 | Медленная база (запрос до 4 с) | ждущие получают 503 через `max_wait` (1 с), а не копятся в очереди к пулу | doc |
| 23 | 40 параллельных команд к одному агрегату, одна попытка | все приняты, журнал 1..40, обработчик видит события в порядке коммитов | `per_aggregate_turns_accept_forty_parallel_commands_without_conflicts` |
| 24 | Горячий ключ и пул соединений | ключ держит одно соединение вместо десяти, пул свободен для других ключей | замер |
| 25 | Замок кассы проверяется до блокировки агрегата | команда получает пропуск (`BoxOfficeLock::admit`), Encore! после взятия замка ждёт все выданные пропуски (`wait_idle`) и только потом стирает модели | `a_rebuild_wipes_nothing_until_the_commands_admitted_before_it_have_finished`, `a_running_command_holds_back_a_rebuild_and_a_later_one_is_locked_out`, `lock.rs`, E2E |
| 26 | Цепочечные задачи `Fan` | тот же исполнитель и реестр, что у HTTP; `ENCORE_COMMAND_TURN_MAX_WAIT_MS` должен быть меньше `OUTBOX_HANDLER_TIMEOUT_SECS`, иначе старт отклоняется | `command_ordering_names_the_variable_it_rejects` |
| 27 | Отказ в блокировке у цепочечной задачи | `Unavailable` → повтор через 1 с (`map_fan_outcome`) | прежний тест |
| 28 | Тексты 503 | нейтральные: «журнал событий недоступен или концерт занят» | ревью |
| 29 | Остановка процесса | axum ждёт запросы, включая ждущих блокировки: не дольше `max_wait` + команда | doc |
| 30 | Запрос уничтожен после коммита | обработчики не выполнятся (как раньше) | 08b |
| 31 | Две ноды | `BALANCE=key`: все команды ключа на одной ноде; `round_robin`: по претенденту с ноды, дальше база | замер, серия B |
| 32 | VU больше `max_waiting` + 1 на ключ | немедленные 503; k6 ждёт 50 мс | doc |

## Тесты

**Unit, `cqrs/command/turns.rs`.** Будущие опрашиваются вручную (`Waker::noop()`) на остановленных часах, время
двигает только `tokio::time::advance`. Ждущих нельзя ждать через `.await`: простаивающий рантайм сам продвигает
остановленные часы до ближайшего таймера и выводит всех ждущих по таймауту. Тесты — в таблице выше. Модельный
тест `turns_match_a_fifo_model` (`proptest`, 256 случаев до 60 шагов): операции `Enter`, `Release`,
`ReleaseAndDropHead`, `Cancel`, `Advance` по трём ключам против модели «держатель + FIFO с дедлайнами»; после
каждого шага сверяются вошедшие, отказы, `waiting` и число записей. Модель проверена на зубы: без уменьшения
счётчика ждущих и без удаления пустой записи она падает.

**Unit, исполнитель** (`cqrs/command/executor.rs`, хранилище с проверкой версии, уступающее управление между
загрузкой и записью): `turns_accept_twenty_parallel_commands_with_a_single_attempt_where_concurrency_conflicts`
(с блокировкой 20 из 20, контроль без неё — 1 и 19 конфликтов) и тесты 16, 17, 19, 20 из таблицы.

**PostgreSQL:** 23 и 21 из таблицы. Конфликтный тест `a_conflicting_command_reloads_the_winner_…` закреплён на
`Concurrent`: ему нужны две команды в базе одновременно.

**demo:**
- `config.rs`: `command_ordering_defaults_to_turns_with_the_library_limits`, `command_ordering_names_the_variable_it_rejects`;
- замок кассы — тесты случая 25;
- `tests/write_side.rs`: `parallel_bookings_of_one_concert_take_turns_and_need_no_retry` (20 броней одного
  концерта, одна попытка: все приняты).

## Замер

2026-10-21. Образы собраны заранее, во время серии ничего не компилировалось:
- `encore-load-backend:pre08a` — код до 08a (бэкенд не менялся с `a747f06`);
- `encore-load-backend:08a` — рабочее дерево.

Варианты:
- `pre` — образ до 08a;
- `conc` — 08a с `ORDERING=concurrent`;
- `turn` — 08a с `ORDERING=per_aggregate` (блокировка агрегата).

3 прохода по 30 прогонов, порядок внутри прохода перемешан, `POOL=20`, k6, CPU под квотой. Префиксы результатов:
`perf-08a-{pre,conc,turn}-{db,app}-` (серия A, одна нода) и `perf-08a-{n1lb,n2rr,n2key}-{conc,turn}-` (серия B,
профиль `app`). Медианы трёх повторов, в скобках — изменение, `≠` — диапазоны повторов не пересекаются.

```bash
cd demo/docs/backlog/02_postgres_performance/perf
node tools/compare.mjs ../../../../load/results perf-08a-conc-app- perf-08a-turn-app- --md
```

**Серия A, горячий ключ, профиль `app`** (бэкенд 2 ядра):

| VU | принято/с: concurrent → turn | 409 % | p50, мс | p99, мс | CPU PG, мс на 1000 | CPU бэкенда, мс на 1000 | запросов PG на команду |
|---|---|---|---|---|---|---|---|
| 1 | — → 414 | — → 0 | — → 2.04 | — → 5.3 | — → 1302 | — → 2110 | — → 6.8 |
| 5 | 522 → 525 (+1 %) | 11.9 → 0 | 3.7 → 8.9 | 24.3 → 19.0 | 1935 → 1570 (−19 % ≠) | 1900 → 1707 (−10 % ≠) | 11.3 → 8.2 |
| 10 | 607 → 527 (−13 % ≠) | 28.2 → 0 | 9.3 → 17.9 | 26.0 → 38.6 | 2236 → 1718 (−23 % ≠) | 1707 → 1546 (−9 % ≠) | 14.5 → 9.0 |
| 20 | 670 → 511 (−24 % ≠) | 49.8 → 0 | 18.5 → 37.0 | 28.5 → 73.8 | 2828 → 1830 (−35 % ≠) | 1824 → 1438 (−21 % ≠) | 20.7 → 9.7 |
| 40 | 641 → 494 (−23 % ≠) | 69.7 → 0 | 20.6 → 78.2 | 41.2 → 156 | 3966 → 2040 (−49 % ≠) | 2489 → 1528 (−39 % ≠) | 33.2 → 10.5 |

**Серия A, горячий ключ, профиль `db`** (PostgreSQL 2 ядра):

| VU | принято/с: concurrent → turn | 409 % | p50, мс | p99, мс | CPU PG, мс на 1000 | CPU бэкенда, мс на 1000 | запросов PG на команду |
|---|---|---|---|---|---|---|---|
| 5 | 544 → 515 (−5 %) | 12.2 → 0 | 3.2 → 9.0 | 23.6 → 16.5 | 1921 → 1607 (−16 % ≠) | 2095 → 1817 (−13 % ≠) | 11.1 → 8.2 |
| 10 | 633 → 516 (−18 %) | 28.0 → 0 | 9.0 → 18.2 | 24.9 → 43.5 | 2283 → 1786 (−22 % ≠) | 1977 → 1723 (−13 % ≠) | 14.4 → 9.1 |
| 20 | 598 → 529 (−12 % ≠) | 51.0 → 0 | 18.8 → 37.1 | 43.4 → 47.6 | 3000 → 1877 (−37 % ≠) | 2272 → 1637 (−28 % ≠) | 21.2 → 9.8 |
| 40 | 450 → 495 (+10 % ≠) | 73.0 → 0 | 23.7 → 78.5 | 59.4 → 122 | 4389 → 2004 (−54 % ≠) | 3351 → 1668 (−50 % ≠) | 35.1 → 10.4 |

**Контроль, серия A:**
- `pre` против `conc` на горячем ключе при 40 VU: `db` — 462 и 450 принятых/с, `app` — 639 и 641; CPU и запросы на команду
  совпадают. Путь `Concurrent` не изменился.
- `02b-spread-vus50` (`db`): `pre`, `conc` и `turn` дают 3629, 3629 и 3752 команд/с; диапазоны пересекаются, CPU на
  команду совпадает (бэкенд 655–657, PG 552–556 мс на 1000). Реестр на тысячах ключей не виден.

**Серия B, горячий ключ, 40 VU, профиль `app`, nginx:**

| Топология | принято/с: concurrent → turn | 409 % | p50, мс | CPU PG, мс на 1000 | CPU бэкенда, мс на 1000 |
|---|---|---|---|---|---|
| 1 нода за nginx | 646 → 481 (−25 % ≠) | 69.4 → 0 | 20.7 → 78.7 | 3984 → 2022 (−49 % ≠) | 2535 → 1529 (−40 % ≠) |
| 2 ноды, по очереди | 676 → 532 (−21 % ≠) | 68.0 → 4.0 | 20.5 → 71.3 | 4230 → 2175 (−49 % ≠) | 2284 → 1409 (−38 % ≠) |
| 2 ноды, по ключу | 544 → 507 (−7 % ≠) | 69.1 → 0 | 24.8 → 74.6 | 4224 → 2082 (−51 % ≠) | 2110 → 1342 (−36 % ≠) |

`02b-spread-vus50` на двух нодах по очереди: 4354 → 4357 команд/с, CPU на команду одинаковый.

### Выводы

1. **Блокировка делает то, что обещала.**
   - 409 исчезают на одной ноде и на двух с привязкой по ключу. На двух нодах по очереди остаётся 4 %: это
     конфликты между нодами, их разбирает база.
   - Работа на принятую команду на горячем ключе падает вдвое: запросов PG 33 → 10.5 на команду, CPU PG −49 %,
     бэкенда −39 % (40 VU, `app`).
   - Накладных расходов на многих ключах нет, путь `Concurrent` не изменился.
2. **Потолок ключа с блокировкой — задержка одной команды.**
   - Одна команда держит блокировку ~2 мс: загрузка, решение, запись и inline-проекция в MongoDB, при VUS=1 p50 = 2.04 мс.
     Отсюда ~500 принятых/с при любом числе VU.
   - `concurrent` перекрывает команды друг с другом и в профиле `app` берёт 600–670/с, платя вдвое большим CPU и
     50–70 % ответов 409.
   - В профиле `db` при 40 VU повторы `concurrent` выжигают CPU PostgreSQL, и блокировка выигрывает 10 %.
3. **Ожидание в очереди видно в задержке:** p50 ≈ VU × время под блокировкой (закон Литтла), при 40 VU — ~78 мс против ~21.
   503 нет: 40 ждущих × 2 мс далеко от `max_wait` = 1 с.
4. **Умолчание demo — `ENCORE_COMMAND_ORDERING=concurrent`.** Это правило из плана: `per_aggregate` только при
   принятых/с не хуже в обоих профилях, а в `app` блокировка теряет 13–24 %. `per_aggregate` включается переменной, когда
   важнее CPU на команду и отсутствие 409, чем пиковая пропускная способность одного ключа.
5. **Рычаг — шаг B ([08b](08b_ordered_projection_delivery.md)).** Вынести проекцию из-под блокировки: время под
   блокировкой сожмётся до «загрузка + решение + запись». Насколько, покажет замер 08b с `DELIVERY=ordered`. До этого времени, которое
   проекция занимает под блокировкой, отдельно не измерено.

## Критерии приёмки

- [x] `AggregateKey`, `CommandOrdering`, `TurnLimits`, `AggregateTurns`, `AggregateTurn`, `TurnRefused` с
      комментариями и unit-тестами из списка выше, включая модельный тест.
- [x] Исполнитель: блокировка вокруг повторов, `with_turns`, отказ → `Unavailable`; `CommandOrdering` в `PostgresCommandConfig::new`.
- [x] Интеграционные тесты на PostgreSQL: 40 команд без конфликтов, «две ноды».
- [x] Константы по умолчанию в `cqrsx`; demo: цепочка env → config → cqrsx, `.env.example`, сборка; E2E smoke
      (брони, Encore! посреди всплеска в трёх раундах, verify `ok` после каждого, машина времени).
- [x] `components.md`: строка «Command turns», дерево исходников, правила имён (turn, в 08c — lock), правило о вложенных командах.
- [x] Стенд: `ORDERING=`, `TURN_MAX_WAIT_MS=`, `TURN_MAX_WAITING=`; замер (одна и две ноды) записан в тикет.
- [x] Encore! ждёт команды, допущенные до замка (случай 25).

Проверки: полный набор из корневого `CLAUDE.md` зелёный, покрытие 97.78 % строк (`turns.rs` — 99.58 %); demo —
fmt, clippy, тесты (включая `write_side` на PostgreSQL), фронтенд — typecheck, lint, 54 теста.
