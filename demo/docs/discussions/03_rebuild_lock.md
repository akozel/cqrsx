# 03. Rebuild-lock: касса закрыта во время Encore!

Статус: обсуждается. Откуда: аудит K-1 (§4.1 [`../BOUNDARY_AUDIT.md`](../BOUNDARY_AUDIT.md)).
Решение владельца демо: оставить lock как осознанное упрощение или перейти к «живой» пересборке (PLAN.md §2.9, P2).

## 1. Контекст

Пока идёт Encore!, касса закрыта: `BoxOfficeLock` (`AtomicBool` + RAII-guard) держится всё время пересборки.
Это не требование CQRS/ES — write side не зависит от read side — а способ сделать сцену демо детерминированной
(PLAN.md §3.5: «простой детерминированный сценарий; „живой“ rebuild — P2»).

## 2. След в коде

| Где | Что |
|---|---|
| `application/lock.rs` | сам lock |
| `application/command_service.rs` | `run`: `if lock.is_locked() → Executed::locked()` до любого I/O |
| `application/concert/cancel_booking.rs` | вторая проверка до чтения журнала (задача строится заранее) |
| `application/workflows.rs` | `wait_for_unlock` (до 3 с), `LOCKED_RETRY_AFTER` (2 с), исход `Locked → Retry { rebuild_in_progress }` |
| `application/operations/rebuild.rs` | берёт lock в `start`, `RunningGuard` отпускает |
| `presentation/http/concerts/execute_command.rs` | `423 rebuild_in_progress` раньше валидации тела |
| `presentation/http/projections/verify.rs` | verify отказывает `423` во время rebuild |
| `config.rs` | `ENCORE_CHAIN_MAX_ATTEMPTS = 60`: бюджет попыток задач цепочки нужен только потому, что они тратят попытки на ожидание lock |
| front-end | `states.locked` («Касса закрыта на минутку»), блокировка кнопок при `running`, обработка 423 в `commands.ts` |
| `bootstrap.rs`, `testing.rs`, тесты use case'ов | прокидывание `Arc<BoxOfficeLock>` |

Итого lock или его следствия в 12 файлах back-end помимо `lock.rs`.

## 3. Варианты

### A. Оставить и назвать упрощением явно

Ничего не менять в коде; в README «Architecture» добавить абзац «the box-office lock is a demo simplification:
a production service would rebuild live». Плюсы: нулевая цена, детерминированная сцена, честный показ `423` и
повтора задач по `retry_after` (это само по себе хорошая демонстрация outbox). Минусы: самый заметный
«не production» паттерн остаётся в пути записи; читатель кода видит проверку lock в каждом use case.

### B. «Живой» rebuild (P2)

Убрать lock целиком. Пересборка идёт параллельно с live-обновлениями; корректность обеспечивают свойства витрин:

- LWW по `sequence` на запись места/билета: событие из replay с меньшим `sequence` не перетирает более новую
  live-запись; производные поля (`sold`, `revenue`, `sold_out`, `active_count`) пересчитываются из карты.
- Документ, созданный live-обновлением после wipe (`incomplete: true`), получает `ConcertAnnounced` из replay и
  становится полным.
- Конфликты версий между replay и live-обработчиком решает `ViewProjection` (до 16 повторов).

Что уходит: `lock.rs`, проверки в `command_service`/`cancel_booking`, `wait_for_unlock` и `Locked` в `workflows`,
`423` в HTTP и UI, `ENCORE_CHAIN_MAX_ATTEMPTS` (цепочка возвращается к обычному бюджету). Что теряется в сцене:
гарантия «во время Encore! ничего не мерцает» и показ `423`. Что появляется: тезис «write side не зависит от
read side» становится буквальным — во время Encore! можно бронировать.

Риски: мерцание зала при одновременных бронированиях во время медленного replay (80 мс/событие); `verify` без
lock уже сейчас живёт с честным `differs` при гонке — для rebuild это станет нормой.

### C. Lock только на wipe

Держать lock миллисекунды — на время `drop` коллекций — и отпускать до replay. Убирает окно «витрина пуста,
а касса принимает» (появление `incomplete` документов), но сохраняет весь код lock. Промежуточный вариант без
явного выигрыша: цена как у A, честность как у B лишь наполовину.

## 4. Что нужно, чтобы решить

- Насколько важна детерминированность сцены Encore! на проекторе (A) против чистоты пути записи (B).
- Хочет ли демо показывать `423` + повтор задач как отдельный «aha» (тогда A), или достаточно `🎲 retry`
  почтовых задач (тогда B).
- Если B: добавить сценарий «бронируем во время Encore!» в README «Demo script» и тест на сходимость (replay +
  live-запись того же места → один и тот же документ).

Рекомендация аудита: A сейчас (явно задокументировать), B — отдельной фазой, когда демо-сценарий устоится.
