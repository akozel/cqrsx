// Что такое каждый блок диаграммы архитектуры, что он делает в демо и, для агрегатов, правила.
// Ключи повторяют id узлов и рамок из components/pipeline/graph.ts.
const concertRules = [
  { code: 'R1 concert_not_found', when: 'BookSeat, CancelBooking', check: 'концерт анонсирован' },
  { code: 'R2 already_announced', when: 'AnnounceConcert', check: 'концерт анонсируют один раз' },
  {
    code: 'R3 unknown_seat',
    when: 'BookSeat, CancelBooking',
    check: 'место существует: ряды A–E, места 1–8',
  },
  { code: 'R4 sold_out', when: 'BookSeat', check: 'ещё есть свободные места' },
  { code: 'R5 seat_taken', when: 'BookSeat', check: 'место свободно' },
  { code: 'R6 ticket_limit', when: 'BookSeat', check: 'у фаната меньше 4 билетов на этот концерт' },
  { code: 'R7 seat_not_booked', when: 'CancelBooking', check: 'место забронировано' },
  { code: 'R8 not_your_ticket', when: 'CancelBooking', check: 'отменяет только владелец' },
]
const fanRules = [
  {
    code: 'R9 already_recorded',
    when: 'RecordTicket',
    check: 'бронь ещё не записана — иначе задача считается выполненной (повторная доставка)',
  },
  {
    code: 'R10 unknown_ticket',
    when: 'RecordCancellation',
    check: 'билет существует — иначе retry через 1 с (отмена обогнала задачу записи)',
  },
  {
    code: 'R11 already_cancelled',
    when: 'RecordCancellation',
    check: 'билет ещё активен — иначе задача считается выполненной (повторная доставка)',
  },
]

export default {
  _head: { code: 'Правило', when: 'Команда', check: 'Проверяет, что' },
  svc_concert: {
    title: 'Concert service',
    lead: 'Сторона записи кассы: по одному потоку событий на концерт, все бизнес-правила и outbox-задачи, которые приводят в движение сервис Fan.',
    points: [
      'Получает команды из UI по HTTP: POST /api/concerts/{id}/commands.',
      'Проверяет правила синхронно. Отклонённый клик не оставляет следа в базе (Δ events = 0).',
      'Фиксирует событие и его outbox-задачу в одной транзакции PostgreSQL.',
      'Никогда не зовёт сервис Fan напрямую: это делает обработчик outbox-задачи — позже и надёжно.',
    ],
  },
  svc_fan: {
    title: 'Fan service',
    lead: 'Нижестоящая сторона: по потоку на пару фанат + концерт с билетами фаната на этот концерт. Команды сюда приходят только из outbox-задач, поэтому правила здесь про идемпотентность, а не про бизнес.',
    points: [
      'Команды приходят от outbox-воркера сервиса Concert с correlation_id исходного клика.',
      'Его события запускают вторую цепочку outbox: письмо с e-ticket или возвратом.',
      'Проецируется в кошелёк фаната слева.',
    ],
  },
  svc_projection: {
    title: 'projection',
    lead: 'Сторона чтения: документы MongoDB, собранные из событий и удобные для экрана.',
    points: [
      'concert_halls — схема зала со счётчиками sold и revenue; fan_wallets — билеты каждого фаната.',
      'Обновляется в процессе сразу после коммита, без гарантий доставки. Поэтому и существует Encore! (replay).',
      '🧨 Стереть — опустошает коллекции; ⏪ Encore! — проигрывает весь журнал через те же проекции.',
      'Почтовый ящик — не проекция: replay никогда не отправляет письма заново.',
    ],
  },
  ui_in: {
    title: 'UI (касса)',
    lead: 'Тот же экран со стороны записи: каждый клик превращается в команду, отправленную по HTTP.',
    points: [
      'BookSeat на свободное (или занятое!) место, CancelBooking на своё; кнопки 🤖 и 🌊 шлют их пачками.',
      'UI сам правила не проверяет. Решает агрегат, UI лишь показывает ответ.',
    ],
  },
  user: {
    title: 'user',
    lead: 'Вы в роли одного из персонажей: 🦊 Fox, 🐼 Panda, 🦄 Unicorn или 🤖 Arbitrage Bot (или любого фаната из толпы).',
    points: [
      'Клик по месту — команда: BookSeat на свободное (или занятое!) место, CancelBooking на своё.',
      'UI сам правила не проверяет. Решает агрегат, UI лишь показывает ответ.',
      '🤖 Перекупщик и 🌊 Толпа шлют много команд подряд; ⚡ Гонка — две на одно место одновременно.',
    ],
  },
  command: {
    title: 'Command → Concert',
    lead: 'Просьба, а не факт: AnnounceConcert, BookSeat или CancelBooking.',
    points: [
      'Несёт метаданные: command_id, correlation_id (проходит всю цепочку), causation_id и source = ui.',
      'Принята → 200 со списком порождённых outbox-задач; отклонена → 422 с кодом правила; во время rebuild → 423.',
      'Попробуйте 🧪 нарушителей правил: заведомо неверная команда и точное правило, которое её остановит.',
    ],
  },
  aggregate: {
    title: 'Aggregate Concert — правила',
    lead: 'Один поток событий на концерт. Агрегат восстанавливает состояние из журнала (снапшот + хвост событий) и проверяет правила в таком порядке:',
    rules: concertRules,
    points: [
      'Ровно одно событие на принятую команду: ConcertAnnounced, SeatBooked или BookingCancelled.',
      'Оптимистическая блокировка: две команды на один концерт одновременно — одна побеждает, вторая повторяется поверх нового состояния (⚡ Гонка).',
    ],
  },
  store: {
    title: 'events + outbox (Concert)',
    lead: 'PostgreSQL: таблица events — источник истины, а outbox-задача пишется в той же транзакции.',
    points: [
      'Таблицы: events, snapshots (каждые 10 событий, закладка для быстрой загрузки) и transactional_outbox.',
      'Задачи, которые здесь появляются: encore_record_ticket после SeatBooked, encore_record_cancellation после BookingCancelled.',
      'Отклонённая команда не пишет ничего: сравните счётчик событий до и после.',
      'Смотрите на рентген: новая строка events и новая строка outbox появляются вместе.',
    ],
  },
  handler_a: {
    title: 'Outbox worker → Fan',
    lead: 'Воркер outbox: каждые 300 мс опрашивает transactional_outbox, берёт задачу в аренду и запускает её обработчик.',
    points: [
      'encore_record_ticket → Fan.RecordTicket; encore_record_cancellation → Fan.RecordCancellation.',
      'Ответ Fan превращается в результат задачи: R9/R11 (уже сделано) = выполнена; R10 (ещё рано) = retry через 1 с; касса закрыта на rebuild = retry через 2 с.',
      'Доставка at-least-once: повторная задача безвредна, потому что правила Fan идемпотентны.',
      'До 6 попыток на задачу; статус виден в панели outbox рентгена.',
    ],
  },
  fan_command: {
    title: 'Command → Fan',
    lead: 'RecordTicket или RecordCancellation: внутренние команды, которые шлёт только outbox-воркер сервиса Concert.',
    points: [
      'source = task:encore_record_ticket, causation_id = id задачи, correlation_id = исходный клик.',
      'Наружу по HTTP не выставлены: UI не может писать в кошелёк фаната напрямую.',
    ],
  },
  fan_aggregate: {
    title: 'Aggregate Fan — правила',
    lead: 'Один поток на пару фанат + концерт: билеты фаната на этот концерт. Правила защищают от повторной и переставленной доставки задач:',
    rules: fanRules,
    points: [
      'События: TicketRecorded (с запомненной суммой возврата) и TicketCancelled.',
      'Бизнес-правил здесь нет: лимит билетов уже проверил агрегат Concert.',
    ],
  },
  fan_store: {
    title: 'events + outbox (Fan)',
    lead: 'Те же таблицы PostgreSQL, вторая транзакция: событие Fan и почтовая задача вместе.',
    points: [
      'encore_send_e_ticket после TicketRecorded, encore_issue_refund после TicketCancelled.',
      'Обе несут correlation_id клика, поэтому рентген может проследить всю цепочку.',
    ],
  },
  handler_b: {
    title: 'Outbox worker → email',
    lead: 'Выполняет почтовые задачи encore_send_e_ticket (🎫) и encore_issue_refund (💸) через почтовый сервер.',
    points: [
      'Неудачная попытка не теряется: задача возвращается в outbox с available_at в будущем.',
      'Backoff 1 → 2 → 4 → 8 → 8 → 8 с, максимум 6 попыток, затем задача помечается failed.',
      '🎲 на почтовом сервере бросается при каждой попытке.',
    ],
  },
  mail_server: {
    title: 'mail server 🎲',
    lead: 'ТОЛЬКО ДЛЯ ДЕМО: капризный внешний сервис в sidecar, который отказывает в 20 % доставок (SIDECAR_MAIL_FAILURE_RATE). Back-end знает только его HTTP API: POST /mail, 200 доставлено, 503 попробуйте позже.',
    points: [
      'Причины отказа: ⏱ таймаут SMTP, 🔥 сервер горит, 🐌 ограничение скорости, 📦 ящик переполнен. См. панель outbox.',
      'Доставка — идемпотентный upsert в mail_service.inbox (MongoDB) по заголовку Idempotency-Key: повтор лишь увеличивает счётчик deliveries.',
      'Не проекция: ⏪ Encore! не трогает эту базу и никогда не отправляет письмо заново.',
    ],
  },
  email: {
    title: 'email',
    lead: 'Почтовый ящик фаната: e-ticket (e_ticket:<booking_id>) или письмо о возврате.',
    points: [
      'Кошелёк показывает «📬 доставлен (попытка n)», когда письмо уже в ящике, а до этого — обратный отсчёт до повтора.',
      'Панель почты в рентгене перечисляет все письма с номером попытки, на которой они доставлены.',
    ],
  },
  read_db: {
    title: 'read database',
    lead: 'MongoDB encore_read: concert_halls (схема зала, sold, revenue) и fan_wallets (билеты, возвраты).',
    points: [
      'Каждый документ помнит последнее применённое событие (last_sequence) и принимает только следующее: повтор пропускается, дыра отвергается, поэтому доставка может повторяться и идти с разных нод.',
      'Обновляется проекциями в процессе сразу после коммита; сбой — лишь предупреждение, журнал уже зафиксирован.',
      '🧨 Стереть → на схеме зала «?», а правила продолжают работать по журналу; ⏪ Encore! восстанавливает всё из событий.',
      'Машина времени вычисляет прошлое состояние из событий 1…n, нигде его не храня.',
    ],
  },
  ui: {
    title: 'UI',
    lead: 'Экран: схема зала, кошелёк и концерты — всё читается из базы чтения через GET /api/….',
    points: [
      'Обновляется по SSE-ленте activity (projection.updated) с подстраховочным опросом раз в 5 с.',
      'Рентген справа читает базы напрямую через sidecar только для чтения.',
    ],
  },
}
