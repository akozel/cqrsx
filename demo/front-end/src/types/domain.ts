// Shapes returned by the back-end (/api). Kept close to PLAN.md §2.8 and §6.4.
export type Locale = 'en' | 'ru'
export type ZoneId = 'vip' | 'standard' | 'balcony'

export interface ZoneConfig {
  id: ZoneId
  rows: string[]
  price: number
}

export interface Persona {
  fan_id: string
  name: string
  avatar: string
}

/** The box office's reference data (`GET /api/config`): the venue and its rule settings. The cast is
 * a demo prop and comes from the sidecar (`DemoInfo`). */
export interface AppConfig {
  venue: { rows: string[]; seats_per_row: number; zones: ZoneConfig[] }
  ticket_limit: number
  snapshot_every: number
}

export interface ConcertSummary {
  concert_id: string
  artist: string | null
  city: string | null
  date: string | null
  sold: number
  capacity: number
  revenue: number
  sold_out: boolean
  incomplete: boolean
  last_sequence: number
  version: number
}

export interface SeatEntry {
  state: 'free' | 'booked'
  fan_id?: string
  fan_name?: string
  zone?: ZoneId
  price?: number
  booking_id?: string
  sequence: number
  changed_at: string
}

export interface ConcertHallView {
  concert: { artist: string; city: string; date: string } | null
  capacity: number
  seats: Record<string, SeatEntry>
  sold: number
  revenue: number
  sold_out: boolean
  last_sequence: number
  incomplete: boolean
}

export interface ConcertHallDoc {
  concert_id: string
  version: number
  view: ConcertHallView
}

export interface TicketEntry {
  concert_id: string
  seat: string
  price: number
  state: 'active' | 'cancelled'
  refund?: number
  sequence: number
  changed_at: string
}

export interface FanWalletView {
  fan_id: string
  fan_name: string
  tickets: Record<string, TicketEntry>
  active_count: number
  total_spent: number
  last_sequence: number
}

/** One ledger behind the wallet: the `fan_wallets` document of a fan and a concert (D-14). */
export interface LedgerRef {
  concert_id: string
  version: number
  last_sequence: number
}

/** The wallet as `GET /api/fans/{fan_id}/wallet` returns it: assembled from the fan's ledgers. */
export interface FanWalletDoc {
  fan_id: string
  ledgers: LedgerRef[]
  view: FanWalletView
}

/** An e-mail as the wallet shows it: the `mail_service.inbox` document with `_id` as `inbox_id`. */
export interface InboxMail {
  inbox_id: string
  kind: 'e_ticket' | 'refund'
  to_fan_id: string
  to_name: string
  subject: string
  concert_id: string
  seat: string
  amount: number
  task_id: string
  delivered_on_attempt: number
  deliveries: number
  first_delivered_at: string
}

export type CommandBody =
  | { type: 'AnnounceConcert'; artist: string; city: string; date: string }
  | { type: 'BookSeat'; seat: string; fan_id: string; fan_name: string }
  | { type: 'CancelBooking'; seat: string; fan_id: string }

export interface CommandError {
  code: string
  params: Record<string, unknown>
  message: string
}

export interface CommandResponse {
  status: number
  outcome: 'accepted' | 'rejected' | 'conflict' | 'unavailable' | 'locked' | 'invalid' | 'error'
  command_id?: string
  concert_id?: string
  tasks?: string[]
  warnings?: { code: string; projection: string }[]
  error?: CommandError
}

export interface TimelineEntry {
  sequence: number
  event_type: string
  recorded_at: string | null
  summary: string
}

export interface HistoryState {
  concert_id: string
  sequence: number
  last_sequence: number
  state: {
    info: { artist: string; city: string; date: string } | null
    bookings: Record<string, { fan_id: string; fan_name: string; booking_id: string; price: number }>
    sold: number
    free_seats: number
    sold_out: boolean
    revenue: number
  }
  event: { sequence: number; event_type: string; payload: unknown; metadata: Record<string, string>; summary: string }
}

export interface RebuildStatus {
  state: 'idle' | 'running'
  rebuild_id?: string
  speed?: 'slow' | 'fast'
  started_at?: string
  total?: number
  replayed?: number
  last_result: {
    rebuild_id: string
    speed: 'slow' | 'fast'
    ok: boolean
    replayed: number
    aggregates: number
    duration_ms: number
    finished_at: string
    error?: string
  } | null
}

/** The props of the demo (sidecar `GET /inspect/demo`): what it starts from, who plays in it, and
 * whether a reset is allowed. */
export interface DemoInfo {
  default_concerts: { concert_id: string; artist: string; city: string; date: string }[]
  personas: Persona[]
  reset_enabled: boolean
}
