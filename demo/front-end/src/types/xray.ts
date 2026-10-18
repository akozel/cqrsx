// Shapes of the sidecar (/inspect) — raw database rows, as they are stored.
export interface EventRow {
  aggregate_type: string
  aggregate_id: string
  sequence: number
  event_type: string
  event_version: string
  payload: Record<string, unknown>
  metadata: Record<string, string>
}

export interface SnapshotRow {
  aggregate_type: string
  aggregate_id: string
  last_sequence: number
  current_snapshot: number
  payload: unknown
}

export type OutboxDbStatus = 'pending' | 'processing' | 'completed' | 'failed'

export interface OutboxRow {
  id: string
  aggregate_type: string
  aggregate_id: string
  aggregate_sequence: number
  task_type: string
  task_version: number
  payload: Record<string, unknown>
  metadata: Record<string, unknown>
  status: OutboxDbStatus
  created_at: string
  available_at: string
  finished_at: string | null
  attempts: number
  max_attempts: number
  last_error: string | null
  locked_by: string | null
  lock_token: string | null
  locked_until: string | null
}

export interface PgTable<Row> {
  source: 'postgres'
  table: string
  query: string
  total: number
  rows: Row[]
}

/** One `mail_service.inbox` document, as the mail service stored it. */
export interface InboxDoc {
  _id: string
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

export interface MongoCollection<Doc = Record<string, unknown>> {
  source: 'mongodb'
  database: string
  collection: string
  query: string
  total: number
  docs: Doc[]
}

export interface XrayStats {
  events_total?: number
  events_by_aggregate?: { aggregate_type: string; aggregate_id: string; events: number }[]
  tasks_by_status?: Record<string, number>
  snapshots_total?: number
  documents?: Record<string, number>
}

export interface XrayState {
  events: PgTable<EventRow> | null
  snapshots: PgTable<SnapshotRow> | null
  outbox: PgTable<OutboxRow> | null
  mongo: Record<string, MongoCollection>
  stats: XrayStats
  db_time: string | null
  polled_at: string
  errors: { source: string; message: string }[]
}
