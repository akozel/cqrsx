import { getJson, postJson } from './http'
import type { EventRow, InboxDoc, MongoCollection, OutboxRow, PgTable, SnapshotRow, XrayState } from '@/types/xray'
import type { DemoInfo } from '@/types/domain'

/** The sidecar (`/inspect`): the X-ray of both databases, the fan's mailbox as the mail service
 * stored it, and the demo lifecycle (props, wipe, reset). */
export const sidecar = {
  health: () => getJson<{ status: string; postgres: string; mongodb: string }>('/inspect/health'),
  events: (aggregateId?: string, limit = 200) =>
    getJson<PgTable<EventRow>>(`/inspect/postgres/events?limit=${limit}${aggregateId ? `&aggregate_id=${encodeURIComponent(aggregateId)}` : ''}`),
  snapshots: () => getJson<PgTable<SnapshotRow>>('/inspect/postgres/snapshots'),
  outbox: (limit = 200) => getJson<PgTable<OutboxRow>>(`/inspect/postgres/outbox?limit=${limit}`),
  collection: (database: string, collection: string, limit = 100) =>
    getJson<MongoCollection>(`/inspect/mongo/${database}/${collection}?limit=${limit}`),
  /** The fan's e-mails, oldest first: raw `mail_service.inbox` documents. */
  inbox: (fanId: string) => getJson<MongoCollection<InboxDoc>>(`/inspect/mail/${encodeURIComponent(fanId)}`),
  stats: () => getJson<Pick<XrayState, 'stats' | 'db_time' | 'polled_at' | 'errors'>>('/inspect/stats'),
  /** DEMO ONLY: the default concerts, the cast and whether a reset is allowed. */
  demo: () => getJson<DemoInfo>('/inspect/demo'),
  /** DEMO ONLY: 🧨 drop the read models; the journal is intact, Encore! rebuilds them. */
  wipe: () => postJson<{ outcome?: string; dropped?: string[]; error?: { message: string } }>('/inspect/projections/wipe'),
  /** DEMO ONLY: empty everything and announce the default concerts again. */
  reset: () => postJson<{ outcome?: string; error?: { message: string } }>('/inspect/reset'),
}
