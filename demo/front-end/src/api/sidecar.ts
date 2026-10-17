import { getJson } from './http'
import type { EventRow, MongoCollection, OutboxRow, PgTable, SnapshotRow, XrayState } from '@/types/xray'

export const sidecar = {
  health: () => getJson<{ status: string; postgres: string; mongodb: string }>('/inspect/health'),
  events: (aggregateId?: string, limit = 200) =>
    getJson<PgTable<EventRow>>(`/inspect/postgres/events?limit=${limit}${aggregateId ? `&aggregate_id=${encodeURIComponent(aggregateId)}` : ''}`),
  snapshots: () => getJson<PgTable<SnapshotRow>>('/inspect/postgres/snapshots'),
  outbox: (limit = 200) => getJson<PgTable<OutboxRow>>(`/inspect/postgres/outbox?limit=${limit}`),
  collection: (database: string, collection: string, limit = 100) =>
    getJson<MongoCollection>(`/inspect/mongo/${database}/${collection}?limit=${limit}`),
  stats: () => getJson<Pick<XrayState, 'stats' | 'db_time' | 'polled_at' | 'errors'>>('/inspect/stats'),
}
