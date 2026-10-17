import { getJson, postJson, HttpError } from './http'
import type {
  AppConfig,
  CommandBody,
  CommandResponse,
  ConcertHallDoc,
  ConcertSummary,
  FanWalletDoc,
  HistoryState,
  InboxMail,
  RebuildStatus,
  TimelineEntry,
} from '@/types/domain'

export const backend = {
  config: () => getJson<AppConfig>('/api/config'),
  concerts: () => getJson<ConcertSummary[]>('/api/concerts'),
  concert: async (id: string): Promise<ConcertHallDoc | null> => {
    try {
      return await getJson<ConcertHallDoc>(`/api/concerts/${encodeURIComponent(id)}`)
    } catch (e) {
      if (e instanceof HttpError && e.status === 404) return null
      throw e
    }
  },
  wallet: async (fanId: string): Promise<FanWalletDoc | null> => {
    try {
      return await getJson<FanWalletDoc>(`/api/fans/${encodeURIComponent(fanId)}/wallet`)
    } catch (e) {
      if (e instanceof HttpError && e.status === 404) return null
      throw e
    }
  },
  inbox: (fanId: string) => getJson<InboxMail[]>(`/api/fans/${encodeURIComponent(fanId)}/inbox`),
  timeline: (id: string) => getJson<TimelineEntry[]>(`/api/concerts/${encodeURIComponent(id)}/timeline`),
  stateAt: (id: string, sequence: number) =>
    getJson<HistoryState>(`/api/concerts/${encodeURIComponent(id)}/state-at/${sequence}`),
  command: async (concertId: string, body: CommandBody): Promise<CommandResponse> => {
    const { status, body: response } = await postJson<Partial<CommandResponse>>(
      `/api/concerts/${encodeURIComponent(concertId)}/commands`,
      body,
    )
    return { outcome: 'error', ...(response ?? {}), status } as CommandResponse
  },
  wipe: () => postJson<{ outcome: string; collections?: string[] }>('/api/projections/wipe'),
  rebuild: (speed: 'slow' | 'fast') => postJson<{ outcome: string; rebuild_id?: string }>('/api/projections/rebuild', { speed }),
  rebuildStatus: () => getJson<RebuildStatus>('/api/projections/status'),
}
