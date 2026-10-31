import { getJson, postJson, HttpError } from './http'
import type {
  AppConfig,
  CommandBody,
  CommandResponse,
  ConcertHallDoc,
  ConcertSummary,
  FanWalletDoc,
  HistoryState,
  RebuildStatus,
  TimelineEntry,
  VerifyReport,
} from '@/types/domain'

/** The box office (`/api`): commands, read models, the time machine, Encore!. Demo operations and
 * the fan's e-mails are the sidecar's (`sidecar.ts`). */
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
  /** ⏪ Encore!: every read model rebuilt in place, `paceMs` between the events of one key (D-18). */
  rebuild: (paceMs: number) => postJson<RebuildAnswer>('/api/projections/rebuild', { pace_ms: paceMs }),
  /** One concert hall rebuilt from its history. */
  rebuildConcert: (id: string) => postJson<RebuildAnswer>(`/api/concerts/${encodeURIComponent(id)}/rebuild`),
  status: (signal?: AbortSignal) => getJson<RebuildStatus>('/api/projections/status', signal),
  verify: () => getJson<VerifyReport>('/api/projections/verify'),
}

/** `202 { outcome: "started" | "requested" }`, or the error of a refusal (`409 rebuild_in_progress`, `400`, `503`). */
export interface RebuildAnswer {
  outcome?: string
  rebuild_id?: string
  error?: { code: string; message: string }
}
