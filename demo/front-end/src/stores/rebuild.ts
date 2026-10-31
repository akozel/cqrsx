import { defineStore } from 'pinia'
import { computed, ref, watch } from 'vue'
import type { RebuildStatus } from '@/types/domain'
import { backend, type RebuildAnswer } from '@/api/backend'
import { sidecar } from '@/api/sidecar'
import { useConcertsStore } from './concerts'
import { useWalletStore } from './wallet'
import { useSessionStore } from './session'
import { useTimeMachineStore } from './timemachine'

/** How often the status of the projections is read. */
export const STATUS_INTERVAL_MS = 1000
/** A status read that takes longer is abandoned: the next one starts on time instead of piling up. */
export const STATUS_TIMEOUT_MS = 2500
/** While Encore! runs, the open hall is read this often, so its seats light up one by one. */
export const REPLAY_REFRESH_MS = 250

/** A refused or failed read-side operation: a known `code` is translated, anything else shown as `message`. */
export interface ReadSideError {
  code?: string
  message: string
}

/** Encore! as the feed reports it: documents rebuilt so far and their events, against the totals at the start. */
export interface RebuildProgress {
  documents: number
  events: number
  documentsTotal: number
  eventsTotal: number
}

const NO_STATUS: RebuildStatus = { state: 'starting', stale: false, projections: [], rebuild: null, last_result: null }
const NO_PROGRESS: RebuildProgress = { documents: 0, events: 0, documentsTotal: 0, eventsTotal: 0 }

function refusal(code: number, body: RebuildAnswer | null): ReadSideError {
  return { code: body?.error?.code, message: body?.error?.message ?? `HTTP ${code}` }
}

/** The read side as the operator sees it: the projections' status, ⏪ Encore! (every read model rebuilt in place
 * while the box office stays open, D-16), the rebuild of one hall, and the demo's 🧨 wipe and ⟲ reset. */
export const useRebuildStore = defineStore('rebuild', () => {
  const concerts = useConcertsStore()
  const wallet = useWalletStore()
  const session = useSessionStore()
  const timeMachine = useTimeMachineStore()
  const status = ref<RebuildStatus>(NO_STATUS)
  /** The status did not answer (503, a timeout or the back-end is down): `status` is the last one seen. */
  const statusDown = ref(false)
  const progress = ref<RebuildProgress>({ ...NO_PROGRESS })
  const slow = ref(true)
  const lastError = ref<ReadSideError | null>(null)
  const busy = ref(false)
  const resetting = ref(false)
  /** Rebuilds the feed reported finished: a status read before the end must not bring them back. */
  const ended = new Set<string>()
  let pending: AbortController | null = null
  let poller: ReturnType<typeof setInterval> | null = null
  let hallPoller: ReturnType<typeof setInterval> | null = null

  const running = computed(() => status.value.rebuild !== null)

  function adopt(next: RebuildStatus) {
    const rebuild = next.rebuild && ended.has(next.rebuild.rebuild_id) ? null : next.rebuild
    status.value = { ...next, rebuild }
    // A page opened during Encore! learns the total from the status.
    if (rebuild && progress.value.documentsTotal === 0)
      progress.value = { ...progress.value, documentsTotal: rebuild.keys_total }
  }

  /** One status read at a time; a read that hangs is abandoned after `STATUS_TIMEOUT_MS`. */
  async function refreshStatus() {
    if (pending) return
    const request = new AbortController()
    pending = request
    const timeout = setTimeout(() => request.abort(), STATUS_TIMEOUT_MS)
    try {
      adopt(await backend.status(request.signal))
      statusDown.value = false
    } catch {
      statusDown.value = true
    } finally {
      clearTimeout(timeout)
      pending = null
    }
  }

  /** Reads the status every `STATUS_INTERVAL_MS` while the page is visible. */
  function startPolling() {
    if (poller) return
    void refreshStatus()
    poller = setInterval(() => {
      if (typeof document === 'undefined' || document.visibilityState !== 'hidden') void refreshStatus()
    }, STATUS_INTERVAL_MS)
  }

  function stopPolling() {
    if (poller) clearInterval(poller)
    poller = null
    pending?.abort()
  }

  // The hall is replaced in place during Encore!: read it often enough to watch its seats come back.
  watch(running, (now) => {
    if (now && !hallPoller) hallPoller = setInterval(() => void concerts.refreshConcert(), REPLAY_REFRESH_MS)
    if (!now && hallPoller) {
      clearInterval(hallPoller)
      hallPoller = null
    }
  })

  /** DEMO ONLY: the sidecar empties both databases and announces the default concerts again. */
  async function resetDemo(): Promise<boolean> {
    resetting.value = true
    lastError.value = null
    try {
      const { status: code, body } = await sidecar.reset()
      if (code !== 200) {
        lastError.value = { message: body?.error?.message ?? `HTTP ${code}` }
        return false
      }
      const first = session.demo.default_concerts[0]
      if (first) session.concertId = first.concert_id
      concerts.stale = false
      wallet.clear()
      await Promise.all([concerts.refreshAll(), wallet.refresh(), timeMachine.refreshTimeline(), refreshStatus()])
      return true
    } catch (e) {
      lastError.value = { message: String(e) }
      return false
    } finally {
      resetting.value = false
    }
  }

  /** DEMO ONLY: 🧨 the sidecar drops the read models and then their checkpoints; the ledger is intact, and the
   * processors read it again from the start, so every hall comes back by itself. */
  async function wipe() {
    busy.value = true
    lastError.value = null
    try {
      const { status: code, body } = await sidecar.wipe()
      if (code === 200) {
        concerts.markWiped()
        wallet.clear()
      } else lastError.value = { message: body?.error?.message ?? `HTTP ${code}` }
    } catch (e) {
      lastError.value = { message: String(e) }
    } finally {
      busy.value = false
    }
  }

  /** ⏪ Encore!: in slow motion with the pause the back-end suggests between the events of one key. */
  async function start() {
    busy.value = true
    lastError.value = null
    try {
      const { status: code, body } = await backend.rebuild(slow.value ? session.config.replay_slow_delay_ms : 0)
      if (code === 202) progress.value = { ...NO_PROGRESS, documentsTotal: progress.value.documentsTotal }
      else lastError.value = refusal(code, body)
      await refreshStatus()
    } catch (e) {
      lastError.value = { message: String(e) }
    } finally {
      busy.value = false
    }
  }

  /** ⟲ One hall rebuilt from its history by the owner of the halls' checkpoint; `null` when it was asked. */
  async function rebuildConcert(concertId: string): Promise<ReadSideError | null> {
    try {
      const { status: code, body } = await backend.rebuildConcert(concertId)
      return code === 202 ? null : refusal(code, body)
    } catch (e) {
      return { message: String(e) }
    }
  }

  /** Routed from the activity stream: the Encore! this node started, as its watcher reports it. */
  function onActivity(type: string, entry: Record<string, unknown>) {
    switch (type) {
      case 'rebuild.started': {
        const rebuildId = String(entry.rebuild_id ?? '')
        const keysTotal = Number(entry.keys_total ?? 0)
        progress.value = { ...NO_PROGRESS, documentsTotal: keysTotal, eventsTotal: Number(entry.events_total ?? 0) }
        if (!ended.has(rebuildId))
          status.value = {
            ...status.value,
            rebuild: { rebuild_id: rebuildId, pace_ms: Number(entry.pace_ms ?? 0), started_at: String(entry.at ?? ''), keys_total: keysTotal },
          }
        break
      }
      case 'rebuild.progress':
        // The checkpoint rows count a page of keys at a time; the feed counts every document.
        progress.value = { ...progress.value, documents: Math.max(progress.value.documents, Number(entry.keys ?? 0)) }
        break
      case 'rebuild.finished': {
        const rebuildId = String(entry.rebuild_id ?? '')
        ended.add(rebuildId)
        status.value = {
          ...status.value,
          rebuild: null,
          last_result: {
            rebuild_id: rebuildId,
            keys: Number(entry.keys ?? 0),
            duration_ms: Number(entry.duration_ms ?? 0),
            finished_at: String(entry.at ?? ''),
          },
        }
        void refreshStatus()
        void concerts.refreshAll()
        void wallet.refresh()
        break
      }
    }
  }

  /** `projection.rebuilt`: one document replaced by the fold of its history; a requested one counts for Encore!. */
  function onRebuilt(entry: Record<string, unknown>) {
    if (!running.value || entry.reason !== 'requested') return
    progress.value = {
      ...progress.value,
      documents: progress.value.documents + 1,
      events: progress.value.events + Number(entry.events ?? 0),
    }
  }

  return {
    status,
    statusDown,
    running,
    progress,
    slow,
    lastError,
    busy,
    resetting,
    refreshStatus,
    startPolling,
    stopPolling,
    wipe,
    start,
    rebuildConcert,
    resetDemo,
    onActivity,
    onRebuilt,
  }
})
