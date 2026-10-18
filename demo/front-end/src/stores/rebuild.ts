import { defineStore } from 'pinia'
import { ref } from 'vue'
import type { RebuildStatus } from '@/types/domain'
import { backend } from '@/api/backend'
import { sidecar } from '@/api/sidecar'
import { useConcertsStore } from './concerts'
import { useWalletStore } from './wallet'
import { useSessionStore } from './session'
import { useTimeMachineStore } from './timemachine'

export const useRebuildStore = defineStore('rebuild', () => {
  const concerts = useConcertsStore()
  const wallet = useWalletStore()
  const session = useSessionStore()
  const timeMachine = useTimeMachineStore()
  const resetting = ref(false)
  const status = ref<RebuildStatus>({ state: 'idle', last_result: null })
  const replayed = ref(0)
  const total = ref(0)
  const slow = ref(true)
  const lastError = ref<string | null>(null)
  const busy = ref(false)

  async function refreshStatus() {
    try {
      status.value = await backend.rebuildStatus()
      if (status.value.state === 'running') {
        replayed.value = status.value.replayed ?? replayed.value
        total.value = status.value.total ?? total.value
      }
    } catch {
      // ignore: the status is best-effort
    }
  }

  /** DEMO ONLY: the sidecar empties both databases and announces the default concerts again. */
  async function resetDemo(): Promise<boolean> {
    resetting.value = true
    lastError.value = null
    try {
      const { status: code, body } = await sidecar.reset()
      if (code !== 200) {
        lastError.value = body?.error?.message ?? `HTTP ${code}`
        return false
      }
      const first = session.demo.default_concerts[0]
      if (first) session.concertId = first.concert_id
      concerts.stale = false
      wallet.clear()
      await Promise.all([
        concerts.refreshAll(),
        wallet.refresh(),
        timeMachine.refreshTimeline(),
        refreshStatus(),
      ])
      return true
    } catch (e) {
      lastError.value = String(e)
      return false
    } finally {
      resetting.value = false
    }
  }

  /** DEMO ONLY: 🧨 the sidecar drops the read models; the ledger is intact, Encore! rebuilds them. */
  async function wipe() {
    busy.value = true
    lastError.value = null
    try {
      const { status: code, body } = await sidecar.wipe()
      if (code === 200) {
        concerts.markWiped()
        wallet.clear()
      } else lastError.value = body?.error?.message ?? `HTTP ${code}`
    } catch (e) {
      lastError.value = String(e)
    } finally {
      busy.value = false
    }
  }

  async function start() {
    busy.value = true
    lastError.value = null
    try {
      const { status: code, body } = await backend.rebuild(slow.value ? 'slow' : 'fast')
      if (code !== 202) lastError.value = body?.outcome ?? `HTTP ${code}`
      replayed.value = 0
      await refreshStatus()
    } finally {
      busy.value = false
    }
  }

  /** Routed from the activity stream. */
  function onActivity(type: string, entry: Record<string, unknown>) {
    switch (type) {
      case 'rebuild.started':
        status.value = { ...status.value, state: 'running', total: Number(entry.total ?? 0) }
        total.value = Number(entry.total ?? 0)
        replayed.value = 0
        break
      case 'rebuild.progress':
        replayed.value = Number(entry.replayed ?? replayed.value)
        concerts.refreshThrottled()
        break
      case 'rebuild.finished':
      case 'rebuild.failed':
        void refreshStatus()
        void concerts.refreshAll()
        void wallet.refresh()
        break
    }
  }

  return {
    status,
    replayed,
    total,
    slow,
    lastError,
    busy,
    resetting,
    refreshStatus,
    wipe,
    start,
    resetDemo,
    onActivity,
  }
})
