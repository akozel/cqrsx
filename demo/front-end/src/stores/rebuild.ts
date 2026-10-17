import { defineStore } from 'pinia'
import { ref } from 'vue'
import type { RebuildStatus } from '@/types/domain'
import { backend } from '@/api/backend'
import { useConcertsStore } from './concerts'
import { useWalletStore } from './wallet'

export const useRebuildStore = defineStore('rebuild', () => {
  const concerts = useConcertsStore()
  const wallet = useWalletStore()
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

  async function wipe() {
    busy.value = true
    try {
      const { status: code, body } = await backend.wipe()
      if (code === 200) concerts.markWiped()
      else lastError.value = body?.outcome ?? `HTTP ${code}`
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
      case 'projections.wiped':
        concerts.markWiped()
        wallet.clear()
        break
    }
  }

  return { status, replayed, total, slow, lastError, busy, refreshStatus, wipe, start, onActivity }
})
