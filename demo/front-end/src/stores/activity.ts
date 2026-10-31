import { defineStore } from 'pinia'
import { ref } from 'vue'
import type { ActivityEntry, ActivityHello } from '@/types/activity'
import { subscribe, type SseHandle } from '@/api/sse'
import { useConcertsStore } from './concerts'
import { useRebuildStore } from './rebuild'
import { useSessionStore } from './session'
import { useWalletStore } from './wallet'
import { isLedgerOf } from '@/lib/ledger'

const MAX_ENTRIES = 300

export const useActivityStore = defineStore('activity', () => {
  const concerts = useConcertsStore()
  const rebuild = useRebuildStore()
  const wallet = useWalletStore()
  const session = useSessionStore()
  const entries = ref<ActivityEntry[]>([])
  const connected = ref(false)
  /** Last task-level signal for the PipelineStrip (worker animations). */
  const taskSignal = ref<{ type: string; entry: ActivityEntry; seq: number } | null>(null)
  /** Last `command.*` signal from the back-end: the Fan aggregate only ever gets commands this way (from tasks). */
  const commandSignal = ref<{ type: string; entry: ActivityEntry; seq: number } | null>(null)
  let handle: SseHandle | null = null
  let seq = 0

  function push(entry: ActivityEntry) {
    entries.value = [entry, ...entries.value].slice(0, MAX_ENTRIES)
  }

  function route(entry: ActivityEntry) {
    const type = entry.type
    if (type.startsWith('task.')) taskSignal.value = { type, entry, seq: ++seq }
    if (type.startsWith('command.')) commandSignal.value = { type, entry, seq: ++seq }
    if (type.startsWith('rebuild.')) rebuild.onActivity(type, entry)
    if (type === 'projection.updated' || type === 'projection.rebuilt') {
      // A document moved: the screen follows the read models through the feed (D-17), whoever sent the command.
      if (entry.projection === 'concert_halls') concerts.refreshDebounced()
      else if (entry.projection === 'fan_wallets' && isLedgerOf(String(entry.view_id ?? ''), session.persona))
        wallet.refreshDebounced()
    }
    if (type === 'projection.rebuilt') rebuild.onRebuilt(entry)
    // A failing round or a new owner changes the status the Encore! panel shows.
    if (type === 'projection.failed' || type === 'projection.lease') void rebuild.refreshStatus()
    if (type === 'task.attempt.succeeded') wallet.refreshDebounced()
  }

  function connect() {
    if (handle) return
    handle = subscribe(
      '/api/activity',
      {
        hello: (data) => {
          try {
            const hello = JSON.parse(data) as ActivityHello
            entries.value = [...hello.backlog].reverse().slice(0, MAX_ENTRIES)
          } catch {
            // ignore malformed hello
          }
        },
        activity: (data) => {
          try {
            const entry = JSON.parse(data) as ActivityEntry
            if (entry.type !== 'rebuild.progress') push(entry)
            else if (entries.value[0]?.type === 'rebuild.progress')
              entries.value = [entry, ...entries.value.slice(1)]
            else push(entry)
            route(entry)
          } catch {
            // ignore malformed entry
          }
        },
      },
      (state) => {
        connected.value = state
      },
    )
  }

  function disconnect() {
    handle?.close()
    handle = null
  }

  return { entries, connected, taskSignal, commandSignal, connect, disconnect }
})
