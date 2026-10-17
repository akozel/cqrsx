import { defineStore } from 'pinia'
import { ref } from 'vue'
import type { ActivityEntry, ActivityHello } from '@/types/activity'
import { subscribe, type SseHandle } from '@/api/sse'
import { useConcertsStore } from './concerts'
import { useRebuildStore } from './rebuild'
import { useWalletStore } from './wallet'

const MAX_ENTRIES = 300

export const useActivityStore = defineStore('activity', () => {
  const concerts = useConcertsStore()
  const rebuild = useRebuildStore()
  const wallet = useWalletStore()
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
    if (type.startsWith('rebuild.') || type === 'projections.wiped') rebuild.onActivity(type, entry)
    if (type === 'projection.updated' || type === 'projection.failed') {
      // Possibly from another tab or from the task chain: refresh with a small debounce.
      concerts.refreshDebounced()
      void wallet.refresh()
    }
    if (type === 'task.attempt.succeeded') void wallet.refresh()
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
