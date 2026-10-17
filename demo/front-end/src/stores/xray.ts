import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import type { XrayState } from '@/types/xray'
import { subscribe, type SseHandle } from '@/api/sse'

const EMPTY: XrayState = { events: null, snapshots: null, outbox: null, mongo: {}, stats: {}, db_time: null, polled_at: '', errors: [] }

export const useXrayStore = defineStore('xray', () => {
  const state = ref<XrayState>(EMPTY)
  const previous = ref<XrayState>(EMPTY)
  const connected = ref(false)
  const receivedAt = ref<number>(0)
  /** `Date.now() + dbTimeOffset ≈ database now` — countdowns use the database clock. */
  const dbTimeOffset = ref(0)
  const seenEvents = ref<Set<string>>(new Set())
  const seenTasks = ref<Set<string>>(new Set())
  const freshEvents = ref<Set<string>>(new Set())
  const freshTasks = ref<Set<string>>(new Set())
  let handle: SseHandle | null = null

  const eventsTotal = computed(() => state.value.stats.events_total ?? state.value.events?.total ?? 0)
  const pgDown = computed(() => state.value.errors.some((e) => e.source === 'postgres'))
  const mongoDown = computed(() => state.value.errors.some((e) => e.source === 'mongodb'))

  function eventKey(e: { aggregate_type: string; aggregate_id: string; sequence: number }) {
    return `${e.aggregate_type}/${e.aggregate_id}#${e.sequence}`
  }

  function apply(next: XrayState) {
    previous.value = state.value
    state.value = next
    receivedAt.value = Date.now()
    if (next.db_time) dbTimeOffset.value = Date.parse(next.db_time) - Date.now()
    const first = seenEvents.value.size === 0 && seenTasks.value.size === 0
    const newEvents = new Set<string>()
    for (const e of next.events?.rows ?? []) {
      const k = eventKey(e)
      if (!seenEvents.value.has(k)) {
        seenEvents.value.add(k)
        if (!first) newEvents.add(k)
      }
    }
    const newTasks = new Set<string>()
    for (const t of next.outbox?.rows ?? []) {
      if (!seenTasks.value.has(t.id)) {
        seenTasks.value.add(t.id)
        if (!first) newTasks.add(t.id)
      }
    }
    if (newEvents.size) {
      freshEvents.value = new Set([...freshEvents.value, ...newEvents])
      setTimeout(() => {
        const s = new Set(freshEvents.value)
        for (const k of newEvents) s.delete(k)
        freshEvents.value = s
      }, 2000)
    }
    if (newTasks.size) {
      freshTasks.value = new Set([...freshTasks.value, ...newTasks])
      setTimeout(() => {
        const s = new Set(freshTasks.value)
        for (const k of newTasks) s.delete(k)
        freshTasks.value = s
      }, 2000)
    }
  }

  function connect() {
    if (handle) return
    handle = subscribe(
      '/inspect/stream',
      {
        state: (data) => {
          try {
            apply(JSON.parse(data) as XrayState)
          } catch {
            // ignore malformed state
          }
        },
      },
      (c) => {
        connected.value = c
      },
    )
  }

  function disconnect() {
    handle?.close()
    handle = null
  }

  return { state, previous, connected, receivedAt, dbTimeOffset, freshEvents, freshTasks, eventsTotal, pgDown, mongoDown, eventKey, connect, disconnect }
})
