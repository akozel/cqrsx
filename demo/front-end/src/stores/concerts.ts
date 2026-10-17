import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import type { ConcertHallDoc, ConcertSummary } from '@/types/domain'
import { backend } from '@/api/backend'
import { useSessionStore } from './session'

export const useConcertsStore = defineStore('concerts', () => {
  const session = useSessionStore()
  const list = ref<ConcertSummary[]>([])
  const docs = ref<Record<string, ConcertHallDoc | null>>({})
  const stale = ref(false)
  const backendDown = ref(false)
  const previousSoldOut = ref<Record<string, boolean>>({})
  const soldOutJustHappened = ref<string | null>(null)
  let debounce: ReturnType<typeof setTimeout> | null = null
  let lastRebuildRefresh = 0

  /** Tabs come from the read model; when it is empty, from config.seed_concerts with a "?" mark. */
  const tabs = computed(() => {
    const fromView = list.value.map((c) => ({ ...c, known: true }))
    const seen = new Set(fromView.map((c) => c.concert_id))
    const fallback = session.config.seed_concerts
      .filter((id) => !seen.has(id))
      .map((id) => ({
        concert_id: id,
        artist: null,
        city: null,
        date: null,
        sold: 0,
        capacity: session.config.venue.rows.length * session.config.venue.seats_per_row,
        revenue: 0,
        sold_out: false,
        incomplete: false,
        last_sequence: 0,
        version: 0,
        known: false,
      }))
    return [...fromView, ...fallback]
  })

  const current = computed(() => docs.value[session.concertId] ?? null)
  const currentView = computed(() => current.value?.view ?? null)

  async function refreshList() {
    try {
      list.value = await backend.concerts()
      backendDown.value = false
    } catch {
      backendDown.value = true
    }
  }

  async function refreshConcert(id: string = session.concertId) {
    try {
      const doc = await backend.concert(id)
      docs.value = { ...docs.value, [id]: doc }
      backendDown.value = false
      const soldOut = doc?.view.sold_out ?? false
      if (doc && previousSoldOut.value[id] === false && soldOut) soldOutJustHappened.value = id
      previousSoldOut.value = { ...previousSoldOut.value, [id]: soldOut }
      if (doc) stale.value = false
    } catch {
      backendDown.value = true
    }
  }

  async function refreshAll() {
    await Promise.all([refreshList(), refreshConcert()])
  }

  function refreshDebounced(ms = 150) {
    if (debounce) clearTimeout(debounce)
    debounce = setTimeout(() => void refreshAll(), ms)
  }

  /** During a rebuild: at most once per 250 ms so seats "light up" one by one. */
  function refreshThrottled(ms = 250) {
    const now = Date.now()
    if (now - lastRebuildRefresh < ms) return
    lastRebuildRefresh = now
    void refreshAll()
  }

  function markWiped() {
    docs.value = {}
    list.value = []
    stale.value = true
    previousSoldOut.value = {}
  }

  function ackSoldOut() {
    soldOutJustHappened.value = null
  }

  return {
    list,
    docs,
    stale,
    backendDown,
    tabs,
    current,
    currentView,
    soldOutJustHappened,
    refreshList,
    refreshConcert,
    refreshAll,
    refreshDebounced,
    refreshThrottled,
    markWiped,
    ackSoldOut,
  }
})
