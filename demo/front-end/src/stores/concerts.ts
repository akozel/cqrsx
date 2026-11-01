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
  let soon: ReturnType<typeof setTimeout> | null = null
  let fallback: ReturnType<typeof setTimeout> | null = null

  /** Tabs come from the read model; when it is empty, from the demo's default concerts with a "?" mark. */
  const tabs = computed(() => {
    const fromView = list.value.map((c) => ({ ...c, known: true }))
    const seen = new Set(fromView.map((c) => c.concert_id))
    const fallback = session.demo.default_concerts
      .filter((c) => !seen.has(c.concert_id))
      .map((c) => ({
        concert_id: c.concert_id,
        artist: c.artist,
        city: c.city,
        date: c.date,
        sold: 0,
        capacity: session.config.venue.rows.length * session.config.venue.seats_per_row,
        revenue: 0,
        sold_out: false,
        last_sequence: 0,
        revision: 0,
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

  /** A read model moved (`projection.applied`): one refresh `ms` later covers every update until then, so a
   * stream of updates never postpones it. */
  function refreshDebounced(ms = 150) {
    if (soon) return
    soon = setTimeout(() => {
      soon = null
      void refreshAll()
    }, ms)
  }

  /** After an accepted command: the hall follows the journal a moment later (D-17) and the feed says when;
   * this one refresh is for a silent feed. */
  function refreshFallback(ms = 1000) {
    if (fallback) return
    fallback = setTimeout(() => {
      fallback = null
      void refreshAll()
    }, ms)
  }

  /** 🧨 The read models and their checkpoints were dropped: the processors read the journal again from the
   * start, and every hall comes back by itself (`stale` until the first document returns). */
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
    refreshFallback,
    markWiped,
    ackSoldOut,
  }
})
