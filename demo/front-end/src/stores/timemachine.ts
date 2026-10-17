import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import type { HistoryState, TimelineEntry } from '@/types/domain'
import { backend } from '@/api/backend'
import { useSessionStore } from './session'

export const useTimeMachineStore = defineStore('timemachine', () => {
  const session = useSessionStore()
  const timeline = ref<TimelineEntry[]>([])
  const sequence = ref<number | null>(null) // null = present
  const historyState = ref<HistoryState | null>(null)
  const loadedFor = ref<string | null>(null)

  const lastSequence = computed(() => timeline.value[timeline.value.length - 1]?.sequence ?? 0)
  const active = computed(() => sequence.value !== null && sequence.value < lastSequence.value && historyState.value !== null)

  async function refreshTimeline() {
    const id = session.concertId
    try {
      const rows = await backend.timeline(id)
      if (id !== session.concertId) return
      timeline.value = rows
      loadedFor.value = id
      if (sequence.value !== null && sequence.value >= lastSequence.value) backToPresent()
    } catch {
      // keep the previous timeline
    }
  }

  async function goTo(n: number) {
    if (n >= lastSequence.value || n < 1) {
      backToPresent()
      return
    }
    sequence.value = n
    try {
      historyState.value = await backend.stateAt(session.concertId, n)
    } catch {
      historyState.value = null
    }
  }

  function backToPresent() {
    sequence.value = null
    historyState.value = null
  }

  function reset() {
    timeline.value = []
    backToPresent()
  }

  return { timeline, sequence, historyState, loadedFor, lastSequence, active, refreshTimeline, goTo, backToPresent, reset }
})
