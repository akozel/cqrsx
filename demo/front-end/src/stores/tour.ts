import { defineStore } from 'pinia'
import { ref } from 'vue'

export const CHAPTERS = ['events', 'rules', 'outbox', 'projections', 'encore'] as const
export type Chapter = (typeof CHAPTERS)[number]

/** [P1] Chapters ①–⑤: which one is open; panels not related to it are dimmed. */
export const useTourStore = defineStore('tour', () => {
  const current = ref<Chapter | null>(null)
  function open(chapter: Chapter) {
    current.value = chapter
  }
  function next() {
    if (!current.value) return
    const i = CHAPTERS.indexOf(current.value)
    current.value = CHAPTERS[(i + 1) % CHAPTERS.length] ?? null
  }
  function close() {
    current.value = null
  }
  return { current, open, next, close }
})

/** Panels highlighted by each chapter; everything else is dimmed to 40 %. */
export const CHAPTER_PANELS: Record<Chapter, string[]> = {
  events: ['events', 'snapshots', 'timemachine', 'seatmap'],
  rules: ['rulebreaker', 'seatmap', 'events', 'actions'],
  outbox: ['outbox', 'inbox', 'activity', 'seatmap', 'wallet'],
  projections: ['mongo', 'seatmap', 'wallet', 'actions'],
  encore: ['encore', 'mongo', 'inbox', 'seatmap'],
}
