import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import { useSessionStore } from '@/stores/session'

export const CHAPTERS = ['events', 'rules', 'outbox', 'projections', 'encore'] as const
export type Chapter = (typeof CHAPTERS)[number]
/** Step 0 ("how to read this screen") followed by chapters ①–⑤. */
export const STEPS = ['intro', ...CHAPTERS] as const
export type Step = (typeof STEPS)[number]

/**
 * [P1] The guided tour: which step is open; panels not related to the open chapter are dimmed.
 * The intro opens by itself on the first visit; any move away from it marks it as seen.
 */
export const useTourStore = defineStore('tour', () => {
  const session = useSessionStore()
  const step = ref<Step | null>(session.bannerDismissed ? null : 'intro')
  const current = computed<Chapter | null>(() => (step.value && step.value !== 'intro' ? step.value : null))
  /** 1-based chapter number, 0 for the intro. */
  const index = computed(() => (step.value ? STEPS.indexOf(step.value) : -1))

  function go(next: Step | null) {
    if (step.value === 'intro') session.bannerDismissed = true
    step.value = next
  }
  const start = () => go('intro')
  const next = () => go(STEPS[index.value + 1] ?? null)
  const back = () => go(STEPS[index.value - 1] ?? null)
  const close = () => go(null)
  return { step, current, index, start, next, back, close }
})

/** Panels highlighted by each chapter; everything else is dimmed to 40 %. */
export const CHAPTER_PANELS: Record<Chapter, string[]> = {
  events: ['events', 'snapshots', 'timemachine', 'seatmap'],
  rules: ['rulebreaker', 'seatmap', 'events', 'actions'],
  outbox: ['outbox', 'inbox', 'activity', 'seatmap', 'wallet'],
  projections: ['mongo', 'seatmap', 'wallet', 'actions'],
  encore: ['encore', 'mongo', 'inbox', 'seatmap'],
}
