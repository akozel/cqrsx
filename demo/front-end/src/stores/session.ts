import { defineStore } from 'pinia'
import { ref, watch } from 'vue'
import type { AppConfig, Persona } from '@/types/domain'
import { DEFAULT_PERSONAS } from '@/lib/personas'
import { DEFAULT_VENUE } from '@/lib/venue'
import { backend } from '@/api/backend'

function readLocal(key: string): string | null {
  try {
    return localStorage.getItem(key)
  } catch {
    return null
  }
}
function writeLocal(key: string, value: string) {
  try {
    localStorage.setItem(key, value)
  } catch {
    // persistence is a convenience only
  }
}

export const useSessionStore = defineStore('session', () => {
  const config = ref<AppConfig>({
    venue: DEFAULT_VENUE,
    personas: DEFAULT_PERSONAS,
    seed_concerts: ['tbilisi-2026', 'berlin-2026', 'lisbon-2027'],
    ticket_limit: 4,
    task_failure_rate: 0.2,
    task_max_attempts: 6,
    snapshot_every: 10,
  })
  const configLoaded = ref(false)
  const persona = ref<string>(readLocal('encore.persona') ?? 'alice')
  const concertId = ref<string>(readLocal('encore.concert') ?? 'tbilisi-2026')
  const bannerDismissed = ref<boolean>(readLocal('encore.banner') === 'dismissed')
  const reducedMotion = ref<boolean>(
    typeof matchMedia === 'function' && matchMedia('(prefers-reduced-motion: reduce)').matches,
  )

  watch(persona, (v) => writeLocal('encore.persona', v))
  watch(concertId, (v) => writeLocal('encore.concert', v))
  watch(bannerDismissed, (v) => writeLocal('encore.banner', v ? 'dismissed' : ''))

  const currentPersona = (): Persona =>
    config.value.personas.find((p) => p.fan_id === persona.value) ?? { fan_id: persona.value, name: persona.value, avatar: '🙂' }

  async function loadConfig() {
    try {
      config.value = await backend.config()
      configLoaded.value = true
    } catch {
      configLoaded.value = false
    }
  }

  return { config, configLoaded, persona, concertId, bannerDismissed, reducedMotion, currentPersona, loadConfig }
})
