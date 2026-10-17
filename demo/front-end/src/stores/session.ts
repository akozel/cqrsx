import { defineStore } from 'pinia'
import { ref, watch } from 'vue'
import type { AppConfig, Persona } from '@/types/domain'
import { DEFAULT_PERSONAS } from '@/lib/personas'
import { DEFAULT_VENUE } from '@/lib/venue'
import { backend } from '@/api/backend'
import { sidecar } from '@/api/sidecar'
import type { DemoInfo } from '@/types/domain'

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

/** The animation speeds the diagram offers, as multiples of the base pace. */
export const PACES = [0.5, 1, 2] as const
/** The base pace: 1× plays 1.33 times slower than the choreography's own timings. */
export const PACE_BASE = 1 / 1.33

export const useSessionStore = defineStore('session', () => {
  const config = ref<AppConfig>({
    venue: DEFAULT_VENUE,
    personas: DEFAULT_PERSONAS,
    ticket_limit: 4,
    task_failure_rate: 0.2,
    task_max_attempts: 6,
    snapshot_every: 10,
  })
  const configLoaded = ref(false)
  /** From the sidecar: the concerts the demo starts from (fallback tabs) and the reset switch. */
  const demo = ref<DemoInfo>({
    default_concerts: [
      { concert_id: 'radiohead-2026-11-21', artist: 'Radiohead', city: 'Minsk', date: '2026-11-21' },
      { concert_id: 'rammstein-2026-12-05', artist: 'Rammstein', city: 'Tokyo', date: '2026-12-05' },
      { concert_id: 'muse-2027-01-16', artist: 'Muse', city: 'Tbilisi', date: '2027-01-16' },
    ],
    reset_enabled: false,
  })
  const persona = ref<string>(readLocal('encore.persona') ?? 'fox')
  const concertId = ref<string>(readLocal('encore.concert') ?? 'radiohead-2026-11-21')
  const bannerDismissed = ref<boolean>(readLocal('encore.banner') === 'dismissed')
  /** Diagram animation speed: 0.5 (slow), 1, 2 (fast). */
  const stored = Number(readLocal('encore.pace'))
  const pace = ref<number>((PACES as readonly number[]).includes(stored) ? stored : 1)
  const reducedMotion = ref<boolean>(
    typeof matchMedia === 'function' && matchMedia('(prefers-reduced-motion: reduce)').matches,
  )

  watch(persona, (v) => writeLocal('encore.persona', v))
  watch(concertId, (v) => writeLocal('encore.concert', v))
  watch(bannerDismissed, (v) => writeLocal('encore.banner', v ? 'dismissed' : ''))
  watch(pace, (v) => writeLocal('encore.pace', String(v)))

  const currentPersona = (): Persona =>
    config.value.personas.find((p) => p.fan_id === persona.value) ?? {
      fan_id: persona.value,
      name: persona.value,
      avatar: '🙂',
    }

  async function loadConfig() {
    try {
      config.value = await backend.config()
      configLoaded.value = true
    } catch {
      configLoaded.value = false
    }
    try {
      demo.value = await sidecar.demo()
    } catch {
      // keep the built-in defaults; the reset button stays hidden
    }
  }

  return {
    config,
    configLoaded,
    demo,
    persona,
    concertId,
    bannerDismissed,
    reducedMotion,
    pace,
    currentPersona,
    loadConfig,
  }
})
