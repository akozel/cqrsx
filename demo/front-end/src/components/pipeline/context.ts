// What the custom nodes and edges of the diagram read: the choreography state plus the two
// standing modes (read models wiped, Encore! running). Provided by PipelineStrip.
import type { ComputedRef, InjectionKey } from 'vue'
import type { Choreography } from './choreography'
import type { Persona } from '@/types/domain'

export interface PipelineContext {
  choreo: Choreography
  wiped: ComputedRef<boolean>
  replaying: ComputedRef<boolean>
  reducedMotion: ComputedRef<boolean>
  /** Who is clicking: the selected persona, shown on the user diamond. */
  persona: ComputedRef<Persona>
}

export const PIPELINE: InjectionKey<PipelineContext> = Symbol('pipeline')
