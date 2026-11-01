import { defineStore } from 'pinia'
import { ref } from 'vue'
import type { CommandBody, CommandResponse } from '@/types/domain'
import { backend } from '@/api/backend'
import { useConcertsStore } from './concerts'

export interface Toast {
  id: number
  kind: 'success' | 'error' | 'info'
  code?: string
  params?: Record<string, unknown>
  text?: string
  at: number
}

/** Signals for the PipelineStrip: the last outcome and a counter so repeated outcomes re-trigger. */
export interface PipelineSignal {
  phase: 'sent' | 'accepted' | 'rejected' | 'failed'
  code?: string
  seq: number
}

export const useCommandsStore = defineStore('commands', () => {
  const concerts = useConcertsStore()
  const inFlight = ref<Set<string>>(new Set())
  const lastOutcome = ref<Record<string, 'accepted' | 'rejected' | 'failed' | null>>({})
  const toasts = ref<Toast[]>([])
  const signal = ref<PipelineSignal>({ phase: 'accepted', seq: 0 })
  let nextToast = 1
  let seq = 0

  function pushToast(toast: Omit<Toast, 'id' | 'at'>) {
    const id = nextToast++
    toasts.value = [...toasts.value.slice(-4), { ...toast, id, at: Date.now() }]
    setTimeout(() => dismiss(id), toast.kind === 'error' ? 6000 : 4000)
  }

  function dismiss(id: number) {
    toasts.value = toasts.value.filter((t) => t.id !== id)
  }

  function emit(phase: PipelineSignal['phase'], code?: string) {
    signal.value = { phase, code, seq: ++seq }
  }

  function key(concertId: string, seat?: string) {
    return `${concertId}:${seat ?? '*'}`
  }

  function isPending(concertId: string, seat: string) {
    return inFlight.value.has(key(concertId, seat))
  }

  /** Sends one command. The UI never checks the rules: the aggregate decides. */
  async function send(concertId: string, body: CommandBody, options: { quiet?: boolean } = {}): Promise<CommandResponse> {
    const seat = 'seat' in body ? body.seat : undefined
    const k = key(concertId, seat)
    inFlight.value = new Set([...inFlight.value, k])
    emit('sent')
    let response: CommandResponse
    try {
      response = await backend.command(concertId, body)
    } catch (e) {
      response = { status: 0, outcome: 'error', error: { code: 'backend_down', params: {}, message: String(e) } }
    } finally {
      const next = new Set(inFlight.value)
      next.delete(k)
      inFlight.value = next
    }
    if (seat) lastOutcome.value = { ...lastOutcome.value, [k]: response.outcome === 'accepted' ? 'accepted' : response.outcome === 'rejected' ? 'rejected' : 'failed' }
    if (response.outcome === 'accepted') {
      emit('accepted')
      if (!options.quiet) pushToast({ kind: 'success', text: `${body.type}${seat ? ` ${seat}` : ''}` })
      // Accepted = in the journal; the hall follows a moment later (D-17). Reading it now would show the seat as
      // it was, so the screen waits for the hall's projection.applied in the feed, with one fallback refresh.
      // The wallet follows the task chain the same way.
      concerts.refreshFallback()
    } else {
      if (response.outcome === 'rejected') emit('rejected', response.error?.code)
      else emit('failed', response.error?.code ?? response.outcome)
      const code = response.error?.code ?? response.outcome
      if (!options.quiet) pushToast({ kind: 'error', code, params: response.error?.params })
      // Nothing was written, but the screen may be behind the journal the aggregate judged by: read it again.
      void concerts.refreshAll()
    }
    return response
  }

  return { inFlight, lastOutcome, toasts, signal, send, isPending, pushToast, dismiss }
})
