import { defineStore } from 'pinia'
import { ref } from 'vue'
import type { CommandBody, CommandResponse } from '@/types/domain'
import { backend } from '@/api/backend'
import { useConcertsStore } from './concerts'
import { useWalletStore } from './wallet'

export interface Toast {
  id: number
  kind: 'success' | 'error' | 'warning' | 'info'
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
  const wallet = useWalletStore()
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
      if (!options.quiet) {
        pushToast({ kind: 'success', text: `${body.type}${seat ? ` ${seat}` : ''}` })
        for (const warning of response.warnings ?? []) pushToast({ kind: 'warning', code: warning.code })
      }
    } else if (response.outcome === 'rejected') {
      emit('rejected', response.error?.code)
      if (!options.quiet) pushToast({ kind: 'error', code: response.error?.code ?? 'rejected', params: response.error?.params })
    } else {
      emit('failed', response.error?.code ?? response.outcome)
      if (!options.quiet) pushToast({ kind: 'error', code: response.error?.code ?? response.outcome, params: response.error?.params })
    }
    // After our own command: refresh the hall, the list and the wallet.
    void concerts.refreshAll()
    void wallet.refresh()
    return response
  }

  return { inFlight, lastOutcome, toasts, signal, send, isPending, pushToast, dismiss }
})
