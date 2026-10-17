import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import type { FanWalletDoc, InboxMail } from '@/types/domain'
import { backend } from '@/api/backend'
import { useSessionStore } from './session'

export const useWalletStore = defineStore('wallet', () => {
  const session = useSessionStore()
  const wallet = ref<FanWalletDoc | null>(null)
  const inbox = ref<InboxMail[]>([])
  const loadedFor = ref<string | null>(null)

  const tickets = computed(() => {
    const t = wallet.value?.view.tickets ?? {}
    return Object.entries(t)
      .map(([booking_id, ticket]) => ({ booking_id, ...ticket }))
      .sort((a, b) => a.sequence - b.sequence)
  })

  const mailByBooking = computed(() => {
    const map: Record<string, InboxMail> = {}
    for (const mail of inbox.value) {
      const [kind, id] = mail.inbox_id.split(':', 2)
      if (kind === 'e_ticket' && id) map[id] = mail
    }
    return map
  })

  const refundMails = computed(() => inbox.value.filter((m) => m.kind === 'refund'))

  async function refresh() {
    const fanId = session.persona
    try {
      const [w, i] = await Promise.all([backend.wallet(fanId), backend.inbox(fanId)])
      // The persona may have changed while we were waiting.
      if (fanId !== session.persona) return
      wallet.value = w
      inbox.value = i
      loadedFor.value = fanId
    } catch {
      // keep the previous state; the next refresh will try again
    }
  }

  function clear() {
    wallet.value = null
    inbox.value = []
    loadedFor.value = null
  }

  return { wallet, inbox, loadedFor, tickets, mailByBooking, refundMails, refresh, clear }
})
