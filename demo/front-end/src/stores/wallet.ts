import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import type { FanWalletDoc, InboxMail } from '@/types/domain'
import type { InboxDoc } from '@/types/xray'
import { backend } from '@/api/backend'
import { sidecar } from '@/api/sidecar'
import { useSessionStore } from './session'

/** The mail service stores the business id as `_id`; the wallet calls it `inbox_id`. */
export function toInboxMail({ _id, ...doc }: InboxDoc): InboxMail {
  return { inbox_id: _id, ...doc }
}

export const useWalletStore = defineStore('wallet', () => {
  const session = useSessionStore()
  const wallet = ref<FanWalletDoc | null>(null)
  const inbox = ref<InboxMail[]>([])
  const loadedFor = ref<string | null>(null)

  const tickets = computed(() => {
    const t = wallet.value?.view.tickets ?? {}
    // The wallet is assembled from one ledger per concert, so `sequence` is only comparable inside a
    // ledger: order by time (a fixed-width RFC 3339 string), then by id for a stable tie-break.
    return Object.entries(t)
      .map(([booking_id, ticket]) => ({ booking_id, ...ticket }))
      .sort((a, b) => a.changed_at.localeCompare(b.changed_at) || a.booking_id.localeCompare(b.booking_id))
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

  /** The wallet is the box office's read model; the e-mails are the mail service's, read through
   * the sidecar (the fan opening their mail client). */
  async function refresh() {
    const fanId = session.persona
    try {
      const [w, i] = await Promise.all([backend.wallet(fanId), sidecar.inbox(fanId)])
      // The persona may have changed while we were waiting.
      if (fanId !== session.persona) return
      wallet.value = w
      inbox.value = i.docs.map(toInboxMail)
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
