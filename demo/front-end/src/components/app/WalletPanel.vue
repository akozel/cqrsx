<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import { useSessionStore } from '@/stores/session'
import { useConcertsStore } from '@/stores/concerts'
import { useWalletStore } from '@/stores/wallet'
import { useCommandsStore } from '@/stores/commands'
import { useXrayStore } from '@/stores/xray'
import { deriveTaskStatus, retryCountdownMs } from '@/lib/outboxStatus'
import StatusChip from '@/components/ui/StatusChip.vue'
import { euro } from '@/lib/format'

const { t } = useI18n()
const session = useSessionStore()
const concerts = useConcertsStore()
const wallet = useWalletStore()
const commands = useCommandsStore()
const xray = useXrayStore()

/** Seats of mine in the hall that the wallet does not know yet: the record_ticket task is in flight. */
const recording = computed(() => {
  const seats = concerts.currentView?.seats ?? {}
  const known = new Set(Object.keys(wallet.wallet?.view.tickets ?? {}))
  return Object.entries(seats)
    .filter(([, e]) => e.state === 'booked' && e.fan_id === session.persona && e.booking_id && !known.has(e.booking_id))
    .map(([seat]) => seat)
})

function mailTask(bookingId: string) {
  return (xray.state.outbox?.rows ?? []).find((r) => r.task_type === 'encore_send_e_ticket' && r.payload.booking_id === bookingId)
}

async function cancel(concertId: string, seat: string) {
  await commands.send(concertId, { type: 'CancelBooking', seat, fan_id: session.persona })
}
</script>

<template>
  <div class="text-sm">
    <div class="flex items-center gap-2 font-semibold">
      {{ t('wallet.title') }}
      <span class="text-xs opacity-60 font-normal">{{ session.currentPersona().avatar }} {{ session.currentPersona().name }}</span>
      <span v-if="wallet.wallet" class="ml-auto text-xs font-mono opacity-70">{{ t('wallet.spent', { amount: wallet.wallet.view.total_spent }) }}</span>
    </div>
    <ul class="mt-1 space-y-1">
      <li v-for="seat in recording" :key="'rec-' + seat" class="text-xs opacity-70 animate-pulse">{{ seat }} · {{ t('wallet.recording') }}</li>
      <li v-for="ticket in wallet.tickets" :key="ticket.booking_id" class="flex flex-wrap items-center gap-2 text-xs rounded border border-border/60 px-2 py-1" :class="{ 'opacity-50 line-through': ticket.state === 'cancelled' }">
        <span class="font-mono">{{ ticket.concert_id }} · {{ ticket.seat }} · {{ euro(ticket.price) }}</span>
        <template v-if="ticket.state === 'active'">
          <span v-if="wallet.mailByBooking[ticket.booking_id]" class="text-neon-lime">{{ t('wallet.delivered', { attempt: wallet.mailByBooking[ticket.booking_id]?.delivered_on_attempt }) }}</span>
          <StatusChip v-else-if="mailTask(ticket.booking_id)" :status="deriveTaskStatus(mailTask(ticket.booking_id)!)" :attempt="mailTask(ticket.booking_id)!.attempts" :retry-seconds="Math.ceil(retryCountdownMs(mailTask(ticket.booking_id)!, Date.now() + xray.dbTimeOffset) / 1000)" />
          <span v-else class="opacity-60">{{ t('wallet.pending') }}</span>
          <button v-if="ticket.concert_id === session.concertId" type="button" class="ml-auto rounded border border-border px-2 py-0.5 hover:border-neon-red" @click="cancel(ticket.concert_id, ticket.seat)">{{ t('wallet.cancel') }}</button>
        </template>
        <span v-else class="opacity-80">{{ t('wallet.cancelled') }} · {{ t('wallet.refund_mail', { amount: ticket.refund ?? ticket.price }) }}</span>
      </li>
    </ul>
    <p v-if="wallet.tickets.length === 0 && recording.length === 0" class="text-xs opacity-60 mt-1">{{ t('wallet.empty') }}</p>
    <p v-else class="text-[10px] opacity-50 mt-1">{{ t('wallet.behind') }}</p>
  </div>
</template>
