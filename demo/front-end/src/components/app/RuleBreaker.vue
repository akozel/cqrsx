<script setup lang="ts">
import { ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { useSessionStore } from '@/stores/session'
import { useConcertsStore } from '@/stores/concerts'
import { useCommandsStore } from '@/stores/commands'
import { useXrayStore } from '@/stores/xray'
import type { CommandBody, CommandResponse } from '@/types/domain'
import { allSeats, pickRandom } from '@/lib/venue'
import { seatState } from '@/lib/seatState'
import JsonView from '@/components/xray/JsonView.vue'

const { t, te } = useI18n()
const session = useSessionStore()
const concerts = useConcertsStore()
const commands = useCommandsStore()
const xray = useXrayStore()

const last = ref<{ sent: CommandBody; concertId: string; response: CommandResponse; before: number; after: number | null } | null>(null)
const busy = ref(false)

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

function seatsBy(state: 'free' | 'booked', persona = session.persona): string[] {
  const view = concerts.currentView
  return allSeats(session.config.venue)
    .map((s) => s.id)
    .filter((id) => {
      const s = seatState(id, { mode: 'live', view, persona })
      return state === 'free' ? s === 'free' : s === 'booked'
    })
}

async function fire(concertId: string, body: CommandBody) {
  busy.value = true
  const before = xray.eventsTotal
  const response = await commands.send(concertId, body)
  last.value = { sent: body, concertId, response, before, after: null }
  // The sidecar reports the next poll within ~1 s; the delta must be 0 for a rejected command.
  await sleep(1200)
  if (last.value) last.value = { ...last.value, after: xray.eventsTotal }
  busy.value = false
}

const me = () => session.currentPersona()

const buttons = [
  { key: 'unknown_seat', run: () => fire(session.concertId, { type: 'BookSeat', seat: 'Z9', fan_id: me().fan_id, fan_name: me().name }) },
  {
    key: 'taken_seat',
    run: async () => {
      let taken = seatsBy('booked')
      if (taken.length === 0) {
        const seat = pickRandom(seatsBy('free'))
        if (seat) await commands.send(session.concertId, { type: 'BookSeat', seat, fan_id: 'unicorn', fan_name: 'Unicorn' }, { quiet: true })
        taken = seat ? [seat] : []
      }
      const seat = pickRandom(taken)
      if (seat) await fire(session.concertId, { type: 'BookSeat', seat, fan_id: me().fan_id === 'unicorn' ? 'fox' : me().fan_id, fan_name: me().fan_id === 'unicorn' ? 'Fox' : me().name })
    },
  },
  {
    key: 'fifth_ticket',
    run: async () => {
      // Arbitrage Bot tops up to the limit, then asks for one more.
      for (let i = 0; i < session.config.ticket_limit + 1; i++) {
        const seat = pickRandom(seatsBy('free', 'arbitrage-bot').filter((id) => seatState(id, { mode: 'live', view: concerts.currentView, persona: 'arbitrage-bot' }) === 'free'))
        if (!seat) break
        if (i < session.config.ticket_limit) {
          const r = await commands.send(session.concertId, { type: 'BookSeat', seat, fan_id: 'arbitrage-bot', fan_name: 'Arbitrage Bot' }, { quiet: true })
          if (r.outcome === 'rejected' && r.error?.code === 'ticket_limit') {
            await fire(session.concertId, { type: 'BookSeat', seat, fan_id: 'arbitrage-bot', fan_name: 'Arbitrage Bot' })
            return
          }
        } else {
          await fire(session.concertId, { type: 'BookSeat', seat, fan_id: 'arbitrage-bot', fan_name: 'Arbitrage Bot' })
        }
      }
    },
  },
  {
    key: 'not_yours',
    run: async () => {
      const seat = pickRandom(seatsBy('booked'))
      if (seat) await fire(session.concertId, { type: 'CancelBooking', seat, fan_id: me().fan_id })
    },
  },
  {
    key: 'cancel_free',
    run: async () => {
      const seat = pickRandom(seatsBy('free'))
      if (seat) await fire(session.concertId, { type: 'CancelBooking', seat, fan_id: me().fan_id })
    },
  },
  { key: 're_announce', run: () => fire(session.concertId, { type: 'AnnounceConcert', artist: 'Encore', city: 'Nowhere', date: '2030-01-01' }) },
  { key: 'ghost_concert', run: () => fire('atlantis-2099', { type: 'BookSeat', seat: 'B4', fan_id: me().fan_id, fan_name: me().name }) },
]

function ruleText(r: CommandResponse) {
  const code = r.error?.code
  if (!code) return r.outcome
  return te(`rules.${code}`) ? t(`rules.${code}`, r.error?.params ?? {}) : code
}
</script>

<template>
  <div>
    <div class="flex items-center gap-2 text-sm font-semibold">{{ t('rulebreaker.title') }} <span class="text-xs opacity-60 font-normal">{{ t('rulebreaker.hint') }}</span></div>
    <div class="flex flex-wrap gap-1.5 mt-1">
      <button v-for="b in buttons" :key="b.key" type="button" class="action-btn text-xs" :disabled="busy" @click="b.run()">{{ t(`rulebreaker.${b.key}`) }}</button>
    </div>
    <div v-if="last" class="mt-2 rounded border border-border bg-bg/60 p-2 text-xs grid grid-cols-1 md:grid-cols-2 gap-2">
      <div>
        <div class="opacity-60 mb-1">{{ t('rulebreaker.sent') }} → {{ last.concertId }}</div>
        <JsonView :value="last.sent" expanded />
      </div>
      <div>
        <div class="opacity-60 mb-1">{{ t('rulebreaker.answer') }} · HTTP {{ last.response.status }}</div>
        <div class="font-mono" :class="last.response.outcome === 'accepted' ? 'text-neon-lime' : 'text-neon-red'">{{ last.response.error?.code ?? last.response.outcome }}</div>
        <div>{{ ruleText(last.response) }}</div>
        <div class="mt-1 font-mono" :class="last.after !== null && last.after - last.before === 0 ? 'text-neon-lime' : ''">
          {{ t('rulebreaker.events_delta', { before: last.before, after: last.after ?? '…', delta: last.after === null ? '…' : last.after - last.before }) }}
        </div>
      </div>
    </div>
  </div>
</template>
