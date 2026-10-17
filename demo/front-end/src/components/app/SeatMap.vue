<script setup lang="ts">
import { computed, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import SeatButton from './SeatButton.vue'
import { allSeats, ZONE_COLORS } from '@/lib/venue'
import { seatState, type SeatContext } from '@/lib/seatState'
import { useSessionStore } from '@/stores/session'
import { useConcertsStore } from '@/stores/concerts'
import { useCommandsStore } from '@/stores/commands'
import { useTimeMachineStore } from '@/stores/timemachine'
import { nameOf } from '@/lib/personas'

const { t } = useI18n()
const session = useSessionStore()
const concerts = useConcertsStore()
const commands = useCommandsStore()
const timeMachine = useTimeMachineStore()

const seats = computed(() => allSeats(session.config.venue))
const rows = computed(() => session.config.venue.rows.map((row) => ({ row, seats: seats.value.filter((s) => s.row === row) })))
const context = computed<SeatContext>(() =>
  timeMachine.active && timeMachine.historyState
    ? { mode: 'history', historyState: timeMachine.historyState.state, persona: session.persona }
    : { mode: 'live', view: concerts.currentView, persona: session.persona },
)
const artist = computed(() => concerts.currentView?.concert?.artist ?? timeMachine.historyState?.state.info?.artist ?? '')
const soldOut = computed(() => concerts.currentView?.sold_out ?? false)
const cancelPrompt = ref<{ seat: string; refund: number } | null>(null)

function occupant(seat: string): { fanId?: string; fanName?: string } {
  if (context.value.mode === 'history') {
    const b = context.value.historyState.bookings[seat]
    return { fanId: b?.fan_id, fanName: b?.fan_name }
  }
  const e = concerts.currentView?.seats[seat]
  return { fanId: e?.fan_id, fanName: e?.fan_name }
}

async function onSeat(seat: string) {
  if (timeMachine.active) return
  const state = seatState(seat, context.value)
  const me = session.currentPersona()
  if (state === 'mine') {
    const price = concerts.currentView?.seats[seat]?.price ?? 0
    cancelPrompt.value = { seat, refund: price }
    return
  }
  // Free, unknown or somebody else's seat: send BookSeat anyway — the aggregate is the judge.
  await commands.send(session.concertId, { type: 'BookSeat', seat, fan_id: me.fan_id, fan_name: me.name })
}

/** [P1] Arrow keys move focus across the hall; Enter/Space click as usual. */
function onKey(event: KeyboardEvent) {
  const target = event.target as HTMLElement | null
  const id = target?.getAttribute('data-seat')
  if (!id) return
  const rowsList = session.config.venue.rows
  const row = rowsList.indexOf(id[0] ?? '')
  const n = Number(id.slice(1))
  let next: string | null = null
  if (event.key === 'ArrowLeft') next = n > 1 ? `${id[0]}${n - 1}` : null
  if (event.key === 'ArrowRight') next = n < session.config.venue.seats_per_row ? `${id[0]}${n + 1}` : null
  if (event.key === 'ArrowUp') next = row > 0 ? `${rowsList[row - 1]}${n}` : null
  if (event.key === 'ArrowDown') next = row < rowsList.length - 1 ? `${rowsList[row + 1]}${n}` : null
  if (next) {
    event.preventDefault()
    ;(target?.closest('.seat-grid')?.querySelector(`[data-seat="${next}"]`) as HTMLElement | null)?.focus()
  }
}

async function confirmCancel() {
  const prompt = cancelPrompt.value
  cancelPrompt.value = null
  if (!prompt) return
  await commands.send(session.concertId, { type: 'CancelBooking', seat: prompt.seat, fan_id: session.persona })
}
</script>

<template>
  <div class="relative" :class="{ 'history-mode': timeMachine.active }">
    <div v-if="timeMachine.active" class="mb-2 rounded border border-neon-amber/60 bg-neon-amber/10 px-3 py-1 text-xs">
      {{ t('states.history', { n: timeMachine.sequence }) }}
    </div>
    <div v-else-if="concerts.stale" class="mb-2 rounded border border-border px-3 py-1 text-xs opacity-80">{{ t('states.wiped') }}</div>
    <div v-else-if="concerts.currentView?.incomplete" class="mb-2 rounded border border-neon-amber/60 px-3 py-1 text-xs text-neon-amber">{{ t('states.incomplete') }}</div>
    <div class="relative mx-auto w-fit">
      <div class="mx-auto mb-3 w-64 rounded-b-3xl border border-neon-pink/60 stage-glow py-1 text-center text-xs tracking-widest">
        🎤 {{ t('seatmap.stage') }} <span class="opacity-70">· {{ artist }}</span>
      </div>
      <div v-if="soldOut" class="absolute left-1/2 top-10 -translate-x-1/2 text-3xl font-extrabold text-neon-pink sold-out-sign pointer-events-none">SOLD OUT</div>
      <div class="seat-grid flex flex-col gap-1.5" @keydown="onKey">
        <div v-for="r in rows" :key="r.row" class="flex items-center gap-1.5">
          <span class="w-5 text-xs font-mono opacity-60">{{ r.row }}</span>
          <SeatButton
            v-for="s in r.seats"
            :key="s.id"
            :seat="s"
            :state="seatState(s.id, context)"
            v-bind="occupant(s.id)"
            :pending="commands.isPending(session.concertId, s.id)"
            :outcome="commands.lastOutcome[`${session.concertId}:${s.id}`] ?? null"
            :history="timeMachine.active"
            @click="onSeat(s.id)"
          />
          <span class="ml-2 text-[10px] font-mono opacity-60 w-24" :style="{ color: ZONE_COLORS[r.seats[0]?.zone ?? 'balcony'] }">{{ t(`zones.${r.seats[0]?.zone ?? 'balcony'}`) }} {{ r.seats[0]?.price }} €</span>
        </div>
      </div>
    </div>
    <div v-if="cancelPrompt" class="absolute inset-0 flex items-center justify-center bg-bg/70 rounded-lg">
      <div class="rounded-lg border border-border bg-panel p-4 text-sm shadow-xl">
        <div class="mb-3">{{ t('actions.cancel_prompt', { refund: cancelPrompt.refund }) }} <span class="opacity-60 font-mono">({{ cancelPrompt.seat }})</span></div>
        <div class="flex gap-2 justify-end">
          <button type="button" class="px-3 py-1 rounded border border-border" @click="cancelPrompt = null">{{ t('actions.cancel_no') }}</button>
          <button type="button" class="px-3 py-1 rounded bg-neon-red text-black font-semibold" @click="confirmCancel">{{ t('actions.cancel_yes') }}</button>
        </div>
      </div>
    </div>
    <div class="sr-only">{{ nameOf(session.persona, session.config.personas) }}</div>
  </div>
</template>
