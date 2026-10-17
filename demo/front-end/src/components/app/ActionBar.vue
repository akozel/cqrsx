<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { useSessionStore } from '@/stores/session'
import { useConcertsStore } from '@/stores/concerts'
import { useCommandsStore } from '@/stores/commands'
import { allSeats, pickRandom } from '@/lib/venue'
import { seatState } from '@/lib/seatState'
import { nameOf } from '@/lib/personas'

const { t } = useI18n()
const session = useSessionStore()
const concerts = useConcertsStore()
const commands = useCommandsStore()
const running = ref<'scalper' | 'crowd' | 'race' | null>(null)
const raceNote = ref<string | null>(null)

// The race note stays until the seat map changes again: its own booking arms the watcher (or a
// short timeout, if that update came before the note); the next change — any booking, cancel,
// wipe, concert switch — closes it.
const raceSeat = ref<string | null>(null)
const raceArmed = ref(false)
let raceArmTimer: ReturnType<typeof setTimeout> | null = null
const seatSignature = computed(() => {
  const seats = concerts.currentView?.seats ?? {}
  return Object.keys(seats)
    .sort()
    .map((id) => `${id}:${seats[id]?.state}:${seats[id]?.fan_id ?? ''}`)
    .join('|')
})
const raceSeatBooked = () =>
  raceSeat.value !== null && concerts.currentView?.seats[raceSeat.value]?.state === 'booked'
function closeRaceNote() {
  raceNote.value = null
  raceSeat.value = null
  raceArmed.value = false
  if (raceArmTimer) clearTimeout(raceArmTimer)
  raceArmTimer = null
}
watch(seatSignature, () => {
  if (!raceNote.value) return
  if (raceArmed.value) closeRaceNote()
  else if (raceSeatBooked()) raceArmed.value = true
})
watch(() => session.concertId, closeRaceNote)
watch(
  () => concerts.stale,
  (stale) => stale && closeRaceNote(),
)

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

/** Free seats from the latest read model; when there is none, the whole hall. */
function freeSeats(): string[] {
  const view = concerts.currentView
  return allSeats(session.config.venue)
    .map((s) => s.id)
    .filter((id) => !view || seatState(id, { mode: 'live', view, persona: '' }) !== 'booked')
}

async function scalper() {
  running.value = 'scalper'
  let bought = 0
  let rejected = 0
  try {
    for (let i = 0; i < 6; i++) {
      const seat = pickRandom(freeSeats())
      if (!seat) break
      const r = await commands.send(
        session.concertId,
        { type: 'BookSeat', seat, fan_id: 'arbitrage-bot', fan_name: 'Arbitrage Bot' },
        { quiet: true },
      )
      if (r.outcome === 'accepted') bought++
      else rejected++
      await sleep(250)
    }
    commands.pushToast({ kind: 'info', text: t('actions.scalper_result', { bought, rejected }) })
  } finally {
    running.value = null
  }
}

async function crowd() {
  running.value = 'crowd'
  let accepted = 0
  let rejected = 0
  try {
    for (let i = 0; i < 10; i++) {
      const seat = pickRandom(freeSeats())
      if (!seat) {
        commands.pushToast({ kind: 'info', text: t('actions.no_free_seats') })
        break
      }
      const fanId = `crowd-${1 + Math.floor(Math.random() * 60)}`
      const r = await commands.send(
        session.concertId,
        { type: 'BookSeat', seat, fan_id: fanId, fan_name: nameOf(fanId) },
        { quiet: true },
      )
      if (r.outcome === 'accepted') accepted++
      else rejected++
      await sleep(150)
    }
    commands.pushToast({ kind: 'info', text: t('actions.crowd_result', { accepted, rejected }) })
  } finally {
    running.value = null
  }
}

/** [P1] Race: Fox and Panda hit the same free seat at the same moment. */
async function race() {
  running.value = 'race'
  closeRaceNote()
  try {
    const seat = pickRandom(freeSeats())
    if (!seat) return
    const [a, b] = await Promise.all([
      commands.send(
        session.concertId,
        { type: 'BookSeat', seat, fan_id: 'fox', fan_name: 'Fox' },
        { quiet: true },
      ),
      commands.send(
        session.concertId,
        { type: 'BookSeat', seat, fan_id: 'panda', fan_name: 'Panda' },
        { quiet: true },
      ),
    ])
    const winner = a.outcome === 'accepted' ? '🦊 Fox' : b.outcome === 'accepted' ? '🐼 Panda' : '—'
    const loser = a.outcome === 'accepted' ? `🐼 Panda` : `🦊 Fox`
    const code = (a.outcome === 'accepted' ? b : a).error?.code ?? '?'
    raceNote.value = t('actions.race_result', { winner, loser, code })
    raceSeat.value = seat
    raceArmed.value = raceSeatBooked()
    if (raceArmTimer) clearTimeout(raceArmTimer)
    raceArmTimer = setTimeout(() => (raceArmed.value = true), 5000)
    commands.pushToast({ kind: 'info', text: raceNote.value })
  } finally {
    running.value = null
  }
}
</script>

<template>
  <div class="flex flex-wrap items-center gap-2 text-xs">
    <button
      type="button"
      class="action-btn"
      :disabled="running !== null"
      :title="t('actions.scalper_hint')"
      @click="scalper"
    >
      {{ t('actions.scalper') }}
    </button>
    <button
      type="button"
      class="action-btn"
      :disabled="running !== null"
      :title="t('actions.crowd_hint')"
      @click="crowd"
    >
      {{ t('actions.crowd') }}
    </button>
    <button
      type="button"
      class="action-btn"
      :disabled="running !== null"
      :title="t('actions.race_hint')"
      @click="race"
    >
      {{ t('actions.race') }}
    </button>
    <span v-if="running" class="text-xs opacity-70 animate-pulse">…</span>
  </div>
  <div v-if="raceNote" class="mt-2 rounded border border-neon-amber/60 bg-neon-amber/10 px-3 py-2 text-xs">
    <div class="font-semibold">{{ raceNote }}</div>
    <div class="opacity-80 mt-1">{{ t('states.race') }}</div>
  </div>
</template>
