<script setup lang="ts">
import { ref } from 'vue'
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
      const r = await commands.send(session.concertId, { type: 'BookSeat', seat, fan_id: 'scalper-bot', fan_name: 'Scalper Bot' }, { quiet: true })
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
      const r = await commands.send(session.concertId, { type: 'BookSeat', seat, fan_id: fanId, fan_name: nameOf(fanId) }, { quiet: true })
      if (r.outcome === 'accepted') accepted++
      else rejected++
      await sleep(150)
    }
    commands.pushToast({ kind: 'info', text: t('actions.crowd_result', { accepted, rejected }) })
  } finally {
    running.value = null
  }
}

/** [P1] Race: Alice and Bob hit the same free seat at the same moment. */
async function race() {
  running.value = 'race'
  raceNote.value = null
  try {
    const seat = pickRandom(freeSeats())
    if (!seat) return
    const [a, b] = await Promise.all([
      commands.send(session.concertId, { type: 'BookSeat', seat, fan_id: 'alice', fan_name: 'Alice' }, { quiet: true }),
      commands.send(session.concertId, { type: 'BookSeat', seat, fan_id: 'bob', fan_name: 'Bob' }, { quiet: true }),
    ])
    const winner = a.outcome === 'accepted' ? '🦊 Alice' : b.outcome === 'accepted' ? '🐼 Bob' : '—'
    const loser = a.outcome === 'accepted' ? `🐼 Bob` : `🦊 Alice`
    const code = (a.outcome === 'accepted' ? b : a).error?.code ?? '?'
    raceNote.value = t('actions.race_result', { winner, loser, code })
    commands.pushToast({ kind: 'info', text: raceNote.value })
  } finally {
    running.value = null
  }
}
</script>

<template>
  <div class="flex flex-wrap items-center gap-2 text-sm">
    <button type="button" class="action-btn" :disabled="running !== null" :title="t('actions.scalper_hint')" @click="scalper">{{ t('actions.scalper') }}</button>
    <button type="button" class="action-btn" :disabled="running !== null" :title="t('actions.crowd_hint')" @click="crowd">{{ t('actions.crowd') }}</button>
    <button type="button" class="action-btn" :disabled="running !== null" :title="t('actions.race_hint')" @click="race">{{ t('actions.race') }}</button>
    <span v-if="running" class="text-xs opacity-70 animate-pulse">…</span>
  </div>
  <div v-if="raceNote" class="mt-2 rounded border border-neon-amber/60 bg-neon-amber/10 px-3 py-2 text-xs">
    <div class="font-semibold">{{ raceNote }}</div>
    <div class="opacity-80 mt-1">{{ t('states.race') }}</div>
  </div>
</template>
