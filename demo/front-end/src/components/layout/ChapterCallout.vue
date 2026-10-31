<script setup lang="ts">
import { useI18n } from 'vue-i18n'
import { CHAPTERS, useTourStore } from '@/stores/tour'
import { useSessionStore } from '@/stores/session'
import { useConcertsStore } from '@/stores/concerts'
import { useCommandsStore } from '@/stores/commands'
import { useRebuildStore } from '@/stores/rebuild'
import { allSeats, pickRandom } from '@/lib/venue'
import { seatState } from '@/lib/seatState'

const { t } = useI18n()
const tour = useTourStore()
const session = useSessionStore()
const concerts = useConcertsStore()
const commands = useCommandsStore()
const rebuild = useRebuildStore()

function seatsIn(state: 'free' | 'booked') {
  const view = concerts.currentView
  return allSeats(session.config.venue)
    .map((s) => s.id)
    .filter((id) => seatState(id, { mode: 'live', view, persona: '' }) === state)
}

async function tryIt() {
  const me = session.currentPersona()
  const book = async (seat: string | undefined) => {
    if (seat) await commands.send(session.concertId, { type: 'BookSeat', seat, fan_id: me.fan_id, fan_name: me.name })
  }
  switch (tour.current) {
    case 'events':
    case 'outbox':
    case 'projections':
      await book(pickRandom(seatsIn('free')))
      break
    case 'rules':
      await book(pickRandom(seatsIn('booked')) ?? pickRandom(seatsIn('free')))
      break
    case 'encore':
      await rebuild.wipe()
      break
  }
}
</script>

<template>
  <Transition name="fade">
    <aside
      v-if="tour.current"
      role="dialog"
      :aria-label="t(`chapters.${tour.current}.title`)"
      class="fixed left-4 bottom-4 z-40 w-[28rem] max-w-[90vw] rounded-xl border border-neon-violet bg-panel p-4 shadow-2xl text-sm"
    >
      <div class="flex items-start gap-2 mb-1">
        <div class="font-semibold text-neon-violet">{{ t(`chapters.${tour.current}.title`) }}</div>
        <span class="ml-auto text-xs opacity-60 whitespace-nowrap">{{ t('chapters.step', { n: tour.index, total: CHAPTERS.length }) }}</span>
        <button type="button" class="text-xs opacity-70 hover:opacity-100" :aria-label="t('chapters.done')" @click="tour.close()">✕</button>
      </div>
      <p class="leading-relaxed opacity-90">{{ t(`chapters.${tour.current}.body`) }}</p>
      <p class="mt-2 text-xs opacity-70">{{ t(`chapters.${tour.current}.try`) }}</p>
      <div class="mt-3 flex gap-2">
        <button type="button" class="action-btn text-xs" @click="tryIt">{{ t('chapters.try_it') }}</button>
        <button type="button" class="ml-auto action-btn text-xs" @click="tour.back()">{{ t('chapters.back') }}</button>
        <button type="button" class="action-btn text-xs" @click="tour.next()">
          {{ tour.index === CHAPTERS.length ? t('chapters.finish') : t('chapters.next') }}
        </button>
      </div>
    </aside>
  </Transition>
</template>
