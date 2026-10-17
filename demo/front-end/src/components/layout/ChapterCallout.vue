<script setup lang="ts">
import { useI18n } from 'vue-i18n'
import { useTourStore } from '@/stores/tour'
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
    <aside v-if="tour.current" class="fixed left-4 bottom-4 z-40 w-[28rem] max-w-[90vw] rounded-xl border border-neon-violet bg-panel p-4 shadow-2xl text-sm">
      <div class="font-semibold text-neon-violet mb-1">{{ t(`chapters.${tour.current}.title`) }}</div>
      <p class="leading-relaxed opacity-90">{{ t(`chapters.${tour.current}.body`) }}</p>
      <p class="mt-2 text-xs opacity-70">{{ t(`chapters.${tour.current}.try`) }}</p>
      <div class="mt-3 flex gap-2">
        <button type="button" class="action-btn text-xs" @click="tryIt">{{ t('chapters.try_it') }}</button>
        <button type="button" class="action-btn text-xs" @click="tour.next()">{{ t('chapters.next') }}</button>
        <button type="button" class="ml-auto action-btn text-xs" @click="tour.close()">{{ t('chapters.done') }}</button>
      </div>
    </aside>
  </Transition>
</template>
