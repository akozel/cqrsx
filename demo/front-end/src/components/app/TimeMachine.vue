<script setup lang="ts">
import { computed, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { useTimeMachineStore } from '@/stores/timemachine'
import { useSessionStore } from '@/stores/session'

const { t } = useI18n()
const timeMachine = useTimeMachineStore()
const session = useSessionStore()

const position = computed({
  get: () => timeMachine.sequence ?? timeMachine.lastSequence,
  set: (n: number) => void timeMachine.goTo(n),
})
const colors: Record<string, string> = { ConcertAnnounced: '#a78bfa', SeatBooked: '#a3e635', BookingCancelled: '#f43f5e' }
const current = computed(() => timeMachine.timeline.find((e) => e.sequence === position.value))

watch(() => session.concertId, () => timeMachine.reset(), { immediate: false })
</script>

<template>
  <div class="text-xs">
    <div class="flex items-center gap-2">
      <span class="font-semibold">{{ t('timemachine.title') }}</span>
      <span class="text-xs font-mono opacity-70">{{ t('timemachine.event_of', { n: position, last: timeMachine.lastSequence }) }}</span>
      <span v-if="current" class="text-xs opacity-80" :style="{ color: colors[current.event_type] }">{{ current.event_type }} · {{ current.summary }}</span>
      <button v-if="timeMachine.active" type="button" class="ml-auto action-btn text-xs" @click="timeMachine.backToPresent()">{{ t('timemachine.back_to_present') }}</button>
    </div>
    <div class="flex items-center gap-2 mt-1">
      <button type="button" class="px-1" :title="t('timemachine.prev')" :disabled="position <= 1" @click="position = position - 1">⏮</button>
      <div class="relative flex-1">
        <input v-model.number="position" type="range" min="1" :max="Math.max(1, timeMachine.lastSequence)" class="w-full accent-neon-pink" :disabled="timeMachine.lastSequence === 0" />
        <div class="pointer-events-none absolute inset-x-0 -bottom-1 h-1 flex">
          <span v-for="e in timeMachine.timeline" :key="e.sequence" class="absolute h-1 w-0.5" :style="{ left: ((e.sequence - 1) / Math.max(1, timeMachine.lastSequence - 1)) * 100 + '%', background: colors[e.event_type] }"></span>
        </div>
      </div>
      <button type="button" class="px-1" :title="t('timemachine.next')" :disabled="position >= timeMachine.lastSequence" @click="position = position + 1">⏭</button>
    </div>
  </div>
</template>
