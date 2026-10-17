<script setup lang="ts">
import { useI18n } from 'vue-i18n'
import { useConcertsStore } from '@/stores/concerts'
import { useSessionStore } from '@/stores/session'

const { t } = useI18n()
const concerts = useConcertsStore()
const session = useSessionStore()
</script>

<template>
  <div class="flex flex-wrap gap-2">
    <button
      v-for="c in concerts.tabs"
      :key="c.concert_id"
      type="button"
      class="rounded-lg border px-3 py-1.5 text-left text-sm"
      :class="session.concertId === c.concert_id ? 'border-neon-pink bg-neon-pink/10' : 'border-border hover:border-neon-cyan'"
      @click="session.concertId = c.concert_id"
    >
      <div class="font-semibold">{{ c.city ?? c.concert_id }} <span v-if="!c.known" class="opacity-50">{{ t('concerts.unknown') }}</span></div>
      <div class="text-xs opacity-70">{{ c.artist ?? '…' }} · {{ c.date ?? '' }}</div>
      <div class="text-xs font-mono mt-0.5 flex items-center gap-2">
        <span>{{ t('concerts.sold', { sold: c.sold, capacity: c.capacity }) }}</span>
        <span class="h-1.5 w-20 rounded bg-border overflow-hidden"><span class="block h-1.5 bg-neon-cyan" :style="{ width: (c.capacity ? (100 * c.sold) / c.capacity : 0) + '%' }"></span></span>
        <span v-if="c.sold_out" class="rounded bg-neon-pink text-black px-1 text-[10px] font-bold">{{ t('concerts.sold_out') }}</span>
        <span v-if="c.incomplete" class="text-neon-amber" :title="t('states.incomplete')">⚠️</span>
      </div>
    </button>
  </div>
</template>
