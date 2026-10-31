<script setup lang="ts">
import { useI18n } from 'vue-i18n'
import { useConcertsStore } from '@/stores/concerts'
import { useSessionStore } from '@/stores/session'
import { wheelToHorizontal } from '@/lib/hscroll'
import AnnounceDialog from './AnnounceDialog.vue'

const { t } = useI18n()
const concerts = useConcertsStore()
const session = useSessionStore()
</script>

<template>
  <!-- One row that scrolls sideways (thin scrollbar, mouse wheel) when the concerts do not fit -->
  <div
    class="flex min-w-0 flex-1 items-stretch gap-2 overflow-x-auto pb-1"
    @wheel="wheelToHorizontal"
  >
    <button
      v-for="c in concerts.tabs"
      :key="c.concert_id"
      type="button"
      class="shrink-0 rounded-lg border px-2 py-1 text-left text-xs"
      :class="
        session.concertId === c.concert_id
          ? 'border-neon-pink bg-neon-pink/10'
          : 'border-border hover:border-neon-cyan'
      "
      @click="session.concertId = c.concert_id"
    >
      <div class="font-semibold">
        {{ c.city ?? c.concert_id }}
        <span v-if="!c.known" class="opacity-50">{{ t('concerts.unknown') }}</span>
      </div>
      <div class="text-xs opacity-70">{{ c.artist ?? '…' }} · {{ c.date ?? '' }}</div>
      <div class="text-xs font-mono mt-0.5 flex items-center gap-2">
        <span>{{ t('concerts.sold', { sold: c.sold, capacity: c.capacity }) }}</span>
        <span class="h-1.5 w-20 rounded bg-border overflow-hidden"
          ><span
            class="block h-1.5 bg-neon-cyan"
            :style="{ width: (c.capacity ? (100 * c.sold) / c.capacity : 0) + '%' }"
          ></span
        ></span>
        <span v-if="c.sold_out" class="rounded bg-neon-pink text-black px-1 text-[10px] font-bold">{{
          t('concerts.sold_out')
        }}</span>
      </div>
    </button>
    <!-- The last card of the row announces a new concert -->
    <AnnounceDialog />
  </div>
</template>
