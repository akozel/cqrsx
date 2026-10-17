<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import type { SeatInfo } from '@/lib/venue'
import { ZONE_COLORS } from '@/lib/venue'
import type { SeatUiState } from '@/lib/seatState'
import { avatarOf } from '@/lib/personas'
import { useSessionStore } from '@/stores/session'

const props = defineProps<{ seat: SeatInfo; state: SeatUiState; fanId?: string; fanName?: string; pending: boolean; outcome: 'accepted' | 'rejected' | 'failed' | null; history: boolean }>()
const emit = defineEmits<{ click: [] }>()
const { t } = useI18n()
const session = useSessionStore()

const label = computed(() => t('seatmap.seat_label', { seat: props.seat.id, zone: t(`zones.${props.seat.zone}`), price: props.seat.price, state: t(`seatmap.${props.state}`) }))
const color = computed(() => ZONE_COLORS[props.seat.zone])
</script>

<template>
  <button
    type="button"
    class="seat relative w-9 h-9 rounded-md border text-base flex items-center justify-center transition-transform"
    :class="{
      'seat-free': state === 'free',
      'seat-booked': state === 'booked',
      'seat-mine': state === 'mine',
      'seat-unknown': state === 'unknown',
      'seat-pending': pending,
      'seat-pop': outcome === 'accepted' && !pending,
      'seat-shake': outcome === 'rejected' && !pending,
    }"
    :style="{ '--zone': color }"
    :data-seat="seat.id"
    :aria-label="label"
    :title="label + (fanName ? ` · ${fanName}` : '')"
    :disabled="history"
    @click="emit('click')"
  >
    <span v-if="state === 'booked' || state === 'mine'">{{ avatarOf(fanId ?? '', session.config.personas) }}</span>
    <span v-else-if="state === 'unknown'" class="opacity-50">?</span>
    <span v-else class="text-[9px] opacity-60 font-mono">{{ seat.id }}</span>
  </button>
</template>
