<script setup lang="ts">
import { useI18n } from 'vue-i18n'
import type { UiTaskStatus } from '@/lib/outboxStatus'

const props = defineProps<{ status: UiTaskStatus; attempt?: number; retrySeconds?: number }>()
const { t } = useI18n()

const classes: Record<UiTaskStatus, string> = {
  queued: 'bg-gray-600/40 text-gray-200',
  in_progress: 'bg-cyan-500/30 text-neon-cyan animate-pulse',
  retry_scheduled: 'bg-amber-500/30 text-neon-amber',
  delivered: 'bg-lime-500/30 text-neon-lime',
  failed: 'bg-rose-500/30 text-neon-red',
}

function label() {
  switch (props.status) {
    case 'in_progress':
      return t('status.in_progress', { n: props.attempt ?? 1 })
    case 'retry_scheduled':
      return props.retrySeconds !== undefined && props.retrySeconds > 0
        ? t('status.retry_scheduled', { s: props.retrySeconds })
        : t('xray.waiting_worker')
    default:
      return t(`status.${props.status}`)
  }
}
</script>

<template>
  <span class="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs whitespace-nowrap" :class="classes[status]">
    <span v-if="status === 'retry_scheduled'">🎲</span>{{ label() }}
  </span>
</template>
