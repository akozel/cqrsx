<script setup lang="ts">
// The status of an outbox task as one icon. Hovering (or focusing) it shows a small table with the
// row as the database holds it: status, attempts, the retry countdown, the task format, ids and the
// last error.
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import type { OutboxRow } from '@/types/xray'
import HoverTip from '@/components/ui/HoverTip.vue'
import { shortId } from '@/lib/format'

const props = defineProps<{ row: OutboxRow; retrySeconds: number; reason?: string }>()
const { t } = useI18n()

const icon = computed(() => {
  switch (props.row.status) {
    case 'completed':
      return { glyph: '✅', cls: 'text-neon-lime' }
    case 'failed':
      return { glyph: '❌', cls: 'text-neon-red' }
    case 'processing':
      return { glyph: '⚙', cls: 'text-neon-cyan animate-pulse' }
    default:
      return props.row.attempts > 0
        ? { glyph: '🎲', cls: 'text-neon-amber' }
        : { glyph: '⏳', cls: 'opacity-70' }
  }
})
const next = computed(() => {
  if (props.row.status !== 'pending') return null
  if (props.row.attempts === 0) return t('xray.queued')
  return props.retrySeconds > 0 ? t('xray.retry_in', { s: props.retrySeconds }) : t('xray.waiting_worker')
})
const rows = computed(() => {
  const r = props.row
  const list: [string, string][] = [
    [t('xray.tip.status'), r.status],
    [
      t('xray.tip.attempt'),
      `${r.attempts}/${r.max_attempts}${r.status === 'failed' ? ` · ${t('xray.attempt_limit')}` : ''}`,
    ],
  ]
  if (next.value) list.push([t('xray.tip.next'), next.value])
  list.push(
    [t('xray.tip.task'), `${r.task_type} v${r.task_version}`],
    [t('xray.tip.id'), shortId(r.id)],
    [t('xray.tip.correlation'), shortId(String(r.metadata.correlation_id ?? '')) || '—'],
    [t('xray.tip.aggregate'), `${r.aggregate_type}/${r.aggregate_id} #${r.aggregate_sequence}`],
  )
  if (r.last_error) list.push([t('xray.tip.reason'), props.reason ?? r.last_error])
  return list
})
</script>

<template>
  <HoverTip align="right" :aria-label="`${row.status}: ${rows.map(([k, v]) => `${k} ${v}`).join(', ')}`">
    <span class="cursor-help text-sm leading-none" :class="icon.cls">{{ icon.glyph }}</span>
    <template #tip>
      <table class="xray-tip">
        <tbody>
          <tr v-for="[k, v] in rows" :key="k">
            <th>{{ k }}</th>
            <td>{{ v }}</td>
          </tr>
        </tbody>
      </table>
      <span class="mt-1 block text-[10px] opacity-50">{{ t('xray.db_status') }}</span>
    </template>
  </HoverTip>
</template>
