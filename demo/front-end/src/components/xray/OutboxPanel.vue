<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import XrayPanel from './XrayPanel.vue'
import JsonView from './JsonView.vue'
import StatusChip from '@/components/ui/StatusChip.vue'
import Countdown from '@/components/ui/Countdown.vue'
import { useXrayStore } from '@/stores/xray'
import { deriveTaskStatus, retryCountdownMs, taskKind } from '@/lib/outboxStatus'
import { shortId } from '@/lib/format'

const xray = useXrayStore()
const { t, te } = useI18n()
const now = ref(Date.now())
let timer: ReturnType<typeof setInterval> | null = null
onMounted(() => (timer = setInterval(() => (now.value = Date.now()), 250)))
onUnmounted(() => timer && clearInterval(timer))

const rows = computed(() => xray.state.outbox?.rows ?? [])
const counters = computed(() => xray.state.stats.tasks_by_status ?? {})
const icon = (taskType: string) => ({ chain: '🔁', e_ticket: '🎫', refund: '💸', other: '📦' })[taskKind(taskType)]
const dbNow = () => now.value + xray.dbTimeOffset
function reason(code: string | null) {
  if (!code) return ''
  return te(`chaos.${code}`) ? t(`chaos.${code}`) : code
}
</script>

<template>
  <XrayPanel db="pg" :title="t('panels.outbox.title')" :count="xray.state.outbox?.total" :explain="t('panels.outbox.explain')" :query="xray.state.outbox?.query" :warning="xray.pgDown ? t('xray.pg_down') : undefined">
    <div class="flex flex-wrap gap-2 text-[10px] font-mono mb-2 opacity-80">
      <span v-for="(n, status) in counters" :key="status" class="rounded border border-border px-1">{{ status }}: {{ n }}</span>
    </div>
    <p v-if="rows.length === 0" class="text-xs opacity-60">{{ t('xray.no_rows') }}</p>
    <TransitionGroup name="row" tag="ul" class="space-y-1">
      <li v-for="row in rows" :key="row.id" class="rounded border border-border/60 px-2 py-1 text-xs" :class="{ 'row-fresh': xray.freshTasks.has(row.id) }">
        <div class="flex flex-wrap items-center gap-2 font-mono">
          <span>{{ icon(row.task_type) }}</span>
          <span class="opacity-60">{{ shortId(row.id) }}</span>
          <span class="font-semibold">{{ row.task_type }} v{{ row.task_version }}</span>
          <StatusChip :status="deriveTaskStatus(row)" :attempt="row.attempts" :retry-seconds="Math.ceil(retryCountdownMs(row, dbNow()) / 1000)" />
          <span class="opacity-70">{{ t('xray.attempt', { n: row.attempts, max: row.max_attempts }) }}</span>
          <span v-if="row.attempts > 1" class="text-neon-amber">{{ '🎲'.repeat(Math.min(row.attempts - 1, 6)) }}</span>
          <Countdown v-if="deriveTaskStatus(row) === 'retry_scheduled' && retryCountdownMs(row, dbNow()) > 0" :remaining-ms="retryCountdownMs(row, dbNow())" :total-ms="8000" />
          <span class="ml-auto opacity-50">{{ row.aggregate_type }}/{{ row.aggregate_id }} #{{ row.aggregate_sequence }}</span>
        </div>
        <div v-if="taskKind(row.task_type) === 'chain'" class="text-[10px] opacity-60">{{ t('xray.chain_task') }}</div>
        <div v-if="row.last_error" class="text-[11px] text-neon-amber">{{ t('xray.last_error') }}: {{ reason(row.last_error) }}</div>
        <JsonView :value="row.payload" />
      </li>
    </TransitionGroup>
  </XrayPanel>
</template>
