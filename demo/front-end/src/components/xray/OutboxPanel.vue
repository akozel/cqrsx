<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import XrayPanel from './XrayPanel.vue'
import TaskStatusIcon from './TaskStatusIcon.vue'
import Countdown from '@/components/ui/Countdown.vue'
import { useXrayStore } from '@/stores/xray'
import { deriveTaskStatus, retryCountdownMs } from '@/lib/outboxStatus'
import { useTraceStore } from '@/stores/trace'

const xray = useXrayStore()
const trace = useTraceStore()
const { t, te } = useI18n()
const now = ref(Date.now())
let timer: ReturnType<typeof setInterval> | null = null
onMounted(() => (timer = setInterval(() => (now.value = Date.now()), 250)))
onUnmounted(() => timer && clearInterval(timer))

const rows = computed(() => xray.state.outbox?.rows ?? [])
/** Cards opened by a click: the raw payload + metadata. */
const opened = ref(new Set<string>())
function toggle(id: string) {
  const next = new Set(opened.value)
  if (!next.delete(id)) next.add(id)
  opened.value = next
}
/** Status counters in a fixed order; pending and processing are always shown, even at zero. */
const ALWAYS = ['pending', 'processing'] as const
const counters = computed(() => {
  const by = xray.state.stats.tasks_by_status ?? {}
  const order = [
    ...ALWAYS,
    ...Object.keys(by)
      .filter((k) => !(ALWAYS as readonly string[]).includes(k))
      .sort(),
  ]
  return order.map((status) => [status, by[status] ?? 0] as const)
})
const dbNow = () => now.value + xray.dbTimeOffset
/** The business meaning of a task format, falling back to the format itself. */
function taskLabel(taskType: string) {
  return te(`tasks.${taskType}`) ? t(`tasks.${taskType}`) : taskType
}
function reason(code: string | null) {
  if (!code) return ''
  return te(`chaos.${code}`) ? t(`chaos.${code}`) : code
}
</script>

<template>
  <XrayPanel
    db="pg"
    :title="t('panels.outbox.title')"
    :count="xray.state.outbox?.total"
    :explain="t('panels.outbox.explain')"
    :query="xray.state.outbox?.query"
    :warning="xray.pgDown ? t('xray.pg_down') : undefined"
  >
    <div class="xray-counters flex flex-wrap gap-2 text-[10px] font-mono mb-2 opacity-80">
      <span
        v-for="[status, n] in counters"
        :key="status"
        class="rounded border border-border px-1"
        :class="{ 'opacity-50': n === 0 }"
        >{{ status }}: {{ n }}</span
      >
    </div>
    <p v-if="rows.length === 0" class="text-xs opacity-60">{{ t('xray.no_rows') }}</p>
    <TransitionGroup name="row" tag="ul" class="space-y-1">
      <li
        v-for="row in rows"
        :key="row.id"
        class="xray-task grid cursor-pointer items-center gap-x-2 rounded border border-border/60 px-2 py-0.5 text-xs"
        :aria-expanded="opened.has(row.id)"
        :class="{
          'row-fresh': xray.freshTasks.has(row.id),
          'ring-1 ring-neon-violet':
            trace.correlationId !== null && row.metadata.correlation_id === trace.correlationId,
        }"
        @mouseenter="trace.set(String(row.metadata.correlation_id ?? ''), row.aggregate_id)"
        @mouseleave="trace.clear()"
        @click="toggle(row.id)"
      >
        <span class="flex min-w-0 items-center">
          <span
            class="mr-1 shrink-0 rounded px-1 font-mono text-[10px]"
            :class="
              row.aggregate_type === 'Concert'
                ? 'bg-cyan-500/20 text-neon-cyan'
                : 'bg-violet-500/20 text-neon-violet'
            "
            >{{ row.aggregate_type }}</span
          >
          <span class="mr-1.5 max-w-28 shrink truncate font-mono opacity-70">{{ row.aggregate_id }}</span>
          <span class="min-w-0 truncate font-semibold">{{ taskLabel(row.task_type) }}</span>
        </span>
        <span class="flex items-center gap-1 font-mono opacity-70">
          {{ row.attempts }}/{{ row.max_attempts }}
          <Countdown
            v-if="deriveTaskStatus(row) === 'retry_scheduled' && retryCountdownMs(row, dbNow()) > 0"
            :remaining-ms="retryCountdownMs(row, dbNow())"
            :total-ms="8000"
          />
        </span>
        <TaskStatusIcon
          class="justify-self-end"
          :row="row"
          :retry-seconds="Math.ceil(retryCountdownMs(row, dbNow()) / 1000)"
          :reason="row.last_error ? reason(row.last_error) : undefined"
          @click.stop
        />
        <pre v-if="opened.has(row.id)" class="xray-json col-span-3">{{
          JSON.stringify({ payload: row.payload, metadata: row.metadata }, null, 2)
        }}</pre>
      </li>
    </TransitionGroup>
  </XrayPanel>
</template>
