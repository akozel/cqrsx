<script setup lang="ts">
import { computed, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import XrayPanel from './XrayPanel.vue'
import JsonView from './JsonView.vue'
import { useXrayStore } from '@/stores/xray'
import { useSessionStore } from '@/stores/session'
import { useTimeMachineStore } from '@/stores/timemachine'
import { msAgo } from '@/lib/format'
import { useTraceStore } from '@/stores/trace'

const xray = useXrayStore()
const session = useSessionStore()
const timeMachine = useTimeMachineStore()
const trace = useTraceStore()
const { t } = useI18n()
const onlyConcert = ref(false)
const onlyFan = ref(false)

const rows = computed(() => {
  const all = xray.state.events?.rows ?? []
  return all.filter((r) => (!onlyConcert.value || r.aggregate_id === session.concertId) && (!onlyFan.value || r.aggregate_id === session.persona))
})
const updated = computed(() => msAgo(xray.state.polled_at))
</script>

<template>
  <XrayPanel db="pg" :title="t('panels.events.title')" :count="xray.state.stats.events_total ?? xray.state.events?.total" :explain="t('panels.events.explain')" :query="xray.state.events?.query" :updated-ms="updated" :warning="xray.pgDown ? t('xray.pg_down') : undefined">
    <div class="flex gap-3 text-xs mb-2">
      <label class="flex items-center gap-1"><input v-model="onlyConcert" type="checkbox" /> {{ t('xray.only_concert') }}</label>
      <label class="flex items-center gap-1"><input v-model="onlyFan" type="checkbox" /> {{ t('xray.only_fan') }}</label>
    </div>
    <p v-if="rows.length === 0" class="text-xs opacity-60">{{ t('xray.no_rows') }}</p>
    <p v-else class="text-[10px] opacity-40 mb-1">{{ t('trace.hint') }}</p>
    <TransitionGroup name="row" tag="ul" class="space-y-1">
      <li
        v-for="row in rows"
        :key="xray.eventKey(row)"
        class="rounded border border-border/60 px-2 py-1 text-xs"
        :class="{
          'row-fresh': xray.freshEvents.has(xray.eventKey(row)),
          'ring-1 ring-neon-amber': timeMachine.active && row.aggregate_id === session.concertId && row.sequence === timeMachine.sequence,
          'ring-1 ring-neon-violet': trace.correlationId !== null && row.metadata.correlation_id === trace.correlationId,
        }"
        @mouseenter="trace.set(row.metadata.correlation_id, row.aggregate_id)"
        @mouseleave="trace.clear()"
      >
        <div class="flex items-center gap-2 font-mono">
          <span class="rounded px-1 text-[10px]" :class="row.aggregate_type === 'Concert' ? 'bg-cyan-500/20 text-neon-cyan' : 'bg-violet-500/20 text-neon-violet'">{{ row.aggregate_type }}</span>
          <span class="opacity-70">{{ row.aggregate_id }}</span>
          <span class="text-neon-pink">#{{ row.sequence }}</span>
          <span class="font-semibold">{{ row.event_type }}</span>
          <span v-if="row.sequence % session.config.snapshot_every === 0" :title="t('xray.snapshot_marker')">📸</span>
          <span class="ml-auto opacity-50">{{ row.metadata.recorded_at?.slice(11, 23) }}</span>
        </div>
        <div class="grid grid-cols-1 gap-1 mt-1">
          <JsonView :value="row.payload" />
          <JsonView :value="row.metadata" />
        </div>
      </li>
    </TransitionGroup>
  </XrayPanel>
</template>
