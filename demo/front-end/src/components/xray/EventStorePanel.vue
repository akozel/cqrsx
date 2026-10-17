<script setup lang="ts">
import { computed, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import XrayPanel from './XrayPanel.vue'
import { useXrayStore } from '@/stores/xray'
import { useSessionStore } from '@/stores/session'
import { useTimeMachineStore } from '@/stores/timemachine'
import { msAgo } from '@/lib/format'
import { useTraceStore } from '@/stores/trace'
import { summarize } from '@/lib/summary'
import type { EventRow } from '@/types/xray'

const xray = useXrayStore()
const session = useSessionStore()
const timeMachine = useTimeMachineStore()
const trace = useTraceStore()
const { t } = useI18n()
const onlyConcert = ref(false)
const onlyFan = ref(false)
/** Cards opened by a click: the raw payload + metadata. */
const opened = ref(new Set<string>())
function toggle(key: string) {
  const next = new Set(opened.value)
  if (!next.delete(key)) next.add(key)
  opened.value = next
}

const rows = computed(() => {
  const all = xray.state.events?.rows ?? []
  return all.filter(
    (r) =>
      (!onlyConcert.value || r.aggregate_id === session.concertId) &&
      (!onlyFan.value || r.aggregate_id === session.persona),
  )
})
const updated = computed(() => msAgo(xray.state.polled_at))
/** The payload chips: a Fan event already names the fan (its aggregate), so leave the fan out. */
const chips = (row: EventRow) =>
  summarize(row.payload, row.aggregate_type === 'Fan' ? { omit: ['fan_name', 'fan_id'] } : {})
</script>

<template>
  <XrayPanel
    db="pg"
    :title="t('panels.events.title')"
    :count="xray.state.stats.events_total ?? xray.state.events?.total"
    :explain="t('panels.events.explain')"
    :query="xray.state.events?.query"
    :updated-ms="updated"
    :warning="xray.pgDown ? t('xray.pg_down') : undefined"
  >
    <div class="flex gap-3 text-xs mb-2">
      <label class="flex items-center gap-1"
        ><input v-model="onlyConcert" type="checkbox" /> {{ t('xray.only_concert') }}</label
      >
      <label class="flex items-center gap-1"
        ><input v-model="onlyFan" type="checkbox" /> {{ t('xray.only_fan') }}</label
      >
    </div>
    <p v-if="rows.length === 0" class="text-xs opacity-60">{{ t('xray.no_rows') }}</p>
    <TransitionGroup name="row" tag="ul" class="space-y-1">
      <li
        v-for="row in rows"
        :key="xray.eventKey(row)"
        class="xray-event flex cursor-pointer flex-wrap items-center gap-x-1.5 rounded border border-border/60 px-2 py-0.5 text-xs"
        :aria-expanded="opened.has(xray.eventKey(row))"
        :class="{
          'row-fresh': xray.freshEvents.has(xray.eventKey(row)),
          'ring-1 ring-neon-amber':
            timeMachine.active &&
            row.aggregate_id === session.concertId &&
            row.sequence === timeMachine.sequence,
          'ring-1 ring-neon-violet':
            trace.correlationId !== null && row.metadata.correlation_id === trace.correlationId,
        }"
        @mouseenter="trace.set(row.metadata.correlation_id, row.aggregate_id)"
        @mouseleave="trace.clear()"
        @click="toggle(xray.eventKey(row))"
      >
        <span
          class="shrink-0 rounded px-1 text-center font-mono text-[10px]"
          :class="
            row.aggregate_type === 'Concert'
              ? 'bg-cyan-500/20 text-neon-cyan'
              : 'bg-violet-500/20 text-neon-violet'
          "
          >{{ row.aggregate_type }}</span
        >
        <span class="max-w-28 shrink truncate font-mono opacity-70">{{ row.aggregate_id }}</span>
        <span class="shrink-0 font-mono text-neon-pink">#{{ row.sequence }}</span>
        <span class="shrink-0 opacity-40">→</span>
        <span class="shrink-0 font-semibold">{{ row.event_type }}</span>
        <span class="shrink-0 opacity-40">→</span>
        <span class="flex min-w-0 flex-1 items-center gap-1 overflow-hidden">
          <span v-for="(chip, i) in chips(row)" :key="i" class="xray-chip">{{ chip }}</span>
        </span>
        <span class="w-5 shrink-0 text-center">
          <span v-if="row.sequence % session.config.snapshot_every === 0" :title="t('xray.snapshot_marker')"
            >📸</span
          >
        </span>
        <pre v-if="opened.has(xray.eventKey(row))" class="xray-json basis-full">{{
          JSON.stringify({ payload: row.payload, metadata: row.metadata }, null, 2)
        }}</pre>
      </li>
    </TransitionGroup>
  </XrayPanel>
</template>
