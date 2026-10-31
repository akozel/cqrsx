<script setup lang="ts">
import { computed, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import XrayPanel from './XrayPanel.vue'
import JsonView from './JsonView.vue'
import { useXrayStore } from '@/stores/xray'
import { useSessionStore } from '@/stores/session'
import { changedPaths } from '@/lib/diff'
import { isLedgerOf } from '@/lib/ledger'
import { useTraceStore } from '@/stores/trace'

const xray = useXrayStore()
const session = useSessionStore()
const trace = useTraceStore()
const { t } = useI18n()
const TABS = ['concert_halls', 'fan_wallets', 'projection_checkpoints'] as const
const tab = ref<(typeof TABS)[number]>('concert_halls')

/** A checkpoint's `_id` is `{ projection, aggregate_type }`: it reads as `projection/aggregate_type`. */
function docId(id: unknown): string {
  if (id && typeof id === 'object' && 'projection' in id && 'aggregate_type' in id)
    return `${(id as { projection: unknown }).projection}/${(id as { aggregate_type: unknown }).aggregate_type}`
  return String(id)
}

const collection = computed(() => xray.state.mongo[tab.value] ?? null)
const previousDocs = computed(() => {
  const map: Record<string, unknown> = {}
  for (const d of xray.previous.mongo[tab.value]?.docs ?? []) map[docId(d._id)] = d
  return map
})
/** "Current" documents come first and open expanded: the hall of the current concert, every ledger of the
 * current persona (one `fan_wallets` document per fan and concert, D-14), or every checkpoint (there are two). */
const isCurrent = (id: string) =>
  tab.value === 'concert_halls'
    ? id === session.concertId
    : tab.value === 'fan_wallets'
      ? isLedgerOf(id, session.persona)
      : true
const docs = computed(() => {
  const list = (collection.value?.docs ?? []).map((d) => ({ id: docId(d._id), doc: d }))
  list.sort((a, b) => {
    const ca = isCurrent(a.id)
    const cb = isCurrent(b.id)
    return ca === cb ? a.id.localeCompare(b.id) : ca ? -1 : 1
  })
  return list.map(({ id, doc }) => ({
    id,
    doc,
    changed: changedPaths(previousDocs.value[id], doc),
    current: isCurrent(id),
  }))
})
</script>

<template>
  <XrayPanel
    db="mongo"
    :title="t(`panels.${tab}.title`)"
    :count="collection?.total ?? 0"
    :explain="t(`panels.${tab}.explain`)"
    :query="collection?.query"
    :warning="xray.mongoDown ? t('xray.mongo_down') : undefined"
  >
    <div class="flex gap-2 text-xs mb-2">
      <button
        v-for="name in TABS"
        :key="name"
        type="button"
        class="px-2 py-0.5 rounded border border-border font-mono"
        :class="{ 'bg-neon-lime text-black': tab === name }"
        @click="tab = name"
      >
        {{ name }}
      </button>
    </div>
    <p v-if="docs.length === 0" class="text-xs opacity-60">
      {{ tab === 'projection_checkpoints' ? t('xray.no_checkpoints') : t('states.wiped') }}
    </p>
    <ul class="space-y-1">
      <li
        v-for="d in docs"
        :key="d.id"
        class="rounded border border-border/60 px-2 py-1"
        :class="{
          'border-neon-lime/60': d.current,
          'ring-1 ring-neon-violet': trace.aggregateId !== null && d.id === trace.aggregateId,
        }"
      >
        <JsonView :value="d.doc" :changed="d.changed" :expanded="d.current" />
      </li>
    </ul>
  </XrayPanel>
</template>
