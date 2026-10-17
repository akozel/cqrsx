<script setup lang="ts">
import { computed, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import XrayPanel from './XrayPanel.vue'
import JsonView from './JsonView.vue'
import { useXrayStore } from '@/stores/xray'
import { useSessionStore } from '@/stores/session'
import { changedPaths } from '@/lib/diff'
import { useTraceStore } from '@/stores/trace'

const xray = useXrayStore()
const session = useSessionStore()
const trace = useTraceStore()
const { t } = useI18n()
const tab = ref<'concert_halls' | 'fan_wallets'>('concert_halls')

const collection = computed(() => xray.state.mongo[tab.value] ?? null)
const previousDocs = computed(() => {
  const map: Record<string, unknown> = {}
  for (const d of xray.previous.mongo[tab.value]?.docs ?? []) map[String(d._id)] = d
  return map
})
const docs = computed(() => {
  const list = [...(collection.value?.docs ?? [])]
  const currentId = tab.value === 'concert_halls' ? session.concertId : session.persona
  list.sort((a, b) => (a._id === currentId ? -1 : b._id === currentId ? 1 : String(a._id).localeCompare(String(b._id))))
  return list.map((d) => ({ id: String(d._id), doc: d, changed: changedPaths(previousDocs.value[String(d._id)], d), current: d._id === currentId }))
})
</script>

<template>
  <XrayPanel db="mongo" :title="t(`panels.${tab}.title`)" :count="collection?.total ?? 0" :explain="t(`panels.${tab}.explain`)" :query="collection?.query" :warning="xray.mongoDown ? t('xray.mongo_down') : undefined">
    <div class="flex gap-2 text-xs mb-2">
      <button v-for="name in ['concert_halls', 'fan_wallets'] as const" :key="name" type="button" class="px-2 py-0.5 rounded border border-border font-mono" :class="{ 'bg-neon-lime text-black': tab === name }" @click="tab = name">
        {{ name }}
      </button>
    </div>
    <p v-if="docs.length === 0" class="text-xs opacity-60">{{ t('states.wiped') }}</p>
    <ul class="space-y-1">
      <li v-for="d in docs" :key="d.id" class="rounded border border-border/60 px-2 py-1" :class="{ 'border-neon-lime/60': d.current, 'ring-1 ring-neon-violet': trace.aggregateId !== null && d.id === trace.aggregateId }">
        <JsonView :value="d.doc" :changed="d.changed" :expanded="d.current" />
      </li>
    </ul>
  </XrayPanel>
</template>
