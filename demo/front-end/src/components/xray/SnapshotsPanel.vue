<script setup lang="ts">
import { useI18n } from 'vue-i18n'
import XrayPanel from './XrayPanel.vue'
import JsonView from './JsonView.vue'
import { useXrayStore } from '@/stores/xray'

const xray = useXrayStore()
const { t } = useI18n()
</script>

<template>
  <XrayPanel
    db="pg"
    :title="t('panels.snapshots.title')"
    :count="xray.state.snapshots?.total"
    :explain="t('panels.snapshots.explain')"
    :query="xray.state.snapshots?.query"
  >
    <p class="text-xs opacity-60 mb-2">{{ t('xray.snapshots_hint') }}</p>
    <p v-if="!xray.state.snapshots?.rows.length" class="text-xs opacity-60">{{ t('xray.no_rows') }}</p>
    <ul class="space-y-1">
      <li
        v-for="row in xray.state.snapshots?.rows ?? []"
        :key="`${row.aggregate_type}/${row.aggregate_id}`"
        class="rounded border border-border/60 px-2 py-1 text-xs"
      >
        <div class="font-mono flex gap-2">
          <span class="opacity-70">{{ row.aggregate_type }}</span>
          <span>{{ row.aggregate_id }}</span>
          <span class="text-neon-pink">last_sequence {{ row.last_sequence }}</span>
          <span class="opacity-60">{{ t('xray.version') }} {{ row.current_snapshot }}</span>
        </div>
        <JsonView :value="row.payload" />
      </li>
    </ul>
  </XrayPanel>
</template>
