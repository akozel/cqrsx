<script setup lang="ts">
import { useI18n } from 'vue-i18n'
import QueryBadge from './QueryBadge.vue'
import Tooltip from '@/components/ui/Tooltip.vue'

defineProps<{ db: 'pg' | 'mongo' | 'mail' | 'log'; title: string; count?: number | string; explain: string; query?: string; updatedMs?: number | null; warning?: string }>()
const { t } = useI18n()
</script>

<template>
  <section class="flex flex-col rounded-lg border border-border bg-panel min-h-0 h-full">
    <header class="flex items-center gap-2 px-3 py-2 border-b border-border text-sm">
      <span class="font-semibold" :class="{ 'text-neon-cyan': db === 'pg', 'text-neon-lime': db === 'mongo', 'text-neon-amber': db === 'mail', 'text-neon-violet': db === 'log' }">{{ title }}</span>
      <span v-if="count !== undefined" class="font-mono text-xs opacity-70">· {{ count }}</span>
      <span v-if="warning" class="text-xs text-neon-red">{{ warning }}</span>
      <span class="ml-auto flex items-center gap-2">
        <span v-if="updatedMs != null" class="text-[10px] opacity-50 font-mono">{{ t('xray.updated', { ms: updatedMs }) }}</span>
        <QueryBadge v-if="query" :query="query" />
        <Tooltip :text="explain" />
      </span>
    </header>
    <div class="flex-1 overflow-auto p-2 min-h-0">
      <slot />
    </div>
  </section>
</template>
