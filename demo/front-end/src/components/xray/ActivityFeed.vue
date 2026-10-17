<script setup lang="ts">
import { useI18n } from 'vue-i18n'
import XrayPanel from './XrayPanel.vue'
import { useActivityStore } from '@/stores/activity'
import { shortId } from '@/lib/format'
import type { ActivityEntry } from '@/types/activity'

const activity = useActivityStore()
const { t, te } = useI18n()

const ICONS: Record<string, string> = {
  'command.accepted': '✅',
  'command.rejected': '🛑',
  'command.failed': '⚠️',
  'projection.updated': '🍃',
  'projection.failed': '🍂',
  'task.attempt.started': '⚙',
  'task.attempt.failed': '🎲',
  'task.attempt.succeeded': '📬',
  'projections.wiped': '🧨',
  'rebuild.started': '⏪',
  'rebuild.progress': '⏪',
  'rebuild.finished': '🎉',
  'rebuild.failed': '❌',
}

function label(entry: ActivityEntry) {
  const base = te(`activity.${entry.type}`) ? t(`activity.${entry.type}`) : entry.type
  switch (entry.type) {
    case 'command.accepted':
    case 'command.rejected':
      return `${base}: ${entry.command_type} ${entry.aggregate_type}/${entry.aggregate_id}${entry.code ? ` · ${entry.code}` : ''}`
    case 'task.attempt.started':
    case 'task.attempt.failed':
    case 'task.attempt.succeeded':
      return `${base}: ${entry.format} #${entry.attempt}${entry.reason_code ? ` · ${entry.reason_code}` : ''} (${shortId(String(entry.task_id))})`
    case 'projection.updated':
    case 'projection.failed':
      return `${base}: ${entry.projection}/${entry.view_id}`
    case 'rebuild.progress':
      return `${base} ${entry.replayed} — ${entry.aggregate_type}/${entry.aggregate_id} #${entry.sequence}`
    case 'rebuild.finished':
      return `${base}: ${entry.replayed} → ${entry.aggregates} · ${entry.duration_ms} ms`
    default:
      return base
  }
}
</script>

<template>
  <XrayPanel db="log" :title="t('panels.activity.title')" :count="activity.connected ? t('common.connected') : t('common.disconnected')" :explain="t('panels.activity.explain')">
    <ul class="space-y-0.5 text-xs font-mono">
      <li v-for="entry in activity.entries" :key="entry.id" class="flex gap-2" :class="{ 'opacity-60': String(entry.source ?? '').startsWith('task:'), 'text-neon-red': entry.type.endsWith('.failed') || entry.type === 'command.rejected' }">
        <span class="opacity-50">{{ entry.at.slice(11, 23) }}</span>
        <span>{{ ICONS[entry.type] ?? '•' }}</span>
        <span class="break-all">{{ label(entry) }}</span>
      </li>
    </ul>
  </XrayPanel>
</template>
