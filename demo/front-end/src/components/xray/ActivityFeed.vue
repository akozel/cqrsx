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
  'projection.applied': '🍃',
  'projection.failed': '🍂',
  'projection.rebuilt': '♻️',
  'projection.lease_changed': '🔑',
  'task.attempt.started': '⚙️',
  'task.attempt.failed': '🎲',
  'task.attempt.succeeded': '📬',
  'rebuild.started': '⏪',
  'rebuild.progress': '⏪',
  'rebuild.finished': '🎉',
}

/** A sub-label of the feed, or the raw value when there is no text for it. */
function word(group: 'lease' | 'reason', value: unknown) {
  return te(`activity.${group}.${value}`) ? t(`activity.${group}.${value}`) : String(value)
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
    case 'projection.applied':
      return `${base}: ${entry.projection}/${entry.view_id} #${entry.sequence} (+${entry.events})`
    case 'projection.failed':
      return `${base}: ${entry.projection}/${entry.view_id ?? '—'} · ${entry.category} · ${t('activity.failures', { n: Number(entry.failures ?? 0) })}`
    case 'projection.rebuilt':
      return `${base}: ${entry.projection}/${entry.view_id} · ${t('activity.events', { n: Number(entry.events ?? 0) })} · ${word('reason', entry.reason)}`
    case 'projection.lease_changed':
      return `${base}: ${entry.projection} · ${word('lease', entry.lease)} · ${entry.owner}`
    case 'rebuild.started':
      return `${base}: ${t('activity.documents', { n: Number(entry.keys_total ?? 0) })} · ${t('activity.events', { n: Number(entry.events_total ?? 0) })} · pace_ms ${entry.pace_ms}`
    case 'rebuild.progress':
      return `${base}: ${t('activity.documents', { n: Number(entry.keys ?? 0) })}`
    case 'rebuild.finished':
      return `${base}: ${t('activity.documents', { n: Number(entry.keys ?? 0) })} · ${entry.duration_ms} ms`
    default:
      return base
  }
}
</script>

<template>
  <XrayPanel
    db="log"
    :title="t('panels.activity.title')"
    :count="activity.connected ? t('common.connected') : t('common.disconnected')"
    :explain="t('panels.activity.explain')"
  >
    <ul class="space-y-0.5 text-xs font-mono">
      <li
        v-for="entry in activity.entries"
        :key="entry.id"
        class="flex gap-2"
        :class="{
          'opacity-60': String(entry.source ?? '').startsWith('task:'),
          'text-neon-red': entry.type.endsWith('.failed') || entry.type === 'command.rejected',
          'text-neon-amber': entry.type === 'projection.lease_changed' && entry.lease === 'lost',
        }"
      >
        <span class="opacity-50">{{ entry.at.slice(11, 23) }}</span>
        <span>{{ ICONS[entry.type] ?? '•' }}</span>
        <span class="break-all">{{ label(entry) }}</span>
      </li>
    </ul>
  </XrayPanel>
</template>
