<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import XrayPanel from './XrayPanel.vue'
import { useXrayStore } from '@/stores/xray'
import { shortId } from '@/lib/format'

const xray = useXrayStore()
const { t } = useI18n()
const inbox = computed(() => xray.state.mongo.inbox ?? null)
const mails = computed(() => [...(inbox.value?.docs ?? [])].reverse())
</script>

<template>
  <XrayPanel db="mail" :title="t('panels.inbox.title')" :count="inbox?.total ?? 0" :explain="t('panels.inbox.explain')" :query="inbox?.query">
    <div class="text-[10px] mb-2 inline-block rounded border border-neon-amber/60 text-neon-amber px-1">{{ t('xray.not_projection') }}</div>
    <p v-if="mails.length === 0" class="text-xs opacity-60">{{ t('xray.no_rows') }}</p>
    <ul class="space-y-1">
      <li v-for="mail in mails" :key="String(mail._id)" class="rounded border border-border/60 px-2 py-1 text-xs">
        <div>{{ mail.subject }}</div>
        <div class="font-mono opacity-70 flex flex-wrap gap-2">
          <span>{{ t('xray.mail_to') }} {{ mail.to_name }} ({{ mail.to_fan_id }})</span>
          <span>{{ t('xray.mail_attempt', { n: mail.delivered_on_attempt }) }}</span>
          <span :class="{ 'text-neon-amber': Number(mail.deliveries) > 1 }">{{ t('xray.mail_deliveries', { n: mail.deliveries }) }}<template v-if="Number(mail.deliveries) > 1"> · {{ t('xray.duplicate_suppressed') }}</template></span>
          <span class="opacity-50">task {{ shortId(String(mail.task_id)) }}</span>
        </div>
      </li>
    </ul>
  </XrayPanel>
</template>
