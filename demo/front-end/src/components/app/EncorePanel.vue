<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import { useRebuildStore } from '@/stores/rebuild'
import { useXrayStore } from '@/stores/xray'
import { useConcertsStore } from '@/stores/concerts'
import ProgressBar from '@/components/ui/ProgressBar.vue'
import { ref } from 'vue'
import { getJson } from '@/api/http'

const { t } = useI18n()
const rebuild = useRebuildStore()
const xray = useXrayStore()
const concerts = useConcertsStore()

const running = computed(() => rebuild.status.state === 'running')
const total = computed(() => rebuild.total || xray.eventsTotal)
const result = computed(() => rebuild.status.last_result)
const verifyText = ref<string | null>(null)
const verifying = ref(false)

async function verify() {
  verifying.value = true
  verifyText.value = t('verify.running')
  try {
    const r = await getJson<{ ok: boolean; checked: Record<string, number>; differences: unknown[] }>('/api/projections/verify')
    verifyText.value = r.ok ? t('verify.ok', { halls: r.checked.concert_halls ?? 0, wallets: r.checked.fan_wallets ?? 0 }) : t('verify.diff', { n: r.differences.length })
  } catch (e) {
    verifyText.value = String(e)
  } finally {
    verifying.value = false
  }
}
</script>

<template>
  <div class="text-xs">
    <div class="flex flex-wrap items-center gap-2">
      <span class="font-semibold">{{ t('encore.title') }}</span>
      <button type="button" class="action-btn" :disabled="running || rebuild.busy" @click="rebuild.wipe()">{{ t('encore.wipe') }}</button>
      <button type="button" class="action-btn border-neon-pink" :disabled="running || rebuild.busy" @click="rebuild.start()">{{ t('encore.rebuild') }}</button>
      <label class="flex items-center gap-1 text-xs"><input v-model="rebuild.slow" type="checkbox" /> {{ t('encore.slow') }}</label>
      <button type="button" class="action-btn text-xs" :disabled="running || verifying" @click="verify">{{ t('encore.verify') }}</button>
    </div>
    <div v-if="verifyText" class="mt-1 text-xs">{{ verifyText }}</div>
    <div v-if="running" class="mt-2">
      <ProgressBar :value="rebuild.replayed" :max="total" :label="t('encore.running', { replayed: rebuild.replayed, total })" />
    </div>
    <div v-else-if="rebuild.lastError" class="mt-1 text-xs text-neon-red">{{ t('encore.failed', { message: rebuild.lastError }) }}</div>
    <div v-else-if="result" class="mt-1 text-xs" :class="result.ok ? 'text-neon-lime' : 'text-neon-red'">
      {{ result.ok ? t('encore.result', { replayed: result.replayed, aggregates: result.aggregates, ms: result.duration_ms }) : t('encore.failed', { message: result.error ?? '' }) }}
    </div>
    <div v-if="concerts.stale && !running" class="mt-1 text-xs opacity-70">{{ t('encore.wiped') }}</div>
  </div>
</template>
