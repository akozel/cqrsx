<script setup lang="ts">
import { computed, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { useRebuildStore, type ReadSideError } from '@/stores/rebuild'
import { useConcertsStore } from '@/stores/concerts'
import { useSessionStore } from '@/stores/session'
import { useCommandsStore } from '@/stores/commands'
import { backend } from '@/api/backend'
import type { ProjectionState, ProjectionStatus } from '@/types/domain'
import ProgressBar from '@/components/ui/ProgressBar.vue'

const { t, te } = useI18n()
const rebuild = useRebuildStore()
const concerts = useConcertsStore()
const session = useSessionStore()
const commands = useCommandsStore()

const STATE_ICONS: Record<ProjectionState, string> = {
  live: '🟢',
  rebuilding: '⏪',
  starting: '⏳',
  unclaimed: '⚪',
  stalled: '⚠️',
}

const result = computed(() => rebuild.status.last_result)
const verifyText = ref<string | null>(null)
const verifying = ref(false)

/** A refusal with a known code reads as the rule it broke (`409 rebuild_in_progress`). */
function errorText(error: ReadSideError) {
  return error.code && te(`rules.${error.code}`) ? t(`rules.${error.code}`) : error.message
}

function lagText(p: ProjectionStatus) {
  if (p.lag === undefined) return ''
  return t(p.lag_capped ? 'encore.lag_capped' : 'encore.lag', { n: p.lag })
}

/** DEMO ONLY: asks once, then the sidecar empties both databases and announces the default concerts. */
async function resetDemo() {
  if (!window.confirm(t('encore.reset_confirm'))) return
  const ok = await rebuild.resetDemo()
  commands.pushToast(
    ok
      ? { kind: 'success', text: t('encore.reset_done') }
      : { kind: 'error', text: t('encore.failed', { message: rebuild.lastError ? errorText(rebuild.lastError) : '' }) },
  )
}

/** Verify is allowed at any time, during Encore! too (D-20): a document behind the ledger is no error. */
async function verify() {
  verifying.value = true
  verifyText.value = t('verify.running')
  try {
    const r = await backend.verify()
    const halls = r.checked.concert_halls ?? 0
    const wallets = r.checked.fan_wallets ?? 0
    if (!r.ok) verifyText.value = t('verify.diff', { n: r.differences.length })
    else if (r.behind.documents === 0) verifyText.value = t('verify.ok', { halls, wallets })
    else
      verifyText.value = t('verify.behind', {
        ok: halls + wallets - r.behind.documents,
        behind: r.behind.documents,
        events: r.behind.events,
      })
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
      <button
        type="button"
        class="action-btn"
        :disabled="rebuild.running || rebuild.busy"
        :title="t('encore.wipe_hint')"
        @click="rebuild.wipe()"
      >
        {{ t('encore.wipe') }}
      </button>
      <button
        type="button"
        class="action-btn border-neon-pink"
        :disabled="rebuild.running || rebuild.busy"
        :title="t('encore.rebuild_hint')"
        @click="rebuild.start()"
      >
        {{ t('encore.rebuild') }}
      </button>
      <label class="flex items-center gap-1 text-xs"
        ><input v-model="rebuild.slow" type="checkbox" />
        {{ t('encore.slow', { ms: session.config.rebuild_slow_pace_ms }) }}</label
      >
      <button type="button" class="action-btn text-xs" :disabled="verifying" @click="verify">
        {{ t('encore.verify') }}
      </button>
      <button
        v-if="session.demo.reset_enabled"
        type="button"
        class="action-btn ml-auto border-neon-red/60 text-xs hover:border-neon-red"
        :disabled="rebuild.running || rebuild.busy || rebuild.resetting"
        :title="t('encore.reset_hint')"
        @click="resetDemo"
      >
        {{ rebuild.resetting ? t('encore.resetting') : t('encore.reset') }}
      </button>
    </div>
    <!-- Where every read model stands against the ledger: state, lag, owner; a stall names its cause -->
    <ul class="mt-2 space-y-0.5 font-mono" data-test="projections">
      <li v-for="p in rebuild.status.projections" :key="p.projection" :data-state="p.state">
        <span>{{ STATE_ICONS[p.state] }}</span>
        {{ p.projection }}: {{ t(`encore.states.${p.state}`) }}<template v-if="p.lag !== undefined">
          · {{ lagText(p) }}</template
        ><template v-if="p.owner"> · {{ t('encore.owner', { owner: p.owner }) }}</template>
        <div v-if="p.state === 'stalled'" class="pl-5 text-neon-red">
          {{ t('encore.stalled_reason', { failures: p.failures ?? 0, error: p.last_error ?? '' }) }}
        </div>
      </li>
    </ul>
    <div v-if="rebuild.statusDown" class="mt-1 opacity-70">{{ t('encore.status_down') }}</div>
    <div v-else-if="rebuild.status.from_cache" class="mt-1 opacity-70">{{ t('encore.stale') }}</div>
    <div v-if="verifyText" class="mt-1 text-xs" data-test="verify">{{ verifyText }}</div>
    <div v-if="rebuild.running" class="mt-2">
      <ProgressBar
        :value="rebuild.progress.documents"
        :max="rebuild.progress.documentsTotal"
        :label="
          t('encore.running', {
            documents: rebuild.progress.documents,
            total: rebuild.progress.documentsTotal,
            events: rebuild.progress.events,
          })
        "
      />
    </div>
    <div v-else-if="rebuild.lastError" class="mt-1 text-xs text-neon-red">
      {{ t('encore.failed', { message: errorText(rebuild.lastError) }) }}
    </div>
    <div v-else-if="result" class="mt-1 text-xs text-neon-lime">
      {{ t('encore.result', { documents: result.keys, ms: result.duration_ms }) }}
    </div>
    <div v-if="concerts.stale" class="mt-1 text-xs opacity-70">{{ t('encore.wiped') }}</div>
  </div>
</template>
