<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { useCommandsStore } from '@/stores/commands'
import { useActivityStore } from '@/stores/activity'
import { useRebuildStore } from '@/stores/rebuild'
import { useConcertsStore } from '@/stores/concerts'
import Dice from '@/components/ui/Dice.vue'

const { t } = useI18n()
const commands = useCommandsStore()
const activity = useActivityStore()
const rebuild = useRebuildStore()
const concerts = useConcertsStore()

type Node = 'command' | 'aggregate' | 'store' | 'projections' | 'screen' | 'worker' | 'mail' | 'fan_command'
const lit = ref<Record<Node, 'idle' | 'pulse' | 'ok' | 'error' | 'busy'>>({
  command: 'idle', aggregate: 'idle', store: 'idle', projections: 'idle', screen: 'idle', worker: 'idle', mail: 'idle', fan_command: 'idle',
})
const bubble = ref<string | null>(null)
const diceRoll = ref(0)
const transactionFlash = ref(0)
const timers: ReturnType<typeof setTimeout>[] = []

function flash(node: Node, state: 'pulse' | 'ok' | 'error' | 'busy', ms = 900) {
  lit.value = { ...lit.value, [node]: state }
  timers.push(setTimeout(() => (lit.value = { ...lit.value, [node]: 'idle' }), ms))
}

watch(
  () => commands.signal,
  (s) => {
    bubble.value = null
    if (s.phase === 'sent') {
      flash('command', 'pulse', 600)
      flash('aggregate', 'busy', 1200)
    } else if (s.phase === 'accepted') {
      flash('aggregate', 'ok')
      flash('store', 'ok', 1200)
      transactionFlash.value++
      timers.push(setTimeout(() => flash('projections', 'ok'), 300))
      timers.push(setTimeout(() => flash('screen', 'ok'), 600))
    } else if (s.phase === 'rejected') {
      flash('aggregate', 'error', 1500)
      bubble.value = s.code ?? 'rejected'
      timers.push(setTimeout(() => (bubble.value = null), 2500))
    } else {
      flash('aggregate', 'error', 1500)
      bubble.value = s.code ?? 'failed'
      timers.push(setTimeout(() => (bubble.value = null), 2500))
    }
  },
)

watch(
  () => activity.taskSignal,
  (s) => {
    if (!s) return
    const chain = String(s.entry.format ?? '').startsWith('encore_record_')
    if (s.type === 'task.attempt.started') {
      flash('store', 'pulse', 500)
      flash('worker', 'busy', 1500)
      if (chain) flash('fan_command', 'pulse', 1200)
    } else if (s.type === 'task.attempt.failed') {
      diceRoll.value++
      flash('worker', 'error', 1500)
    } else if (s.type === 'task.attempt.succeeded') {
      flash('worker', 'ok')
      flash('mail', 'ok', 1500)
    }
  },
)

const replaying = computed(() => rebuild.status.state === 'running')
const wiped = computed(() => concerts.stale)

const nodeClass = (node: Node) => ({
  'ring-2 ring-neon-cyan': lit.value[node] === 'pulse',
  'ring-2 ring-neon-lime': lit.value[node] === 'ok',
  'ring-2 ring-neon-red animate-shake': lit.value[node] === 'error',
  'ring-2 ring-neon-amber animate-pulse': lit.value[node] === 'busy',
  'opacity-40': node === 'projections' && wiped.value,
})
</script>

<template>
  <section class="px-4 py-2 border-b border-border bg-panel/60 text-xs select-none">
    <div class="flex flex-wrap items-center gap-2">
      <span class="pipe-node" :class="nodeClass('command')">📨 {{ t('pipeline.command') }}</span>
      <span class="pipe-arrow">─▶</span>
      <span class="relative pipe-node" :class="nodeClass('aggregate')">
        🛡 {{ t('pipeline.aggregate') }}
        <span v-if="bubble" class="absolute -top-6 left-0 whitespace-nowrap rounded bg-neon-red text-black px-2 py-0.5 font-mono text-[10px]">{{ bubble }}</span>
      </span>
      <span class="pipe-arrow">─▶</span>
      <span class="pipe-node flex flex-col gap-0.5 border-dashed" :class="[nodeClass('store'), { 'transaction-flash': transactionFlash > 0 }]" :key="transactionFlash">
        <span>🐘 {{ t('pipeline.events') }}</span>
        <span>🐘 {{ t('pipeline.outbox') }}</span>
        <span class="text-[10px] text-neon-pink">┗ {{ t('pipeline.transaction') }} ┛</span>
        <span v-if="commands.signal.phase === 'rejected'" class="text-[10px] opacity-60">{{ t('pipeline.nothing_written') }}</span>
      </span>
      <span class="pipe-arrow" :class="{ 'arrow-flow': replaying }">─▶<span v-if="replaying" class="ml-1 text-neon-pink">{{ t('pipeline.replaying') }}</span></span>
      <span class="pipe-node" :class="nodeClass('projections')">🍃 {{ t('pipeline.projections') }}<span v-if="wiped" class="ml-1 opacity-70">({{ t('pipeline.empty') }})</span></span>
      <span class="pipe-arrow">─▶</span>
      <span class="pipe-node" :class="nodeClass('screen')">🖥 {{ t('pipeline.screen') }}</span>
    </div>
    <div class="flex flex-wrap items-center gap-2 mt-1 pl-[14.5rem]">
      <span class="pipe-arrow">└─▶</span>
      <span class="pipe-node" :class="nodeClass('worker')">⚙ {{ t('pipeline.worker') }} <Dice :roll="diceRoll" /></span>
      <span class="pipe-arrow">─▶</span>
      <span class="pipe-node" :class="nodeClass('mail')">📬 {{ t('pipeline.mail') }}</span>
      <span class="pipe-arrow">·</span>
      <span class="pipe-arrow">⚙ ─▶</span>
      <span class="pipe-node" :class="nodeClass('fan_command')">📨 {{ t('pipeline.fan_command') }} ─▶ 🛡 Fan ─▶ ┃🐘 {{ t('pipeline.transaction') }}┃</span>
    </div>
  </section>
</template>
