<script setup lang="ts">
// A component inside a service: its name and the live state from the choreography (glow ring,
// rule bubble, "nothing written", the transaction flash, the mail server's dice). Click → details.
import { computed, inject } from 'vue'
import { Handle, type NodeProps } from '@vue-flow/core'
import { useI18n } from 'vue-i18n'
import Dice from '@/components/ui/Dice.vue'
import { PIPELINE } from '../context'
import { SIDE_POSITION, type PipeNode, type Side } from '../graph'

const props = defineProps<NodeProps<PipeNode>>()
const { t } = useI18n()
const ctx = inject(PIPELINE)!
const SIDES: Side[] = ['top', 'right', 'bottom', 'left', 'bottom-left']

const node = computed(() => props.data)
const glow = computed(() => ctx.choreo.glow[node.value.id])
const bubble = computed(() =>
  ctx.choreo.bubble.value?.node === node.value.id ? ctx.choreo.bubble.value.code : null,
)
const isStore = computed(() => node.value.id === 'store' || node.value.id === 'fan_store')
const cutHere = computed(
  () =>
    (node.value.id === 'store' && ctx.choreo.cut.value === 'aggregate-store') ||
    (node.value.id === 'fan_store' && ctx.choreo.cut.value === 'fan_aggregate-fan_store'),
)
const dimmed = computed(() => node.value.id === 'read_db' && ctx.wiped.value)
const dice = computed(() => ctx.choreo.diceRoll.value)
const flash = computed(() => (node.value.id === 'store' ? ctx.choreo.transactionFlash.value : 0))
</script>

<template>
  <div
    :key="flash"
    class="pipe-node"
    :class="[
      `pipe-glow-${glow}`,
      {
        'pipe-store': isStore,
        'pipe-dim': dimmed,
        'animate-shake': glow === 'error',
        'transaction-flash': flash > 0,
      },
    ]"
  >
    <Handle
      v-for="side in SIDES"
      :id="side"
      :key="side"
      type="source"
      :position="SIDE_POSITION[side]"
      :style="side === 'bottom-left' ? { left: '25%' } : undefined"
    />
    <span class="pipe-label">
      {{ node.icon }} {{ t(`pipeline.${node.label}`) }}
      <Dice v-if="node.id === 'mail_server'" :roll="dice" />
    </span>
    <span v-if="cutHere" class="pipe-note text-neon-red">{{ t('pipeline.nothing_written') }}</span>
    <span v-else-if="dimmed" class="pipe-note">({{ t('pipeline.empty') }})</span>
    <span v-if="bubble" class="pipe-bubble">{{ bubble }}</span>
  </div>
</template>
