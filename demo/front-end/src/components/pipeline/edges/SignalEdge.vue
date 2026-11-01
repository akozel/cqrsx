<script setup lang="ts">
// An edge a signal can travel: a straight or L-shaped arrow and the circle(s) in flight on it.
// The circle rides the edge path with a CSS motion path, so it always follows the arrow.
import { computed, inject } from 'vue'
import { getSmoothStepPath, type EdgeProps } from '@vue-flow/core'
import { useI18n } from 'vue-i18n'
import { PIPELINE } from '../context'
import type { Color, Lane } from '../choreography'
import type { PipeEdge, PipeEdgeId } from '../graph'

defineOptions({ inheritAttrs: false })
const props = defineProps<EdgeProps<PipeEdge>>()
const { t } = useI18n()
const ctx = inject(PIPELINE)!
const LANES: Lane[] = ['cmd', 'fan', 'mail']
const COLOR: Record<Color, string> = {
  cyan: 'var(--color-neon-cyan)',
  amber: 'var(--color-neon-amber)',
  lime: 'var(--color-neon-lime)',
  red: 'var(--color-neon-red)',
}
const REBUILD_EDGES: PipeEdgeId[] = ['store-read_db', 'fan_store-read_db']

const path = computed(() =>
  getSmoothStepPath({
    sourceX: props.sourceX,
    sourceY: props.sourceY,
    sourcePosition: props.sourcePosition,
    targetX: props.targetX,
    targetY: props.targetY,
    targetPosition: props.targetPosition,
    borderRadius: 10,
  }),
)
const d = computed(() => path.value[0])
const labelAt = computed(() => ({ x: path.value[1], y: path.value[2] - 8 }))
const id = computed(() => props.id as PipeEdgeId)

const state = computed<'idle' | 'lit' | 'cut' | 'flow'>(() => {
  if (ctx.choreo.cut.value === id.value) return 'cut'
  if (ctx.replaying.value && REBUILD_EDGES.includes(id.value)) return 'flow'
  if (ctx.choreo.litEdges.has(id.value)) return 'lit'
  return 'idle'
})
const marker = computed(
  () => `url(#pipe-arrow-${state.value === 'idle' && props.data.kind === 'retry' ? 'retry' : state.value})`,
)
const tokens = computed(() =>
  LANES.flatMap((lane) => {
    const token = ctx.choreo.tokens[lane]
    return token && token.edge === id.value ? [token] : []
  }),
)
</script>

<template>
  <path
    :d="d"
    class="pipe-edge"
    :class="[`pipe-edge-${state}`, { 'pipe-edge-retry': data.kind === 'retry' }]"
    :marker-end="marker"
  />
  <text v-if="state === 'flow'" :x="labelAt.x" :y="labelAt.y" class="pipe-edge-label" text-anchor="middle">
    {{ t('pipeline.replaying') }}
  </text>
  <g
    v-for="token in tokens"
    :key="token.seq"
    class="pipe-token"
    :class="{ 'pipe-token-still': ctx.reducedMotion.value }"
    :style="{ color: COLOR[token.color], offsetPath: `path('${d}')`, animationDuration: `${token.ms}ms` }"
  >
    <circle r="9" fill="currentColor" opacity="0.25" />
    <circle r="5" fill="currentColor" />
  </g>
</template>
