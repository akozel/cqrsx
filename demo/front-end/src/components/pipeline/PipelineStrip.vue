<script setup lang="ts">
// The architecture diagram block, half of the top row next to the events and outbox panels: service boundaries, the
// components inside them, the actors outside, and the signals travelling between them, drawn on a
// Vue Flow canvas. The canvas is static (no drag, pan or zoom) and scales to the strip's width.
// The layout is data (`graph.ts`); what glows and moves is the choreography (`choreography.ts`),
// fed from the stores here.
import { computed, onBeforeUnmount, onMounted, provide, ref, watch } from 'vue'
import { VueFlow, useVueFlow, type Edge, type Node, type NodeMouseEvent } from '@vue-flow/core'
import '@vue-flow/core/dist/style.css'
import { useI18n } from 'vue-i18n'
import { useCommandsStore } from '@/stores/commands'
import { useActivityStore } from '@/stores/activity'
import { useRebuildStore } from '@/stores/rebuild'
import { useConcertsStore } from '@/stores/concerts'
import { PACES, PACE_BASE, useSessionStore } from '@/stores/session'
import { createChoreography } from './choreography'
import { PIPELINE } from './context'
import { CANVAS, EDGES, GROUPS, GROUP_BY_ID, NODES, type PipeGroupId, type PipeNodeId } from './graph'
import PipelineExplain from './PipelineExplain.vue'
import ServiceNode from './nodes/ServiceNode.vue'
import BoxNode from './nodes/BoxNode.vue'
import ActorNode from './nodes/ActorNode.vue'
import SignalEdge from './edges/SignalEdge.vue'

const { t } = useI18n()
const commands = useCommandsStore()
const activity = useActivityStore()
const rebuild = useRebuildStore()
const concerts = useConcertsStore()
const session = useSessionStore()

const choreo = createChoreography({ speed: () => session.pace * PACE_BASE })
provide(PIPELINE, {
  choreo,
  wiped: computed(() => concerts.stale),
  replaying: computed(() => rebuild.running),
  reducedMotion: computed(() => session.reducedMotion),
  persona: computed(() => session.currentPersona()),
})

const still = { draggable: false, selectable: false, focusable: false, connectable: false } as const
const nodes: Node[] = [
  ...GROUPS.map<Node>((g) => ({
    id: g.id,
    type: 'service',
    position: { x: g.x, y: g.y },
    width: g.w,
    height: g.h,
    data: g,
    ...still,
  })),
  ...NODES.map<Node>((n) => {
    const parent = n.parent ? GROUP_BY_ID[n.parent] : null
    return {
      id: n.id,
      type: n.kind,
      parentNode: n.parent,
      position: { x: n.x - (parent?.x ?? 0), y: n.y - (parent?.y ?? 0) },
      width: n.w,
      height: n.h,
      data: n,
      ...still,
    }
  }),
]
const edges: Edge[] = EDGES.map<Edge>((e) => ({
  id: e.id,
  type: 'signal',
  source: e.from,
  target: e.to,
  sourceHandle: e.fromSide,
  targetHandle: e.toSide,
  data: e,
  selectable: false,
  focusable: false,
  updatable: false,
}))

const MARKERS: [string, string][] = [
  ['idle', 'var(--pipe-edge)'],
  ['retry', 'var(--pipe-edge-retry)'],
  ['lit', 'var(--color-neon-lime)'],
  ['flow', 'var(--color-neon-pink)'],
  ['cut', 'var(--color-neon-red)'],
]

// --- Signals from the stores -------------------------------------------------------------------
watch(
  () => commands.signal,
  (s) => choreo.onCommand(s),
)
watch(
  () => activity.commandSignal,
  (s) => s && choreo.onFanCommand(s),
)
watch(
  () => activity.taskSignal,
  (s) => s && choreo.onTask(s),
)

// --- Click a block → its explanation ------------------------------------------------------------
const explain = ref<PipeNodeId | PipeGroupId | null>(null)
const onNodeClick = (e: NodeMouseEvent) => (explain.value = e.node.id as PipeNodeId | PipeGroupId)

// --- Fit the canvas to the strip ---------------------------------------------------------------
// Fit the drawing's own bounds (not the nodes' bounding box) so the margins are symmetric; the
// canvas is authored at the page's font size, so never zoom past 1 (`max-zoom` on the component).
const FIT = { padding: 0.045 }
const { fitBounds, onNodesInitialized } = useVueFlow()
const fit = () => void fitBounds({ x: 0, y: 0, width: CANVAS.w, height: CANVAS.h }, FIT)
onNodesInitialized(fit)
const rootEl = ref<HTMLElement | null>(null)
let observer: ResizeObserver | null = null
onMounted(() => {
  if (typeof ResizeObserver !== 'undefined' && rootEl.value) {
    observer = new ResizeObserver(fit)
    observer.observe(rootEl.value)
  }
})
onBeforeUnmount(() => {
  observer?.disconnect()
  choreo.dispose()
})
</script>

<template>
  <section
    class="pipe-strip flex h-full min-h-0 flex-col rounded-lg border border-border bg-panel select-none"
  >
    <header class="flex items-center gap-2 border-b border-border px-3 py-2 text-sm">
      <span class="font-semibold text-neon-pink">{{ t('layout.pipeline') }}</span>
      <span class="ml-auto flex items-center gap-1 text-xs" role="group" :aria-label="t('pipeline.speed')">
        <span class="opacity-60" :title="t('pipeline.hint')">{{ t('pipeline.speed') }}</span>
        <button
          v-for="p in PACES"
          :key="p"
          type="button"
          class="pipe-pace rounded border px-1.5 py-0.5 font-mono"
          :class="
            session.pace === p
              ? 'border-neon-pink bg-neon-pink/10 text-neon-pink'
              : 'border-border opacity-70 hover:opacity-100'
          "
          :aria-pressed="session.pace === p"
          @click="session.pace = p"
        >
          {{ p }}×
        </button>
      </span>
    </header>
    <div ref="rootEl" class="pipe-canvas relative min-h-0 flex-1">
      <svg width="0" height="0" class="absolute" aria-hidden="true">
        <defs>
          <marker
            v-for="[id, color] in MARKERS"
            :id="`pipe-arrow-${id}`"
            :key="id"
            viewBox="0 0 8 8"
            refX="7"
            refY="4"
            markerWidth="7"
            markerHeight="7"
            orient="auto"
          >
            <path d="M0 0 L8 4 L0 8 z" :fill="color" />
          </marker>
        </defs>
      </svg>
      <VueFlow
        :nodes="nodes"
        :edges="edges"
        :nodes-draggable="false"
        :nodes-connectable="false"
        :nodes-focusable="false"
        :edges-focusable="false"
        :edges-updatable="false"
        :elements-selectable="false"
        :pan-on-drag="false"
        :pan-on-scroll="false"
        :zoom-on-scroll="false"
        :zoom-on-pinch="false"
        :zoom-on-double-click="false"
        :prevent-scrolling="false"
        :min-zoom="0.1"
        :max-zoom="1"
        :delete-key-code="null"
        :selection-key-code="null"
        :multi-selection-key-code="null"
        :zoom-activation-key-code="null"
        @node-click="onNodeClick"
      >
        <template #node-service="p"><ServiceNode v-bind="p" /></template>
        <template #node-box="p"><BoxNode v-bind="p" /></template>
        <template #node-actor="p"><ActorNode v-bind="p" /></template>
      <template #node-person="p"><ActorNode v-bind="p" /></template>
        <template #edge-signal="p"><SignalEdge v-bind="p" /></template>
      </VueFlow>
    </div>
    <PipelineExplain :id="explain" @close="explain = null" />
  </section>
</template>
