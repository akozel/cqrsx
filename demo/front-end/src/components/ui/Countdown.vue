<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{ remainingMs: number; totalMs: number; size?: number }>()
const r = 8
const circumference = 2 * Math.PI * r
const fraction = computed(() => (props.totalMs > 0 ? Math.min(1, Math.max(0, props.remainingMs / props.totalMs)) : 0))
const seconds = computed(() => Math.max(0, Math.ceil(props.remainingMs / 1000)))
</script>

<template>
  <span class="inline-flex items-center gap-1 font-mono text-xs text-neon-amber">
    <svg :width="size ?? 20" :height="size ?? 20" viewBox="0 0 20 20" aria-hidden="true">
      <circle cx="10" cy="10" :r="r" fill="none" stroke="#26264a" stroke-width="3" />
      <circle
        cx="10"
        cy="10"
        :r="r"
        fill="none"
        stroke="#fbbf24"
        stroke-width="3"
        stroke-linecap="round"
        :stroke-dasharray="circumference"
        :stroke-dashoffset="circumference * (1 - fraction)"
        transform="rotate(-90 10 10)"
        style="transition: stroke-dashoffset 0.4s linear"
      />
    </svg>
    {{ seconds }}s
  </span>
</template>
