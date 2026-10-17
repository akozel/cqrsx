<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{ value: number; max: number; label?: string }>()
const percent = computed(() => (props.max > 0 ? Math.min(100, Math.round((props.value / props.max) * 100)) : 0))
</script>

<template>
  <div class="w-full">
    <div class="h-2 w-full rounded bg-border overflow-hidden">
      <div class="h-2 rounded bg-neon-pink transition-all duration-200" :style="{ width: percent + '%' }"></div>
    </div>
    <div v-if="label" class="text-xs opacity-70 mt-1 font-mono">{{ label }} · {{ percent }} %</div>
  </div>
</template>
