<script setup lang="ts">
import { ref, watch } from 'vue'

const props = defineProps<{ roll: number; size?: string }>()
const rolling = ref(false)
watch(
  () => props.roll,
  () => {
    rolling.value = false
    requestAnimationFrame(() => (rolling.value = true))
    setTimeout(() => (rolling.value = false), 700)
  },
)
</script>

<template>
  <span class="inline-block" :class="{ 'dice-roll': rolling }" :style="{ fontSize: size ?? '1em' }">🎲</span>
</template>
