<script setup lang="ts">
import { ref } from 'vue'

defineProps<{ text: string }>()
const open = ref(false)
</script>

<template>
  <span class="relative inline-block">
    <button
      type="button"
      class="rounded-full border border-border w-5 h-5 text-xs leading-none opacity-70 hover:opacity-100 hover:border-neon-cyan"
      :aria-expanded="open"
      @click="open = !open"
      @blur="open = false"
    >
      ⓘ
    </button>
    <Transition name="fade">
      <span
        v-if="open"
        class="absolute right-0 z-30 mt-1 w-72 rounded-lg border border-border bg-bg p-3 text-xs leading-relaxed shadow-xl text-left font-sans"
        role="tooltip"
      >
        {{ text }}
      </span>
    </Transition>
  </span>
</template>
