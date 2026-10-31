<script setup lang="ts">
// A hover (and keyboard-focus) tooltip: the default slot is the trigger, the `tip` slot the content.
defineProps<{ align?: 'left' | 'right' }>()
</script>

<template>
  <span class="relative inline-block group" tabindex="0">
    <slot />
    <span
      class="pointer-events-none absolute top-full z-30 mt-1 hidden w-max max-w-72 rounded-lg border border-border bg-bg px-2 py-1.5 text-left text-xs font-normal shadow-xl group-hover:block group-focus-within:block"
      :class="align === 'right' ? 'right-0' : 'left-0'"
      role="tooltip"
    >
      <slot name="tip" />
    </span>
  </span>
</template>
