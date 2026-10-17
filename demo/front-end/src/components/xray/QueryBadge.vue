<script setup lang="ts">
import { ref } from 'vue'
import { useI18n } from 'vue-i18n'

defineProps<{ query: string }>()
const { t } = useI18n()
const open = ref(false)
</script>

<template>
  <span class="relative">
    <button
      type="button"
      class="font-mono text-xs px-1 rounded border border-border opacity-70 hover:opacity-100"
      :title="t('xray.query')"
      @click="open = !open"
    >
      &lt;/&gt;
    </button>
    <Transition name="fade">
      <pre
        v-if="open"
        class="absolute right-0 z-30 mt-1 w-[28rem] max-w-[80vw] whitespace-pre-wrap break-all rounded-lg border border-border bg-bg p-3 text-xs font-mono shadow-xl"
        >{{ query }}</pre>
    </Transition>
  </span>
</template>
