<script setup lang="ts">
import { useI18n } from 'vue-i18n'
import { CHAPTERS, useTourStore } from '@/stores/tour'

const { t } = useI18n()
const tour = useTourStore()
</script>

<template>
  <nav class="flex flex-wrap items-center gap-1 text-xs" :aria-label="t('chapters.bar')">
    <button
      v-for="chapter in CHAPTERS"
      :key="chapter"
      type="button"
      class="rounded px-2 py-0.5 border border-transparent hover:border-border"
      :class="{ 'bg-neon-violet/30 border-neon-violet': tour.current === chapter }"
      @click="tour.current === chapter ? tour.close() : tour.open(chapter)"
    >
      {{ t(`chapters.${chapter}.title`) }}
    </button>
  </nav>
</template>
