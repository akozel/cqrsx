<script setup lang="ts">
import { useI18n } from 'vue-i18n'
import { setLocale } from '@/i18n'
import { useTourStore } from '@/stores/tour'

const { t, locale } = useI18n()
const tour = useTourStore()
</script>

<template>
  <header class="flex items-center gap-4 px-4 py-2 border-b border-border bg-panel">
    <span class="text-xl font-extrabold text-neon-pink tracking-wide">🎸 {{ t('app.title').toUpperCase() }}</span>
    <span class="text-sm opacity-70">· {{ t('app.subtitle') }}</span>
    <span class="ml-auto flex items-center gap-2 text-sm">
      <button
        v-for="code in ['ru', 'en'] as const"
        :key="code"
        type="button"
        class="px-2 py-0.5 rounded border border-border"
        :class="{ 'bg-neon-pink text-black': locale === code }"
        @click="setLocale(code)"
      >
        {{ code.toUpperCase() }}
      </button>
      <button type="button" class="ml-2 rounded-full border border-border w-7 h-7" :title="t('layout.help')" :aria-label="t('layout.help')" @click="tour.start()">ⓘ</button>
    </span>
  </header>
</template>
