<script setup lang="ts">
import { useI18n } from 'vue-i18n'
import { setLocale } from './i18n'

const { t, locale } = useI18n()
</script>

<template>
  <div class="min-h-screen flex flex-col">
    <header class="flex items-center gap-4 px-4 py-2 border-b border-border bg-panel">
      <span class="text-xl font-extrabold text-neon-pink">🎸 {{ t('app.title') }}</span>
      <span class="text-sm opacity-70">{{ t('app.subtitle') }}</span>
      <span class="ml-auto flex gap-1 text-sm">
        <button
          v-for="code in ['ru', 'en'] as const"
          :key="code"
          class="px-2 py-0.5 rounded border border-border"
          :class="{ 'bg-neon-pink text-black': locale === code }"
          @click="setLocale(code)"
        >
          {{ code.toUpperCase() }}
        </button>
      </span>
    </header>
    <section class="px-4 py-2 border-b border-border text-sm opacity-80">{{ t('layout.pipeline') }} · {{ t('common.placeholder') }}</section>
    <main class="flex-1 grid grid-cols-1 xl:grid-cols-2 gap-4 p-4">
      <section class="rounded-lg border border-border bg-panel p-4">
        <h2 class="font-semibold text-neon-cyan">{{ t('layout.app') }}</h2>
        <p class="opacity-70">{{ t('common.placeholder') }}</p>
      </section>
      <section class="rounded-lg border border-border bg-panel p-4 font-mono text-sm">
        <h2 class="font-semibold text-neon-lime">{{ t('layout.xray') }}</h2>
        <p class="opacity-70">{{ t('common.placeholder') }}</p>
      </section>
    </main>
  </div>
</template>
