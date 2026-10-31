<script setup lang="ts">
import { useI18n } from 'vue-i18n'
import { onKeyStroke } from '@vueuse/core'
import { useTourStore } from '@/stores/tour'

const { t } = useI18n()
const tour = useTourStore()

onKeyStroke('Escape', () => {
  if (tour.step === 'intro') tour.close()
})
</script>

<template>
  <Transition name="fade">
    <div v-if="tour.step === 'intro'" class="fixed inset-0 z-50 flex items-center justify-center bg-bg/70 p-4" @click.self="tour.close()">
      <div role="dialog" aria-modal="true" aria-labelledby="tutorial-title" class="w-[32rem] max-w-full rounded-xl border border-neon-cyan bg-panel p-5 shadow-2xl text-sm">
        <div class="flex items-start gap-2 mb-2">
          <h2 id="tutorial-title" class="font-semibold text-base text-neon-cyan">{{ t('banner.title') }}</h2>
          <button type="button" class="ml-auto text-xs opacity-70 hover:opacity-100" :aria-label="t('banner.close')" @click="tour.close()">✕</button>
        </div>
        <p class="leading-relaxed opacity-90">{{ t('banner.body') }}</p>
        <div class="mt-4 flex justify-end gap-2">
          <button type="button" class="rounded border border-border px-3 py-1 text-xs" @click="tour.close()">{{ t('banner.close') }}</button>
          <button type="button" class="action-btn text-xs" autofocus @click="tour.next()">{{ t('banner.start_tour') }}</button>
        </div>
      </div>
    </div>
  </Transition>
</template>
