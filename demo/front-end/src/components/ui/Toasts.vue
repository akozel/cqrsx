<script setup lang="ts">
import { useI18n } from 'vue-i18n'
import { useCommandsStore } from '@/stores/commands'

const commands = useCommandsStore()
const { t, te } = useI18n()

function text(toast: { code?: string; params?: Record<string, unknown>; text?: string }) {
  if (toast.code && te(`rules.${toast.code}`)) return t(`rules.${toast.code}`, toast.params ?? {})
  if (toast.code && te(`states.${toast.code}`)) return t(`states.${toast.code}`)
  return toast.text ?? toast.code ?? ''
}
</script>

<template>
  <div class="fixed bottom-4 right-4 z-50 flex flex-col gap-2 w-80" aria-live="polite">
    <TransitionGroup name="toast">
      <div
        v-for="toast in commands.toasts"
        :key="toast.id"
        class="rounded-lg border px-3 py-2 text-sm shadow-lg bg-panel cursor-pointer"
        :class="{
          'border-neon-lime': toast.kind === 'success',
          'border-neon-red': toast.kind === 'error',
          'border-border': toast.kind === 'info',
        }"
        @click="commands.dismiss(toast.id)"
      >
        <div v-if="toast.kind === 'error' && toast.code" class="font-mono text-xs opacity-60">{{ toast.code }}</div>
        <div>{{ toast.kind === 'success' ? t('toasts.accepted', { what: text(toast) }) : text(toast) }}</div>
      </div>
    </TransitionGroup>
  </div>
</template>

<style scoped>
.toast-enter-active,
.toast-leave-active {
  transition: all 0.25s ease;
}
.toast-enter-from,
.toast-leave-to {
  opacity: 0;
  transform: translateY(8px);
}
</style>
