<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import { useSessionStore } from '@/stores/session'
import { useConcertsStore } from '@/stores/concerts'

const { t } = useI18n()
const session = useSessionStore()
const concerts = useConcertsStore()

const ticketsOf = computed(() => {
  const seats = concerts.currentView?.seats ?? {}
  const counts: Record<string, number> = {}
  for (const entry of Object.values(seats)) if (entry.state === 'booked' && entry.fan_id) counts[entry.fan_id] = (counts[entry.fan_id] ?? 0) + 1
  return counts
})
</script>

<template>
  <div class="flex flex-wrap items-center gap-2">
    <span class="text-sm opacity-70">{{ t('personas.title') }}</span>
    <button
      v-for="p in session.config.personas"
      :key="p.fan_id"
      type="button"
      class="flex items-center gap-2 rounded-lg border px-2 py-1"
      :class="session.persona === p.fan_id ? 'border-neon-cyan bg-neon-cyan/10' : 'border-border hover:border-neon-cyan/60'"
      @click="session.persona = p.fan_id"
    >
      <span class="text-2xl">{{ p.avatar }}</span>
      <span class="text-left">
        <span class="block text-sm font-semibold">{{ p.name }}</span>
        <span class="block text-[10px] font-mono opacity-70">{{ t('personas.tickets', { n: ticketsOf[p.fan_id] ?? 0, limit: session.config.ticket_limit }) }}</span>
      </span>
    </button>
  </div>
</template>
