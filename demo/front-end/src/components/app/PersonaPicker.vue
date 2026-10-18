<script setup lang="ts">
// One strip of people: the cast (a demo prop, from the sidecar's /inspect/demo) followed by the
// guests, every other fan holding a ticket for the current concert (the crowd). Any of them can be
// "you". The strip scrolls sideways (thin scrollbar, mouse wheel) when it is longer than the block.
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import { useSessionStore } from '@/stores/session'
import { useConcertsStore } from '@/stores/concerts'
import { avatarOf, nameOf } from '@/lib/personas'
import { wheelToHorizontal } from '@/lib/hscroll'

const { t } = useI18n()
const session = useSessionStore()
const concerts = useConcertsStore()

const ticketsOf = computed(() => {
  const seats = concerts.currentView?.seats ?? {}
  const counts: Record<string, number> = {}
  for (const entry of Object.values(seats))
    if (entry.state === 'booked' && entry.fan_id) counts[entry.fan_id] = (counts[entry.fan_id] ?? 0) + 1
  return counts
})

const people = computed(() => {
  const cast = session.demo.personas
  const known = new Set(cast.map((p) => p.fan_id))
  const guestIds = new Set(Object.keys(ticketsOf.value).filter((id) => !known.has(id)))
  if (!known.has(session.persona)) guestIds.add(session.persona) // the selected guest stays listed
  const guests = [...guestIds]
    .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }))
    .map((fan_id) => ({ fan_id, name: nameOf(fan_id, cast), avatar: avatarOf(fan_id, cast) }))
  return [...cast, ...guests]
})
</script>

<template>
  <div class="flex items-center gap-2 overflow-x-auto pb-1" @wheel="wheelToHorizontal">
    <button
      v-for="p in people"
      :key="p.fan_id"
      type="button"
      class="persona-chip flex shrink-0 items-center gap-1.5 rounded-lg border px-1.5 py-0.5"
      :class="
        session.persona === p.fan_id
          ? 'border-neon-cyan bg-neon-cyan/10'
          : 'border-border hover:border-neon-cyan/60'
      "
      @click="session.persona = p.fan_id"
    >
      <span class="text-lg">{{ p.avatar }}</span>
      <span class="text-left">
        <span class="block text-xs font-semibold whitespace-nowrap">{{ p.name }}</span>
        <span class="block text-[10px] font-mono opacity-70 whitespace-nowrap">{{
          t('personas.tickets', { n: ticketsOf[p.fan_id] ?? 0, limit: session.config.ticket_limit })
        }}</span>
      </span>
    </button>
  </div>
</template>
