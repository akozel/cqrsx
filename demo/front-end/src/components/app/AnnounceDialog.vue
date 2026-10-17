<script setup lang="ts">
import { computed, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { useCommandsStore } from '@/stores/commands'
import { useSessionStore } from '@/stores/session'

const { t } = useI18n()
const commands = useCommandsStore()
const session = useSessionStore()
const open = ref(false)
const ARTISTS = ['The Event Sourcerers', 'Eventually Consistent', 'The Idempotents', 'Outbox Orchestra', 'Snapshot Sisters']
const CITIES = ['Tbilisi', 'Berlin', 'Lisbon', 'Prague', 'Oslo', 'Porto']
const artist = ref(ARTISTS[3] ?? '')
const city = ref(CITIES[3] ?? '')
const date = ref('2027-03-14')
const suffix = ref(Math.random().toString(16).slice(2, 6))

/** concert_id = <city-slug>-<4 hex> */
const concertId = computed(() => `${city.value.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '') || 'concert'}-${suffix.value}`)

async function submit() {
  const r = await commands.send(concertId.value, { type: 'AnnounceConcert', artist: artist.value, city: city.value, date: date.value })
  if (r.outcome === 'accepted') {
    session.concertId = concertId.value
    open.value = false
    suffix.value = Math.random().toString(16).slice(2, 6)
  }
}
</script>

<template>
  <button type="button" class="action-btn text-xs" @click="open = true">{{ t('announce.button') }}</button>
  <div v-if="open" class="fixed inset-0 z-40 flex items-center justify-center bg-bg/70" @click.self="open = false">
    <form class="rounded-xl border border-border bg-panel p-4 w-80 text-sm space-y-2" @submit.prevent="submit">
      <div class="font-semibold">{{ t('announce.title') }}</div>
      <label class="block text-xs">{{ t('announce.artist') }}<input v-model="artist" list="artists" class="mt-0.5 w-full rounded border border-border bg-bg px-2 py-1" required /></label>
      <datalist id="artists"><option v-for="a in ARTISTS" :key="a" :value="a" /></datalist>
      <label class="block text-xs">{{ t('announce.city') }}<input v-model="city" list="cities" class="mt-0.5 w-full rounded border border-border bg-bg px-2 py-1" required /></label>
      <datalist id="cities"><option v-for="c in CITIES" :key="c" :value="c" /></datalist>
      <label class="block text-xs">{{ t('announce.date') }}<input v-model="date" type="date" class="mt-0.5 w-full rounded border border-border bg-bg px-2 py-1" required /></label>
      <div class="text-[10px] font-mono opacity-60">{{ t('announce.id_hint', { id: concertId }) }}</div>
      <div class="flex gap-2 justify-end">
        <button type="button" class="action-btn text-xs" @click="open = false">{{ t('common.close') }}</button>
        <button type="submit" class="action-btn text-xs border-neon-pink">{{ t('announce.submit') }}</button>
      </div>
    </form>
  </div>
</template>
