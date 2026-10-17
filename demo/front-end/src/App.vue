<script setup lang="ts">
import { onMounted, onUnmounted, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import confetti from 'canvas-confetti'
import AppHeader from '@/components/layout/AppHeader.vue'
import ChapterBar from '@/components/layout/ChapterBar.vue'
import ChapterCallout from '@/components/layout/ChapterCallout.vue'
import AnnounceDialog from '@/components/app/AnnounceDialog.vue'
import PipelineStrip from '@/components/pipeline/PipelineStrip.vue'
import ConcertTabs from '@/components/app/ConcertTabs.vue'
import PersonaPicker from '@/components/app/PersonaPicker.vue'
import SeatMap from '@/components/app/SeatMap.vue'
import TimeMachine from '@/components/app/TimeMachine.vue'
import ActionBar from '@/components/app/ActionBar.vue'
import RuleBreaker from '@/components/app/RuleBreaker.vue'
import WalletPanel from '@/components/app/WalletPanel.vue'
import EncorePanel from '@/components/app/EncorePanel.vue'
import EventStorePanel from '@/components/xray/EventStorePanel.vue'
import SnapshotsPanel from '@/components/xray/SnapshotsPanel.vue'
import OutboxPanel from '@/components/xray/OutboxPanel.vue'
import MongoPanel from '@/components/xray/MongoPanel.vue'
import MailboxPanel from '@/components/xray/MailboxPanel.vue'
import ActivityFeed from '@/components/xray/ActivityFeed.vue'
import Toasts from '@/components/ui/Toasts.vue'
import { useSessionStore } from '@/stores/session'
import { useConcertsStore } from '@/stores/concerts'
import { useWalletStore } from '@/stores/wallet'
import { useActivityStore } from '@/stores/activity'
import { useXrayStore } from '@/stores/xray'
import { useRebuildStore } from '@/stores/rebuild'
import { useTimeMachineStore } from '@/stores/timemachine'
import { CHAPTER_PANELS, useTourStore } from '@/stores/tour'

const { t } = useI18n()
const session = useSessionStore()
const concerts = useConcertsStore()
const wallet = useWalletStore()
const activity = useActivityStore()
const xray = useXrayStore()
const rebuild = useRebuildStore()
const timeMachine = useTimeMachineStore()
const tour = useTourStore()
const dim = (panel: string) => (tour.current && !CHAPTER_PANELS[tour.current].includes(panel) ? 'opacity-40 transition-opacity' : 'transition-opacity')

type XrayTab = 'events' | 'outbox' | 'mongo' | 'mail' | 'snapshots' | 'activity'
const xrayTab = ref<XrayTab>('events')
let poll: ReturnType<typeof setInterval> | null = null

onMounted(async () => {
  await session.loadConfig()
  activity.connect()
  xray.connect()
  await Promise.all([concerts.refreshAll(), wallet.refresh(), rebuild.refreshStatus(), timeMachine.refreshTimeline()])
  // Safety net: a refresh every 5 s even if every push channel is silent.
  poll = setInterval(() => {
    void concerts.refreshAll()
    void wallet.refresh()
    void timeMachine.refreshTimeline()
  }, 5000)
})
onUnmounted(() => {
  activity.disconnect()
  xray.disconnect()
  if (poll) clearInterval(poll)
})

watch(() => session.concertId, () => void Promise.all([concerts.refreshConcert(), timeMachine.refreshTimeline()]))
watch(() => session.persona, () => void wallet.refresh())
// The timeline follows the ledger: refresh it whenever the X-ray reports new events.
watch(() => xray.eventsTotal, () => void timeMachine.refreshTimeline())
watch(
  () => concerts.soldOutJustHappened,
  (id) => {
    if (!id) return
    if (!session.reducedMotion) confetti({ particleCount: 160, spread: 80, origin: { y: 0.4 }, colors: ['#ff3ea5', '#22d3ee', '#a3e635', '#fbbf24'] })
    concerts.ackSoldOut()
  },
)
</script>

<template>
  <div class="min-h-screen flex flex-col">
    <AppHeader />
    <div class="px-4 py-1 border-b border-border bg-panel/40"><ChapterBar /></div>
    <PipelineStrip />
    <div v-if="!session.bannerDismissed" class="mx-4 mt-3 rounded-lg border border-neon-cyan/50 bg-neon-cyan/5 px-4 py-3 text-sm flex gap-3">
      <div>
        <div class="font-semibold text-neon-cyan">{{ t('banner.title') }}</div>
        <div class="opacity-90">{{ t('banner.body') }}</div>
      </div>
      <button type="button" class="ml-auto self-start rounded border border-border px-2 py-0.5 text-xs" @click="session.bannerDismissed = true">✕ {{ t('banner.close') }}</button>
    </div>
    <div v-if="concerts.backendDown" class="mx-4 mt-2 rounded border border-neon-red/60 px-3 py-1 text-xs text-neon-red">{{ t('xray.backend_down') }}</div>
    <main class="flex-1 grid grid-cols-1 xl:grid-cols-[minmax(0,1fr)_minmax(0,1.1fr)] gap-4 p-4 min-h-0">
      <!-- APP: the box office -->
      <section class="flex flex-col gap-3 rounded-lg border border-border bg-panel p-4 min-w-0">
        <div class="flex flex-wrap items-start gap-2"><ConcertTabs /><AnnounceDialog /></div>
        <PersonaPicker />
        <div :class="dim('seatmap')"><SeatMap /></div>
        <div :class="dim('timemachine')"><TimeMachine /></div>
        <div :class="dim('actions')"><ActionBar /></div>
        <div :class="dim('rulebreaker')"><RuleBreaker /></div>
        <div :class="dim('wallet')"><WalletPanel /></div>
        <div :class="dim('encore')"><EncorePanel /></div>
      </section>
      <!-- X-RAY: what the databases hold -->
      <section class="min-w-0 flex flex-col gap-3">
        <div class="hidden 2xl:grid grid-cols-2 grid-rows-2 gap-3 h-[calc(100vh-11rem)] min-h-[40rem]">
          <div :class="dim('events')"><EventStorePanel /></div>
          <div :class="dim('outbox')"><OutboxPanel /></div>
          <div :class="dim('mongo')"><MongoPanel /></div>
          <div class="grid grid-rows-2 gap-3 min-h-0">
            <div :class="dim('inbox')"><MailboxPanel /></div>
            <div :class="dim('activity')"><ActivityFeed /></div>
          </div>
        </div>
        <div class="2xl:hidden flex flex-col gap-2 h-[70vh] min-h-[32rem]">
          <div class="flex flex-wrap gap-1 text-xs">
            <button v-for="tab in ['events', 'outbox', 'mongo', 'mail', 'snapshots', 'activity'] as const" :key="tab" type="button" class="px-2 py-0.5 rounded border border-border font-mono" :class="{ 'bg-neon-cyan text-black': xrayTab === tab }" @click="xrayTab = tab">{{ tab }}</button>
          </div>
          <div class="flex-1 min-h-0">
            <EventStorePanel v-if="xrayTab === 'events'" />
            <OutboxPanel v-else-if="xrayTab === 'outbox'" />
            <MongoPanel v-else-if="xrayTab === 'mongo'" />
            <MailboxPanel v-else-if="xrayTab === 'mail'" />
            <SnapshotsPanel v-else-if="xrayTab === 'snapshots'" />
            <ActivityFeed v-else />
          </div>
        </div>
        <div class="hidden 2xl:block h-56"><SnapshotsPanel /></div>
      </section>
    </main>
    <ChapterCallout />
    <Toasts />
  </div>
</template>
