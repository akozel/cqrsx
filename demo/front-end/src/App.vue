<script setup lang="ts">
import { onMounted, onUnmounted, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import confetti from 'canvas-confetti'
import AppHeader from '@/components/layout/AppHeader.vue'
import ChapterCallout from '@/components/layout/ChapterCallout.vue'
import TutorialDialog from '@/components/layout/TutorialDialog.vue'
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

let poll: ReturnType<typeof setInterval> | null = null

onMounted(async () => {
  await session.loadConfig()
  activity.connect()
  xray.connect()
  rebuild.startPolling()
  await Promise.all([concerts.refreshAll(), wallet.refresh(), timeMachine.refreshTimeline()])
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
  rebuild.stopPolling()
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
    <div v-if="concerts.backendDown" class="mx-4 mt-2 rounded border border-neon-red/60 px-3 py-1 text-xs text-neon-red">{{ t('xray.backend_down') }}</div>
    <main class="flex-1 flex flex-col gap-4 p-4 min-h-0">
      <!-- Top: the architecture diagram with the live events and outbox under it; the box office on the right -->
      <div class="grid grid-cols-1 xl:grid-cols-[minmax(0,2fr)_minmax(0,1fr)] gap-4">
        <div class="flex flex-col gap-4 min-w-0">
          <div class="h-[32rem] shrink-0"><PipelineStrip /></div>
          <!-- About twelve rows each; the rest scrolls inside the panel -->
          <div class="grid grid-cols-1 md:grid-cols-[minmax(0,1.15fr)_minmax(0,0.85fr)] gap-4 h-[25rem] shrink-0">
            <div class="h-full min-h-0" :class="dim('events')"><EventStorePanel /></div>
            <div class="h-full min-h-0" :class="dim('outbox')"><OutboxPanel /></div>
          </div>
          <div class="panel" :class="dim('wallet')"><WalletPanel /></div>
        </div>
        <!-- APP: the box office, one block per function -->
        <div class="flex flex-col gap-4 min-w-0">
          <div class="panel"><PersonaPicker /></div>
          <div class="panel"><ConcertTabs /></div>
          <div class="panel flex flex-col gap-4">
            <div :class="dim('seatmap')"><SeatMap /></div>
            <div :class="dim('timemachine')"><TimeMachine /></div>
            <div :class="dim('actions')"><ActionBar /></div>
          </div>
          <div class="panel" :class="dim('rulebreaker')"><RuleBreaker /></div>
          <div class="panel" :class="dim('encore')"><EncorePanel /></div>
        </div>
      </div>
      <!-- X-RAY: what the other databases hold -->
      <div class="grid grid-cols-1 md:grid-cols-2 2xl:grid-cols-4 gap-4 h-auto 2xl:h-[26rem]">
        <div class="h-[26rem] 2xl:h-full min-h-0" :class="dim('mongo')"><MongoPanel /></div>
        <div class="h-[26rem] 2xl:h-full min-h-0" :class="dim('inbox')"><MailboxPanel /></div>
        <div class="h-[26rem] 2xl:h-full min-h-0" :class="dim('activity')"><ActivityFeed /></div>
        <div class="h-[26rem] 2xl:h-full min-h-0"><SnapshotsPanel /></div>
      </div>
    </main>
    <ChapterCallout />
    <TutorialDialog />
    <Toasts />
  </div>
</template>
