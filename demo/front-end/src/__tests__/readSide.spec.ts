// @vitest-environment jsdom
// The read side in the UI: Encore! with pace_ms, progress from the feed, verify with `behind`, the
// command that waits for the feed, the projection status and the new feed events.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { createI18n } from 'vue-i18n'
import en from '../i18n/en'
import ru from '../i18n/ru'
import EncorePanel from '../components/app/EncorePanel.vue'
import ActivityFeed from '../components/xray/ActivityFeed.vue'
import MongoPanel from '../components/xray/MongoPanel.vue'
import { useRebuildStore } from '../stores/rebuild'
import { useCommandsStore } from '../stores/commands'
import { useConcertsStore } from '../stores/concerts'
import { useActivityStore } from '../stores/activity'
import { useXrayStore } from '../stores/xray'
import type { RebuildStatus } from '../types/domain'
import type { ActivityEntry } from '../types/activity'

const i18n = (locale: 'en' | 'ru' = 'en') =>
  createI18n({ legacy: false, locale, fallbackLocale: 'en', messages: { en, ru } })

const reply = (status: number, body: unknown) => ({ ok: status < 400, status, json: async () => body })

function status(over: Partial<RebuildStatus> = {}): RebuildStatus {
  return {
    state: 'live',
    stale: false,
    projections: [
      { projection: 'concert_halls', aggregate_type: 'Concert', state: 'live', lag: 0, lag_capped: false, owner: 'backend' },
      { projection: 'fan_wallets', aggregate_type: 'Fan', state: 'live', lag: 0, lag_capped: false, owner: 'backend' },
    ],
    rebuild: null,
    last_result: null,
    ...over,
  }
}

let calls: string[]
beforeEach(() => {
  setActivePinia(createPinia())
  calls = []
  vi.stubGlobal('matchMedia', () => ({ matches: false }))
})
afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
})

function stubFetch(handler: (url: string, init?: RequestInit) => unknown) {
  vi.stubGlobal(
    'fetch',
    vi.fn(async (url: string, init?: RequestInit) => {
      calls.push(`${init?.method ?? 'GET'} ${url}${init?.body ? ' ' + String(init.body) : ''}`)
      return handler(url, init) ?? reply(404, null)
    }),
  )
}

describe('Encore!', () => {
  it('sends the slow-motion pace from the config, or 0, and reports a refusal by its code', async () => {
    stubFetch((url) => {
      if (url === '/api/projections/rebuild') return reply(202, { outcome: 'started' })
      if (url === '/api/projections/status') return reply(200, status())
    })
    const rebuild = useRebuildStore()
    await rebuild.start()
    rebuild.slow = false
    await rebuild.start()
    expect(calls.filter((c) => c.startsWith('POST'))).toEqual([
      'POST /api/projections/rebuild {"pace_ms":80}',
      'POST /api/projections/rebuild {"pace_ms":0}',
    ])
    stubFetch((url) =>
      url === '/api/projections/rebuild'
        ? reply(409, { outcome: 'conflict', error: { code: 'rebuild_in_progress', message: 'x' } })
        : reply(200, status()),
    )
    await rebuild.start()
    expect(rebuild.lastError).toEqual({ code: 'rebuild_in_progress', message: 'x' })
  })

  it('counts the progress from the feed and ends on rebuild.finished, even if an old status says running', async () => {
    const running = { rebuild_id: 'r-1', pace_ms: 0, started_at: '', keys_total: 4 }
    stubFetch((url) => (url === '/api/projections/status' ? reply(200, status({ rebuild: running })) : reply(404, null)))
    const rebuild = useRebuildStore()
    rebuild.onActivity('rebuild.started', { rebuild_id: 'r-1', pace_ms: 0, keys_total: 4, events_total: 12 })
    expect(rebuild.running).toBe(true)
    rebuild.onRebuilt({ reason: 'requested', events: 3 })
    rebuild.onRebuilt({ reason: 'healed', events: 9 })
    expect(rebuild.progress).toEqual({ documents: 1, events: 3, documentsTotal: 4, eventsTotal: 12 })
    rebuild.onActivity('rebuild.progress', { keys: 3 })
    expect(rebuild.progress.documents).toBe(3)
    rebuild.onActivity('rebuild.finished', { rebuild_id: 'r-1', keys: 4, duration_ms: 50 })
    expect(rebuild.running).toBe(false)
    expect(rebuild.status.last_result?.keys).toBe(4)
    await rebuild.refreshStatus() // read before the owner closed the row: still lists the rebuild
    expect(rebuild.running).toBe(false)
  })

  it('keeps the last status when the read fails and marks it down', async () => {
    stubFetch(() => reply(503, null))
    const rebuild = useRebuildStore()
    await rebuild.refreshStatus()
    expect(rebuild.statusDown).toBe(true)
    stubFetch((url) => (url === '/api/projections/status' ? reply(200, status()) : undefined))
    await rebuild.refreshStatus()
    expect(rebuild.statusDown).toBe(false)
    expect(rebuild.status.projections).toHaveLength(2)
  })

  it('asks one hall to be rebuilt', async () => {
    stubFetch((url) => (url.endsWith('/rebuild') ? reply(202, { outcome: 'requested' }) : undefined))
    expect(await useRebuildStore().rebuildConcert('c 1')).toBeNull()
    expect(calls).toEqual(['POST /api/concerts/c%201/rebuild'])
    stubFetch(() => reply(503, { error: { code: 'unavailable', message: 'down' } }))
    expect(await useRebuildStore().rebuildConcert('c-1')).toEqual({ code: 'unavailable', message: 'down' })
  })
})

describe('commands wait for the feed', () => {
  it('an accepted command does not read the hall at once; a fallback refresh follows a second later', async () => {
    vi.useFakeTimers()
    stubFetch((url) => {
      if (url.endsWith('/commands')) return reply(200, { outcome: 'accepted' })
      if (url === '/api/concerts') return reply(200, [])
      return reply(404, null)
    })
    await useCommandsStore().send('c-1', { type: 'BookSeat', seat: 'A1', fan_id: 'fox', fan_name: 'Fox' })
    expect(calls.filter((c) => c === 'GET /api/concerts')).toHaveLength(0)
    await vi.advanceTimersByTimeAsync(1000)
    expect(calls.filter((c) => c === 'GET /api/concerts')).toHaveLength(1)
  })

  it('a rejected command reads the hall again, and the feed refreshes the hall once for a burst of updates', async () => {
    vi.useFakeTimers()
    stubFetch((url) => {
      if (url.endsWith('/commands')) return reply(422, { outcome: 'rejected', error: { code: 'seat_taken', params: {} } })
      if (url === '/api/concerts') return reply(200, [])
      return reply(404, null)
    })
    await useCommandsStore().send('c-1', { type: 'BookSeat', seat: 'A1', fan_id: 'fox', fan_name: 'Fox' })
    expect(calls.filter((c) => c === 'GET /api/concerts')).toHaveLength(1)
    const concerts = useConcertsStore()
    for (let i = 0; i < 5; i++) concerts.refreshDebounced()
    await vi.advanceTimersByTimeAsync(150)
    expect(calls.filter((c) => c === 'GET /api/concerts')).toHaveLength(2)
  })
})

describe('Encore panel', () => {
  it('shows every projection with its state, lag and owner, and a stalled one with its cause', async () => {
    stubFetch(() => reply(404, null))
    const rebuild = useRebuildStore()
    rebuild.status = status({
      state: 'stalled',
      projections: [
        { projection: 'concert_halls', aggregate_type: 'Concert', state: 'stalled', lag: 100000, lag_capped: true, owner: 'backend-2', failures: 3, last_error: 'connection' },
        { projection: 'fan_wallets', aggregate_type: 'Fan', state: 'starting' },
      ],
    })
    const w = mount(EncorePanel, { global: { plugins: [i18n()] } })
    const text = w.find('[data-test=projections]').text()
    expect(text).toContain('concert_halls: stalled')
    expect(text).toContain('events behind: ≥ 100000')
    expect(text).toContain('owner backend-2')
    expect(text).toContain('failed rounds in a row: 3 — connection')
    expect(text).toContain('fan_wallets: starting')
  })

  it('verify is allowed during Encore! and words correct, behind and wrong documents apart', async () => {
    const reports = [
      { ok: true, checked: { concert_halls: 3, fan_wallets: 5 }, behind: { documents: 0, events: 0 }, differences: [] },
      { ok: true, checked: { concert_halls: 3, fan_wallets: 5 }, behind: { documents: 2, events: 7 }, differences: [] },
      { ok: false, checked: {}, behind: { documents: 0, events: 0 }, differences: [{ collection: 'concert_halls', id: 'c', kind: 'differs' }] },
    ]
    stubFetch((url) => (url === '/api/projections/verify' ? reply(200, reports.shift()) : undefined))
    const rebuild = useRebuildStore()
    rebuild.status = status({ rebuild: { rebuild_id: 'r', pace_ms: 0, started_at: '', keys_total: 1 } })
    const w = mount(EncorePanel, { global: { plugins: [i18n()] } })
    const verify = w.findAll('button').find((b) => b.text().includes('Verify'))!
    expect(verify.attributes('disabled')).toBeUndefined()
    const texts: string[] = []
    for (let i = 0; i < 3; i++) {
      await verify.trigger('click')
      await vi.waitFor(() => expect(w.find('[data-test=verify]').text()).not.toContain('verifying'))
      texts.push(w.find('[data-test=verify]').text())
    }
    expect(texts[0]).toContain('every document matches the ledger (3 halls, 5 wallet ledgers)')
    expect(texts[1]).toContain('correct: 6; behind the ledger: 2 (by 7 events)')
    expect(texts[2]).toContain('1 differences')
  })

  it('both locales speak of the new states', () => {
    for (const locale of [en, ru]) {
      for (const state of ['live', 'rebuilding', 'starting', 'unclaimed', 'stalled'])
        expect(locale.encore.states[state as keyof typeof locale.encore.states]).toBeTruthy()
    }
  })
})

describe('feed and X-ray', () => {
  const entry = (type: string, fields: Record<string, unknown>, id: number): ActivityEntry => ({
    id,
    at: '2026-10-25T10:00:00.000Z',
    type,
    ...fields,
  })

  it('labels the projection and Encore! events with their fields', () => {
    stubFetch(() => undefined)
    const activity = useActivityStore()
    activity.entries = [
      entry('rebuild.finished', { keys: 4, duration_ms: 50 }, 6),
      entry('rebuild.started', { keys_total: 4, events_total: 12, pace_ms: 80 }, 5),
      entry('projection.lease', { projection: 'concert_halls', lease: 'lost', owner: 'backend' }, 4),
      entry('projection.rebuilt', { projection: 'concert_halls', view_id: 'c-1', events: 9, reason: 'healed' }, 3),
      entry('projection.failed', { projection: 'fan_wallets', view_id: 'f:c', category: 'connection', failures: 2 }, 2),
      entry('projection.updated', { projection: 'concert_halls', view_id: 'c-1', sequence: 7, events: 2 }, 1),
    ]
    const text = mount(ActivityFeed, { global: { plugins: [i18n()] } }).text()
    for (const part of [
      'projection.updated: concert_halls/c-1 #7 (+2)',
      'fan_wallets/f:c · connection · failures in a row: 2',
      'concert_halls/c-1 · events: 9 · healed a gap',
      'projection.lease: concert_halls · lost · backend',
      'documents: 4 · events: 12 · pace_ms 80',
      'rebuild.finished: documents: 4 · 50 ms',
    ])
      expect(text).toContain(part)
  })

  it('the checkpoints tab names a document by projection and aggregate type', () => {
    stubFetch(() => undefined)
    const xray = useXrayStore()
    xray.state = {
      ...xray.state,
      mongo: {
        projection_checkpoints: {
          source: 'mongodb',
          database: 'encore_read',
          collection: 'projection_checkpoints',
          query: '',
          total: 1,
          docs: [{ _id: { projection: 'concert_halls', aggregate_type: 'Concert' }, position: '12' }],
        },
      },
    }
    const w = mount(MongoPanel, { global: { plugins: [i18n()] } })
    const tab = w.findAll('button').find((b) => b.text() === 'projection_checkpoints')!
    return tab.trigger('click').then(() => {
      expect(w.text()).not.toContain('No checkpoints yet')
      expect(w.find('li').exists()).toBe(true)
      xray.state = { ...xray.state, mongo: {} }
      return w.vm.$nextTick().then(() => expect(w.text()).toContain('No checkpoints yet'))
    })
  })
})
