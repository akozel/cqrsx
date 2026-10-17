// @vitest-environment jsdom
// Smoke tests: every component mounts and renders its i18n texts with empty/offline stores.
import { describe, expect, it, vi, beforeEach } from 'vitest'
import { mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { createI18n } from 'vue-i18n'
import en from '../i18n/en'
import ru from '../i18n/ru'
import App from '../App.vue'
import StatusChip from '../components/ui/StatusChip.vue'
import Countdown from '../components/ui/Countdown.vue'
import ProgressBar from '../components/ui/ProgressBar.vue'
import JsonView from '../components/xray/JsonView.vue'
import SeatButton from '../components/app/SeatButton.vue'
import OutboxPanel from '../components/xray/OutboxPanel.vue'
import EventStorePanel from '../components/xray/EventStorePanel.vue'
import { useXrayStore } from '../stores/xray'
import { useCommandsStore } from '../stores/commands'
import { useConcertsStore } from '../stores/concerts'
import type { XrayState } from '../types/xray'

class FakeEventSource {
  static CLOSED = 2
  readyState = 1
  onopen: (() => void) | null = null
  onerror: (() => void) | null = null
  addEventListener() {}
  close() {}
}

function i18n(locale: 'en' | 'ru' = 'en') {
  return createI18n({ legacy: false, locale, fallbackLocale: 'en', messages: { en, ru } })
}

beforeEach(() => {
  setActivePinia(createPinia())
  vi.stubGlobal('EventSource', FakeEventSource)
  vi.stubGlobal('fetch', vi.fn(async () => ({ ok: false, status: 503, json: async () => null })))
  vi.stubGlobal('matchMedia', () => ({ matches: false }))
})

describe('UI primitives', () => {
  it('status chip, countdown, progress bar and json view render', () => {
    const plugins = [i18n()]
    expect(mount(StatusChip, { props: { status: 'retry_scheduled', retrySeconds: 3 }, global: { plugins } }).text()).toContain('3 s')
    expect(mount(StatusChip, { props: { status: 'delivered' }, global: { plugins } }).text()).toBe('delivered')
    expect(mount(Countdown, { props: { remainingMs: 2500, totalMs: 8000 } }).text()).toContain('3s')
    expect(mount(ProgressBar, { props: { value: 5, max: 10, label: 'x' } }).text()).toContain('50 %')
    const json = mount(JsonView, { props: { value: { a: 1, b: { c: 2 } }, expanded: true, changed: ['b.c'] } })
    expect(json.findAll('.json-hot').length).toBe(1)
  })

  it('seat button carries an accessible label', () => {
    const w = mount(SeatButton, {
      props: { seat: { id: 'B4', row: 'B', number: 4, zone: 'standard', price: 80 }, state: 'free', pending: false, outcome: null, history: false },
      global: { plugins: [i18n('ru'), createPinia()] },
    })
    expect(w.attributes('aria-label')).toBe('B4, Standard, 80 €, свободно')
  })
})

describe('X-ray panels', () => {
  it('render sidecar rows and translate chaos reasons', () => {
    const pinia = createPinia()
    setActivePinia(pinia)
    const xray = useXrayStore()
    const state: XrayState = {
      events: { source: 'postgres', table: 'events', query: 'SELECT 1', total: 1, rows: [{ aggregate_type: 'Concert', aggregate_id: 'tbilisi-2026', sequence: 10, event_type: 'SeatBooked', event_version: '1.0', payload: { seat: 'B4' }, metadata: { recorded_at: '2026-10-17T09:12:03.000000Z' } }] },
      snapshots: null,
      outbox: { source: 'postgres', table: 'transactional_outbox', query: 'SELECT 2', total: 1, rows: [{ id: 'abc', aggregate_type: 'Fan', aggregate_id: 'alice', aggregate_sequence: 1, task_type: 'encore_send_e_ticket', task_version: 1, payload: {}, metadata: {}, status: 'pending', created_at: '', available_at: '2099-01-01T00:00:00Z', finished_at: null, attempts: 2, max_attempts: 6, last_error: 'server_on_fire', locked_by: null, lock_token: null, locked_until: null }] },
      mongo: {},
      stats: { events_total: 1, tasks_by_status: { pending: 1 } },
      db_time: '2026-10-17T09:12:03.000Z',
      polled_at: '2026-10-17T09:12:03.000Z',
      errors: [],
    }
    xray.state = state
    const plugins = [i18n(), pinia]
    const outbox = mount(OutboxPanel, { global: { plugins } })
    expect(outbox.text()).toContain('🔥 The mail server is on fire')
    expect(outbox.text()).toContain('attempt 2/6')
    const events = mount(EventStorePanel, { global: { plugins } })
    expect(events.text()).toContain('SeatBooked')
    expect(events.text()).toContain('📸')
  })
})

describe('App', () => {
  it('mounts with offline services and shows the banner in both languages', async () => {
    for (const locale of ['en', 'ru'] as const) {
      const pinia = createPinia()
      setActivePinia(pinia)
      const w = mount(App, { global: { plugins: [i18n(locale), pinia] } })
      await new Promise((r) => setTimeout(r, 0))
      expect(w.text()).toContain(locale === 'en' ? 'How to read this screen' : 'Как читать этот экран')
      expect(w.text()).toContain('STAGE'.toUpperCase() === 'STAGE' && locale === 'ru' ? 'СЦЕНА' : 'STAGE')
      expect(w.findAll('button.seat').length).toBe(40)
      w.unmount()
    }
  })

  it('toasts translate rule codes with parameters', async () => {
    const pinia = createPinia()
    setActivePinia(pinia)
    const commands = useCommandsStore()
    useConcertsStore()
    const w = mount(App, { global: { plugins: [i18n('ru'), pinia] } })
    commands.pushToast({ kind: 'error', code: 'seat_taken', params: { seat: 'B4', by: 'Alice' } })
    await new Promise((r) => setTimeout(r, 0))
    expect(w.text()).toContain('Место B4 уже занято (Alice)')
    w.unmount()
  })
})
