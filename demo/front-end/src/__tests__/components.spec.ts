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
import PersonaPicker from '../components/app/PersonaPicker.vue'
import ActionBar from '../components/app/ActionBar.vue'
import { useSessionStore } from '../stores/session'
import { avatarOf, nameOf, DEFAULT_PERSONAS } from '../lib/personas'
import OutboxPanel from '../components/xray/OutboxPanel.vue'
import EventStorePanel from '../components/xray/EventStorePanel.vue'
import { useXrayStore } from '../stores/xray'
import { useCommandsStore } from '../stores/commands'
import { useConcertsStore } from '../stores/concerts'
import { useTraceStore } from '../stores/trace'
import { useRebuildStore } from '../stores/rebuild'
import type { XrayState } from '../types/xray'
import { stubFlowGlobals } from './flowStubs'

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
  vi.stubGlobal(
    'fetch',
    vi.fn(async () => ({ ok: false, status: 503, json: async () => null })),
  )
  vi.stubGlobal('matchMedia', () => ({ matches: false }))
  stubFlowGlobals()
})

describe('UI primitives', () => {
  it('status chip, countdown, progress bar and json view render', () => {
    const plugins = [i18n()]
    expect(
      mount(StatusChip, {
        props: { status: 'retry_scheduled', retrySeconds: 3 },
        global: { plugins },
      }).text(),
    ).toContain('3 s')
    expect(mount(StatusChip, { props: { status: 'delivered' }, global: { plugins } }).text()).toBe(
      'delivered',
    )
    expect(mount(Countdown, { props: { remainingMs: 2500, totalMs: 8000 } }).text()).toContain('3s')
    expect(mount(ProgressBar, { props: { value: 5, max: 10, label: 'x' } }).text()).toContain('50 %')
    const json = mount(JsonView, {
      props: { value: { a: 1, b: { c: 2 } }, expanded: true, changed: ['b.c'] },
    })
    expect(json.findAll('.json-hot').length).toBe(1)
  })

  it('seat button carries an accessible label', () => {
    const w = mount(SeatButton, {
      props: {
        seat: { id: 'B4', row: 'B', number: 4, zone: 'standard', price: 80 },
        state: 'free',
        pending: false,
        outcome: null,
        history: false,
      },
      global: { plugins: [i18n('ru'), createPinia()] },
    })
    expect(w.attributes('aria-label')).toBe('B4, Standard, 80 €, свободно')
  })
})

describe('X-ray panels', () => {
  it('render sidecar rows and translate chaos reasons', async () => {
    const pinia = createPinia()
    setActivePinia(pinia)
    const xray = useXrayStore()
    const state: XrayState = {
      events: {
        source: 'postgres',
        table: 'events',
        query: 'SELECT 1',
        total: 1,
        rows: [
          {
            aggregate_type: 'Concert',
            aggregate_id: 'radiohead-2026-11-21',
            sequence: 10,
            event_type: 'SeatBooked',
            event_version: '1.0',
            payload: { seat: 'B4' },
            metadata: { recorded_at: '2026-10-17T09:12:03.000000Z' },
          },
        ],
      },
      snapshots: null,
      outbox: {
        source: 'postgres',
        table: 'transactional_outbox',
        query: 'SELECT 2',
        total: 1,
        rows: [
          {
            id: 'abc',
            aggregate_type: 'Fan',
            aggregate_id: 'fox:radiohead-2026-11-21',
            aggregate_sequence: 1,
            task_type: 'encore_send_e_ticket',
            task_version: 1,
            payload: {},
            metadata: { correlation_id: 'corr-0001-abcd' },
            status: 'pending',
            created_at: '',
            available_at: '2099-01-01T00:00:00Z',
            finished_at: null,
            attempts: 2,
            max_attempts: 6,
            last_error: 'server_on_fire',
            locked_by: null,
            lock_token: null,
            locked_until: null,
          },
        ],
      },
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
    expect(outbox.text()).toContain('2/6')
    // Counters: pending and processing are always listed, even at zero; the rest as reported.
    expect(outbox.findAll('.xray-counters span').map((c) => c.text())).toEqual([
      'pending: 1',
      'processing: 0',
    ])
    // The status icon: a die for a pending retry; its tooltip is a table with the row as the DB holds it.
    expect(outbox.find('.xray-task > span:nth-child(3)').text()).toContain('🎲')
    const tip = outbox.find('[role="tooltip"] table')
    const cells = tip.findAll('tr').map((tr) => tr.findAll('th, td').map((c) => c.text()))
    expect(cells).toEqual([
      ['status', 'pending'],
      ['attempts', '2/6'],
      ['next', expect.stringMatching(/^retry in \d+ s$/)],
      ['task', 'encore_send_e_ticket v1'],
      ['id', 'abc'],
      ['correlation', 'corr-000'],
      ['aggregate', 'Fan/fox:radiohead-2026-11-21 #1'],
      ['reason', '🔥 The mail server is on fire'],
    ])
    // Only the service tag and the business name on the row: no kind icon, no payload summary.
    expect(outbox.text()).toContain('E-mail the ticket to the fan')
    expect(outbox.find('.xray-task > span').text()).toBe('Fanfox:radiohead-2026-11-21E-mail the ticket to the fan') // tag, ledger id, action
    expect(outbox.findAll('.xray-task > span')).toHaveLength(3) // name, attempts, status
    await outbox.find('li').trigger('mouseenter')
    expect(useTraceStore().correlationId).toBe('corr-0001-abcd')
    expect(useTraceStore().aggregateId).toBe('fox:radiohead-2026-11-21')
    await outbox.find('li').trigger('mouseleave')
    expect(useTraceStore().correlationId).toBeNull()
    const events = mount(EventStorePanel, { global: { plugins } })
    expect(events.text()).toContain('SeatBooked')
    expect(events.text()).toContain('📸')
    expect(events.text()).toContain('B4')
    expect(events.text()).not.toContain('09:12:03') // the sequence orders events; no time on the card
    // The raw JSON opens on a click on the card and closes on the next one.
    expect(events.find('pre.xray-json').exists()).toBe(false)
    await events.find('li').trigger('click')
    expect(events.find('pre.xray-json').text()).toContain('"recorded_at"')
    await events.find('li').trigger('click')
    expect(events.find('pre.xray-json').exists()).toBe(false)
    expect(outbox.find('pre.xray-json').exists()).toBe(false)
    await outbox.find('li').trigger('click')
    expect(outbox.find('pre.xray-json').text()).toContain('"payload"')
  })
})

describe('crowd identities', () => {
  it('gives the sixty crowd fans sixty different avatars, none of them a cast avatar', () => {
    const ids = Array.from({ length: 60 }, (_, i) => `crowd-${i + 1}`)
    const avatars = ids.map((id) => avatarOf(id))
    expect(new Set(avatars).size).toBe(60)
    for (const p of DEFAULT_PERSONAS) expect(avatars).not.toContain(p.avatar)
    expect(avatarOf('crowd-7')).toBe(avatarOf('crowd-7')) // stable
    expect(nameOf('crowd-7')).toMatch(/ #7$/)
  })
})

describe('PersonaPicker', () => {
  it('lists the cast and, as avatars, every other fan with a ticket for the concert', async () => {
    const pinia = createPinia()
    setActivePinia(pinia)
    const concerts = useConcertsStore()
    const seat = (fan_id: string, fan_name: string) => ({
      state: 'booked' as const,
      fan_id,
      fan_name,
      zone: 'standard' as const,
      price: 80,
      sequence: 1,
      changed_at: '',
    })
    concerts.docs = {
      'radiohead-2026-11-21': {
        view: {
          concert: { artist: 'Radiohead', city: 'Minsk', date: '2026-11-21' },
          seats: {
            A1: seat('fox', 'Fox'),
            A2: seat('crowd-7', 'Mia #7'),
            A3: seat('crowd-12', 'Leo #12'),
            A4: seat('crowd-7', 'Mia #7'),
          },
          capacity: 40,
          sold: 4,
          revenue: 320,
          sold_out: false,
        },
      } as never,
    }
    const w = mount(PersonaPicker, { global: { plugins: [i18n('en'), pinia] } })
    expect(w.text()).toContain('Fox')
    expect(w.text()).toContain('Arbitrage Bot')
    const chips = w.findAll('.persona-chip')
    expect(chips).toHaveLength(6) // the cast of four, then the two crowd fans, in one strip
    expect(chips[4]!.text()).toContain('#7')
    expect(chips[4]!.text()).toContain('2/4 tickets')
    await chips[5]!.trigger('click')
    expect(useSessionStore().persona).toBe('crowd-12')
  })
})

describe('ActionBar race', () => {
  it("shows the result until the seat map changes again, ignoring the race's own booking", async () => {
    vi.useFakeTimers()
    try {
      const pinia = createPinia()
      setActivePinia(pinia)
      let calls = 0
      vi.stubGlobal(
        'fetch',
        vi.fn(async () => {
          calls++
          return calls === 1
            ? { ok: true, status: 200, json: async () => ({ outcome: 'accepted', tasks: [] }) }
            : {
                ok: false,
                status: 422,
                json: async () => ({ outcome: 'rejected', error: { code: 'seat_taken', params: {} } }),
              }
        }),
      )
      const concerts = useConcertsStore()
      const session = useSessionStore()
      const view = (seats: Record<string, unknown>) =>
        ({
          view: {
            concert: { artist: 'Radiohead', city: 'Minsk', date: '2026-11-21' },
            seats,
            capacity: 40,
            sold: 0,
            revenue: 0,
            sold_out: false,
          },
        }) as never
      concerts.docs = { [session.concertId]: view({}) }
      const w = mount(ActionBar, { global: { plugins: [i18n('en'), pinia] } })
      await w.findAll('button')[2]!.trigger('click') // ⚡ Race
      await vi.advanceTimersByTimeAsync(10)
      expect(w.text()).toContain('won')
      const seat = /(?:Fox|Panda) won/.test(w.text())
        ? (fetch as unknown as { mock: { calls: [string, RequestInit][] } }).mock.calls[0]![1].body
        : ''
      const raced = JSON.parse(String(seat)).seat as string
      const booked = {
        state: 'booked',
        fan_id: 'fox',
        fan_name: 'Fox',
        zone: 'standard',
        price: 80,
        sequence: 1,
        changed_at: '',
      }
      // The race's own booking arrives: the note stays.
      concerts.docs = { [session.concertId]: view({ [raced]: booked }) }
      await w.vm.$nextTick()
      expect(w.text()).toContain('won')
      // Any later change of the seat map closes it.
      concerts.docs = {
        [session.concertId]: view({ [raced]: booked, Z9: { ...booked, fan_id: 'panda', fan_name: 'Panda' } }),
      }
      await w.vm.$nextTick()
      expect(w.text()).not.toContain('won')
    } finally {
      vi.useRealTimers()
    }
  })
})

describe('demo operations (sidecar)', () => {
  it('a wipe asks the sidecar and empties the hall and the wallet locally', async () => {
    const pinia = createPinia()
    setActivePinia(pinia)
    const calls: string[] = []
    vi.stubGlobal(
      'fetch',
      vi.fn(async (url: string, init?: RequestInit) => {
        calls.push(`${init?.method ?? 'GET'} ${url}`)
        if (url === '/inspect/projections/wipe')
          return {
            ok: true,
            status: 200,
            json: async () => ({ outcome: 'wiped', dropped: ['encore_read.concert_halls', 'encore_read.fan_wallets'] }),
          }
        return { ok: false, status: 404, json: async () => null }
      }),
    )
    const concerts = useConcertsStore()
    const rebuild = useRebuildStore()
    await rebuild.wipe()
    expect(calls).toEqual(['POST /inspect/projections/wipe'])
    expect(concerts.stale).toBe(true)
    expect(rebuild.lastError).toBeNull()
  })

  it('the wallet reads the fan\'s e-mails from the mail service through the sidecar', async () => {
    const pinia = createPinia()
    setActivePinia(pinia)
    const calls: string[] = []
    vi.stubGlobal(
      'fetch',
      vi.fn(async (url: string, init?: RequestInit) => {
        calls.push(`${init?.method ?? 'GET'} ${url}`)
        const ok = (body: unknown) => ({ ok: true, status: 200, json: async () => body })
        if (url === '/api/fans/fox/wallet')
          return ok({
            fan_id: 'fox',
            ledgers: [{ concert_id: 'radiohead-2026-11-21', version: 1, last_sequence: 0 }],
            view: { fan_id: 'fox', fan_name: 'Fox', tickets: {}, active_count: 0, total_spent: 0, last_sequence: 0 },
          })
        if (url === '/inspect/mail/fox')
          return ok({
            source: 'mongodb',
            database: 'mail_service',
            collection: 'inbox',
            query: '',
            total: 1,
            docs: [{ _id: 'e_ticket:b-1', kind: 'e_ticket', to_fan_id: 'fox', delivered_on_attempt: 2, deliveries: 1 }],
          })
        return { ok: false, status: 404, json: async () => null }
      }),
    )
    useSessionStore().persona = 'fox' // another test may have left a crowd fan in localStorage
    const { useWalletStore } = await import('../stores/wallet')
    const wallet = useWalletStore()
    await wallet.refresh()
    expect(calls).toEqual(['GET /api/fans/fox/wallet', 'GET /inspect/mail/fox'])
    expect(wallet.inbox[0]?.inbox_id).toBe('e_ticket:b-1')
    expect(wallet.mailByBooking['b-1']?.delivered_on_attempt).toBe(2)
  })

  it('the cast comes from the sidecar and falls back to the built-in one', async () => {
    const pinia = createPinia()
    setActivePinia(pinia)
    vi.stubGlobal(
      'fetch',
      vi.fn(async (url: string) => {
        if (url === '/inspect/demo')
          return {
            ok: true,
            status: 200,
            json: async () => ({
              default_concerts: [],
              personas: [{ fan_id: 'owl', name: 'Owl', avatar: '🦉' }],
              reset_enabled: true,
            }),
          }
        return { ok: false, status: 503, json: async () => null }
      }),
    )
    const session = useSessionStore()
    expect(session.demo.personas).toEqual(DEFAULT_PERSONAS)
    await session.loadConfig()
    expect(session.demo.personas.map((p) => p.fan_id)).toEqual(['owl'])
    expect(session.demo.reset_enabled).toBe(true)
    session.persona = 'owl'
    expect(session.currentPersona().avatar).toBe('🦉')
  })

  it('a reset asks the sidecar, then selects the first default concert and refreshes the stores', async () => {
    const pinia = createPinia()
    setActivePinia(pinia)
    const calls: string[] = []
    vi.stubGlobal(
      'fetch',
      vi.fn(async (url: string, init?: RequestInit) => {
        calls.push(`${init?.method ?? 'GET'} ${url}`)
        const ok = (body: unknown) => ({ ok: true, status: 200, json: async () => body })
        if (url === '/inspect/reset') return ok({ outcome: 'reset', report: { announced: [] } })
        if (url === '/api/concerts') return ok([])
        if (url === '/api/projections/status') return ok({ state: 'idle', last_result: null })
        return { ok: false, status: 404, json: async () => null }
      }),
    )
    const session = useSessionStore()
    session.concertId = 'muse-2027-01-16'
    const concerts = useConcertsStore()
    concerts.stale = true
    const ok = await useRebuildStore().resetDemo()
    expect(ok).toBe(true)
    expect(calls[0]).toBe('POST /inspect/reset')
    expect(session.concertId).toBe('radiohead-2026-11-21')
    expect(concerts.stale).toBe(false)
    expect(calls).toContain('GET /api/concerts')
  })

  it('reports a refused reset without touching the selection', async () => {
    const pinia = createPinia()
    setActivePinia(pinia)
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => ({
        ok: false,
        status: 404,
        json: async () => ({ error: { source: 'demo', message: 'reset is disabled' } }),
      })),
    )
    const session = useSessionStore()
    session.concertId = 'muse-2027-01-16'
    const rebuild = useRebuildStore()
    expect(await rebuild.resetDemo()).toBe(false)
    expect(rebuild.lastError).toBe('reset is disabled')
    expect(session.concertId).toBe('muse-2027-01-16')
  })
})

describe('App', () => {
  it('mounts with offline services and opens the tutorial modal in both languages', async () => {
    for (const locale of ['en', 'ru'] as const) {
      const pinia = createPinia()
      setActivePinia(pinia)
      const w = mount(App, { global: { plugins: [i18n(locale), pinia] } })
      await new Promise((r) => setTimeout(r, 0))
      expect(w.text()).toContain(locale === 'en' ? 'Tutorial' : 'Туториал')
      expect(w.find('[role="dialog"][aria-modal="true"]').exists()).toBe(true)
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
    commands.pushToast({ kind: 'error', code: 'seat_taken', params: { seat: 'B4', by: 'Fox' } })
    await new Promise((r) => setTimeout(r, 0))
    expect(w.text()).toContain('Место B4 уже занято (Fox)')
    w.unmount()
  })
})
