// @vitest-environment jsdom
// The architecture diagram: service frames, components, actors and edges on the Vue Flow canvas,
// and the DOM effects of the store signals (glow rings, bubbles, cut edges, tokens).
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, type VueWrapper } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { createI18n } from 'vue-i18n'
import en from '../i18n/en'
import ru from '../i18n/ru'
import PipelineStrip from '../components/pipeline/PipelineStrip.vue'
import { useCommandsStore } from '../stores/commands'
import { useActivityStore } from '../stores/activity'
import { useConcertsStore } from '../stores/concerts'
import { useRebuildStore } from '../stores/rebuild'
import { PACE_BASE, useSessionStore } from '../stores/session'
import { stubFlowGlobals } from './flowStubs'

function mountStrip(locale: 'en' | 'ru' = 'ru') {
  const pinia = createPinia()
  setActivePinia(pinia)
  const i18n = createI18n({ legacy: false, locale, fallbackLocale: 'en', messages: { en, ru } })
  useSessionStore().pace = 1 / PACE_BASE // the timings below are the choreography's own (effective speed 1)
  return mount(PipelineStrip, { global: { plugins: [i18n, pinia] }, attachTo: document.body })
}

const tokens = (w: VueWrapper) => w.findAll('g.pipe-token')
const glow = (w: VueWrapper, cls: string) => w.findAll(`.pipe-glow-${cls}`).length
const edge = (w: VueWrapper, id: string) => w.find(`.vue-flow__edge[data-id="${id}"] path.pipe-edge`)

let w: VueWrapper
beforeEach(() => {
  localStorage.clear()
  vi.useFakeTimers()
  vi.stubGlobal('matchMedia', () => ({ matches: false }))
  vi.stubGlobal(
    'fetch',
    vi.fn(async () => ({ ok: false, status: 503, json: async () => null })),
  )
  stubFlowGlobals()
})
afterEach(() => {
  w?.unmount()
  vi.useRealTimers()
})

describe('PipelineStrip', () => {
  it('draws the sketch: three service frames, the components, the actors and every edge, no captions', async () => {
    w = mountStrip('ru')
    await w.vm.$nextTick()
    const titles = w.findAll('.pipe-service-title').map((t) => t.text())
    expect(titles).toEqual(['Concert service', 'Fan service', 'projection'])
    expect(w.findAll('.pipe-node')).toHaveLength(10)
    expect(w.findAll('.pipe-actor')).toHaveLength(4) // three diamonds and the person
    expect(w.findAll('.pipe-person-shape')).toHaveLength(1)
    expect(w.findAll('.vue-flow__edge')).toHaveLength(17)
    expect(w.findAll('path.pipe-edge')).toHaveLength(17)
    for (const text of [
      'Command',
      'Aggregate (rules)',
      'Outbox worker → Fan',
      'Outbox worker → email',
      'events + outbox',
      'mail server',
      'read database',
      'Fox',
      'email',
      'UI',
    ])
      expect(w.text()).toContain(text)
    // Nothing but the names on the canvas: no sub-labels, no edge labels, no bubbles.
    expect(w.findAll('text.pipe-edge-label')).toHaveLength(0)
    expect(w.find('.pipe-bubble').exists()).toBe(false)
    expect(w.text()).not.toContain('R1')
    expect(edge(w, 'mail_server-fan_store').classes()).toContain('pipe-edge-retry')
    expect(tokens(w)).toHaveLength(0)
    expect(w.findAll('.pipe-pace').map((b) => b.text())).toEqual(['0.5×', '1×', '2×'])
    expect(w.find('header').text()).toContain('Путь команды')
    // Static canvas: nothing is draggable or selectable.
    expect(w.findAll('.vue-flow__node.draggable')).toHaveLength(0)
  })

  it('opens the explanation of a block on click, with the rules table for an aggregate', async () => {
    w = mountStrip('ru')
    await w.vm.$nextTick()
    expect(w.find('[role="dialog"]').exists()).toBe(false)
    await w.find('.vue-flow__node[data-id="aggregate"]').trigger('click')
    const dialog = w.find('[role="dialog"]')
    expect(dialog.exists()).toBe(true)
    expect(dialog.find('h2').text()).toBe('Aggregate Concert — правила')
    expect(dialog.findAll('table.pipe-rules tbody tr')).toHaveLength(8)
    expect(dialog.text()).toContain('R6 ticket_limit')
    expect(dialog.text()).toContain('меньше 4 билетов')
    await dialog.find('button[aria-label]').trigger('click')
    expect(w.find('[role="dialog"]').exists()).toBe(false)

    await w.find('.vue-flow__node[data-id="svc_projection"]').trigger('click')
    expect(w.find('[role="dialog"] h2').text()).toBe('projection')
    expect(w.findAll('[role="dialog"] table')).toHaveLength(0)
    expect(w.findAll('[role="dialog"] li').length).toBeGreaterThan(2)

    await w.find('.vue-flow__node[data-id="fan_aggregate"]').trigger('click')
    expect(w.findAll('table.pipe-rules tbody tr')).toHaveLength(3)
  })

  it('the speed buttons set the pace, and the pace stretches the animations', async () => {
    w = mountStrip('ru')
    await w.vm.$nextTick()
    await w.findAll('.pipe-pace')[0]!.trigger('click')
    expect(useSessionStore().pace).toBe(0.5)
    useCommandsStore().signal = { phase: 'sent', seq: 1 }
    await vi.advanceTimersByTimeAsync(300) // at 1× the token would already be on command → aggregate
    // 250 ms of animation at 0.5× of the base pace (1/1.33): 250 / (0.5 / 1.33) = 665 ms
    expect(w.find('g.pipe-token').attributes('style')).toContain('animation-duration: 665ms')
    expect(edge(w, 'ui_in-command').classes()).not.toContain('pipe-edge-lit')
    await vi.advanceTimersByTimeAsync(400)
    expect(edge(w, 'ui_in-command').classes()).toContain('pipe-edge-lit')
  })

  it('the user diamond wears the persona and its explanation switches personas', async () => {
    w = mountStrip('ru')
    await w.vm.$nextTick()
    const user = () => w.find('.vue-flow__node[data-id="user"]')
    expect(user().text()).toContain('🦊')
    expect(user().text()).toContain('Fox')
    await user().trigger('click')
    const chips = w.findAll('[role="dialog"] .pipe-persona')
    expect(chips.map((c) => c.text())).toEqual(['🦊Fox', '🐼Panda', '🦄Unicorn', '🤖Arbitrage Bot'])
    await chips[1]!.trigger('click')
    expect(useSessionStore().persona).toBe('panda')
    expect(user().text()).toContain('🐼')
    expect(user().text()).toContain('Panda')
  })

  it('walks our own command through the Concert service to the UI and hides the token', async () => {
    w = mountStrip()
    const commands = useCommandsStore()
    commands.signal = { phase: 'sent', seq: 1 }
    await w.vm.$nextTick()
    expect(tokens(w)).toHaveLength(1)
    expect(edge(w, 'user-ui_in').classes()).toContain('pipe-edge-lit')
    await vi.advanceTimersByTimeAsync(800)
    expect(glow(w, 'busy')).toBe(1)

    await vi.advanceTimersByTimeAsync(200) // the "sent" scene and its gap are over
    commands.signal = { phase: 'accepted', seq: 2 }
    await vi.advanceTimersByTimeAsync(700)
    expect(glow(w, 'ok')).toBeGreaterThan(0)
    expect(w.findAll('path.pipe-edge-lit').length).toBeGreaterThan(0)
    expect(w.find('.pipe-node.transaction-flash').exists()).toBe(true)
    await vi.advanceTimersByTimeAsync(3000)
    expect(tokens(w)).toHaveLength(0)
    expect(glow(w, 'ok')).toBe(0)
  })

  it('a rejected command shows the rule on the aggregate, cuts the arrow and says nothing was written', async () => {
    w = mountStrip()
    useCommandsStore().signal = { phase: 'rejected', code: 'seat_taken', seq: 1 }
    await w.vm.$nextTick()
    expect(w.find('.pipe-bubble').text()).toBe('seat_taken')
    expect(w.text()).toContain('ничего не записано')
    expect(edge(w, 'aggregate-store').classes()).toContain('pipe-edge-cut')
    expect(edge(w, 'aggregate-store').attributes('marker-end')).toBe('url(#pipe-arrow-cut)')
    expect(glow(w, 'error')).toBe(1)
    await vi.advanceTimersByTimeAsync(3000)
    expect(w.find('.pipe-bubble').exists()).toBe(false)
    expect(w.findAll('path.pipe-edge-cut')).toHaveLength(0)
    expect(tokens(w)).toHaveLength(0)
  })

  it('a Fan command from the activity log walks into the Fan service', async () => {
    w = mountStrip()
    const activity = useActivityStore()
    activity.commandSignal = {
      type: 'command.accepted',
      seq: 1,
      entry: { id: 1, at: '', type: 'command.accepted', aggregate_type: 'Concert' },
    }
    await w.vm.$nextTick()
    expect(tokens(w)).toHaveLength(0)

    activity.commandSignal = {
      type: 'command.accepted',
      seq: 2,
      entry: { id: 2, at: '', type: 'command.accepted', aggregate_type: 'Fan' },
    }
    await vi.advanceTimersByTimeAsync(50)
    expect(tokens(w)).toHaveLength(1)
    expect(edge(w, 'store-handler_a').classes()).toContain('pipe-edge-lit')
    await vi.advanceTimersByTimeAsync(1200)
    expect(glow(w, 'ok')).toBeGreaterThan(0)
    await vi.advanceTimersByTimeAsync(3000)
    expect(tokens(w)).toHaveLength(0)

    activity.commandSignal = {
      type: 'command.rejected',
      seq: 3,
      entry: { id: 3, at: '', type: 'command.rejected', aggregate_type: 'Fan', code: 'unknown_ticket' },
    }
    await vi.advanceTimersByTimeAsync(1000)
    expect(w.find('.pipe-bubble').text()).toBe('unknown_ticket')
    expect(edge(w, 'fan_aggregate-fan_store').classes()).toContain('pipe-edge-cut')
  })

  it('mail attempts roll the dice: a failure runs back along the retry edge, a success reaches the mailbox', async () => {
    w = mountStrip()
    const activity = useActivityStore()
    const entry = (type: string, extra: Record<string, unknown> = {}) => ({
      id: 1,
      at: '',
      type,
      format: 'encore_send_e_ticket',
      ...extra,
    })
    activity.taskSignal = { type: 'task.attempt.started', seq: 1, entry: entry('task.attempt.started') }
    await vi.advanceTimersByTimeAsync(600)
    expect(glow(w, 'busy')).toBe(1)
    expect(tokens(w)).toHaveLength(1)

    activity.taskSignal = {
      type: 'task.attempt.failed',
      seq: 2,
      entry: entry('task.attempt.failed', { reason_code: 'rate_limited' }),
    }
    await vi.advanceTimersByTimeAsync(150) // queued behind the "started" scene (500 ms + gap)
    expect(glow(w, 'error')).toBe(1)
    expect(w.find('.pipe-bubble').text()).toBe('rate_limited')
    expect(w.find('.pipe-node.pipe-glow-error').text()).toContain('🎲')
    await vi.advanceTimersByTimeAsync(400)
    expect(edge(w, 'mail_server-fan_store').classes()).toContain('pipe-edge-lit')
    await vi.advanceTimersByTimeAsync(1500)
    expect(tokens(w)).toHaveLength(0)

    activity.taskSignal = { type: 'task.attempt.started', seq: 3, entry: entry('task.attempt.started') }
    await vi.advanceTimersByTimeAsync(600)
    activity.taskSignal = { type: 'task.attempt.succeeded', seq: 4, entry: entry('task.attempt.succeeded') }
    await vi.advanceTimersByTimeAsync(450) // the gap after "started", then the trip to the mailbox
    expect(edge(w, 'mail_server-email').classes()).toContain('pipe-edge-lit')
    expect(w.find('.pipe-actor-shape.pipe-glow-ok').exists()).toBe(true)
    await vi.advanceTimersByTimeAsync(2000)
    expect(tokens(w)).toHaveLength(0)
  })

  it('dims the projection after a wipe and runs the Encore! dash on both events → read database edges', async () => {
    w = mountStrip()
    useConcertsStore().stale = true
    await w.vm.$nextTick()
    expect(w.findAll('.pipe-dim').length).toBe(2) // the frame and the read database
    expect(w.text()).toContain('(пусто)')

    useRebuildStore().status = {
      ...useRebuildStore().status,
      running: { rebuild_id: 'r-1', pace_ms: 0, started_at: '', keys_total: 3 },
    }
    await w.vm.$nextTick()
    expect(
      w.findAll('path.pipe-edge-flow').map((p) => p.element.parentElement?.getAttribute('data-id')),
    ).toEqual(['store-read_db', 'fan_store-read_db'])
    expect(w.findAll('text.pipe-edge-label').map((t) => t.text())).toEqual(['⏪ Encore!', '⏪ Encore!'])
  })

  it('renders the English labels too', async () => {
    w = mountStrip('en')
    await w.vm.$nextTick()
    expect(w.findAll('.pipe-service-title').map((t) => t.text())).toEqual([
      'Concert service',
      'Fan service',
      'projection',
    ])
    expect(w.text()).toContain('mail server')
    await w.find('.vue-flow__node[data-id="mail_server"]').trigger('click')
    expect(w.find('[role="dialog"] h2').text()).toBe('mail server 🎲')
    expect(w.find('[role="dialog"]').text()).toContain('20 %')
  })
})
