// The signal choreography of the architecture diagram, driven with fake timers and no rendering.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createChoreography, type Choreography, type Lane } from '../components/pipeline/choreography'
import { EDGES, GROUPS, NODES, EDGE_BY_ID } from '../components/pipeline/graph'

let c: Choreography
beforeEach(() => {
  vi.useFakeTimers()
  c = createChoreography()
})
afterEach(() => {
  c.dispose()
  vi.useRealTimers()
})

const edgeOf = (lane: Lane) => c.tokens[lane]?.edge ?? null
const glowing = (state: string) =>
  Object.entries(c.glow)
    .filter(([, g]) => g === state)
    .map(([n]) => n)

describe('graph', () => {
  it('describes the sketch: three services, fourteen components and actors, seventeen edges', () => {
    expect(GROUPS.map((g) => g.id)).toEqual(['svc_concert', 'svc_fan', 'svc_projection'])
    expect(NODES).toHaveLength(14)
    expect(EDGES).toHaveLength(17)
    const ids = new Set(NODES.map((n) => n.id))
    for (const e of EDGES) {
      expect(e.id).toBe(`${e.from}-${e.to}`)
      expect(ids.has(e.from) && ids.has(e.to)).toBe(true)
    }
    // Every child sits inside its service frame.
    for (const n of NODES.filter((n) => n.parent)) {
      const g = GROUPS.find((g) => g.id === n.parent)!
      expect(n.x >= g.x && n.y >= g.y && n.x + n.w <= g.x + g.w && n.y + n.h <= g.y + g.h).toBe(true)
    }
    expect(EDGE_BY_ID['mail_server-fan_store'].kind).toBe('retry')
  })
})

describe('choreography', () => {
  it('walks our own command user → UI → command → aggregate, then through the store back to the user', () => {
    c.onCommand({ phase: 'sent' })
    expect(edgeOf('cmd')).toBe('user-ui_in')
    expect(c.litEdges.has('user-ui_in')).toBe(true)
    vi.advanceTimersByTime(300)
    expect(edgeOf('cmd')).toBe('ui_in-command')
    expect(c.glow.ui_in).toBe('pulse')
    vi.advanceTimersByTime(300)
    expect(edgeOf('cmd')).toBe('command-aggregate')
    expect(c.glow.command).toBe('pulse')
    vi.advanceTimersByTime(300)
    expect(c.glow.aggregate).toBe('busy')

    vi.advanceTimersByTime(100) // the "sent" scene (750 ms) and its gap are over
    c.onCommand({ phase: 'accepted' })
    expect(c.glow.aggregate).toBe('ok')
    const seen: string[] = [edgeOf('cmd')!]
    for (let i = 0; i < 8; i++) {
      vi.advanceTimersByTime(300)
      seen.push(edgeOf('cmd') ?? '-')
    }
    expect(seen.slice(0, 4)).toEqual(['aggregate-store', 'store-read_db', 'read_db-ui', 'ui-user'])
    expect(c.transactionFlash.value).toBe(1)
    expect(seen.at(-1)).toBe('-')
    expect(glowing('ok')).toEqual([])
    expect(c.litEdges.size).toBe(0)
  })

  it('a rejected command stops at the aggregate: red glow, rule bubble, the arrow to the store is cut', () => {
    c.onCommand({ phase: 'rejected', code: 'seat_taken' })
    expect(c.tokens.cmd).toMatchObject({ edge: 'command-aggregate', color: 'red' })
    expect(c.glow.aggregate).toBe('error')
    expect(c.bubble.value).toEqual({ node: 'aggregate', code: 'seat_taken' })
    expect(c.cut.value).toBe('aggregate-store')
    vi.advanceTimersByTime(1600)
    expect(c.tokens.cmd).toBeNull()
    expect(c.glow.aggregate).toBe('idle')
    vi.advanceTimersByTime(1000)
    expect(c.bubble.value).toBeNull()
    expect(c.cut.value).toBeNull()
  })

  it('a failed command without a code shows the phase', () => {
    c.onCommand({ phase: 'failed' })
    expect(c.bubble.value?.code).toBe('failed')
  })

  it('a Fan command enters through the Concert outbox and the task handler; Concert ones are ignored', () => {
    c.onFanCommand({ type: 'command.accepted', entry: { aggregate_type: 'Concert' } })
    expect(c.tokens.fan).toBeNull()

    c.onFanCommand({ type: 'command.accepted', entry: { aggregate_type: 'Fan' } })
    const seen: string[] = [edgeOf('fan')!]
    for (let i = 0; i < 9; i++) {
      vi.advanceTimersByTime(i < 2 ? 250 : 300)
      seen.push(edgeOf('fan') ?? '-')
    }
    expect(seen.slice(0, 7)).toEqual([
      'store-handler_a',
      'handler_a-fan_command',
      'fan_command-fan_aggregate',
      'fan_aggregate-fan_store',
      'fan_store-read_db',
      'read_db-ui',
      'ui-user',
    ])
    expect(seen.at(-1)).toBe('-')
  })

  it('a rejected Fan command cuts the Fan store arrow and shows the code', () => {
    c.onFanCommand({ type: 'command.rejected', entry: { aggregate_type: 'Fan', code: 'unknown_ticket' } })
    vi.advanceTimersByTime(800)
    expect(c.bubble.value).toEqual({ node: 'fan_aggregate', code: 'unknown_ticket' })
    expect(c.cut.value).toBe('fan_aggregate-fan_store')
    expect(c.tokens.fan).toMatchObject({ edge: 'fan_command-fan_aggregate', color: 'red' })
  })

  it('mail attempts: the dice rolls, a failure goes back to the outbox, a success reaches the mailbox', () => {
    c.onTask({ type: 'task.attempt.started', entry: {} })
    expect(edgeOf('mail')).toBe('fan_store-handler_b')
    vi.advanceTimersByTime(250)
    expect(edgeOf('mail')).toBe('handler_b-mail_server')
    vi.advanceTimersByTime(300)
    expect(c.glow.mail_server).toBe('busy')
    expect(c.tokens.mail?.color).toBe('amber')

    c.onTask({ type: 'task.attempt.failed', entry: { reason_code: 'server_on_fire' } })
    vi.advanceTimersByTime(150) // queued behind the "started" scene (500 ms + gap)
    expect(c.diceRoll.value).toBe(1)
    expect(c.glow.mail_server).toBe('error')
    expect(c.bubble.value).toEqual({ node: 'mail_server', code: 'server_on_fire' })
    vi.advanceTimersByTime(300)
    expect(c.tokens.mail).toMatchObject({ edge: 'mail_server-fan_store', color: 'red' })
    expect(c.litEdges.has('mail_server-fan_store')).toBe(true)
    vi.advanceTimersByTime(1300)
    expect(c.tokens.mail).toBeNull()

    c.onTask({ type: 'task.attempt.started', entry: {} })
    vi.advanceTimersByTime(600)
    c.onTask({ type: 'task.attempt.succeeded', entry: {} })
    vi.advanceTimersByTime(200) // the gap after the "started" scene
    expect(c.diceRoll.value).toBe(2)
    expect(c.glow.mail_server).toBe('ok')
    expect(edgeOf('mail')).toBe('mail_server-email')
    vi.advanceTimersByTime(300)
    expect(c.glow.email).toBe('ok')
    vi.advanceTimersByTime(1000)
    expect(c.tokens.mail).toBeNull()
  })

  it('lanes are independent: a new command does not cancel the mail lane, and dispose stops everything', () => {
    c.onTask({ type: 'task.attempt.started', entry: {} })
    c.onCommand({ phase: 'sent' })
    c.onCommand({ phase: 'accepted' })
    vi.advanceTimersByTime(250)
    expect(edgeOf('mail')).toBe('handler_b-mail_server')
    c.dispose()
    vi.advanceTimersByTime(10000)
    expect(edgeOf('mail')).toBe('handler_b-mail_server') // frozen: no timer fired after dispose
    expect(c.glow.mail_server).toBe('idle')
  })

  it('a new scene on a lane wipes what the previous one lit; bubbles fade by their own clock', () => {
    c.onCommand({ phase: 'sent' })
    vi.advanceTimersByTime(1000) // the "sent" scene is over (500 ms + gap)
    expect(c.litEdges.has('command-aggregate')).toBe(true) // the busy hold outlives the scene
    c.onCommand({ phase: 'rejected', code: 'ticket_limit' })
    expect(c.litEdges.has('command-aggregate')).toBe(false)
    expect(c.glow.command).toBe('idle')
    vi.advanceTimersByTime(2000)
    c.onCommand({ phase: 'sent' }) // the next click
    expect(c.glow.aggregate).toBe('idle') // the error ring is gone with the lane
    expect(c.bubble.value?.code).toBe('ticket_limit') // but the rule stays readable
    vi.advanceTimersByTime(2500)
    expect(c.bubble.value).toBeNull()
    expect(c.cut.value).toBeNull()
  })

  it('plays scenes one after another: the Fan command waits for our own command to finish', () => {
    c.onCommand({ phase: 'sent' })
    vi.advanceTimersByTime(20)
    c.onCommand({ phase: 'accepted' }) // the server answers in 20 ms…
    c.onFanCommand({ type: 'command.accepted', entry: { aggregate_type: 'Fan' } }) // …and the chain follows at once
    expect(edgeOf('cmd')).toBe('user-ui_in') // the "sent" scene is still playing
    expect(c.tokens.fan).toBeNull()
    vi.advanceTimersByTime(950) // sent (750) + gap → accepted starts
    expect(edgeOf('cmd')).toBe('aggregate-store')
    expect(c.tokens.fan).toBeNull()
    vi.advanceTimersByTime(500) // the token reaches events + outbox: the fork
    expect(edgeOf('cmd')).toBe('store-read_db') // one branch goes on to the read model…
    expect(edgeOf('fan')).toBe('store-handler_a') // …the task chain leaves at the same moment
  })

  it('forks again at the Fan store: the mail attempt leaves with the wallet projection', () => {
    c.onFanCommand({ type: 'command.accepted', entry: { aggregate_type: 'Fan' } })
    c.onTask({ type: 'task.attempt.started', entry: {} })
    vi.advanceTimersByTime(1050) // into the Fan service (750) + aggregate → store (300)
    expect(edgeOf('fan')).toBe('fan_store-read_db')
    expect(edgeOf('mail')).toBe('fan_store-handler_b')
  })

  it('runs at the given speed: half speed doubles every delay and travel time', () => {
    c.dispose()
    c = createChoreography({ speed: () => 0.5 })
    c.onCommand({ phase: 'sent' })
    expect(c.tokens.cmd).toMatchObject({ edge: 'user-ui_in', ms: 500 })
    vi.advanceTimersByTime(300)
    expect(edgeOf('cmd')).toBe('user-ui_in') // not yet: 250 ms of animation is 500 ms of clock
    vi.advanceTimersByTime(300)
    expect(edgeOf('cmd')).toBe('ui_in-command')
  })

  it('skips scenes that would start more than four seconds behind reality (a crowd)', () => {
    for (let i = 0; i < 20; i++) c.onCommand({ phase: 'accepted' }) // 20 × 0.5 s (to the fork) queued at once
    vi.advanceTimersByTime(60_000)
    // Only the scenes starting within the lag window ran: about nine, the rest were dropped.
    expect(c.transactionFlash.value).toBeGreaterThanOrEqual(7)
    expect(c.transactionFlash.value).toBeLessThanOrEqual(10)
  })
})
