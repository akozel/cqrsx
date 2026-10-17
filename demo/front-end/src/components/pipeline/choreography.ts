// The signal choreography of the architecture diagram: which node glows and which edge carries a
// token when a command, a Fan command from a task, or a mail attempt is reported. Pure state and
// timers, no rendering: the strip feeds store signals in and the nodes/edges read the state.
// Signals become scenes that play one at a time (see the scene queue below).
import { reactive, ref } from 'vue'
import { EDGE_BY_ID, NODE_IDS, type PipeEdgeId, type PipeNodeId } from './graph'

export type Glow = 'idle' | 'pulse' | 'ok' | 'error' | 'busy'
export type Color = 'cyan' | 'amber' | 'lime' | 'red'
/** One request in flight per lane: our own command, the task chain into Fan, the mail attempt. */
export type Lane = 'cmd' | 'fan' | 'mail'
export type CutEdge = 'aggregate-store' | 'fan_aggregate-fan_store'

export interface Token {
  edge: PipeEdgeId
  color: Color
  /** Travel time along the edge; the token rests at the target afterwards. */
  ms: number
  /** Changes on every move so the edge restarts the travel animation. */
  seq: number
}

export interface CommandSignal {
  phase: 'sent' | 'accepted' | 'rejected' | 'failed'
  code?: string
}
export interface ActivitySignal {
  type: string
  entry: Record<string, unknown>
}

export const STEP_MS = 300

export interface ChoreographyOptions {
  /** Animation speed factor read on every timer: 2 plays twice as fast, 0.5 twice as slow. */
  speed?: () => number
}

export function createChoreography(options: ChoreographyOptions = {}) {
  /** Milliseconds of animation → milliseconds of wall clock at the current speed. */
  const clock = (ms: number) => ms / (options.speed?.() || 1)
  const glow = reactive(Object.fromEntries(NODE_IDS.map((n) => [n, 'idle']))) as Record<PipeNodeId, Glow>
  const tokens = reactive<Record<Lane, Token | null>>({ cmd: null, fan: null, mail: null })
  const litEdges = reactive(new Set<PipeEdgeId>())
  const cut = ref<CutEdge | null>(null)
  const bubble = ref<{ node: PipeNodeId; code: string } | null>(null)
  const diceRoll = ref(0)
  const transactionFlash = ref(0)

  // Every lane owns its timers, the edges it lit and the nodes it lit, so a new signal on the lane
  // can wipe the previous run completely. Bubbles and cuts outlive the lane: they always fade by
  // their own clock (`sticky`), otherwise a quick next signal would leave them on screen for good.
  const timers: Record<Lane, ReturnType<typeof setTimeout>[]> = { cmd: [], fan: [], mail: [] }
  const litBy: Record<Lane, Set<PipeEdgeId>> = { cmd: new Set(), fan: new Set(), mail: new Set() }
  const glowBy: Record<Lane, Set<PipeNodeId>> = { cmd: new Set(), fan: new Set(), mail: new Set() }
  const sticky: ReturnType<typeof setTimeout>[] = []
  let seq = 0

  function cancel(lane: Lane) {
    timers[lane].forEach(clearTimeout)
    timers[lane] = []
    litBy[lane].forEach((edge) => litEdges.delete(edge))
    litBy[lane].clear()
    glowBy[lane].forEach((node) => (glow[node] = 'idle'))
    glowBy[lane].clear()
  }
  function later(lane: Lane, ms: number, fn: () => void) {
    timers[lane].push(setTimeout(fn, clock(ms)))
  }

  function flash(lane: Lane, node: PipeNodeId, state: Glow, ms: number) {
    glow[node] = state
    glowBy[lane].add(node)
    later(lane, ms, () => {
      if (glow[node] === state) glow[node] = 'idle'
      glowBy[lane].delete(node)
    })
  }

  function lightEdge(lane: Lane, edge: PipeEdgeId, ms: number) {
    litEdges.add(edge)
    litBy[lane].add(edge)
    later(lane, ms, () => {
      litEdges.delete(edge)
      litBy[lane].delete(edge)
    })
  }

  /** Shows a bubble on `node` for 2.5 s, optionally cutting `edge` for as long. */
  function note(node: PipeNodeId, code: string, edge: CutEdge | null) {
    bubble.value = { node, code }
    if (edge) cut.value = edge
    sticky.push(
      setTimeout(() => {
        if (bubble.value?.node === node) bubble.value = null
        if (edge && cut.value === edge) cut.value = null
      }, clock(2500)),
    )
  }

  /** Sends the lane's token along `edge`; it arrives after `ms` and lights the target node. */
  function travel(
    lane: Lane,
    edge: PipeEdgeId,
    color: Color,
    ms: number,
    state: Glow = 'ok',
    hold = ms * 2.5,
  ) {
    tokens[lane] = { edge, color, ms: clock(ms), seq: ++seq }
    lightEdge(lane, edge, ms + hold)
    later(lane, ms, () => flash(lane, EDGE_BY_ID[edge].to, state, hold))
  }

  /**
   * Walks a chain of edges, one every `step` ms, starting now. The last arrival may glow with
   * another state (e.g. `busy` while the mail server works). Returns the total duration.
   */
  function walk(
    lane: Lane,
    chain: PipeEdgeId[],
    color: Color,
    step = STEP_MS,
    arrive: Glow = 'ok',
    hold?: number,
  ) {
    chain.forEach((edge, i) => {
      const last = i === chain.length - 1
      const go = () => travel(lane, edge, color, step, last ? arrive : 'ok', last ? hold : undefined)
      if (i === 0) go()
      else later(lane, i * step, go)
    })
    return chain.length * step
  }

  function hide(lane: Lane, after = 0) {
    later(lane, after, () => (tokens[lane] = null))
  }

  /** The aggregate refuses: red glow, the rule code in a bubble, the arrow to the store is cut. */
  function reject(lane: Lane, edgeIn: PipeEdgeId, edgeOut: CutEdge, code: string) {
    const node = EDGE_BY_ID[edgeIn].to
    tokens[lane] = { edge: edgeIn, color: 'red', ms: 0, seq: ++seq }
    flash(lane, node, 'error', 1500)
    note(node, code, edgeOut)
    hide(lane, 1500)
  }

  // --- Our own command: the HTTP round trip (user → Concert service → projection → UI) -----------
  function onCommand(s: CommandSignal) {
    if (s.phase === 'sent') {
      enqueue('cmd', 750, () => {
        travel('cmd', 'user-ui_in', 'cyan', 250, 'pulse', 350)
        later('cmd', 250, () => travel('cmd', 'ui_in-command', 'cyan', 250, 'pulse', 350))
        later('cmd', 500, () => travel('cmd', 'command-aggregate', 'amber', 250, 'busy', 4000))
      })
    } else if (s.phase === 'accepted') {
      // Forks at events + outbox: the read model and the outbox task are written in one transaction.
      enqueue(
        'cmd',
        200 + STEP_MS,
        () => {
          flash('cmd', 'aggregate', 'ok', 600)
          later('cmd', 200, () => transactionFlash.value++)
          const ms = 200 + walk('cmd', ['aggregate-store', 'store-read_db', 'read_db-ui', 'ui-user'], 'lime')
          hide('cmd', ms + 500)
        },
        0,
      )
    } else {
      enqueue('cmd', 1500, () => reject('cmd', 'command-aggregate', 'aggregate-store', s.code ?? s.phase))
    }
  }

  // --- A Fan command: only ever sent by a task handler, so it starts at the Concert outbox --------
  function onFanCommand(s: ActivitySignal) {
    if (s.entry.aggregate_type !== 'Fan') return
    const accepted = s.type === 'command.accepted'
    // Accepted: forks at the Fan events + outbox (wallet projection and the mail task together).
    enqueue(
      'fan',
      750 + (accepted ? STEP_MS : 1500),
      () => {
        const into = walk(
          'fan',
          ['store-handler_a', 'handler_a-fan_command', 'fan_command-fan_aggregate'],
          'cyan',
          250,
        )
        later('fan', into, () => {
          if (accepted) {
            flash('fan', 'fan_aggregate', 'ok', 600)
            const ms = walk(
              'fan',
              ['fan_aggregate-fan_store', 'fan_store-read_db', 'read_db-ui', 'ui-user'],
              'lime',
            )
            hide('fan', ms + 500)
          } else {
            reject(
              'fan',
              'fan_command-fan_aggregate',
              'fan_aggregate-fan_store',
              String(s.entry.code ?? s.entry.outcome ?? s.type),
            )
          }
        })
      },
      accepted ? 0 : GAP_MS,
    )
  }

  // --- Mail: the flaky server rolls the dice on every attempt; a failure goes back to the outbox --
  function onTask(s: ActivitySignal) {
    if (s.type === 'task.attempt.started') {
      enqueue('mail', 500, () => {
        const ms = walk('mail', ['fan_store-handler_b', 'handler_b-mail_server'], 'cyan', 250, 'busy', 6000)
        later(
          'mail',
          ms,
          () => (tokens.mail = { edge: 'handler_b-mail_server', color: 'amber', ms: 0, seq: ++seq }),
        )
      })
    } else if (s.type === 'task.attempt.failed') {
      enqueue('mail', 1500, () => {
        diceRoll.value++
        flash('mail', 'mail_server', 'error', 1500)
        note('mail_server', String(s.entry.reason_code ?? 'retry'), null)
        later('mail', 300, () => travel('mail', 'mail_server-fan_store', 'red', 600, 'pulse', 600))
        hide('mail', 1500)
      })
    } else if (s.type === 'task.attempt.succeeded') {
      enqueue('mail', 2 * STEP_MS + 600, () => {
        diceRoll.value++
        flash('mail', 'mail_server', 'ok', 600)
        const ms = walk('mail', ['mail_server-email', 'email-user'], 'lime')
        hide('mail', ms + 600)
      })
    }
  }

  // --- The scene queue --------------------------------------------------------------------------
  // The server is far faster than the eye: a click, the Fan command and the mail attempt arrive
  // within a few hundred milliseconds. Scenes therefore play one after another, each for its
  // declared duration plus a short gap, so the chain reads as a sequence. During a burst (crowd,
  // scalper) the queue is not allowed to fall more than MAX_LAG_MS behind reality: later scenes
  // are skipped, the panels still show every row.
  const GAP_MS = 200
  const MAX_LAG_MS = 4000
  let nextFree = 0
  const queued: ReturnType<typeof setTimeout>[] = []
  /**
   * `ms` is how long the scene holds the stage: the next scene starts after it plus a gap. A scene
   * that forks (the token reaches events + outbox and both the projection and the outbox task go on
   * from there) declares only the time up to the fork and no gap, so the next scene — the task chain
   * or the mail attempt — starts right as the fork happens while this scene finishes its own branch.
   */
  function enqueue(lane: Lane, ms: number, scene: () => void, gap = GAP_MS) {
    const now = Date.now()
    const start = Math.max(now, nextFree)
    if (start - now > clock(MAX_LAG_MS)) return
    nextFree = start + clock(ms + gap)
    const run = () => {
      cancel(lane)
      scene()
    }
    if (start === now) run()
    else queued.push(setTimeout(run, start - now))
  }

  function dispose() {
    ;(['cmd', 'fan', 'mail'] as Lane[]).forEach(cancel)
    sticky.forEach(clearTimeout)
    sticky.length = 0
    queued.forEach(clearTimeout)
    queued.length = 0
    nextFree = 0
  }

  return {
    glow,
    tokens,
    litEdges,
    cut,
    bubble,
    diceRoll,
    transactionFlash,
    onCommand,
    onFanCommand,
    onTask,
    dispose,
  }
}

export type Choreography = ReturnType<typeof createChoreography>
