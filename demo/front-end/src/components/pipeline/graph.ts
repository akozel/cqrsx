// The architecture diagram as data (docs/img.png): three service boundaries, the components inside
// them, the actors outside, and the edges a signal can travel. Everything sits on a grid so the
// edges are straight or L-shaped: Command above Aggregate, the outbox worker above events + outbox,
// the mail server in the worker's row to the right of the Fan service, the mailbox below it. Positions are
// hand-authored; the block fits the CANVAS bounds into its width (never past zoom 1).
import { Position } from '@vue-flow/core'

export type PipeGroupId = 'svc_concert' | 'svc_fan' | 'svc_projection'
export type PipeNodeId =
  | 'user'
  | 'command'
  | 'aggregate'
  | 'store'
  | 'handler_a'
  | 'fan_command'
  | 'fan_aggregate'
  | 'fan_store'
  | 'handler_b'
  | 'mail_server'
  | 'read_db'
  | 'ui'
  | 'ui_in'
  | 'email'
export type PipeEdgeId = `${PipeNodeId}-${PipeNodeId}`
/** Side of a box a signal leaves from or arrives at; also the handle id (`bottom-left` sits at a
 * quarter of the width, so two edges can leave the bottom without overlapping). */
export type Side = 'top' | 'right' | 'bottom' | 'left' | 'bottom-left'
export type Tone = 'cyan' | 'violet' | 'pink' | 'amber'

export interface PipeGroup {
  id: PipeGroupId
  /** i18n key under `pipeline.` */
  label: string
  tone: Tone
  x: number
  y: number
  w: number
  h: number
}

export interface PipeNode {
  id: PipeNodeId
  /** `box`: a component; `actor`: a diamond (a screen, a mailbox); `person`: a circle (the fan). */
  kind: 'box' | 'actor' | 'person'
  parent?: PipeGroupId
  /** i18n key under `pipeline.` */
  label: string
  icon: string
  /** Absolute canvas position (top-left) and size. */
  x: number
  y: number
  w: number
  h: number
}

export interface PipeEdge {
  id: PipeEdgeId
  from: PipeNodeId
  to: PipeNodeId
  fromSide: Side
  toSide: Side
  kind?: 'retry'
}

const BOX = { w: 170, h: 44 }
const ACTOR = 72

/** The exact bounds of the drawing (frame titles included), so the fit is symmetric. */
export const CANVAS = { w: 1206, h: 472 }

export const GROUPS: readonly PipeGroup[] = [
  { id: 'svc_concert', label: 'concert_service', tone: 'cyan', x: 0, y: 10, w: 458, h: 198 },
  { id: 'svc_fan', label: 'fan_service', tone: 'violet', x: 518, y: 10, w: 458, h: 198 },
  { id: 'svc_projection', label: 'projection', tone: 'pink', x: 400, y: 260, w: 340, h: 86 },
]

const box = (
  id: PipeNodeId,
  label: string,
  icon: string,
  x: number,
  y: number,
  parent?: PipeGroupId,
): PipeNode => ({
  id,
  kind: 'box',
  parent,
  label,
  icon,
  x,
  y,
  ...BOX,
})
const actor = (
  id: PipeNodeId,
  label: string,
  icon: string,
  x: number,
  y: number,
  parent?: PipeGroupId,
  kind: 'actor' | 'person' = 'actor',
): PipeNode => ({
  id,
  kind,
  parent,
  label,
  icon,
  x,
  y,
  w: ACTOR,
  h: ACTOR,
})

export const NODES: readonly PipeNode[] = [
  // The command side of the UI (the box office), below the Concert service
  actor('ui_in', 'ui', '🖥️', 73, 267),
  // Concert service: Command above Aggregate, the outbox worker above events + outbox
  box('command', 'command', '📨', 24, 44, 'svc_concert'),
  box('handler_a', 'task_handler_a', '⚙️', 264, 44, 'svc_concert'),
  box('aggregate', 'aggregate', '🛡️', 24, 140, 'svc_concert'),
  box('store', 'store', '🐘', 264, 140, 'svc_concert'),
  // Fan service: the same shape
  box('fan_command', 'command', '📨', 542, 44, 'svc_fan'),
  box('handler_b', 'task_handler_b', '⚙️', 782, 44, 'svc_fan'),
  box('fan_aggregate', 'aggregate', '🛡️', 542, 140, 'svc_fan'),
  box('fan_store', 'store', '🐘', 782, 140, 'svc_fan'),
  // To the right of the Fan service: the flaky mail server (DEMO ONLY: 20 % failures); the mailbox
  // sits below it, on the same row as the user
  box('mail_server', 'mail_server', '📮', 1036, 44),
  actor('email', 'email', '📬', 1085, 267),
  // Projection
  box('read_db', 'read_database', '🍃', 424, 281, 'svc_projection'),
  actor('ui', 'ui', '🖥️', 652, 267, 'svc_projection'),
  // The person at the bottom: clicks in the UI, watches the screen, receives the e-mail
  actor('user', 'user', '🧑', 652, 400, undefined, 'person'),
]

const edge = (from: PipeNodeId, fromSide: Side, to: PipeNodeId, toSide: Side, kind?: 'retry'): PipeEdge => ({
  id: `${from}-${to}`,
  from,
  to,
  fromSide,
  toSide,
  kind,
})

export const EDGES: readonly PipeEdge[] = [
  edge('user', 'left', 'ui_in', 'bottom'),
  edge('ui_in', 'top', 'command', 'bottom'),
  edge('command', 'bottom', 'aggregate', 'top'),
  edge('aggregate', 'right', 'store', 'left'),
  edge('store', 'top', 'handler_a', 'bottom'),
  edge('handler_a', 'right', 'fan_command', 'left'),
  edge('fan_command', 'bottom', 'fan_aggregate', 'top'),
  edge('fan_aggregate', 'right', 'fan_store', 'left'),
  edge('fan_store', 'top', 'handler_b', 'bottom'),
  edge('handler_b', 'right', 'mail_server', 'left'),
  edge('mail_server', 'bottom', 'email', 'top'),
  edge('mail_server', 'bottom-left', 'fan_store', 'right', 'retry'),
  edge('store', 'bottom', 'read_db', 'top'),
  edge('fan_store', 'bottom', 'read_db', 'top'),
  edge('read_db', 'right', 'ui', 'left'),
  edge('ui', 'bottom', 'user', 'top'),
  edge('email', 'bottom', 'user', 'right'),
]

export const NODE_IDS: readonly PipeNodeId[] = NODES.map((n) => n.id)
export const EDGE_BY_ID: Readonly<Record<PipeEdgeId, PipeEdge>> = Object.fromEntries(
  EDGES.map((e) => [e.id, e]),
) as Record<PipeEdgeId, PipeEdge>
export const GROUP_BY_ID: Readonly<Record<PipeGroupId, PipeGroup>> = Object.fromEntries(
  GROUPS.map((g) => [g.id, g]),
) as Record<PipeGroupId, PipeGroup>

/** The colour family of a node: its service's, or amber for the mail side. */
export function toneOf(id: PipeNodeId | PipeGroupId): Tone {
  if (id in GROUP_BY_ID) return GROUP_BY_ID[id as PipeGroupId].tone
  const node = NODES.find((n) => n.id === id)
  return node?.parent
    ? GROUP_BY_ID[node.parent].tone
    : node?.id === 'user' || node?.id === 'ui_in'
      ? 'cyan'
      : 'amber'
}

/** Vue Flow handle positions by side. */
export const SIDE_POSITION: Record<Side, Position> = {
  top: Position.Top,
  right: Position.Right,
  bottom: Position.Bottom,
  left: Position.Left,
  'bottom-left': Position.Bottom,
}
