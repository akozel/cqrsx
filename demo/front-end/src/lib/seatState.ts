// lib/seatState.ts — a pure function, covered by tests (DESIGN.md §4.7).
import type { ConcertHallView, HistoryState } from '@/types/domain'

export type SeatUiState = 'free' | 'booked' | 'mine' | 'unknown'

export type SeatContext =
  | { mode: 'live'; view: ConcertHallView | null; persona: string }
  | { mode: 'history'; historyState: HistoryState['state']; persona: string }

export function seatState(seat: string, ctx: SeatContext): SeatUiState {
  if (ctx.mode === 'history') {
    // Time machine: aggregate data computed from events; "unknown" cannot happen here.
    const b = ctx.historyState.bookings[seat]
    return !b ? 'free' : b.fan_id === ctx.persona ? 'mine' : 'booked'
  }
  if (!ctx.view) return 'unknown' // read model wiped
  const entry = ctx.view.seats[seat]
  if (!entry) return 'free'
  if (entry.state === 'free') return 'free'
  return entry.fan_id === ctx.persona ? 'mine' : 'booked'
}
// `pending` and animations are a separate overlay on top of the base state.
