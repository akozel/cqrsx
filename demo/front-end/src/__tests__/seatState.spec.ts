import { describe, expect, it } from 'vitest'
import { seatState } from '../lib/seatState'
import type { ConcertHallView, HistoryState } from '../types/domain'

const view: ConcertHallView = {
  concert: { artist: 'A', city: 'C', date: '2026-11-21' },
  capacity: 40,
  seats: {
    B4: { state: 'booked', fan_id: 'fox', fan_name: 'Fox', zone: 'standard', price: 80, sequence: 2, changed_at: '' },
    A1: { state: 'free', sequence: 4, changed_at: '' },
  },
  sold: 1,
  revenue: 80,
  sold_out: false,
  last_sequence: 4,
}

describe('seatState', () => {
  it('distinguishes free, booked, mine and unknown in live mode', () => {
    expect(seatState('B4', { mode: 'live', view, persona: 'fox' })).toBe('mine')
    expect(seatState('B4', { mode: 'live', view, persona: 'panda' })).toBe('booked')
    expect(seatState('A1', { mode: 'live', view, persona: 'panda' })).toBe('free')
    expect(seatState('E8', { mode: 'live', view, persona: 'panda' })).toBe('free')
    expect(seatState('E8', { mode: 'live', view: null, persona: 'panda' })).toBe('unknown')
  })

  it('never answers unknown in history mode', () => {
    const historyState: HistoryState['state'] = {
      info: null,
      bookings: { B4: { fan_id: 'fox', fan_name: 'Fox', booking_id: 'x', price: 80 } },
      sold: 1,
      free_seats: 39,
      sold_out: false,
      revenue: 80,
    }
    expect(seatState('B4', { mode: 'history', historyState, persona: 'fox' })).toBe('mine')
    expect(seatState('B4', { mode: 'history', historyState, persona: 'panda' })).toBe('booked')
    expect(seatState('C1', { mode: 'history', historyState, persona: 'panda' })).toBe('free')
  })
})
