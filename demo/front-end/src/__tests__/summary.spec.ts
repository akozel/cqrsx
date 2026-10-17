import { describe, expect, it } from 'vitest'
import { summarize } from '../lib/summary'

describe('summarize', () => {
  it('shows the readable fields of an event payload in a fixed order, never ids', () => {
    expect(
      summarize({
        seat: 'B4',
        zone: 'standard',
        price: 80,
        fan_id: 'fox',
        fan_name: 'Fox',
        booking_id: 'a1b2c3d4-ffff',
      }),
    ).toEqual(['B4', 'Fox', '80 €'])
    expect(summarize({ artist: 'Muse', city: 'Tbilisi', date: '2026-11-01', capacity: 40 })).toEqual([
      'Muse',
      'Tbilisi',
      '2026-11-01',
      '40 seats',
    ])
  })

  it('falls back to the fan id when there is no name, and formats refunds', () => {
    expect(
      summarize({ seat: 'B4', fan_id: 'fox', booking_id: 'a1b2c3d4-1', cancellation_id: 'x', refund: 80 }),
    ).toEqual(['B4', 'refund 80 €', 'fox'])
    expect(summarize({ concert_id: 'radiohead-2026-11-21', seat: 'C1', fan_id: 'panda', amount: 120 })).toEqual([
      'radiohead-2026-11-21',
      'C1',
      'refund 120 €',
      'panda',
    ])
  })

  it('can leave out whose the record is (for a task shown next to its event)', () => {
    expect(
      summarize(
        {
          concert_id: 'radiohead-2026-11-21',
          seat: 'B4',
          zone: 'vip',
          price: 120,
          fan_id: 'fox',
          fan_name: 'Fox',
          booking_id: 'a1',
        },
        { identity: false },
      ),
    ).toEqual(['B4', '120 €'])
    expect(summarize({ seat: 'B4', fan_id: 'panda', amount: 80 }, { identity: false })).toEqual([
      'B4',
      'refund 80 €',
    ])
  })

  it('can omit fields the row already shows (the fan of a Fan event)', () => {
    expect(
      summarize(
        { concert_id: 'radiohead-2026-11-21', seat: 'A5', fan_name: 'Fox', refund: 120 },
        { omit: ['fan_name', 'fan_id'] },
      ),
    ).toEqual(['radiohead-2026-11-21', 'A5', 'refund 120 €'])
    expect(summarize({ seat: 'A5', fan_id: 'fox' }, { omit: ['fan_id'] })).toEqual(['A5'])
  })

  it('is empty for nothing', () => {
    expect(summarize(null)).toEqual([])
    expect(summarize({})).toEqual([])
  })
})
