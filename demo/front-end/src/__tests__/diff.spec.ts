import { describe, expect, it } from 'vitest'
import { changedPaths, deepEqual } from '../lib/diff'

describe('changedPaths', () => {
  it('lists changed, added and removed paths', () => {
    const before = { view: { sold: 1, seats: { B4: { state: 'booked' } } }, version: 3 }
    const after = { view: { sold: 2, seats: { B4: { state: 'booked' }, C1: { state: 'booked' } } }, version: 4 }
    expect(changedPaths(before, after).sort()).toEqual(['version', 'view.seats.C1', 'view.sold'])
    expect(changedPaths(before, before)).toEqual([])
    expect(changedPaths(1, 2)).toEqual(['$'])
    expect(changedPaths({ a: [1, 2] }, { a: [1, 3] })).toEqual(['a'])
  })

  it('deepEqual handles nested structures', () => {
    expect(deepEqual({ a: [1, { b: null }] }, { a: [1, { b: null }] })).toBe(true)
    expect(deepEqual({ a: 1 }, { a: 1, b: 2 })).toBe(false)
    expect(deepEqual([1], [1, 2])).toBe(false)
  })
})
