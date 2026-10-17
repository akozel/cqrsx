// @vitest-environment jsdom
import { beforeEach, describe, expect, it } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { useTourStore } from '../stores/tour'
import { useSessionStore } from '../stores/session'

beforeEach(() => {
  localStorage.clear()
  setActivePinia(createPinia())
})

describe('tour', () => {
  it('opens the intro on the first visit and walks intro → ①…⑤ → closed', () => {
    const tour = useTourStore()
    expect(tour.step).toBe('intro')
    expect(tour.current).toBeNull()
    tour.next()
    expect(useSessionStore().bannerDismissed).toBe(true)
    expect([tour.current, tour.index]).toEqual(['events', 1])
    for (let i = 0; i < 4; i++) tour.next()
    expect([tour.current, tour.index]).toEqual(['encore', 5])
    tour.next()
    expect(tour.step).toBeNull()
  })

  it('back from chapter ① returns to the intro; the help button restarts from it', () => {
    localStorage.setItem('encore.banner', 'dismissed')
    const tour = useTourStore()
    expect(tour.step).toBeNull()
    tour.start()
    expect(tour.step).toBe('intro')
    tour.next()
    tour.back()
    expect(tour.step).toBe('intro')
    tour.close()
    expect(tour.step).toBeNull()
  })
})
