import { describe, expect, it } from 'vitest'
import { deriveTaskStatus, retryCountdownMs, taskKind } from '../lib/outboxStatus'

describe('deriveTaskStatus', () => {
  it('maps the explicit status column to UI statuses', () => {
    expect(deriveTaskStatus({ status: 'pending', attempts: 0 })).toBe('queued')
    expect(deriveTaskStatus({ status: 'pending', attempts: 2 })).toBe('retry_scheduled')
    expect(deriveTaskStatus({ status: 'processing', attempts: 1 })).toBe('in_progress')
    expect(deriveTaskStatus({ status: 'completed', attempts: 3 })).toBe('delivered')
    expect(deriveTaskStatus({ status: 'failed', attempts: 6 })).toBe('failed')
  })

  it('counts down by the database clock', () => {
    const now = Date.parse('2026-10-17T09:00:00Z')
    expect(retryCountdownMs({ available_at: '2026-10-17T09:00:03Z' }, now)).toBe(3000)
    expect(retryCountdownMs({ available_at: '2026-10-17T08:59:59Z' }, now)).toBeLessThanOrEqual(0)
  })

  it('classifies task formats', () => {
    expect(taskKind('encore_record_ticket')).toBe('chain')
    expect(taskKind('encore_record_cancellation')).toBe('chain')
    expect(taskKind('encore_send_e_ticket')).toBe('e_ticket')
    expect(taskKind('encore_issue_refund')).toBe('refund')
    expect(taskKind('something_else')).toBe('other')
  })
})
