// lib/outboxStatus.ts — a pure function, covered by tests (DESIGN.md §4.6).
// The transactional_outbox.status column is explicit: pending | processing | completed | failed.
import type { OutboxRow } from '@/types/xray'

export type UiTaskStatus = 'queued' | 'in_progress' | 'retry_scheduled' | 'delivered' | 'failed'

export function deriveTaskStatus(row: Pick<OutboxRow, 'status' | 'attempts'>): UiTaskStatus {
  switch (row.status) {
    case 'completed':
      return 'delivered'
    case 'failed':
      return 'failed'
    case 'processing':
      return 'in_progress' // an expired lease before recovery is also in_progress (≤ poll)
    case 'pending':
      return row.attempts > 0 ? 'retry_scheduled' : 'queued'
  }
}

/** Milliseconds until the task becomes claimable again, by the database clock; ≤ 0 ⇒ waiting for a worker. */
export function retryCountdownMs(row: Pick<OutboxRow, 'available_at'>, dbNowMs: number): number {
  return Date.parse(row.available_at) - dbNowMs
}

export type TaskKind = 'chain' | 'e_ticket' | 'refund' | 'other'

export function taskKind(taskType: string): TaskKind {
  if (taskType.startsWith('encore_record_')) return 'chain'
  if (taskType === 'encore_send_e_ticket') return 'e_ticket'
  if (taskType === 'encore_issue_refund') return 'refund'
  return 'other'
}
