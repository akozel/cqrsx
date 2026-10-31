import { describe, expect, it } from 'vitest'
import { isLedgerOf, ledgerFan } from '@/lib/ledger'

describe('fan ledger ids', () => {
  it('recognises the ledgers of a fan and nobody else', () => {
    expect(isLedgerOf('fox:radiohead-2026-11-21', 'fox')).toBe(true)
    expect(isLedgerOf('fox:c-1', 'fox')).toBe(true)
    expect(isLedgerOf('fox-2:c-1', 'fox')).toBe(false)
    expect(isLedgerOf('fox_1:c-1', 'fox')).toBe(false)
    expect(isLedgerOf('fox', 'fox')).toBe(false)
    expect(isLedgerOf('radiohead-2026-11-21', 'fox')).toBe(false)
  })

  it('extracts the fan from a ledger id and leaves a plain id alone', () => {
    expect(ledgerFan('fox:radiohead-2026-11-21')).toBe('fox')
    expect(ledgerFan('radiohead-2026-11-21')).toBe('radiohead-2026-11-21')
  })
})
