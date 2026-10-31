import { describe, expect, it } from 'vitest'
import en from '../i18n/en'
import ru from '../i18n/ru'

function keys(obj: unknown, prefix = ''): string[] {
  if (typeof obj !== 'object' || obj === null) return [prefix]
  return Object.entries(obj as Record<string, unknown>).flatMap(([k, v]) => keys(v, prefix ? `${prefix}.${k}` : k))
}

const RULE_CODES = [
  'concert_not_found',
  'already_announced',
  'unknown_seat',
  'sold_out',
  'seat_taken',
  'ticket_limit',
  'seat_not_booked',
  'not_your_ticket',
  'rebuild_in_progress',
]
const REASON_CODES = ['smtp_timeout', 'server_on_fire', 'rate_limited', 'mailbox_full']

describe('i18n parity', () => {
  it('en and ru expose the same keys', () => {
    expect(keys(ru).sort()).toEqual(keys(en).sort())
  })

  it('has texts for every rule code and chaos reason in both locales', () => {
    for (const locale of [en, ru]) {
      for (const code of RULE_CODES) expect(locale.rules[code as keyof typeof locale.rules]).toBeTruthy()
      for (const reason of REASON_CODES) expect(locale.chaos[reason as keyof typeof locale.chaos]).toBeTruthy()
    }
  })
})
