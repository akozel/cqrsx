import { describe, expect, it } from 'vitest'
import en from '../i18n/en'
import ru from '../i18n/ru'

function keys(obj: unknown, prefix = ''): string[] {
  if (typeof obj !== 'object' || obj === null) return [prefix]
  return Object.entries(obj as Record<string, unknown>).flatMap(([k, v]) => keys(v, prefix ? `${prefix}.${k}` : k))
}

describe('i18n parity', () => {
  it('en and ru expose the same keys', () => {
    expect(keys(ru).sort()).toEqual(keys(en).sort())
  })
})
