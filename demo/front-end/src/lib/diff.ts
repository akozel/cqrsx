// lib/diff.ts — which JSON paths changed between two documents (for the 2 s highlight).
export type Json = null | boolean | number | string | Json[] | { [key: string]: Json }

export function changedPaths(before: unknown, after: unknown, prefix = ''): string[] {
  if (deepEqual(before, after)) return []
  const isObj = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v)
  if (isObj(before) && isObj(after)) {
    const keys = new Set([...Object.keys(before), ...Object.keys(after)])
    const out: string[] = []
    for (const key of keys) {
      const path = prefix ? `${prefix}.${key}` : key
      if (!(key in before) || !(key in after)) out.push(path)
      else out.push(...changedPaths(before[key], after[key], path))
    }
    return out
  }
  return [prefix || '$']
}

export function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true
  if (typeof a !== typeof b || a === null || b === null) return false
  if (Array.isArray(a)) {
    if (!Array.isArray(b) || a.length !== b.length) return false
    return a.every((v, i) => deepEqual(v, b[i]))
  }
  if (typeof a === 'object' && typeof b === 'object') {
    const ka = Object.keys(a as object)
    const kb = Object.keys(b as object)
    if (ka.length !== kb.length) return false
    return ka.every((k) => deepEqual((a as Record<string, unknown>)[k], (b as Record<string, unknown>)[k]))
  }
  return false
}
