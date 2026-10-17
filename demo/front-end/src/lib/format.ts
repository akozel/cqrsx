// Small formatting helpers.
export function shortId(id: string | undefined | null, length = 8): string {
  if (!id) return '—'
  return id.length > length ? id.slice(0, length) : id
}

export function euro(amount: number | undefined | null): string {
  return amount == null ? '—' : `${amount} €`
}

export function clockTime(iso: string | undefined | null): string {
  if (!iso) return '—'
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return iso
  return d.toLocaleTimeString(undefined, { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' })
}

export function msAgo(iso: string | undefined | null, now = Date.now()): number | null {
  if (!iso) return null
  const t = Date.parse(iso)
  return Number.isNaN(t) ? null : Math.max(0, now - t)
}

export function fanLabel(fanId: string | undefined, fanName?: string): string {
  return fanName ? `${fanName}` : (fanId ?? '—')
}
