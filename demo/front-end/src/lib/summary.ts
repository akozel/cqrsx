// One readable line per event or task: the few payload fields a person cares about, in a fixed
// order, instead of the raw JSON (which opens on a click). Ids stay out: correlation is shown by
// the hover highlight.
const EURO = (v: unknown) => `${v} €`

type Field = [key: string, format?: (v: unknown) => string]
const FIELDS: Field[] = [
  ['artist'],
  ['city'],
  ['date'],
  ['capacity', (v) => `${v} seats`],
  ['concert_id'],
  ['seat'],
  ['fan_name'],
  ['price', EURO],
  ['refund', (v) => `refund ${v} €`],
  ['amount', (v) => `refund ${v} €`],
]

/** Fields that say whose the record is; an outbox task next to its event does not repeat them. */
const IDENTITY = new Set(['concert_id', 'fan_name', 'fan_id'])

/** The payload fields worth a glance, formatted, in display order. */
export function summarize(
  payload: Record<string, unknown> | null | undefined,
  options: { identity?: boolean; omit?: string[] } = {},
): string[] {
  if (!payload) return []
  const identity = options.identity ?? true
  const omit = new Set(options.omit ?? [])
  const out: string[] = []
  for (const [key, format] of FIELDS) {
    const v = payload[key]
    if (v === undefined || v === null || v === '') continue
    if ((!identity && IDENTITY.has(key)) || omit.has(key)) continue
    out.push(format ? format(v) : String(v))
  }
  // The fan id is only interesting when there is no name for them.
  if (identity && !omit.has('fan_id') && !('fan_name' in payload) && typeof payload.fan_id === 'string')
    out.push(payload.fan_id)
  return out
}
