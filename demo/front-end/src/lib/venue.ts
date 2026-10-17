// The hall as the UI knows it: rows A–E × 8 seats, three zones. Mirrors domain::venue.
import type { AppConfig, ZoneId } from '@/types/domain'

export const ZONE_COLORS: Record<ZoneId, string> = {
  vip: '#fbbf24',
  standard: '#22d3ee',
  balcony: '#a78bfa',
}

export const DEFAULT_VENUE: AppConfig['venue'] = {
  rows: ['A', 'B', 'C', 'D', 'E'],
  seats_per_row: 8,
  zones: [
    { id: 'vip', rows: ['A'], price: 120 },
    { id: 'standard', rows: ['B', 'C'], price: 80 },
    { id: 'balcony', rows: ['D', 'E'], price: 50 },
  ],
}

export interface SeatInfo {
  id: string
  row: string
  number: number
  zone: ZoneId
  price: number
}

export function zoneOfRow(venue: AppConfig['venue'], row: string): ZoneId {
  return venue.zones.find((z) => z.rows.includes(row))?.id ?? 'balcony'
}

export function priceOfZone(venue: AppConfig['venue'], zone: ZoneId): number {
  return venue.zones.find((z) => z.id === zone)?.price ?? 0
}

export function allSeats(venue: AppConfig['venue'] = DEFAULT_VENUE): SeatInfo[] {
  const seats: SeatInfo[] = []
  for (const row of venue.rows) {
    const zone = zoneOfRow(venue, row)
    for (let n = 1; n <= venue.seats_per_row; n++) {
      seats.push({ id: `${row}${n}`, row, number: n, zone, price: priceOfZone(venue, zone) })
    }
  }
  return seats
}

export function seatInfo(venue: AppConfig['venue'], seat: string): SeatInfo | undefined {
  return allSeats(venue).find((s) => s.id === seat)
}

export function pickRandom<T>(items: T[]): T | undefined {
  if (items.length === 0) return undefined
  return items[Math.floor(Math.random() * items.length)]
}
