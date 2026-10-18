// The hall as the back-end defines it (demo/back-end/src/domain/shared/venue.rs): rows A–E, 8 seats
// each. Seat ids are `^[A-E][1-8]$`.
export const ROWS = ['A', 'B', 'C', 'D', 'E'];
export const SEATS_PER_ROW = 8;
export const SEATS = ROWS.length * SEATS_PER_ROW;

/** Seat number 0..39 → `A1`..`E8`. */
export function seatOf(index) {
  const i = ((index % SEATS) + SEATS) % SEATS;
  return `${ROWS[Math.floor(i / SEATS_PER_ROW)]}${(i % SEATS_PER_ROW) + 1}`;
}
