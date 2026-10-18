/**
 * The `Fan` aggregate is a ledger per fan and concert: its id (and the `_id` of its `fan_wallets`
 * document) is `<fan_id>:<concert_id>` (back-end decision D-14). `:` belongs to neither id alphabet.
 */
export const LEDGER_SEPARATOR = ':'

/** Is `aggregateId` one of `fanId`'s ledgers? (`fox:radiohead-2026-11-21` is fox's; `fox-2:…` is not.) */
export function isLedgerOf(aggregateId: string, fanId: string): boolean {
  return aggregateId.startsWith(fanId + LEDGER_SEPARATOR)
}

/** The fan half of a ledger id; a plain id is returned as it is. */
export function ledgerFan(aggregateId: string): string {
  const at = aggregateId.indexOf(LEDGER_SEPARATOR)
  return at < 0 ? aggregateId : aggregateId.slice(0, at)
}
