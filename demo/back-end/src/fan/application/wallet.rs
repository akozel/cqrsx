//! Use case "show a fan's wallet". A `fan_wallets` document is one ledger (a fan and a concert,
//! D-14); the wallet the fan sees is assembled here from all of their ledgers. The ledgers of a fan
//! sit in one id range (`FanLedgerId::id_range`), so the read is one range query of the engine's
//! view (on the `_id` index in MongoDB) and needs nothing else.
use crate::{
    fan::application::views::fan_wallet::FanWalletView,
    shared::domain::{FanId, FanLedgerId},
};
use cqrsx::{journal::PersistenceError, view::repository::StoredView};
use cqrsx_engine::views::ViewQuery;
use serde::Serialize;

/// One ledger behind the wallet: the concert, the document version and the last event applied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LedgerRef {
    pub concert_id: String,
    pub version: i64,
    pub last_sequence: i64,
}

/// The wallet as the API returns it: the ledgers it was assembled from and the merged view, in the
/// shape of one `FanWalletView` (`tickets` keyed by `booking_id`, counters summed).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FanWallet {
    pub fan_id: String,
    pub ledgers: Vec<LedgerRef>,
    pub view: FanWalletView,
}

/// The wallet of `fan_id` from its ledgers in `wallets`; `None` when the fan has no ledger yet (or the read models
/// were wiped).
pub async fn wallet_of(wallets: &ViewQuery<'_, FanWalletView>, fan_id: &FanId) -> Result<Option<FanWallet>, PersistenceError> {
    let (lower, upper) = FanLedgerId::id_range(fan_id);
    let ledgers = wallets.range(&lower, &upper).await?;
    Ok(assemble(fan_id, ledgers))
}

/// Merges a fan's ledgers (given in id order). `booking_id` is unique across ledgers, so tickets
/// never collide; the counters are sums; `last_sequence` is the maximum and only says "something
/// changed", because sequences are per ledger.
pub fn assemble(fan_id: &FanId, ledgers: Vec<StoredView<FanWalletView>>) -> Option<FanWallet> {
    if ledgers.is_empty() {
        return None;
    }
    let mut view = FanWalletView {
        fan_id: fan_id.as_str().to_owned(),
        ..FanWalletView::default()
    };
    let mut refs = Vec::with_capacity(ledgers.len());
    for StoredView {
        id,
        version,
        view: ledger,
        ..
    } in ledgers
    {
        let concert_id = FanLedgerId::parse(&id)
            .map(|ledger| ledger.concert().as_str().to_owned())
            .unwrap_or(id);
        refs.push(LedgerRef {
            concert_id,
            version,
            last_sequence: ledger.last_sequence,
        });
        let FanWalletView {
            fan_name,
            tickets,
            active_count,
            total_spent,
            last_sequence,
            ..
        } = ledger;
        if view.fan_name.is_empty() {
            view.fan_name = fan_name;
        }
        view.tickets.extend(tickets);
        view.active_count += active_count;
        view.total_spent += total_spent;
        view.last_sequence = view.last_sequence.max(last_sequence);
    }
    Some(FanWallet {
        fan_id: fan_id.as_str().to_owned(),
        ledgers: refs,
        view,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        fan::{
            application::views::fan_wallet::{TicketEntry, TicketEntryState},
            domain::Fan,
        },
        testing,
    };
    use std::collections::BTreeMap;

    /// A ledger document with one active ticket.
    fn ledger(id: &str, version: i64, booking: &str, concert: &str, price: i64, sequence: i64) -> StoredView<FanWalletView> {
        let ticket = TicketEntry {
            concert_id: concert.into(),
            seat: "B4".into(),
            price,
            state: TicketEntryState::Active,
            refund: None,
            sequence,
            changed_at: format!("2026-10-18T10:00:0{sequence}.000000Z"),
        };
        StoredView {
            id: id.into(),
            version,
            last_sequence: sequence as usize,
            view: FanWalletView {
                fan_id: "fox".into(),
                fan_name: "Fox".into(),
                tickets: BTreeMap::from([(booking.to_owned(), ticket)]),
                active_count: 1,
                total_spent: price,
                last_sequence: sequence,
            },
        }
    }

    fn fox() -> FanId {
        FanId::parse("fox").unwrap()
    }

    #[test]
    fn a_wallet_is_the_union_of_the_fans_ledgers() {
        let wallet = assemble(
            &fox(),
            vec![
                ledger("fox:muse-2027-01-16", 2, "b-2", "muse-2027-01-16", 120, 1),
                ledger("fox:radiohead-2026-11-21", 5, "b-1", "radiohead-2026-11-21", 80, 3),
            ],
        )
        .unwrap();
        assert_eq!(wallet.fan_id, "fox");
        assert_eq!(
            wallet.ledgers,
            vec![
                LedgerRef {
                    concert_id: "muse-2027-01-16".into(),
                    version: 2,
                    last_sequence: 1
                },
                LedgerRef {
                    concert_id: "radiohead-2026-11-21".into(),
                    version: 5,
                    last_sequence: 3
                },
            ]
        );
        assert_eq!((wallet.view.fan_id.as_str(), wallet.view.fan_name.as_str()), ("fox", "Fox"));
        assert_eq!(wallet.view.tickets.keys().cloned().collect::<Vec<_>>(), ["b-1", "b-2"]);
        assert_eq!(
            (wallet.view.active_count, wallet.view.total_spent, wallet.view.last_sequence),
            (2, 200, 3)
        );
    }

    #[test]
    fn no_ledger_means_no_wallet_and_a_foreign_id_is_reported_as_it_is() {
        assert_eq!(assemble(&fox(), vec![]), None);
        let wallet = assemble(&fox(), vec![ledger("legacy", 1, "b-1", "c-1", 50, 1)]).unwrap();
        assert_eq!(wallet.ledgers[0].concert_id, "legacy");
    }

    #[tokio::test]
    async fn wallet_of_reads_only_the_fans_own_range() {
        let (cqrs, _) = testing::test_app().await;
        testing::put::<_, Fan>(
            &cqrs,
            vec![
                ledger("fox-2:c-1", 1, "b-9", "c-1", 50, 1),
                ledger("fox:c-1", 1, "b-1", "c-1", 80, 1),
                ledger("fox_1:c-1", 1, "b-8", "c-1", 50, 1),
                ledger("panda:c-1", 1, "b-7", "c-1", 50, 1),
            ],
        );
        let wallets = cqrs.view::<FanWalletView>();
        let wallet = wallet_of(&wallets, &fox()).await.unwrap().unwrap();
        assert_eq!(wallet.ledgers.len(), 1);
        assert_eq!((wallet.ledgers[0].concert_id.as_str(), wallet.view.total_spent), ("c-1", 80));
        assert!(
            wallet_of(&wallets, &FanId::parse("unicorn").unwrap())
                .await
                .unwrap()
                .is_none()
        );
    }
}
