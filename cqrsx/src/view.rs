//! Read models: the [`View`] contract, the type of the observer of a view's persistence errors, and the submodules
//! that keep views up to date ([`projection`]), store them ([`repository`]) and check them ([`verify`]). The
//! checkpoint key of a view, [`CheckpointKey::of`], is also defined here, so that `projection::checkpoint` does not
//! name views.
pub mod projection;
pub mod repository;
pub mod verify;

use crate::{
    domain::{aggregate::Aggregate, event::EventEnvelope},
    journal::PersistenceError,
    projection::checkpoint::CheckpointKey,
};
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;

/// A `View` represents a materialized view, generally serialized for persistence, that is updated by a projection.
/// This is the data of a CQRS read model.
///
/// `apply` must be deterministic and free of side effects: a stored document is the fold of `apply` over the
/// events `1..=last_sequence` of its aggregate, and two writers of one document (two processes, a rebuild next to
/// live delivery) meet at the same document only because the fold is the same.
pub trait View<A: Aggregate>: Debug + Default + Serialize + DeserializeOwned + Send + Sync {
    /// The name of the view: its collection or table, the name of its projection and of the projection's checkpoint
    /// row ([`CheckpointKey::of`](CheckpointKey::of)). One view per name; the name is stored, so it never
    /// changes.
    const NAME: &'static str;

    /// Each implemented view is responsible for updating its state based on events passed via
    /// this method.
    fn apply(&mut self, event: &EventEnvelope<A>);
}

/// Observes persistence errors from projection reads and event handling.
///
/// Borrows the original error while preserving ownership and the result for its caller.
///
/// The observer does not recover, suppress or replace the failure.
///
/// ```rust
/// use {cqrsx::journal::PersistenceError};
/// use {cqrsx::view::PersistenceErrorObserver};
/// fn create_error_observer() {
///     // An error handler that panics when any persistence error is encountered.
///     let error_observer: Box<PersistenceErrorObserver> = Box::new(error_observer);
/// }
///
/// fn error_observer(error: &PersistenceError) {
///     panic!("{}",error);
/// }
/// ```
pub type PersistenceErrorObserver = dyn Fn(&PersistenceError) + Send + Sync + 'static;

impl CheckpointKey {
    /// The row of the projection of the view `V` over the journal of `A`: the view's name and the aggregate's type.
    pub fn of<V: View<A>, A: Aggregate>() -> Self {
        Self::new(V::NAME, A::TYPE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::aggregate::tests::Ledger;
    use serde::Deserialize;

    #[test]
    fn the_key_of_a_view_is_its_name_and_the_type_of_its_aggregate() {
        #[derive(Debug, Default, Serialize, Deserialize)]
        struct Wallets;
        impl View<Ledger> for Wallets {
            const NAME: &'static str = "wallets";
            fn apply(&mut self, _: &EventEnvelope<Ledger>) {}
        }
        assert_eq!(
            CheckpointKey::of::<Wallets, Ledger>(),
            CheckpointKey::new("wallets", Ledger::TYPE)
        );
    }
}
