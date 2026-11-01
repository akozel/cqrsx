// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and cqrsx/licenses/.
use crate::{domain::aggregate::Aggregate, journal::PersistenceError, view::View};
use std::future::Future;

/// Handles the database access needed for a ViewProjection.
pub trait ViewRepository<V, A>: Send + Sync
where
    V: View<A>,
    A: Aggregate,
{
    /// Returns the current view instance.
    fn load(&self, view_id: &str) -> impl Future<Output = Result<Option<V>, PersistenceError>> + Send;

    /// Returns the current view instance with its id, revision and last applied event, used by the `ViewProjection`
    /// to update views with committed events.
    fn load_stored(&self, view_id: &str) -> impl Future<Output = Result<Option<StoredView<V>>, PersistenceError>> + Send;

    /// Updates the view instance and its metadata, used by the `ViewProjection` to update
    /// views with committed events.
    fn update_view(&self, view: V, context: ViewMeta) -> impl Future<Output = Result<(), PersistenceError>> + Send;

    /// Every document, ordered by id: by the bytes of the ids (the order of `str`), whatever the store's collation.
    fn list(&self) -> impl Future<Output = Result<Vec<StoredView<V>>, PersistenceError>> + Send;

    /// The documents whose id lies in `[lower, upper)` (compared as [`list`](Self::list) orders them), ordered by id;
    /// none when `lower >= upper`. A key prefix followed by a separator makes a group of ids one range, e.g. the
    /// ledgers `fox:<concert>` of one fan are `range("fox:", "fox;")`.
    fn range(&self, lower: &str, upper: &str) -> impl Future<Output = Result<Vec<StoredView<V>>, PersistenceError>> + Send;
}

/// One stored document as the queries return it: its id, its optimistic revision, the last event of its aggregate it
/// has applied and the view.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredView<V> {
    pub id: String,
    pub revision: i64,
    pub last_sequence: usize,
    pub view: V,
}

/// The stored document's id, revision and last event: what a write needs besides the view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewMeta {
    /// Unique identifier of the view instance that is being modified.
    pub id: String,
    /// The current revision of the view instance, used for optimistic locking.
    pub revision: i64,
    /// The last event of the view's aggregate applied to this document; 0 for a new document.
    /// A document takes only event `last_sequence + 1` (see `ViewProjection`), so a repository
    /// stores it next to the version and writes it with the same optimistic check.
    pub last_sequence: usize,
}

impl ViewMeta {
    /// Convenience function to create a new [`ViewMeta`].
    pub fn new(id: String, revision: i64, last_sequence: usize) -> Self {
        Self {
            id,
            revision,
            last_sequence,
        }
    }
}
