// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use crate::cqrs::{aggregate::Aggregate, projection::View, store::PersistenceError};
use std::future::Future;

/// Handles the database access needed for a ViewProjection.
pub trait ViewRepository<V, A>: Send + Sync
where
    V: View<A>,
    A: Aggregate,
{
    /// Returns the current view instance.
    fn load(&self, view_id: &str) -> impl Future<Output = Result<Option<V>, PersistenceError>> + Send;

    /// Returns the current view instance and context, used by the `ViewProjection` to update
    /// views with committed events.
    fn load_with_context(&self, view_id: &str)
    -> impl Future<Output = Result<Option<(V, ViewContext)>, PersistenceError>> + Send;

    /// Updates the view instance and context, used by the `ViewProjection` to update
    /// views with committed events.
    fn update_view(&self, view: V, context: ViewContext) -> impl Future<Output = Result<(), PersistenceError>> + Send;
}

/// A data structure maintaining context when updating views.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewContext {
    /// Unique identifier of the view instance that is being modified.
    pub view_instance_id: String,
    /// The current version of the view instance, used for optimistic locking.
    pub version: i64,
    /// The last event of the view's aggregate applied to this document; 0 for a new document.
    /// A document takes only event `last_sequence + 1` (see `ViewProjection`), so a repository
    /// stores it next to the version and writes it with the same optimistic check.
    pub last_sequence: usize,
}

impl ViewContext {
    /// Convenience function to create a new [`ViewContext`].
    pub fn new(view_instance_id: String, version: i64, last_sequence: usize) -> Self {
        Self {
            view_instance_id,
            version,
            last_sequence,
        }
    }
}
