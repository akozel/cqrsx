//! The rebuild of one concert hall: the owner of the halls' checkpoint, on whatever node, replaces the document from
//! the concert's history through the hall's own worker; nothing else is touched.
use crate::{
    concert::application::views::concert_hall::ConcertHallView,
    shared::{application::StorageError, domain::ConcertId},
};
use cqrsx::{projection::checkpoint::CheckpointError, view::View};
use cqrsx_engine::{Cqrs, views::ViewsError};

/// Asks the owner of the halls to rebuild the hall of `concert_id`. A store that fails, or the halls' row not
/// registered yet (the read side is starting), is a storage error with a message for the operator.
pub async fn rebuild_hall(cqrs: &Cqrs, concert_id: &ConcertId) -> Result<(), StorageError> {
    let requested = cqrs
        .views()
        .request_rebuild_key(ConcertHallView::NAME, concert_id.as_str())
        .await;
    requested.map_err(|error| match error {
        ViewsError::Checkpoint(CheckpointError::Invalid(_)) => {
            StorageError("the read side is starting: its checkpoints are not registered yet".into())
        }
        other => StorageError::new(other),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{concert::domain::Concert, testing::test_app};
    use cqrsx::projection::checkpoint::{CheckpointKey, CheckpointStore};
    use std::time::Duration;

    fn halls() -> CheckpointKey {
        CheckpointKey::of::<ConcertHallView, Concert>()
    }

    #[tokio::test(start_paused = true)]
    async fn one_concert_is_rebuilt_on_request_by_the_owner_of_the_halls() {
        let (cqrs, _) = test_app().await;
        let c1 = ConcertId::parse("c-1").unwrap();
        let starting = rebuild_hall(&cqrs, &c1).await.unwrap_err();
        assert_eq!(
            starting.0,
            "the read side is starting: its checkpoints are not registered yet"
        );
        let store = cqrs.checkpoints().unwrap();
        store.register(&halls()).await.unwrap();
        rebuild_hall(&cqrs, &c1).await.unwrap();
        let owner = store
            .claim(&halls(), "node-a", Duration::from_secs(10))
            .await
            .unwrap()
            .unwrap();
        let requests = store.take_rebuild_requests(&halls(), owner.token, 10).await.unwrap().unwrap();
        assert_eq!((requests.rebuild_pace, requests.keys), (None, vec!["c-1".to_owned()]));
        store.set_unavailable(true);
        assert!(rebuild_hall(&cqrs, &c1).await.is_err(), "a store that does not answer");
    }
}
