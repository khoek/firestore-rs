use super::*;
use crate::FirestoreMemListenStateStorage;
use std::time::Duration;
use tokio::sync::Semaphore;

#[derive(Clone)]
struct BlockingDb {
    entered: Arc<Semaphore>,
    dropped: Arc<Semaphore>,
}

struct OnDrop(Arc<Semaphore>);
impl Drop for OnDrop {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}

#[async_trait]
impl FirestoreListenSupport for BlockingDb {
    async fn listen_doc_changes<'a, 'b>(
        &'a self,
        _: Vec<FirestoreListenerTargetParams>,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<ListenResponse>>> {
        let _active_request = OnDrop(self.dropped.clone());
        self.entered.add_permits(1);
        std::future::pending().await
    }
}

async fn signal(semaphore: &Semaphore) {
    tokio::time::timeout(Duration::from_secs(2), semaphore.acquire())
        .await
        .expect("operation completed")
        .unwrap()
        .forget();
}

async fn blocking_listener() -> (
    FirestoreListener<BlockingDb, FirestoreMemListenStateStorage>,
    BlockingDb,
) {
    let db = BlockingDb {
        entered: Arc::new(Semaphore::new(0)),
        dropped: Arc::new(Semaphore::new(0)),
    };
    let mut listener = FirestoreListener::new(
        db.clone(),
        FirestoreMemListenStateStorage::new(),
        FirestoreListenerParams::new(),
    )
    .await
    .unwrap();
    listener
        .add_target(FirestoreListenerTargetParams::new(
            FirestoreListenerTarget::new(1),
            FirestoreTargetType::Documents(FirestoreCollectionDocuments::new(
                "test".into(),
                vec!["one".into()],
            )),
            HashMap::new(),
        ))
        .unwrap();
    listener.start(|_| async { Ok(()) }).await.unwrap();
    signal(&db.entered).await;
    (listener, db)
}

#[tokio::test]
async fn shutdown_cancels_an_unanswered_listen_request() {
    let (mut listener, db) = blocking_listener().await;
    tokio::time::timeout(Duration::from_secs(2), listener.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        db.dropped.available_permits(),
        1,
        "shutdown joins request destruction"
    );
    listener.shutdown().await.unwrap();
    assert_eq!(db.dropped.available_permits(), 1);
}

#[tokio::test]
async fn dropping_a_listener_cancels_its_owned_request() {
    let (listener, db) = blocking_listener().await;
    drop(listener);
    signal(&db.dropped).await;
}

#[tokio::test]
async fn interrupted_shutdown_retains_the_task_for_a_later_join() {
    let (mut listener, db) = blocking_listener().await;
    {
        let shutdown = listener.shutdown();
        futures::pin_mut!(shutdown);
        assert!(futures::poll!(shutdown.as_mut()).is_pending());
    }
    assert!(listener.shutdown_handle.is_some());
    tokio::time::timeout(Duration::from_secs(2), listener.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(db.dropped.available_permits(), 1);
}

#[tokio::test]
async fn double_start_cannot_abandon_the_original_task() {
    let (mut listener, db) = blocking_listener().await;
    assert!(listener.start(|_| async { Ok(()) }).await.is_err());
    listener.shutdown().await.unwrap();
    assert_eq!(db.dropped.available_permits(), 1);
    assert!(listener.start(|_| async { Ok(()) }).await.is_err());
}

#[derive(Clone)]
struct EventDb;

#[async_trait]
impl FirestoreListenSupport for EventDb {
    async fn listen_doc_changes<'a, 'b>(
        &'a self,
        _: Vec<FirestoreListenerTargetParams>,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<ListenResponse>>> {
        Ok(futures::stream::iter([Ok(ListenResponse {
            response_type: Some(FirestoreListenEvent::DocumentChange(
                DocumentChange::default(),
            )),
        })])
        .chain(futures::stream::pending())
        .boxed())
    }
}

#[tokio::test]
async fn shutdown_cancels_a_callback_waiting_on_a_full_consumer_queue() {
    let entered = Arc::new(Semaphore::new(0));
    let dropped = Arc::new(Semaphore::new(0));
    let (sender, _receiver) = tokio::sync::mpsc::channel(1);
    sender.send(()).await.unwrap();
    let mut listener = FirestoreListener::new(
        EventDb,
        FirestoreMemListenStateStorage::new(),
        FirestoreListenerParams::new(),
    )
    .await
    .unwrap();
    listener
        .add_target(FirestoreListenerTargetParams::new(
            FirestoreListenerTarget::new(1),
            FirestoreTargetType::Documents(FirestoreCollectionDocuments::new(
                "test".into(),
                vec!["one".into()],
            )),
            HashMap::new(),
        ))
        .unwrap();
    let callback_entered = entered.clone();
    let callback_dropped = dropped.clone();
    listener
        .start(move |_| {
            let entered = callback_entered.clone();
            let dropped = callback_dropped.clone();
            let sender = sender.clone();
            async move {
                let _callback = OnDrop(dropped);
                entered.add_permits(1);
                sender.send(()).await?;
                Ok(())
            }
        })
        .await
        .unwrap();
    signal(&entered).await;
    tokio::time::timeout(Duration::from_secs(2), listener.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(dropped.available_permits(), 1);
}

#[derive(Clone)]
struct CurrentTokenDb;

#[async_trait]
impl FirestoreListenSupport for CurrentTokenDb {
    async fn listen_doc_changes<'a, 'b>(
        &'a self,
        _: Vec<FirestoreListenerTargetParams>,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<ListenResponse>>> {
        let changes = [
            TargetChange {
                target_change_type: target_change::TargetChangeType::Current as i32,
                target_ids: vec![1],
                resume_token: vec![1, 2, 3],
                ..Default::default()
            },
            TargetChange {
                target_change_type: target_change::TargetChangeType::NoChange as i32,
                target_ids: vec![1],
                ..Default::default()
            },
        ];
        Ok(futures::stream::iter(changes.into_iter().map(|change| {
            Ok(ListenResponse {
                response_type: Some(FirestoreListenEvent::TargetChange(change)),
            })
        }))
        .chain(futures::stream::pending())
        .boxed())
    }
}

#[tokio::test]
async fn current_with_token_is_delivered_before_its_resume_position_is_committed() {
    let storage = FirestoreMemListenStateStorage::new();
    let mut listener = FirestoreListener::new(
        CurrentTokenDb,
        storage.clone(),
        FirestoreListenerParams::new(),
    )
    .await
    .unwrap();
    let target = FirestoreListenerTarget::new(1);
    listener
        .add_target(FirestoreListenerTargetParams::new(
            target.clone(),
            FirestoreTargetType::Documents(FirestoreCollectionDocuments::new(
                "test".into(),
                vec!["one".into()],
            )),
            HashMap::new(),
        ))
        .unwrap();
    let (events, mut received) = tokio::sync::mpsc::channel(2);
    let callback_storage = storage.clone();
    let callback_target = target.clone();
    listener
        .start(move |event| {
            let storage = callback_storage.clone();
            let target = callback_target.clone();
            let events = events.clone();
            async move {
                let FirestoreListenEvent::TargetChange(change) = event else {
                    panic!("expected target change")
                };
                let state = storage.read_resume_state(&target).await?;
                if change.target_change_type == target_change::TargetChangeType::Current as i32 {
                    assert!(
                        state.is_none(),
                        "a resume token cannot cover unconsumed callbacks"
                    );
                } else {
                    let Some(FirestoreListenerTargetResumeType::Token(token)) = state else {
                        panic!("Current token was not persisted")
                    };
                    assert_eq!(token.into_value(), [1, 2, 3]);
                }
                events.send(change.target_change_type).await?;
                Ok(())
            }
        })
        .await
        .unwrap();
    let observed = tokio::time::timeout(Duration::from_secs(2), async {
        [
            received.recv().await.unwrap(),
            received.recv().await.unwrap(),
        ]
    })
    .await
    .unwrap();
    assert_eq!(
        observed,
        [
            target_change::TargetChangeType::Current as i32,
            target_change::TargetChangeType::NoChange as i32
        ]
    );
    listener.shutdown().await.unwrap();
}
