use super::*;
use crate::db::fake_firestore::{FakeFirestore, FakeResponse};
use gcloud_sdk::google::firestore::v1::*;
use gcloud_sdk::prost::Message;
use gcloud_sdk::tonic::Code;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

#[derive(Default)]
struct Calls {
    requests: Mutex<Vec<(String, Vec<u8>)>>,
    changed: Notify,
}

impl Calls {
    fn transactions(&self, method: &str) -> Vec<Vec<u8>> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(name, _)| name == method)
            .map(|(_, id)| id.clone())
            .collect()
    }

    async fn wait_for(&self, method: &str, count: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.transactions(method).len() < count {
                self.changed.notified().await;
            }
        })
        .await
        .expect("request reached the server");
    }

    async fn assert_rollbacks(&self, expected: &[Vec<u8>]) {
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(self.transactions("Rollback"), expected);
    }
}

async fn fixture(
    hold: Option<&'static str>,
    fail: Option<&'static str>,
) -> (FakeFirestore, Arc<Calls>) {
    let calls = Arc::new(Calls::default());
    let recorded = calls.clone();
    let server = FakeFirestore::start(move |path, bytes| {
        let method = path.rsplit('/').next().unwrap();
        let (id, response) = match method {
            "BeginTransaction" => (
                vec![1],
                BeginTransactionResponse {
                    transaction: vec![1],
                }
                .encode_to_vec(),
            ),
            "GetDocument" => {
                let request = GetDocumentRequest::decode(bytes).unwrap();
                let Some(get_document_request::ConsistencySelector::Transaction(id)) =
                    request.consistency_selector
                else {
                    panic!("expected a transaction-scoped read");
                };
                (id, Document::default().encode_to_vec())
            }
            "Commit" => (
                CommitRequest::decode(bytes).unwrap().transaction,
                CommitResponse::default().encode_to_vec(),
            ),
            "Rollback" => (RollbackRequest::decode(bytes).unwrap().transaction, vec![]),
            _ => panic!("unexpected RPC: {method}"),
        };
        let first = {
            let mut requests = recorded.requests.lock().unwrap();
            let first = !requests.iter().any(|(name, _)| name == method);
            requests.push((method.to_owned(), id));
            first
        };
        recorded.changed.notify_one();
        let response = if hold == Some(method) && first {
            FakeResponse::Hang
        } else if fail == Some(method) {
            FakeResponse::Status(Code::Unavailable)
        } else {
            FakeResponse::Message(response)
        };
        (method.to_owned(), response)
    })
    .await;
    (server, calls)
}

#[tokio::test]
async fn cancelled_transaction_read_rolls_back_without_committing() {
    let (server, calls) = fixture(Some("GetDocument"), None).await;
    {
        let run = server.db.run_transaction(|db, _| {
            Box::pin(async move {
                let _: Option<Document> = db.fluent().select().by_id_in("items").one("one").await?;
                Ok::<_, BackoffError<FirestoreError>>(())
            })
        });
        tokio::pin!(run);
        tokio::select! {
            result = &mut run => panic!("read must be held: {result:?}"),
            () = calls.wait_for("GetDocument", 1) => {},
        }
    }
    calls.wait_for("Rollback", 1).await;
    calls.assert_rollbacks(&[vec![1]]).await;
    assert!(calls.transactions("Commit").is_empty());
}

#[tokio::test]
async fn cancelled_explicit_rollback_gets_one_bounded_cleanup_attempt() {
    let (server, calls) = fixture(Some("Rollback"), None).await;
    {
        let rollback = server.db.begin_transaction().await.unwrap().rollback();
        tokio::pin!(rollback);
        tokio::select! {
            result = &mut rollback => panic!("rollback must be held: {result:?}"),
            () = calls.wait_for("Rollback", 1) => {},
        }
    }
    calls.wait_for("Rollback", 2).await;
    calls.assert_rollbacks(&[vec![1], vec![1]]).await;
}

#[tokio::test]
async fn completed_rollback_is_not_retried_even_if_the_response_is_an_error() {
    for fail in [None, Some("Rollback")] {
        let (server, calls) = fixture(None, fail).await;
        let result = server
            .db
            .begin_transaction()
            .await
            .unwrap()
            .rollback()
            .await;
        assert_eq!(result.is_err(), fail.is_some());
        calls.assert_rollbacks(&[vec![1]]).await;
        assert!(calls.transactions("Commit").is_empty());
    }
}

#[tokio::test]
async fn cancelled_commit_is_never_rolled_back() {
    let (server, calls) = fixture(Some("Commit"), None).await;
    {
        let mut transaction = server.db.begin_transaction().await.unwrap();
        transaction.delete_by_id("items", "one", None).unwrap();
        let commit = transaction.commit();
        tokio::pin!(commit);
        tokio::select! {
            result = &mut commit => panic!("commit must be held: {result:?}"),
            () = calls.wait_for("Commit", 1) => {},
        }
    }
    calls.assert_rollbacks(&[]).await;
    assert_eq!(calls.transactions("Commit"), [vec![1]]);
}

#[tokio::test]
async fn completed_commit_is_never_rolled_back_even_if_the_response_is_ambiguous() {
    for fail in [None, Some("Commit")] {
        let (server, calls) = fixture(None, fail).await;
        let mut transaction = server.db.begin_transaction().await.unwrap();
        transaction.delete_by_id("items", "one", None).unwrap();
        let result = transaction.commit().await;
        assert_eq!(result.is_err(), fail.is_some());
        calls.assert_rollbacks(&[]).await;
        assert_eq!(calls.transactions("Commit"), [vec![1]]);
    }
}

#[tokio::test]
async fn cancellation_after_retry_rolls_back_the_current_attempt() {
    use crate::db::fake_firestore::begin_response;
    use std::sync::atomic::AtomicU8;

    let begins = AtomicU8::new(0);
    let server = FakeFirestore::start(move |method, bytes| {
        if method.ends_with("/BeginTransaction") {
            begin_response(&begins)
        } else {
            assert!(method.ends_with("/Rollback"), "unexpected RPC: {method}");
            let request = RollbackRequest::decode(bytes).unwrap();
            (
                format!("Rollback({})", request.transaction[0]),
                FakeResponse::empty(),
            )
        }
    })
    .await;
    let entered = Arc::new(Notify::new());
    {
        let run = server.db.run_transaction(|_, transaction| {
            let entered = entered.clone();
            Box::pin(async move {
                if transaction.transaction_id() == &[1] {
                    return Err::<(), _>(BackoffError::retry_after(
                        std::io::Error::other("retry the first attempt"),
                        Duration::ZERO,
                    ));
                }
                entered.notify_one();
                std::future::pending().await
            })
        });
        tokio::pin!(run);
        tokio::select! {
            result = &mut run => panic!("second callback must remain pending: {result:?}"),
            result = tokio::time::timeout(Duration::from_secs(2), entered.notified()) => {
                result.expect("second callback started");
            },
        }
    }
    tokio::time::timeout(Duration::from_secs(2), server.wait_for_calls(4))
        .await
        .expect("cancelled retry rolled back");
    assert_eq!(
        server.calls(),
        ["Begin→1", "Rollback(1)", "Begin→2", "Rollback(2)"]
    );
}
