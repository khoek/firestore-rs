use super::*;
use crate::db::fake_firestore::{begin_response, read_transaction_id, FakeFirestore, FakeResponse};
use crate::FirestoreGetByIdSupport;
use futures::FutureExt;
use gcloud_sdk::google::firestore::v1::{
    BeginTransactionRequest, BeginTransactionResponse, Document,
};
use gcloud_sdk::prost::Message;
use std::future::{pending, Future};
use std::pin::pin;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{oneshot, Notify};
use tokio::time::timeout;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

async fn fixture<F>(override_response: F) -> FakeFirestore
where
    F: Fn(&str) -> Option<FakeResponse> + Send + Sync + 'static,
{
    let begins = AtomicU8::new(0);
    FakeFirestore::start(move |method, bytes| {
        let (label, response, options) = if method.ends_with("/BeginTransaction") {
            let request = BeginTransactionRequest::decode(bytes).unwrap();
            if begins.load(Ordering::SeqCst) > 0 {
                let mode = request.options.unwrap().mode.unwrap();
                let gcloud_sdk::google::firestore::v1::transaction_options::Mode::ReadWrite(mode) =
                    mode
                else {
                    panic!("retry must be read-write");
                };
                assert_eq!(mode.retry_transaction, vec![1]);
            }
            let (label, response) = begin_response(&begins);
            (label, response, request.request_options)
        } else if method.ends_with("/GetDocument") {
            let label = format!("Get({})", read_transaction_id(bytes));
            return (
                label.clone(),
                override_response(&label)
                    .unwrap_or_else(|| FakeResponse::Message(Document::default().encode_to_vec())),
            );
        } else if method.ends_with("/Rollback") {
            let request = RollbackRequest::decode(bytes).unwrap();
            (
                format!("Rollback({})", request.transaction[0]),
                FakeResponse::empty(),
                request.request_options,
            )
        } else {
            assert!(method.ends_with("/Commit"));
            let request = CommitRequest::decode(bytes).unwrap();
            (
                format!("Commit({})", request.transaction[0]),
                FakeResponse::committed(),
                request.request_options,
            )
        };
        assert_eq!(options.unwrap().request_tags, vec!["cancellation-test"]);
        let response = override_response(&label).unwrap_or(response);
        (label, response)
    })
    .await
}

fn options() -> FirestoreTransactionOptions {
    FirestoreTransactionOptions::new()
        .with_request_options(FirestoreRequestOptions::from_tags(["cancellation-test"]))
}

fn signal() -> (oneshot::Sender<()>, impl Future<Output = ()>) {
    let (send, receive) = oneshot::channel();
    (send, async { receive.await.unwrap() })
}

fn delayed(release: &Arc<Notify>, response: FakeResponse) -> FakeResponse {
    FakeResponse::Delayed(release.clone(), Box::new(response))
}

fn ready<'a>(
    _: FirestoreDb,
    _: &'a mut FirestoreTransaction<'_>,
) -> BoxFuture<'a, Result<(), BackoffError<std::io::Error>>> {
    Box::pin(async { Ok(()) })
}

#[tokio::test]
async fn cancellation_before_begin_sends_nothing_and_never_invokes_callback() {
    let server = fixture(|_| None).await;
    let result: FirestoreResult<Option<()>> = server
        .db
        .run_transaction_cancellable::<(), _, std::io::Error, _>(
            |_, _| panic!("cancelled callback must not even be constructed"),
            options(),
            FirestoreTransactionCancellation::new(async {}),
        )
        .await;
    assert!(result.unwrap().is_none());
    assert!(server.calls().is_empty());
}

#[tokio::test]
async fn cancelled_begin_recovers_id_and_rolls_back_without_callback() {
    let release = Arc::new(Notify::new());
    let server_release = release.clone();
    let server = fixture(move |call| {
        (call == "Begin→1").then(|| {
            let (_, response) = begin_response(&AtomicU8::new(0));
            delayed(&server_release, response)
        })
    })
    .await;
    let (cancel, signal) = signal();
    let callbacks = AtomicUsize::new(0);
    let run = server.db.run_transaction_cancellable(
        |db, transaction| {
            callbacks.fetch_add(1, Ordering::SeqCst);
            ready(db, transaction)
        },
        options(),
        FirestoreTransactionCancellation::new(signal),
    );
    let control = async {
        server.wait_for_calls(1).await;
        cancel.send(()).unwrap();
        release.notify_one();
    };
    let (result, ()) = timeout(TEST_TIMEOUT, async { tokio::join!(run, control) })
        .await
        .unwrap();
    assert!(result.unwrap().is_none());
    assert_eq!(callbacks.load(Ordering::SeqCst), 0);
    assert_eq!(server.calls(), ["Begin→1", "Rollback(1)"]);
}

#[tokio::test]
async fn cancelled_begin_with_no_response_stops_at_settlement_deadline() {
    let server = fixture(|_| Some(FakeResponse::Hang)).await;
    let (cancel, signal) = signal();
    let run = server.db.run_transaction_cancellable(
        ready,
        options(),
        FirestoreTransactionCancellation::new(signal)
            .with_settlement_timeout(Duration::from_millis(20)),
    );
    let control = async {
        server.wait_for_calls(1).await;
        cancel.send(()).unwrap();
    };
    let (result, ()) = timeout(TEST_TIMEOUT, async { tokio::join!(run, control) })
        .await
        .unwrap();
    assert!(result.unwrap().is_none());
    assert_eq!(server.calls(), ["Begin→1"]);
}

#[tokio::test]
async fn cancellation_interrupts_a_read_then_rolls_back() {
    let server = fixture(|call| (call == "Get(1)").then_some(FakeResponse::Hang)).await;
    let (cancel, signal) = signal();
    let run = server.db.run_transaction_cancellable(
        |db, _| {
            Box::pin(async move {
                db.get_doc("items", "one", None).await?;
                Ok::<_, BackoffError<FirestoreError>>(())
            })
        },
        options(),
        FirestoreTransactionCancellation::new(signal),
    );
    let control = async {
        server.wait_for_calls(2).await;
        cancel.send(()).unwrap();
    };
    let (result, ()) = timeout(TEST_TIMEOUT, async { tokio::join!(run, control) })
        .await
        .unwrap();
    assert!(result.unwrap().is_none());
    assert_eq!(server.calls(), ["Begin→1", "Get(1)", "Rollback(1)"]);
}

#[tokio::test]
async fn cancellation_at_callback_success_prevents_commit() {
    let server = fixture(|_| None).await;
    let (cancel, signal) = signal();
    let cancel = Arc::new(Mutex::new(Some(cancel)));
    let result: FirestoreResult<Option<()>> = server
        .db
        .run_transaction_cancellable(
            |_, transaction| {
                let cancel = cancel.clone();
                Box::pin(async move {
                    transaction.delete_by_id("items", "one", None).unwrap();
                    cancel.lock().unwrap().take().unwrap().send(()).unwrap();
                    Ok::<_, BackoffError<std::io::Error>>(())
                })
            },
            options(),
            FirestoreTransactionCancellation::new(signal),
        )
        .await;
    assert!(result.unwrap().is_none());
    assert_eq!(server.calls(), ["Begin→1", "Rollback(1)"]);
}

fn fail_after_read<'a>(
    db: FirestoreDb,
    _: &'a mut FirestoreTransaction<'_>,
) -> BoxFuture<'a, Result<(), BackoffError<std::io::Error>>> {
    Box::pin(async move {
        db.get_doc("items", "one", None).await.unwrap();
        Err(BackoffError::retry_after(
            std::io::Error::other("callback failed"),
            Duration::from_secs(3600),
        ))
    })
}

#[tokio::test]
async fn cancellation_in_backoff_never_begins_another_transaction() {
    for rollback_code in [Code::Ok, Code::Unavailable] {
        let server = fixture(move |call| {
            (call == "Rollback(1)" && rollback_code != Code::Ok)
                .then_some(FakeResponse::Status(rollback_code))
        })
        .await;
        let (cancel, signal) = signal();
        let mut run = pin!(server.db.run_transaction_cancellable(
            fail_after_read,
            options(),
            FirestoreTransactionCancellation::new(signal),
        ));
        // Wait for rollback to finish and retry backoff to begin.
        assert!(timeout(Duration::from_millis(100), &mut run).await.is_err());
        assert_eq!(server.calls(), ["Begin→1", "Get(1)", "Rollback(1)"]);
        cancel.send(()).unwrap();
        assert!(timeout(TEST_TIMEOUT, run).await.unwrap().unwrap().is_none());
        assert_eq!(server.calls(), ["Begin→1", "Get(1)", "Rollback(1)"]);
    }
}

#[tokio::test]
async fn cancellation_during_rollback_awaits_the_rpc() {
    for outcome in [Code::Ok, Code::Unavailable] {
        let release = Arc::new(Notify::new());
        let server_release = release.clone();
        let server = fixture(move |call| {
            (call == "Rollback(1)").then(|| {
                delayed(
                    &server_release,
                    if outcome == Code::Ok {
                        FakeResponse::empty()
                    } else {
                        FakeResponse::Status(outcome)
                    },
                )
            })
        })
        .await;
        let (cancel, signal) = signal();
        let mut run = pin!(server.db.run_transaction_cancellable(
            fail_after_read,
            options(),
            FirestoreTransactionCancellation::new(signal),
        ));
        tokio::select! {
            result = &mut run => panic!("rollback should be pending: {result:?}"),
            result = timeout(TEST_TIMEOUT, server.wait_for_calls(3)) => result.unwrap(),
        }
        cancel.send(()).unwrap();
        assert!(run.as_mut().now_or_never().is_none());
        release.notify_one();
        assert!(timeout(TEST_TIMEOUT, run).await.unwrap().unwrap().is_none());
        assert_eq!(server.calls(), ["Begin→1", "Get(1)", "Rollback(1)"]);
    }
}

#[tokio::test]
async fn rollback_deadline_preserves_a_permanent_callback_error() {
    let server = fixture(|call| (call == "Rollback(1)").then_some(FakeResponse::Hang)).await;
    let (cancel, signal) = signal();
    let run = server.db.run_transaction_cancellable(
        |db, _| {
            Box::pin(async move {
                db.get_doc("items", "one", None).await.unwrap();
                Err::<(), _>(BackoffError::permanent(std::io::Error::other(
                    "callback failed",
                )))
            })
        },
        options(),
        FirestoreTransactionCancellation::new(signal)
            .with_settlement_timeout(Duration::from_millis(20)),
    );
    let control = async {
        server.wait_for_calls(3).await;
        cancel.send(()).unwrap();
    };
    let (result, ()) = timeout(TEST_TIMEOUT, async { tokio::join!(run, control) })
        .await
        .unwrap();
    let FirestoreError::ErrorInTransaction(error) = result.unwrap_err() else {
        panic!("expected callback error");
    };
    assert_eq!(error.transaction_id, vec![1]);
    assert_eq!(
        error
            .source
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .to_string(),
        "callback failed"
    );
    assert_eq!(server.calls(), ["Begin→1", "Get(1)", "Rollback(1)"]);
}

#[tokio::test]
async fn cancellation_during_commit_preserves_success_or_uncertainty_without_replay() {
    for outcome in [Code::Ok, Code::Aborted, Code::Unavailable] {
        let release = Arc::new(Notify::new());
        let server_release = release.clone();
        let server = fixture(move |call| {
            (call == "Commit(1)").then(|| {
                delayed(
                    &server_release,
                    if outcome == Code::Ok {
                        FakeResponse::committed()
                    } else {
                        FakeResponse::Status(outcome)
                    },
                )
            })
        })
        .await;
        let (cancel, signal) = signal();
        let mut run = pin!(server.db.run_transaction_cancellable(
            |_, transaction| {
                Box::pin(async move {
                    transaction.delete_by_id("items", "one", None).unwrap();
                    Ok::<_, BackoffError<std::io::Error>>(42)
                })
            },
            options(),
            FirestoreTransactionCancellation::new(signal),
        ));
        tokio::select! {
            result = &mut run => panic!("commit should be pending: {result:?}"),
            result = timeout(TEST_TIMEOUT, server.wait_for_calls(2)) => result.unwrap(),
        }
        cancel.send(()).unwrap();
        assert!(run.as_mut().now_or_never().is_none(), "commit must settle");
        release.notify_one();
        let result = timeout(TEST_TIMEOUT, run).await.unwrap();
        match outcome {
            Code::Ok => assert_eq!(result.unwrap(), Some(42)),
            Code::Aborted => assert!(result.unwrap().is_none()),
            _ => assert!(matches!(result, Err(FirestoreError::DatabaseError(error))
                if error.public.code == "Unavailable" && !error.retry_possible)),
        }
        assert_eq!(server.calls(), ["Begin→1", "Commit(1)"]);
    }
}

#[tokio::test]
async fn stalled_commit_is_unknown_after_settlement_deadline() {
    let server = fixture(|call| (call == "Commit(1)").then_some(FakeResponse::Hang)).await;
    let (cancel, signal) = signal();
    let run = server.db.run_transaction_cancellable(
        ready,
        options(),
        FirestoreTransactionCancellation::new(signal)
            .with_settlement_timeout(Duration::from_millis(20)),
    );
    let control = async {
        server.wait_for_calls(2).await;
        cancel.send(()).unwrap();
    };
    let (result, ()) = timeout(TEST_TIMEOUT, async { tokio::join!(run, control) })
        .await
        .unwrap();
    assert!(matches!(firestore_err_to_backoff(result.unwrap_err()),
            BackoffError::Permanent(FirestoreError::SystemError(error))
            if error.public.code == "TransactionSettlementTimeout" && error.message.contains("outcome is unknown")));
    assert_eq!(server.calls(), ["Begin→1", "Commit(1)"]);
}

#[tokio::test]
async fn default_limit_is_five_attempts_and_non_send_borrowed_callbacks_still_work() {
    let server = fixture(|_| None).await;
    let attempts = std::rc::Rc::new(std::cell::Cell::new(0));
    let result: FirestoreResult<Option<()>> = server
        .db
        .run_transaction_cancellable(
            |_, _| {
                attempts.set(attempts.get() + 1);
                Box::pin(async {
                    Err(BackoffError::retry_after(
                        std::io::Error::other("retry"),
                        Duration::ZERO,
                    ))
                })
            },
            options(),
            FirestoreTransactionCancellation::new(pending()),
        )
        .await;
    assert!(matches!(result, Err(FirestoreError::ErrorInTransaction(_))));
    assert_eq!(attempts.get(), 5);
    assert_eq!(
        server.calls(),
        [
            "Begin→1",
            "Rollback(1)",
            "Begin→2",
            "Rollback(2)",
            "Begin→3",
            "Rollback(3)",
            "Begin→4",
            "Rollback(4)",
            "Begin→5",
            "Rollback(5)"
        ]
    );
}

#[tokio::test]
async fn dropping_the_runner_does_not_spawn_cleanup() {
    let server = fixture(|call| (call == "Get(1)").then_some(FakeResponse::Hang)).await;
    let mut run = Box::pin(server.db.run_transaction_cancellable(
        fail_after_read,
        options(),
        FirestoreTransactionCancellation::new(pending()),
    ));
    tokio::select! {
        result = &mut run => panic!("read should be pending: {result:?}"),
        result = timeout(TEST_TIMEOUT, server.wait_for_calls(2)) => result.unwrap(),
    }
    drop(run);
    assert!(timeout(Duration::from_millis(50), server.wait_for_calls(3))
        .await
        .is_err());
    assert_eq!(server.calls(), ["Begin→1", "Get(1)"]);
}

#[tokio::test]
async fn cancelled_retry_begin_rolls_back_its_own_id_without_another_callback() {
    let release = Arc::new(Notify::new());
    let server_release = release.clone();
    let server = fixture(move |call| {
        (call == "Begin→2").then(|| {
            delayed(
                &server_release,
                FakeResponse::Message(
                    BeginTransactionResponse {
                        transaction: vec![2],
                    }
                    .encode_to_vec(),
                ),
            )
        })
    })
    .await;
    let (cancel, signal) = signal();
    let callbacks = AtomicUsize::new(0);
    let run = server.db.run_transaction_cancellable(
        |_, _| {
            callbacks.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                Err::<(), _>(BackoffError::retry_after(
                    std::io::Error::other("retry"),
                    Duration::ZERO,
                ))
            })
        },
        options(),
        FirestoreTransactionCancellation::new(signal),
    );
    let control = async {
        server.wait_for_calls(3).await;
        cancel.send(()).unwrap();
        release.notify_one();
    };
    let (result, ()) = timeout(TEST_TIMEOUT, async { tokio::join!(run, control) })
        .await
        .unwrap();
    assert!(result.unwrap().is_none());
    assert_eq!(callbacks.load(Ordering::SeqCst), 1);
    assert_eq!(
        server.calls(),
        ["Begin→1", "Rollback(1)", "Begin→2", "Rollback(2)"]
    );
}

#[tokio::test]
async fn failed_begin_after_cancellation_preserves_the_error_without_retrying() {
    let release = Arc::new(Notify::new());
    let server_release = release.clone();
    let server = fixture(move |_| {
        Some(delayed(
            &server_release,
            FakeResponse::Status(Code::Unavailable),
        ))
    })
    .await;
    let (cancel, signal) = signal();
    let run = server.db.run_transaction_cancellable(
        ready,
        options(),
        FirestoreTransactionCancellation::new(signal),
    );
    let control = async {
        server.wait_for_calls(1).await;
        cancel.send(()).unwrap();
        release.notify_one();
    };
    let (result, ()) = timeout(TEST_TIMEOUT, async { tokio::join!(run, control) })
        .await
        .unwrap();
    assert!(
        matches!(result, Err(FirestoreError::DatabaseError(error)) if error.public.code == "Unavailable")
    );
    assert_eq!(server.calls(), ["Begin→1"]);
}

#[tokio::test]
async fn lost_commit_response_is_unknown_even_without_cancellation() {
    let server = fixture(|call| (call == "Commit(1)").then_some(FakeResponse::Drop)).await;
    let db = server.db.clone();
    let result = timeout(
        TEST_TIMEOUT,
        tokio::spawn(async move {
            db.run_transaction_cancellable(
                ready,
                options(),
                FirestoreTransactionCancellation::new(pending()),
            )
            .await
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(result, Err(FirestoreError::DatabaseError(error))
        if error.public.code == "CONNECTION_ERROR" && !error.retry_possible));
    assert_eq!(server.calls(), ["Begin→1", "Commit(1)"]);
}

#[tokio::test]
async fn rollback_failure_does_not_change_cancellation_to_an_error() {
    let server = fixture(|call| match call {
        "Get(1)" => Some(FakeResponse::Hang),
        "Rollback(1)" => Some(FakeResponse::Status(Code::Unavailable)),
        _ => None,
    })
    .await;
    let (cancel, signal) = signal();
    let run = server.db.run_transaction_cancellable(
        fail_after_read,
        options(),
        FirestoreTransactionCancellation::new(signal),
    );
    let control = async {
        server.wait_for_calls(2).await;
        cancel.send(()).unwrap();
    };
    let (result, ()) = timeout(TEST_TIMEOUT, async { tokio::join!(run, control) })
        .await
        .unwrap();
    assert!(result.unwrap().is_none());
    assert_eq!(server.calls(), ["Begin→1", "Get(1)", "Rollback(1)"]);
}

#[tokio::test]
async fn first_begin_failure_is_returned_without_retrying() {
    let server = fixture(|_| Some(FakeResponse::Status(Code::Unavailable))).await;
    let result: FirestoreResult<Option<()>> = server
        .db
        .run_transaction_cancellable::<(), _, std::io::Error, _>(
            |_, _| panic!("failed Begin must not invoke the callback"),
            options(),
            FirestoreTransactionCancellation::new(pending()),
        )
        .await;
    assert!(matches!(
        result,
        Err(FirestoreError::DatabaseError(error))
            if error.public.code == "Unavailable"
    ));
    assert_eq!(server.calls(), ["Begin→1"]);
}
