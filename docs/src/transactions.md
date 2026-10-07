# Transactions

To manage transactions manually you can use `db.begin_transaction()`, and
then the Fluent API to add the operations needed in the transaction.

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure { some_id: String, some_string: String }
# const TEST_COLLECTION_NAME: &str = "test";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let mut transaction = db.begin_transaction().await?;

db.fluent()
    .update()
    .fields(paths!(MyTestStructure::{
      some_string
    }))
    .in_col(TEST_COLLECTION_NAME)
    .document_id("test-0")
    .object(&MyTestStructure {
        some_id: format!("test-0"),
        some_string: "UpdatedTest".to_string(),
    })
    .add_to_transaction(&mut transaction)?;

db.fluent()
    .delete()
    .from(TEST_COLLECTION_NAME)
    .document_id("test-5")
    .add_to_transaction(&mut transaction)?;

transaction.commit().await?;
# Ok(())
# }
```

You may also execute transactions that automatically retry with exponential backoff using `run_transaction`.

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure { test_string: String }
# const TEST_COLLECTION_NAME: &str = "test";
# const TEST_DOCUMENT_ID: &str = "test_doc_id";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
db.run_transaction(|db, transaction| {
    Box::pin(async move {
        let mut test_structure: MyTestStructure = db
            .fluent()
            .select()
            .by_id_in(TEST_COLLECTION_NAME)
            .obj()
            .one(TEST_DOCUMENT_ID)
            .await?
            .expect("Missing document");

        // Perform some kind of operation that depends on the state of the document
        test_structure.test_string += "a";

        db.fluent()
            .update()
            .fields(paths!(MyTestStructure::{
             test_string
            }))
            .in_col(TEST_COLLECTION_NAME)
            .document_id(TEST_DOCUMENT_ID)
            .object(&test_structure)
            .add_to_transaction(transaction)?;

        Ok(())
    })
})
.await?;
# Ok(())
# }
```

The transaction retries automatically on a transient failure: the callback returning
`BackoffError::Transient` (a plain `?` on a `FirestoreResult` inside the callback already converts
to one), or `commit()` answering `ABORTED`. Either way the whole transaction reruns from the start
as a new attempt, so the callback must be safe to run more than once; a failed attempt is rolled
back first, releasing its locks, before the retry begins. A read inside the transaction that comes
back `ABORTED` is not retried on its own; it fails the attempt, which then retries as usual.

`Commit` is sent only once per attempt. Any failure other than `ABORTED`, such as `UNAVAILABLE` or a
dropped connection, is returned to the caller without retrying, since the writes may
already be applied, and the callback is not run again in that case.

Retries are capped at `max_retries` attempts after the first, 4 by default (5 attempts in total),
the same default as Google's official clients; `max_elapsed_time` adds an optional time cap on top,
off by default. Each retry waits a backoff delay first, or the delay the callback named through
`BackoffError::Transient`'s `retry_after`. Return `Err(BackoffError::Permanent(err))` instead for an
error that must not be retried; it stops immediately and comes back wrapped in
`FirestoreError::ErrorInTransaction`.

Set `max_retries` and `max_elapsed_time` through `run_transaction_with_options`:

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure { test_string: String }
# const TEST_COLLECTION_NAME: &str = "test";
# const TEST_DOCUMENT_ID: &str = "test_doc_id";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let options = FirestoreTransactionOptions::new()
    .with_max_retries(2)
    .with_max_elapsed_time(FirestoreDuration::from_secs(30));

db.run_transaction_with_options(
    |db, _transaction| {
        Box::pin(async move {
            let _existing: Option<MyTestStructure> = db
                .fluent()
                .select()
                .by_id_in(TEST_COLLECTION_NAME)
                .obj()
                .one(TEST_DOCUMENT_ID)
                .await?;
            Ok(())
        })
    },
    options,
)
.await?;
# Ok(())
# }
```

See the complete example available [here](https://github.com/abdolence/firestore-rs/blob/master/examples/read-write-transactions.rs).

Please note that Firestore doesn't support creating documents in the transactions (generating
document IDs automatically), so you need to use `update()` to implicitly create documents and specifying your own IDs.

## Cancellation

```rust,no_run
# use firestore::*;
# use firestore::errors::{BackoffError, FirestoreError};
# use std::time::Duration;
# async fn example(db: FirestoreDb) -> FirestoreResult<Option<()>> {
let cancellation = FirestoreTransactionCancellation::new(
    tokio::time::sleep(Duration::from_secs(10)),
)
.with_settlement_timeout(Duration::from_secs(5));

db.run_transaction_cancellable(
    |_, transaction| Box::pin(async move {
        transaction.delete_by_id("items", "expired", None)?;
        Ok::<_, BackoffError<FirestoreError>>(())
    }),
    FirestoreTransactionOptions::new(),
    cancellation,
)
.await
# }
```

Cancellation interrupts the callback or retry delay and rolls back the open transaction. A pending
Begin is awaited to recover its ID for rollback. A pending Commit is awaited. The result is
`Some(value)` if committed or `None` if cancelled without committing. Errors use `FirestoreError`,
as with the other runners.

`with_settlement_timeout` bounds the wait after cancellation; Begin and its rollback share that
budget. Without it, there is no deadline. Expiry during Commit returns a `TransactionSettlementTimeout`
system error. As with other Commit failures apart from `ABORTED`, the writes may have been applied;
do not automatically retry. Rollback failures and timeouts are logged.

Keep awaiting the transaction after cancellation. Dropping its future abandons cleanup.
