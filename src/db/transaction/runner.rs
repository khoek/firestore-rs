use super::*;
use backoff::backoff::Backoff;
use backoff::ExponentialBackoffBuilder;
use futures::FutureExt;
use std::future::Future;
use std::pin::{pin, Pin};
use std::time::Duration;
use tokio::time::Instant;

struct CancellationState<'a, C> {
    signal: Option<Pin<&'a mut C>>,
    timeout: Option<Duration>,
    deadline: Option<Instant>,
}

impl<C: Future<Output = ()>> CancellationState<'_, C> {
    async fn cancelled(&mut self) {
        if let Some(signal) = &mut self.signal {
            signal.as_mut().await;
            self.signal = None;
            self.deadline = self
                .timeout
                .and_then(|timeout| Instant::now().checked_add(timeout));
        }
    }

    fn is_cancelled(&mut self) -> bool {
        self.cancelled().now_or_never().is_some()
    }

    async fn interrupt<F: Future>(&mut self, work: F) -> Option<F::Output> {
        tokio::select! {
            biased;
            () = self.cancelled() => None,
            result = work => Some(result),
        }
    }

    async fn settle<F: Future>(&mut self, work: F) -> Result<F::Output, ()> {
        let mut work = pin!(work);
        if self.signal.is_some() {
            tokio::select! {
                // Prefer a ready commit response over cancellation.
                biased;
                result = &mut work => return Ok(result),
                () = self.cancelled() => {}
            }
        }
        match self.deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, work)
                .await
                .map_err(|_| ()),
            None => Ok(work.await),
        }
    }

    async fn rollback(&mut self, transaction: FirestoreTransaction<'_>) {
        let span = transaction.data.transaction_span.clone();
        match self.settle(transaction.rollback()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                span.in_scope(|| warn!(%error, "Transaction rollback failed."));
            }
            Err(()) => {
                span.in_scope(|| warn!("Transaction rollback timed out."));
            }
        }
    }
}

impl FirestoreDb {
    /// Same as [`run_transaction_with_options`](Self::run_transaction_with_options), with
    /// cancellation.
    ///
    /// Cancellation interrupts `func` or retry backoff and rolls back the open transaction. A
    /// pending Begin is awaited to recover its ID for rollback. No further attempts are made.
    /// A pending Commit is awaited. Returns `Some(value)` on commit success or `None` on
    /// cancellation without a commit. Commit failures other than `ABORTED`, including a
    /// settlement timeout, are returned without retrying: the writes may have been applied.
    ///
    /// Keep awaiting this future after cancellation; dropping it abandons cleanup.
    pub async fn run_transaction_cancellable<T, FN, E, C>(
        &self,
        func: FN,
        mut options: FirestoreTransactionOptions,
        cancellation: FirestoreTransactionCancellation<C>,
    ) -> FirestoreResult<Option<T>>
    where
        for<'b> FN: Fn(
            FirestoreDb,
            &'b mut FirestoreTransaction,
        ) -> BoxFuture<'b, std::result::Result<T, BackoffError<E>>>,
        E: std::error::Error + Send + Sync + 'static,
        C: Future<Output = ()>,
    {
        let signal = pin!(cancellation.signal);
        let mut cancellation = CancellationState {
            signal: Some(signal),
            timeout: cancellation.settlement_timeout,
            deadline: None,
        };
        let max_elapsed_time = options
            .max_elapsed_time
            .map(Duration::try_from)
            .transpose()?;
        if cancellation.is_cancelled() {
            return Ok(None);
        }
        let mut backoff = None;
        let mut retries = 0;

        loop {
            let error = match cancellation
                .settle(self.begin_transaction_with_options(options.clone()))
                .await
            {
                Err(()) => {
                    warn!("Cancelled BeginTransaction did not settle before its deadline.");
                    return Ok(None);
                }
                // First-Begin failures are returned directly; every retry names this first ID.
                Ok(Err(error)) if retries == 0 => BackoffError::permanent(error),
                Ok(Err(error)) => firestore_err_to_backoff(error),
                Ok(Ok(mut transaction)) => {
                    let transaction_id = transaction.transaction_id().clone();
                    let span = transaction.data.transaction_span.clone();
                    if retries == 0 {
                        options.mode =
                            FirestoreTransactionMode::ReadWriteRetry(transaction_id.clone());
                    }
                    let db = self.clone_with_consistency_selector(
                        FirestoreConsistencySelector::Transaction(transaction_id.clone()),
                    );
                    let result = cancellation
                        .interrupt(async { func(db, &mut transaction).await })
                        .await;
                    let error = match result {
                        Some(Ok(value)) if !cancellation.is_cancelled() => {
                            match cancellation.settle(transaction.commit()).await {
                                Ok(Ok(_)) => return Ok(Some(value)),
                                Ok(Err(error)) => firestore_err_to_backoff(error),
                                Err(()) => {
                                    return Err(FirestoreError::SystemError(FirestoreSystemError::new(
                                        FirestoreErrorPublicGenericDetails::new(
                                            "TransactionSettlementTimeout".into(),
                                        ),
                                        "Transaction commit did not settle before the cancellation deadline; its outcome is unknown.".into(),
                                    )))
                                }
                            }
                        }
                        Some(Err(error)) => {
                            cancellation.rollback(transaction).await;
                            let in_transaction = |error: E| {
                                FirestoreError::ErrorInTransaction(
                                    FirestoreErrorInTransaction::new(
                                        transaction_id,
                                        Box::new(error),
                                    ),
                                )
                            };
                            match error {
                                BackoffError::Transient { err, retry_after } => {
                                    BackoffError::Transient {
                                        err: in_transaction(err),
                                        retry_after,
                                    }
                                }
                                BackoffError::Permanent(err) => {
                                    BackoffError::permanent(in_transaction(err))
                                }
                            }
                        }
                        _ => {
                            cancellation.rollback(transaction).await;
                            return Ok(None);
                        }
                    };
                    if let BackoffError::Transient { err, retry_after } = &error {
                        span.in_scope(|| {
                            warn!(%err, delay = ?retry_after, "Transient error occurred in transaction.");
                        });
                    }
                    error
                }
            };
            let (error, retry_after) = match error {
                BackoffError::Permanent(error) => return Err(error),
                BackoffError::Transient { err, retry_after } => (err, retry_after),
            };
            if cancellation.is_cancelled() {
                return Ok(None);
            }
            if retries == options.max_retries {
                return Err(error);
            }
            // Measure the elapsed-time bound from the first failed attempt.
            let backoff = backoff.get_or_insert_with(|| {
                ExponentialBackoffBuilder::new()
                    .with_max_elapsed_time(max_elapsed_time)
                    .build()
            });
            let Some(delay) = retry_after.or_else(|| backoff.next_backoff()) else {
                return Err(error);
            };
            if cancellation
                .interrupt(tokio::time::sleep(delay))
                .await
                .is_none()
                || cancellation.is_cancelled()
            {
                return Ok(None);
            }
            retries += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn settlement_shares_one_budget_and_never_repolls_a_completed_signal() {
        let mut signalled = false;
        let signal = pin!(std::future::poll_fn(|_| {
            assert!(!signalled, "completed cancellation signal polled twice");
            signalled = true;
            std::task::Poll::Ready(())
        }));
        let mut cancellation = CancellationState {
            signal: Some(signal),
            timeout: Some(Duration::from_secs(5)),
            deadline: None,
        };
        let start = Instant::now();
        assert_eq!(
            cancellation
                .settle(tokio::time::sleep(Duration::from_secs(3)))
                .await,
            Ok(())
        );
        assert!(cancellation.is_cancelled());
        assert_eq!(
            cancellation
                .settle(tokio::time::sleep(Duration::from_secs(3)))
                .await,
            Err(())
        );
        assert_eq!(Instant::now() - start, Duration::from_secs(5));
        assert!(cancellation.is_cancelled());
        assert_eq!(cancellation.settle(async { 42 }).await, Ok(42));
    }

    #[tokio::test(start_paused = true)]
    async fn unrepresentable_settlement_timeout_does_not_panic_or_expire_immediately() {
        let signal = pin!(async {});
        let mut cancellation = CancellationState {
            signal: Some(signal),
            timeout: Some(Duration::MAX),
            deadline: None,
        };
        assert_eq!(
            cancellation
                .settle(tokio::time::sleep(Duration::from_secs(1)))
                .await,
            Ok(())
        );
        assert!(cancellation.is_cancelled());
    }
}
