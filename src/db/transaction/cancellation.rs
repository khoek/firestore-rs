use std::future::Future;
use std::time::Duration;

/// Cancellation settings for [`crate::FirestoreDb::run_transaction_cancellable`].
#[derive(Debug)]
pub struct FirestoreTransactionCancellation<C> {
    pub(super) signal: C,
    pub(super) settlement_timeout: Option<Duration>,
}

impl<C: Future<Output = ()>> FirestoreTransactionCancellation<C> {
    /// Requests cancellation when `signal` resolves, with no settlement deadline by default.
    pub fn new(signal: C) -> Self {
        Self {
            signal,
            settlement_timeout: None,
        }
    }

    /// Bounds the wait for pending Begin, Commit and Rollback requests after cancellation.
    ///
    /// Begin and its rollback share the deadline. Expiry during Commit returns a
    /// `TransactionSettlementTimeout` system error; the writes may have been applied.
    pub fn with_settlement_timeout(mut self, timeout: Duration) -> Self {
        self.settlement_timeout = Some(timeout);
        self
    }
}
