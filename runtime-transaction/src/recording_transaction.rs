use {
    crate::transaction_trace::TransactionTraceContext,
    solana_transaction::{Transaction, versioned::VersionedTransaction},
};

/// Preserves tracing context until the transaction's entry hash is known.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordingTransaction {
    pub transaction: VersionedTransaction,
    pub trace: Option<TransactionTraceContext>,
}

impl From<VersionedTransaction> for RecordingTransaction {
    fn from(transaction: VersionedTransaction) -> Self {
        Self {
            transaction,
            trace: None,
        }
    }
}

impl From<Transaction> for RecordingTransaction {
    fn from(transaction: Transaction) -> Self {
        VersionedTransaction::from(transaction).into()
    }
}
