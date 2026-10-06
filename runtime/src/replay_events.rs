use {
    agave_event_system::{PublisherFactory, event, publisher::Publisher, stream_name::StreamName},
    solana_clock::Slot,
};

pub const ENTRY_EVENT_STREAM: StreamName = agave_event_system::stream_name!("replay.entry_event");
pub const TRANSACTION_EVENT_STREAM: StreamName =
    agave_event_system::stream_name!("replay.transaction_event");

/// Entry lifecycle events observed by replay. Timestamps are monotonic nanoseconds.
/// Entries are identified by `(bank_id, entry_index)`.
#[event]
#[derive(Debug, PartialEq, Eq)]
pub enum EntryEvent {
    Begin {
        timestamp_ns: u64,
        slot: Slot,
        bank_id: u64,
        entry_index: u64,
        starting_transaction_index: u64,
        num_transactions: u64,
    },
    PohVerified {
        timestamp_ns: u64,
        slot: Slot,
        bank_id: u64,
        entry_index: u64,
    },
}

/// Transaction lifecycle events observed by replay. Timestamps are monotonic nanoseconds.
/// Transactions are identified by `(bank_id, transaction_index)`; Begin links them to an entry.
/// Verification and execution can overlap; neither implies that the other has finished.
#[event]
#[derive(Debug, PartialEq, Eq)]
pub enum TransactionEvent {
    Begin {
        timestamp_ns: u64,
        slot: Slot,
        bank_id: u64,
        transaction_index: u64,
        entry_index: u64,
        signature: [u8; 64],
    },
    SignaturesVerified {
        timestamp_ns: u64,
        slot: Slot,
        bank_id: u64,
        transaction_index: u64,
    },
    ExecutionBegin {
        timestamp_ns: u64,
        slot: Slot,
        bank_id: u64,
        transaction_index: u64,
    },
    /// The execution handler returned, including when it returned an error.
    ExecutionComplete {
        timestamp_ns: u64,
        slot: Slot,
        bank_id: u64,
        transaction_index: u64,
    },
}

/// Replay stream handles that can be cloned and passed to worker threads.
#[derive(Clone, Debug)]
pub struct ReplayEventFactories {
    pub entries: PublisherFactory<EntryEvent>,
    pub transactions: PublisherFactory<TransactionEvent>,
}

/// Publishers owned by one replay or verification thread.
#[derive(Default, Debug)]
pub struct ReplayEventPublishers {
    pub entries: Option<Publisher<EntryEvent>>,
    pub transactions: Option<Publisher<TransactionEvent>>,
}

impl ReplayEventFactories {
    /// Creates publishers on the calling thread. Exhausted streams have no publisher.
    pub fn create_publishers(&self) -> ReplayEventPublishers {
        ReplayEventPublishers {
            entries: self.entries.try_create_publisher(),
            transactions: self.transactions.try_create_publisher(),
        }
    }
}
