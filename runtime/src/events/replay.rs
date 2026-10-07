use {
    agave_event_system::{
        PublisherFactory, event,
        publisher::Publisher,
        stream_name::StreamName,
        wincode::{SchemaRead, SchemaWrite},
        wincode_dynamic::{DynPrimitiveTy, PrimitiveTy},
    },
    solana_clock::Slot,
    solana_svm::events::{InstructionExecutionEvent, TransactionProcessingEvent},
};

pub const ENTRY_EVENT_STREAM: StreamName = agave_event_system::stream_name!("replay.entry_event");
pub const TRANSACTION_EVENT_STREAM: StreamName =
    agave_event_system::stream_name!("replay.transaction_event");
pub const TRANSACTION_PROCESSING_STREAM: StreamName =
    agave_event_system::stream_name!("replay.transaction_processing");
pub const INSTRUCTION_EXECUTION_STREAM: StreamName =
    agave_event_system::stream_name!("replay.instruction_execution");

/// Entry lifecycle events observed by replay. Timestamps are monotonic nanoseconds.
/// Entries are identified by `(bank_id, entry_index)`.
#[event]
#[derive(Debug, PartialEq, Eq)]
pub enum EntryEvent {
    Begin {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        slot: Slot,
        bank_id: u64,
        entry_index: u64,
        num_transactions: u64,
    },
    PohVerified {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        slot: Slot,
        bank_id: u64,
        entry_index: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, SchemaRead, SchemaWrite)]
pub enum VerificationOutcome {
    /// The transaction's signatures were checked and passed.
    Passed,
    /// The transaction's signatures were checked and failed.
    Failed,
    /// Signature verification was disabled for this replay.
    Skipped,
    /// An earlier verification in this worker job failed, so this one was not attempted.
    Cancelled,
}

impl DynPrimitiveTy for VerificationOutcome {
    const TYPE: PrimitiveTy = PrimitiveTy::U32;
}

/// Transaction lifecycle events observed by replay. Timestamps are monotonic nanoseconds.
/// Transactions are identified by `(bank_id, transaction_index)`; Begin links them to an entry.
/// Verification and execution can overlap; neither implies that the other has finished.
#[event]
#[derive(Debug, PartialEq, Eq)]
pub enum TransactionEvent {
    Begin {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        slot: Slot,
        bank_id: u64,
        transaction_index: u64,
        entry_index: u64,
        signature: [u8; 64],
    },
    SignatureVerificationComplete {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        slot: Slot,
        bank_id: u64,
        transaction_index: u64,
        outcome: VerificationOutcome,
    },
    ExecutionBegin {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        slot: Slot,
        bank_id: u64,
        transaction_index: u64,
    },
    /// Transaction processing finished on the handler, including commit and synchronous notifications.
    /// Excludes handoff to the scheduler and its final block-cost check.
    ExecutionComplete {
        pid: i32,
        tid: i32,
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
    pub transactions: TransactionEventFactories,
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
            transactions: self.transactions.lifecycle.try_create_publisher(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct TransactionEventFactories {
    pub lifecycle: PublisherFactory<TransactionEvent>,
    pub processing: PublisherFactory<TransactionProcessingEvent>,
    pub instructions: PublisherFactory<InstructionExecutionEvent>,
}

#[derive(Debug, Default)]
pub struct TransactionEventPublishers {
    pub lifecycle: Option<Publisher<TransactionEvent>>,
    pub processing: Option<Publisher<TransactionProcessingEvent>>,
    pub instructions: Option<Publisher<InstructionExecutionEvent>>,
}

impl TransactionEventFactories {
    pub fn create_publishers(&self) -> TransactionEventPublishers {
        TransactionEventPublishers {
            lifecycle: self.lifecycle.try_create_publisher(),
            processing: self.processing.try_create_publisher(),
            instructions: self.instructions.try_create_publisher(),
        }
    }
}
