use {
    agave_event_system::{
        PublisherFactory, event,
        event_metadata::{cached_thread_and_process_id, monotonic_timestamp_ns},
        publisher::Publisher,
        stream_name::StreamName,
        wincode::{SchemaRead, SchemaWrite},
        wincode_dynamic::{DynPrimitiveTy, PrimitiveTy},
    },
    solana_clock::Slot,
    solana_runtime_transaction::transaction_trace::TransactionTraceContext,
    solana_svm::events::{InstructionExecutionEvent, TransactionProcessingEvent},
};

pub const TRANSACTION_EVENT_STREAM: StreamName =
    agave_event_system::stream_name!("tpu.transaction_event");
pub const TRANSACTION_PROCESSING_STREAM: StreamName =
    agave_event_system::stream_name!("tpu.transaction_processing");
pub const INSTRUCTION_EXECUTION_STREAM: StreamName =
    agave_event_system::stream_name!("tpu.instruction_execution");
pub const SCHEDULER_SLOT_EVENT_STREAM: StreamName =
    agave_event_system::stream_name!("scheduler.slot_event");

/// Slot lifecycle in the built-in scheduler. Timestamps are monotonic nanoseconds.
/// Events are best effort and may be missing when the stream is backpressured.
#[event]
#[derive(Debug, Eq, PartialEq)]
pub enum SchedulerSlotEvent {
    /// The controller adopted this bank and can begin scheduling transactions.
    Begin {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        slot: Slot,
        bank_id: u64,
    },
    /// The controller switched away from this bank after all its in-flight batches returned.
    Complete {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        slot: Slot,
        bank_id: u64,
    },
}

#[derive(Clone, Debug)]
pub struct TransactionEventFactories {
    pub lifecycle: PublisherFactory<TransactionEvent>,
    pub processing: PublisherFactory<TransactionProcessingEvent>,
    pub instructions: PublisherFactory<InstructionExecutionEvent>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, SchemaRead, SchemaWrite)]
pub enum BankingDropReason {
    Capacity,
    /// Outside the window for retaining transactions for local block production.
    NotLeader,
    IngressCheckFailed,
    NonceConflict,
    /// No longer valid for inclusion in the bank being checked.
    InvalidTransaction,
    /// Processing rejected the transaction and the worker will not retry it.
    ProcessingRejected,
}

impl DynPrimitiveTy for BankingDropReason {
    const TYPE: PrimitiveTy = PrimitiveTy::U32;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, SchemaRead, SchemaWrite)]
pub enum RetryReason {
    AccountInUse,
    BlockCostLimit,
    VoteCostLimit,
    AccountCostLimit,
    AccountDataLimit,
    BlockDataLimit,
    BankUnavailable,
    RecordingFailed,
}

impl DynPrimitiveTy for RetryReason {
    const TYPE: PrimitiveTy = PrimitiveTy::U32;
}

/// Transaction lifecycle in banking stage. Timestamps are monotonic nanoseconds.
/// A trace identifies a received copy; attempts identify its execution dispatches.
/// Batch IDs are unique within a scheduler instance.
/// Events are best effort and may be missing when the stream is backpressured.
#[event]
#[derive(Debug, Eq, PartialEq)]
pub enum TransactionEvent {
    Dropped {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        trace_id: u64,
        attempt: u32,
        reason: BankingDropReason,
    },
    Buffered {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        trace_id: u64,
        attempt: u32,
    },
    Scheduled {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        trace_id: u64,
        attempt: u32,
        slot: u64,
        batch_id: u64,
    },
    Requeued {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        trace_id: u64,
        attempt: u32,
        reason: RetryReason,
    },
    /// Bounds batch processing, including loading, execution, recording and commit.
    BatchProcessed {
        pid: i32,
        tid: i32,
        batch_id: u64,
        bank_id: u64,
        start_ns: u64,
        end_ns: u64,
        num_transactions: u64,
        num_processed: u64,
        num_succeeded: u64,
    },
}

/// Skips timestamps and event construction for untraced transactions and disabled streams.
pub(crate) fn publish_transaction_event(
    publisher: &mut Option<Publisher<TransactionEvent>>,
    trace: Option<TransactionTraceContext>,
    event: impl FnOnce(u64, TransactionTraceContext) -> TransactionEvent,
) {
    if let Some(trace) = trace
        && let Some(publisher) = publisher
        && publisher.is_enabled()
    {
        let _ = publisher.publish(&event(monotonic_timestamp_ns(), trace));
    }
}

pub(crate) fn publish_banking_drop(
    publisher: &mut Option<Publisher<TransactionEvent>>,
    trace: Option<TransactionTraceContext>,
    reason: BankingDropReason,
) {
    publish_transaction_event(publisher, trace, |timestamp_ns, trace| {
        let (tid, pid) = cached_thread_and_process_id();
        TransactionEvent::Dropped {
            pid,
            tid,
            timestamp_ns,
            trace_id: trace.id.0.get(),
            attempt: trace.attempt,
            reason,
        }
    });
}
