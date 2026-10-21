use {
    agave_event_system::{
        event,
        event_metadata::{cached_thread_and_process_id, monotonic_timestamp_ns},
        publisher::Publisher,
        stream_name::StreamName,
        wincode::{SchemaRead, SchemaWrite},
        wincode_dynamic::{DynPrimitiveTy, PrimitiveTy},
    },
    solana_perf::packet::PacketRef,
    solana_runtime_transaction::transaction_trace::TransactionTraceContext,
};

pub const INGRESS_EVENT_STREAM: StreamName = agave_event_system::stream_name!("tpu.ingress_event");

#[derive(Clone, Copy, Debug, Eq, PartialEq, SchemaRead, SchemaWrite)]
#[wincode(crate = "agave_event_system::wincode")]
pub enum IngressDropReason {
    Capacity,
    Duplicate,
    InvalidSignature,
    InsufficientPriority,
    /// Outside the window for retaining transactions for local block production.
    NotLeader,
}

impl DynPrimitiveTy for IngressDropReason {
    const TYPE: PrimitiveTy = PrimitiveTy::U32;
}

/// QUIC receipt and ingress drops. Timestamps are monotonic nanoseconds.
/// Events are best effort and may be missing when the stream is backpressured.
#[event]
#[derive(Debug, Eq, PartialEq)]
pub enum IngressEvent {
    Received {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        trace_id: u64,
        /// The first transaction signature, before signature verification.
        signature: [u8; 64],
    },
    Dropped {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        trace_id: u64,
        attempt: u32,
        reason: IngressDropReason,
    },
    /// The transaction layout could not be parsed.
    ReceivedMalformed {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        trace_id: u64,
    },
}

pub fn publish_ingress_drop(
    publisher: &mut Option<Publisher<IngressEvent>>,
    trace: Option<TransactionTraceContext>,
    reason: IngressDropReason,
) {
    if let Some(trace) = trace
        && let Some(publisher) = publisher
        && publisher.is_enabled()
    {
        let (tid, pid) = cached_thread_and_process_id();
        let _ = publisher.publish(&IngressEvent::Dropped {
            pid,
            tid,
            timestamp_ns: monotonic_timestamp_ns(),
            trace_id: trace.id.0.get(),
            attempt: trace.attempt,
            reason,
        });
    }
}

/// Publishes drops for packets that have not already been marked discarded.
pub fn publish_ingress_drops<'a>(
    publisher: &mut Option<Publisher<IngressEvent>>,
    packets: impl Iterator<Item = PacketRef<'a>>,
    reason: IngressDropReason,
) {
    if let Some(publisher) = publisher
        && publisher.is_enabled()
    {
        let (tid, pid) = cached_thread_and_process_id();
        for packet in packets {
            if !packet.meta().discard()
                && let Some(trace) = packet.trace()
            {
                let _ = publisher.publish(&IngressEvent::Dropped {
                    pid,
                    tid,
                    timestamp_ns: monotonic_timestamp_ns(),
                    trace_id: trace.id.0.get(),
                    attempt: trace.attempt,
                    reason,
                });
            }
        }
    }
}
