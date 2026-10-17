use agave_event_system::{event, stream_name::StreamName};

pub const TRANSACTION_EVENT_STREAM: StreamName =
    agave_event_system::stream_name!("poh.transaction_event");

/// Links a transaction to the entry containing it. Timestamps are monotonic nanoseconds.
/// Events are best effort and may be missing when the stream is backpressured.
#[event]
#[derive(Debug, Eq, PartialEq)]
pub struct TransactionRecorded {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    pub trace_id: u64,
    pub attempt: u32,
    pub bank_id: u64,
    pub entry_hash: [u8; 32],
}
