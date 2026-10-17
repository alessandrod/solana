use agave_event_system::{event, stream_name::StreamName};

pub const ENTRY_EVENT_STREAM: StreamName = agave_event_system::stream_name!("tpu.entry_event");

/// Shred ranges cover the entire component containing this entry.
/// Timestamps are monotonic nanoseconds.
/// Events are best effort and may be missing when the stream is backpressured.
#[event]
#[derive(Debug, Eq, PartialEq)]
pub struct EntryShredded {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    pub bank_id: u64,
    pub slot: u64,
    pub entry_hash: [u8; 32],
    pub data_shred_start: u32,
    pub data_shred_end: u32,
    pub code_shred_start: u32,
    pub code_shred_end: u32,
}
