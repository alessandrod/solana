use {
    agave_event_system::{event, stream_name::StreamName},
    solana_clock::Slot,
};

pub const ENTRY_EVENT_STREAM: StreamName = agave_event_system::stream_name!("tpu.entry_event");
pub const SLOT_EVENT_STREAM: StreamName = agave_event_system::stream_name!("broadcast.slot_event");

/// Slot lifecycle during standard broadcast. Timestamps are monotonic nanoseconds.
/// Events are best effort and may be missing when the stream is backpressured.
#[event]
#[derive(Debug, Eq, PartialEq)]
pub enum BroadcastSlotEvent {
    /// Broadcast accepted this slot and can begin generating shreds.
    Begin {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        slot: Slot,
    },
    /// The final shreds were generated and dispatched to blockstore and transmit workers.
    ShreddingComplete {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        slot: Slot,
        was_interrupted: bool,
    },
    /// All shred batches for this slot were committed to blockstore.
    BlockstoreInsertComplete {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        slot: Slot,
        was_interrupted: bool,
    },
    /// All shred batches completed local UDP/XDP processing, not peer receipt.
    /// A batch returning a transmit error prevents completion for this slot.
    TransmitComplete {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        slot: Slot,
        was_interrupted: bool,
    },
}

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
