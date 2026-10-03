use {
    agave_event_system::{event, publisher::Publisher, stream_name::StreamName},
    solana_clock::Slot,
};

pub const FEC_SET_COMPLETED_STREAM: StreamName =
    agave_event_system::stream_name!("shred.fec_set_completed");

/// All data shreds of a FEC set have been committed to blockstore, including any
/// recovered shreds. The timestamp is monotonic nanoseconds.
#[event]
#[derive(Debug, PartialEq, Eq)]
pub struct FecSetCompleted {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    pub slot: Slot,
    pub fec_set_index: u32,
}

pub const INGEST_SLOT_EVENT_STREAM: StreamName =
    agave_event_system::stream_name!("ingest.slot_event");

/// Original-slot ingestion lifecycle, independent of replay success.
/// Events are published after successful commits, with Begin before Complete.
/// Timestamps are monotonic nanoseconds.
#[event]
#[derive(Debug, PartialEq, Eq)]
pub enum IngestSlotEvent {
    /// The first data shred was inserted into the write batch, including recovered data.
    Begin {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        slot: Slot,
    },
    /// All data through the slot's last shred have been committed to blockstore.
    Complete {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        slot: Slot,
        num_fec_sets: u64,
    },
}

pub struct IngestPublishers {
    pub fec_sets: Publisher<FecSetCompleted>,
    pub slots: Publisher<IngestSlotEvent>,
}

impl Default for IngestPublishers {
    fn default() -> Self {
        Self {
            fec_sets: Publisher::stub(),
            slots: Publisher::stub(),
        }
    }
}
