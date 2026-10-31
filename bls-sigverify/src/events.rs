use {
    agave_event_system::{event, stream_name::StreamName},
    agave_votor_messages::vote::VoteType,
    solana_clock::Slot,
};

pub const VOTE_RECEIVED_STREAM: StreamName = agave_event_system::stream_name!("vote.received");

/// A decoded peer vote observed before filtering, deduplication, and signature verification.
/// The timestamp is monotonic nanoseconds sampled after decoding.
/// Events are best effort and may be missing when the stream is backpressured.
#[event]
#[derive(Debug, PartialEq, Eq)]
pub struct VoteReceived {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    /// The authenticated peer's identity.
    pub identity: [u8; 32],
    pub slot: Slot,
    pub vote_type: VoteType,
    /// Zero for slot-only votes.
    pub block_id: [u8; 32],
}
