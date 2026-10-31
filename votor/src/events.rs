use {
    agave_event_system::{event, stream_name::StreamName},
    agave_votor_messages::vote::VoteType,
    solana_clock::Slot,
};

pub const VOTE_SENT_STREAM: StreamName = agave_event_system::stream_name!("vote.sent");

/// A vote accepted by the transport's broadcast queue, including rebroadcasts.
/// The timestamp is monotonic nanoseconds sampled after queue acceptance.
/// Events are best effort and may be missing when the stream is backpressured.
#[event]
#[derive(Debug, PartialEq, Eq)]
pub struct VoteSent {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    /// The local validator's identity.
    pub identity: [u8; 32],
    pub slot: Slot,
    pub vote_type: VoteType,
    /// Zero for slot-only votes.
    pub block_id: [u8; 32],
}
