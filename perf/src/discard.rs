use {
    crate::{get_signature_from_packet, packet::PacketBatch},
    rand::{thread_rng, Rng},
    solana_signature::SIGNATURE_BYTES,
    solana_time_utils::timestamp,
};

pub fn discard_batches_randomly(
    batches: &mut Vec<PacketBatch>,
    max_packets: usize,
    mut total_packets: usize,
) -> usize {
    while total_packets > max_packets {
        let index = thread_rng().gen_range(0..batches.len());
        let removed = batches.swap_remove(index);
        for packet in removed.iter() {
            if let Ok(sig) = get_signature_from_packet(&packet) {
                transaction_discarded(sig, timestamp());
            }
        }
        total_packets = total_packets.saturating_sub(removed.len());
    }
    total_packets
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::packet::{BytesPacket, BytesPacketBatch, Meta},
        bytes::Bytes,
    };

    #[test]
    fn test_batch_discard_random() {
        agave_logger::setup();
        let mut batch = BytesPacketBatch::new();
        batch.resize(1, BytesPacket::new(Bytes::new(), Meta::default()));
        let batch = PacketBatch::from(batch);
        let num_batches = 100;
        let mut batches = vec![batch; num_batches];
        let max = 5;
        discard_batches_randomly(&mut batches, max, num_batches);
        assert_eq!(batches.len(), max);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn transaction_discarded(_signature: &[u8; SIGNATURE_BYTES], ts: u64) {
    // Placeholder for FFI hook
}
