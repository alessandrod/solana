use {
    agave_perf_trace::{
        Event, EventsProducer, RetransmitStatsEvent, ShredKind, ShredRecvTimestampEvent,
        ShredTurbineLayerEvent, thread_id_u32, timestamp, u64_from_usize, write_or_record_drop,
    },
    solana_clock::Slot,
};

pub(crate) fn trace_retransmit_stats(
    events_trace: Option<&EventsProducer>,
    num_nodes: usize,
    num_shreds: usize,
) {
    if let Some(events_trace) = events_trace {
        write_or_record_drop(
            events_trace,
            Event::retransmit_stats(RetransmitStatsEvent {
                pid: std::process::id(),
                tid: thread_id_u32(),
                ts: timestamp(),
                num_nodes: u64_from_usize(num_nodes),
                num_shreds: u64_from_usize(num_shreds),
            }),
        );
    }
}

pub(crate) fn trace_shred_recv_timestamp(
    events_trace: Option<&EventsProducer>,
    slot: Slot,
    index: u32,
    shred_kind: ShredKind,
) {
    if let Some(events_trace) = events_trace {
        let ts = timestamp();
        write_or_record_drop(
            events_trace,
            Event::shred_recv_timestamp(ShredRecvTimestampEvent {
                pid: std::process::id(),
                tid: thread_id_u32(),
                ts,
                slot,
                index: u64::from(index),
                shred_kind: shred_kind as u64,
            }),
        );
    }
}

pub(crate) fn trace_shred_turbine_layer(
    events_trace: Option<&EventsProducer>,
    slot: Slot,
    index: u32,
    shred_kind: ShredKind,
    turbine_layer: u8,
) {
    if let Some(events_trace) = events_trace {
        write_or_record_drop(
            events_trace,
            Event::shred_turbine_layer(ShredTurbineLayerEvent {
                pid: std::process::id(),
                tid: thread_id_u32(),
                ts: timestamp(),
                slot,
                index: u64::from(index),
                shred_kind: shred_kind as u64,
                turbine_layer: u64::from(turbine_layer),
            }),
        );
    }
}
