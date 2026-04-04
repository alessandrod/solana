use {
    agave_perf_trace::{
        Event, EventsProducer, ReplaySlotCompleteEvent, ShredFrontierEvent, ShredGapEvent,
        ShredKind, ShredRecvRangeEvent, ShredSource, TurbineSlotCompleteEvent, thread_id_u32,
        timestamp, u64_from_usize, wallclock_millis, write_or_record_drop,
    },
    solana_clock::Slot,
    std::time::Instant,
};

pub(crate) fn trace_replay_slot_complete(
    events_trace: Option<&EventsProducer>,
    slot: Slot,
    started: Instant,
    num_shreds: u64,
    num_entries: usize,
    num_txs: usize,
) {
    if let Some(events_trace) = events_trace {
        let end_ts = timestamp();
        let start_ts =
            end_ts.saturating_sub(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        write_or_record_drop(
            events_trace,
            Event::replay_slot_complete(ReplaySlotCompleteEvent {
                pid: std::process::id(),
                tid: thread_id_u32(),
                slot,
                start_ts,
                end_ts,
                num_shreds,
                num_entries: u64_from_usize(num_entries),
                num_txs: u64_from_usize(num_txs),
            }),
        );
    }
}

pub(crate) fn trace_turbine_slot_complete(
    events_trace: Option<&EventsProducer>,
    slot: Slot,
    first_shred_timestamp: u64,
) {
    if let Some(events_trace) = events_trace {
        let duration_ms = wallclock_millis().saturating_sub(first_shred_timestamp);
        let end_ts = timestamp();
        let start_ts = end_ts.saturating_sub(duration_ms.saturating_mul(1_000_000));
        write_or_record_drop(
            events_trace,
            Event::turbine_slot_complete(TurbineSlotCompleteEvent {
                pid: std::process::id(),
                tid: thread_id_u32(),
                slot,
                start_ts,
                end_ts,
            }),
        );
    }
}

pub(crate) fn trace_shred_recv_range(
    events_trace: Option<&EventsProducer>,
    ts: u64,
    slot: Slot,
    start_index: u32,
    end_index: u32,
    source: ShredSource,
    shred_kind: ShredKind,
    turbine_layer: Option<u8>,
) {
    if let Some(events_trace) = events_trace {
        write_or_record_drop(
            events_trace,
            Event::shred_recv_range(ShredRecvRangeEvent {
                pid: std::process::id(),
                tid: thread_id_u32(),
                ts,
                slot,
                start_index: u64::from(start_index),
                end_index: u64::from(end_index),
                source: source as u64,
                shred_kind: shred_kind as u64,
                turbine_layer: turbine_layer.map(u64::from).unwrap_or(u64::MAX),
            }),
        );
    }
}

pub(crate) fn trace_shred_frontier(
    events_trace: Option<&EventsProducer>,
    ts: u64,
    slot: Slot,
    highest_received: u64,
    consumed: u64,
) {
    if let Some(events_trace) = events_trace {
        write_or_record_drop(
            events_trace,
            Event::shred_frontier(ShredFrontierEvent {
                pid: std::process::id(),
                tid: thread_id_u32(),
                ts,
                slot,
                highest_received,
                consumed,
            }),
        );
    }
}

pub(crate) fn trace_shred_gap(
    events_trace: Option<&EventsProducer>,
    start_ts: u64,
    end_ts: u64,
    slot: Slot,
    start_index: u32,
    end_index: u32,
) {
    if let Some(events_trace) = events_trace {
        write_or_record_drop(
            events_trace,
            Event::shred_gap(ShredGapEvent {
                pid: std::process::id(),
                tid: thread_id_u32(),
                start_ts,
                end_ts,
                slot,
                start_index: u64::from(start_index),
                end_index: u64::from(end_index),
            }),
        );
    }
}
