use {
    crate::{
        events::{IngressDropReason, IngressEvent, publish_ingress_drops},
        streamer::ChannelSend,
    },
    agave_event_system::{
        PublisherFactory,
        event_metadata::{cached_thread_and_process_id, monotonic_timestamp_ns},
        publisher::Publisher,
    },
    agave_transaction_view::transaction_view::UnsanitizedTransactionView,
    crossbeam_channel::TrySendError,
    solana_perf::packet::PacketBatch,
    solana_runtime_transaction::transaction_trace::{TraceId, TransactionTraceContext},
    std::{
        cell::RefCell,
        num::NonZeroU64,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    },
};

thread_local! {
    static INGRESS_EVENTS: RefCell<Option<Publisher<IngressEvent>>> = const { RefCell::new(None) };
}

/// Shared by this TPU's QUIC servers. Each runtime worker owns its own publisher.
pub struct TransactionTracer {
    factory: PublisherFactory<IngressEvent>,
    next_id: AtomicU64,
}

impl TransactionTracer {
    pub fn new(factory: PublisherFactory<IngressEvent>) -> Self {
        Self {
            factory,
            next_id: AtomicU64::new(1),
        }
    }

    pub(crate) fn initialize_thread(&self) {
        INGRESS_EVENTS.set(self.factory.try_create_publisher());
    }
}

#[derive(Clone)]
pub(crate) struct TracedPacketSender<S> {
    pub sender: S,
    pub tracer: Option<Arc<TransactionTracer>>,
}

impl<S: ChannelSend<PacketBatch>> ChannelSend<PacketBatch> for TracedPacketSender<S> {
    fn try_send(&self, mut batch: PacketBatch) -> Result<(), TrySendError<PacketBatch>> {
        let Some(tracer) = &self.tracer else {
            return self.sender.try_send(batch);
        };

        // This synchronous handoff cannot migrate to another Tokio worker while borrowed.
        INGRESS_EVENTS.with_borrow_mut(|publisher| {
            let mut enabled_publisher = publisher
                .as_mut()
                .and_then(|publisher| publisher.is_enabled().then_some(publisher));
            let packets = match &mut batch {
                PacketBatch::Pinned(_) => unreachable!("QUIC batches contain byte packets"),
                PacketBatch::Bytes(packets) => packets.as_mut_slice(),
                PacketBatch::Single(packet) => std::slice::from_mut(packet),
            };
            for packet in packets {
                let id = TraceId(
                    NonZeroU64::new(tracer.next_id.fetch_add(1, Ordering::Relaxed))
                        .expect("transaction trace IDs exhausted"),
                );
                packet.trace = Some(TransactionTraceContext { id, attempt: 0 });
                if let Some(publisher) = enabled_publisher.as_mut() {
                    let timestamp_ns = monotonic_timestamp_ns();
                    let signature = packet
                        .data(..)
                        .and_then(|data| UnsanitizedTransactionView::try_new_unsanitized(data).ok())
                        .and_then(|view| {
                            view.signatures()
                                .first()
                                .map(|signature| (*signature).into())
                        });
                    let (tid, pid) = cached_thread_and_process_id();
                    let event = match signature {
                        Some(signature) => IngressEvent::Received {
                            pid,
                            tid,
                            timestamp_ns,
                            trace_id: id.0.get(),
                            signature,
                        },
                        None => IngressEvent::ReceivedMalformed {
                            pid,
                            tid,
                            timestamp_ns,
                            trace_id: id.0.get(),
                        },
                    };
                    let _ = publisher.publish(&event);
                }
            }

            let result = self.sender.try_send(batch);
            if let Err(TrySendError::Full(batch)) = &result {
                publish_ingress_drops(publisher, batch.iter(), IngressDropReason::Capacity);
            }
            result
        })
    }

    fn is_empty(&self) -> bool {
        self.sender.is_empty()
    }

    fn len(&self) -> usize {
        self.sender.len()
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use {
        super::*,
        crate::events::INGRESS_EVENT_STREAM,
        agave_event_system::{EventSystem, StreamConfig, subscriber::StreamExplorer},
        assert_matches::assert_matches,
        bytes::Bytes,
        crossbeam_channel::bounded,
        solana_packet::Meta,
        solana_perf::packet::BytesPacket,
    };

    #[test]
    fn test_packet_trace_survives_handoff_and_capacity_drop() {
        let directory = tempfile::tempdir().unwrap();
        let event_system = EventSystem::new(directory.path()).unwrap();
        event_system.set_stream_policy("on".parse().unwrap());
        let factory = event_system
            .create_stream::<IngressEvent>(
                INGRESS_EVENT_STREAM,
                StreamConfig {
                    capacity: 8,
                    publisher_slots: 1,
                    subscriber_slots: 1,
                },
            )
            .unwrap();
        let mut subscriber = StreamExplorer::new(directory.path().to_path_buf())
            .available_streams()
            .next()
            .unwrap()
            .try_connect_typed::<IngressEvent>()
            .unwrap();
        let tracer = Arc::new(TransactionTracer::new(factory));
        tracer.initialize_thread();
        let (sender, receiver) = bounded(1);
        let sender = TracedPacketSender {
            sender,
            tracer: Some(tracer),
        };
        let mut meta = Meta::default();
        meta.size = 1;
        let packet = BytesPacket::new(Bytes::from_static(&[0]), meta);
        sender
            .try_send(PacketBatch::Single(packet.clone()))
            .unwrap();
        let Err(TrySendError::Full(dropped)) = sender.try_send(vec![packet].into()) else {
            panic!("the second batch must exceed channel capacity");
        };

        let received = receiver.recv().unwrap();
        assert_eq!(received.get(0).unwrap().trace().unwrap().id.0.get(), 1);
        assert_eq!(dropped.get(0).unwrap().trace().unwrap().id.0.get(), 2);
        for trace_id in [1, 2] {
            assert_matches!(
                subscriber.try_recv().unwrap().decode().unwrap(),
                IngressEvent::ReceivedMalformed { trace_id: received_id, .. }
                    if received_id == trace_id
            );
        }
        assert_matches!(
            subscriber.try_recv().unwrap().decode().unwrap(),
            IngressEvent::Dropped {
                trace_id: 2,
                attempt: 0,
                reason: IngressDropReason::Capacity,
                ..
            }
        );
    }
}
