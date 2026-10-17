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
            for packet in batch.iter_mut() {
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
