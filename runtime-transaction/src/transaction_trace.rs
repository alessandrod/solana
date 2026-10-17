use std::num::NonZeroU64;

/// Identifies one received copy of a transaction within a validator run.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct TraceId(pub NonZeroU64);

/// Local tracing context. It is never part of the serialized transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionTraceContext {
    pub id: TraceId,
    pub attempt: u32,
}
