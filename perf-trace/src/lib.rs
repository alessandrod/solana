#![cfg_attr(
    not(feature = "agave-unstable-api"),
    deprecated(
        since = "3.1.0",
        note = "This crate has been marked for formal inclusion in the Agave Unstable API. From \
                v4.0.0 onward, the `agave-unstable-api` crate feature must be specified to \
                acknowledge use of an interface that may break without warning."
    )
)]
// Activate some of the Rust 2024 lints to make the future migration easier.
#![warn(if_let_rescope)]
#![warn(keyword_idents_2024)]
#![warn(rust_2024_incompatible_pat)]
#![warn(tail_expr_drop_order)]
#![warn(unsafe_attr_outside_unsafe)]
#![warn(unsafe_op_in_unsafe_fn)]

use std::{
    array,
    cell::{OnceCell, UnsafeCell},
    hint::black_box,
    mem,
    sync::{
        atomic::{AtomicUsize, Ordering},
        LazyLock, RwLock,
    },
    time::Duration,
};

#[repr(u64)]
pub enum TransactionState {
    Received = 1,
    Deduped,
    Buffered,
    Scheduled,
    Executed,
}

struct EventBufs {
    event_bufs: RwLock<[UnsafeCell<Vec<Event>>; 100]>,
    next_slot: AtomicUsize,
}

impl EventBufs {
    fn new() -> Self {
        Self {
            event_bufs: RwLock::new(array::from_fn(|_| UnsafeCell::new(Vec::new()))),
            next_slot: AtomicUsize::new(0),
        }
    }

    fn next_slot(&self) -> usize {
        self.next_slot.fetch_add(1, Ordering::Relaxed)
    }
}

unsafe impl Sync for EventBufs {}

#[derive(Clone, Copy)]
#[repr(C)]
struct Event {
    signature: [u8; 64],
    timestamp: u64,
    state: u64,
}

static TRANSACTIONS: LazyLock<EventBufs> = LazyLock::new(|| EventBufs::new());

const SUBMIT_BATCH_SIZE: usize = 1000;

thread_local! {
    // static INIT: bool = false;
    static SLOT: OnceCell<usize> = OnceCell::new();
}

pub fn trace_transaction(signature: &[u8; 64], ts: u64, state: TransactionState) {
    let slot = SLOT.with(|cell| *cell.get_or_init(|| TRANSACTIONS.next_slot()));

    let guard = &TRANSACTIONS.event_bufs.read().unwrap();
    let txs = &guard[slot];
    let txs = unsafe { &mut *txs.get() };
    txs.push(Event {
        signature: *signature,
        timestamp: ts,
        state: state as u64,
    });
    if txs.len() == SUBMIT_BATCH_SIZE {
        perf_trace_transactions(txs.as_ptr(), txs.len() as u64);
        txs.clear();
    }
}

pub fn flush_transactions() {
    let guard = &TRANSACTIONS.event_bufs.write().unwrap();
    for txs_cell in guard.iter() {
        let txs = unsafe { &mut *txs_cell.get() };
        if !txs.is_empty() {
            let len = txs.len();
            txs.resize(
                SUBMIT_BATCH_SIZE,
                Event {
                    signature: [0; 64],
                    timestamp: 0,
                    state: 0,
                },
            );
            perf_trace_transactions(txs.as_ptr(), len as u64);
            txs.clear();
        }
    }
    perf_trace_flush_transactions();
}

#[unsafe(no_mangle)]
#[inline(never)]
fn perf_trace_transactions(txs: *const Event, len: u64) {
    log::trace!("perf_trace_transactions called with len {txs:p} {len}");
}

#[unsafe(no_mangle)]
#[inline(never)]
fn perf_trace_flush_transactions() {
    black_box(());
}

pub fn timestamp() -> u64 {
    let mut time_spec = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe {
        libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut time_spec as *mut _);
    }
    (time_spec.tv_sec as u64) * 1_000_000_000u64 + (time_spec.tv_nsec as u64)
}
