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
    mem::transmute,
    num::NonZero,
    sync::{
        atomic::{AtomicUsize, Ordering},
        LazyLock, Once, RwLock,
    },
};

#[repr(u64)]
pub enum TransactionState {
    Received = 1,
    Deduped,
    Buffered,
    Scheduled,
    Executed,
}

struct EventBufs<T> {
    event_bufs: RwLock<[UnsafeCell<Vec<T>>; 100]>,
    next_slot: AtomicUsize,
}

impl<T> EventBufs<T> {
    fn new(capacity: usize) -> Self {
        Self {
            event_bufs: RwLock::new(array::from_fn(|_| {
                UnsafeCell::new(Vec::with_capacity(capacity))
            })),
            next_slot: AtomicUsize::new(0),
        }
    }

    fn next_slot(&self) -> usize {
        self.next_slot
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |slot| {
                if slot + 1 == 100 {
                    Some(50)
                } else {
                    Some(slot + 1)
                }
            })
            .unwrap()
    }
}

unsafe impl<T> Sync for EventBufs<T> {}

#[derive(Clone, Copy)]
#[repr(C)]
struct TransactionEvent {
    flow_id: u64,
    signature: [u8; 64],
    timestamp: u64,
    state: u64,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct SvmTransactionEvent {
    signature: [u8; 64],
    start: u64,
    end: u64,
    tid: u64,
}

const TRANSACTIONS_BATCH_SIZE: usize = 1000;
static TRANSACTIONS: LazyLock<EventBufs<TransactionEvent>> =
    LazyLock::new(|| EventBufs::new(TRANSACTIONS_BATCH_SIZE));

const SVM_TRANSACTIONS_BATCH_SIZE: usize = 1000;
static SVM_TRANSACTIONS: LazyLock<EventBufs<SvmTransactionEvent>> =
    LazyLock::new(|| EventBufs::new(SVM_TRANSACTIONS_BATCH_SIZE));

thread_local! {
    static TRANSACTIONS_INDEX: OnceCell<usize> = OnceCell::new();
    static SVM_TRANSACTIONS_INDEX: OnceCell<usize> = OnceCell::new();
    static THREAD_ID: OnceCell<u64> = OnceCell::new();
}

pub fn trace_transaction(flow_id: u64, signature: &[u8; 64], ts: u64, state: TransactionState) {
    let slot = TRANSACTIONS_INDEX.with(|cell| *cell.get_or_init(|| TRANSACTIONS.next_slot()));

    let guard = &TRANSACTIONS.event_bufs.read().unwrap();
    let txs = &guard[slot];
    let txs = unsafe { &mut *txs.get() };
    txs.push(TransactionEvent {
        flow_id,
        signature: *signature,
        timestamp: ts,
        state: state as u64,
    });
    if txs.len() == TRANSACTIONS_BATCH_SIZE {
        perf_trace_transactions(txs.as_ptr(), txs.len() as u64);
        txs.clear();
    }
}

pub fn flush_transactions() {
    let guard = &TRANSACTIONS.event_bufs.write().unwrap();
    for txs_cell in guard.iter() {
        let txs = unsafe { &mut *txs_cell.get() };
        if !txs.is_empty() {
            perf_trace_transactions(txs.as_ptr(), txs.len() as u64);
            txs.clear();
        }
    }
    perf_trace_flush_transactions();
}

#[unsafe(no_mangle)]
#[inline(never)]
fn perf_trace_transactions(txs: *const TransactionEvent, len: u64) {
    log::trace!("perf_trace_transactions called with len {txs:p} {len}");
}

#[unsafe(no_mangle)]
#[inline(never)]
fn perf_trace_flush_transactions() {
    black_box(());
}

pub fn trace_svm_transaction(signature: &[u8; 64], start: u64, end: u64) {
    let thread_id: u64 = THREAD_ID.with(|cell| *cell.get_or_init(|| gettid()));
    let slot =
        SVM_TRANSACTIONS_INDEX.with(|cell| *cell.get_or_init(|| SVM_TRANSACTIONS.next_slot()));

    let guard = &SVM_TRANSACTIONS.event_bufs.read().unwrap();
    let txs = &guard[slot];
    let txs = unsafe { &mut *txs.get() };

    txs.push(SvmTransactionEvent {
        signature: *signature,
        start,
        end,
        tid: thread_id,
    });
    if txs.len() == SVM_TRANSACTIONS_BATCH_SIZE {
        perf_trace_svm_transactions(txs.as_ptr(), txs.len() as u64);
        txs.clear();
    }
}

pub fn flush_svm_transactions() {
    let guard = &SVM_TRANSACTIONS.event_bufs.write().unwrap();
    for txs_cell in guard.iter() {
        let txs = unsafe { &mut *txs_cell.get() };
        if !txs.is_empty() {
            perf_trace_svm_transactions(txs.as_ptr(), txs.len() as u64);
            txs.clear();
        }
    }
    perf_trace_flush_transactions();
}

#[unsafe(no_mangle)]
#[inline(never)]
fn perf_trace_svm_transactions(txs: *const SvmTransactionEvent, len: u64) {
    log::trace!("perf_trace_svm_transactions called with len {txs:p} {len}");
}

#[unsafe(no_mangle)]
#[inline(never)]
fn perf_trace_flush_svm_transactions() {
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

fn gettid() -> u64 {
    unsafe { libc::syscall(libc::SYS_gettid) as u64 }
}
