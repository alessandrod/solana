#![allow(clippy::arithmetic_side_effects)]

use {
    solana_hash::Hash,
    solana_runtime::bank::BankStatusCache,
    std::{hint::black_box, time::Instant},
};

#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

const TRANSACTIONS_PER_SLOT: usize = 300_000;
const WARMUP_ITERATIONS: usize = 5;
const BENCH_ITERATIONS: usize = 100;

fn fill_slot(status_cache: &mut BankStatusCache, slot: u64) {
    let blockhash = Hash::new_unique();
    for _ in 0..TRANSACTIONS_PER_SLOT {
        status_cache.insert(&blockhash, Hash::new_unique(), slot, Ok(()));
    }
}

fn main() {
    let mut status_cache = BankStatusCache::default();
    let max_root_entries = status_cache.max_root_entries() as u64;

    for slot in 0..max_root_entries {
        fill_slot(&mut status_cache, slot);
    }
    status_cache.add_roots(0..max_root_entries);
    assert_eq!(status_cache.roots().len(), max_root_entries as usize);

    let mut durations = Vec::with_capacity(BENCH_ITERATIONS);
    for iteration in 0..WARMUP_ITERATIONS + BENCH_ITERATIONS {
        let root = max_root_entries + iteration as u64;

        // Populate the incoming root before starting the timer. add_root() then
        // purges one equally populated old root, keeping the cache at steady state.
        fill_slot(&mut status_cache, root);

        let start = Instant::now();
        black_box(&mut status_cache).add_root(root);
        let duration = start.elapsed();

        assert!(status_cache.roots().contains(&root));
        assert!(!status_cache.roots().contains(&(root - max_root_entries)));
        if iteration >= WARMUP_ITERATIONS {
            durations.push(duration);
        }
    }

    durations.sort_unstable();
    let min = durations[0];
    let median = durations[durations.len() / 2];
    let p95 = durations[durations.len() * 95 / 100];
    let max = durations[durations.len() - 1];

    println!(
        "test bench_status_cache_add_roots ... bench:  {} ns/iter (+/- {})",
        median.as_nanos(),
        (max - min).as_nanos(),
    );
    println!("min: {min:?}, median: {median:?}, p95: {p95:?}, max: {max:?}");
}
