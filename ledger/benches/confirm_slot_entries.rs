#![feature(test)]
extern crate solana_ledger;
extern crate test;

#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
use jemallocator::Jemalloc;
use {
    agave_votor_messages::migration::MigrationStatus,
    rayon::{
        ThreadPool,
        iter::{IntoParallelIterator as _, ParallelIterator as _},
    },
    solana_clock::DEFAULT_HASHES_PER_TICK,
    solana_entry::entry::{Entry, next_entry},
    solana_epoch_schedule::EpochSchedule,
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_ledger::{
        blockstore_processor::{ConfirmationProgress, ConfirmationTiming, confirm_slot_entries},
        genesis_utils::{GenesisConfigInfo, create_genesis_config},
    },
    solana_native_token::LAMPORTS_PER_SOL,
    solana_pubkey::Pubkey,
    solana_runtime::bank::{Bank, test_utils::goto_end_of_slot},
    solana_signer::Signer,
    solana_system_transaction::transfer,
    solana_unified_scheduler_pool::DefaultSchedulerPool,
    test::Bencher,
};

#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const WINDOW_SIZE: usize = 64;
const PARALLEL_SENDERS: usize = 2_048;

fn create_replay_thread_pool() -> ThreadPool {
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .thread_name(|i| format!("solReplayBench{i:02}"))
        .build()
        .expect("failed to create replay thread pool")
}

fn build_slot_entries(
    mut previous_entry_hash: Hash,
    recent_blockhash: Hash,
    hashes_per_tick: u64,
    ticks_per_slot: u64,
    tx_entries_per_slot: usize,
    txs_per_entry: usize,
    sender_keypairs: &[Keypair],
) -> Vec<Entry> {
    assert!(
        !sender_keypairs.is_empty(),
        "sender_keypairs must not be empty"
    );
    let ticks = ticks_per_slot as usize;
    let tx_entries = tx_entries_per_slot;
    let max_tx_entries_per_tick = usize::try_from(hashes_per_tick.saturating_sub(1))
        .expect("hashes_per_tick does not fit into usize");
    assert!(
        max_tx_entries_per_tick > 0,
        "hashes_per_tick must be at least 2 to include tx entries before ticks"
    );
    let max_tx_entries_per_slot = max_tx_entries_per_tick.saturating_mul(ticks);
    assert!(
        tx_entries <= max_tx_entries_per_slot,
        "requested {tx_entries} tx entries exceed slot capacity {max_tx_entries_per_slot}"
    );

    let mut entries = Vec::with_capacity(ticks.saturating_add(tx_entries));
    let base_tx_entries_per_tick = tx_entries / ticks;
    let extra_tx_entries = tx_entries % ticks;
    let mut sender_index = 0usize;

    for tick_index in 0..ticks {
        let tx_entries_this_tick =
            base_tx_entries_per_tick + usize::from(tick_index < extra_tx_entries);
        for _ in 0..tx_entries_this_tick {
            let transactions = (0..txs_per_entry)
                .map(|_| {
                    let sender = &sender_keypairs[sender_index % sender_keypairs.len()];
                    sender_index = sender_index.saturating_add(1);
                    transfer(sender, &Pubkey::new_unique(), 1, recent_blockhash)
                })
                .collect();
            let transaction_entry = next_entry(&previous_entry_hash, 1, transactions);
            previous_entry_hash = transaction_entry.hash;
            entries.push(transaction_entry);
        }
        let tick_hashes = hashes_per_tick
            .checked_sub(tx_entries_this_tick as u64)
            .expect("tx entry count exceeds hashes_per_tick");
        assert!(tick_hashes > 0, "tick hash budget must remain positive");
        let tick_entry = next_entry(&previous_entry_hash, tick_hashes, Vec::new());
        previous_entry_hash = tick_entry.hash;
        entries.push(tick_entry);
    }

    entries
}

fn build_slot_windows(entries: Vec<Entry>, window_size: usize) -> Vec<Vec<Entry>> {
    entries
        .chunks(window_size)
        .map(|entry_window| entry_window.to_vec())
        .collect()
}

fn bench_streamed_confirm_slot_entries(
    b: &mut Bencher,
    skip_verification: bool,
    entries_per_slot: usize,
    txs_per_entry: usize,
    window_size: usize,
) {
    let GenesisConfigInfo {
        mut genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config(10_000 * LAMPORTS_PER_SOL);
    genesis_config.epoch_schedule = EpochSchedule::custom(1_000_000_000, 1_000_000_000, false);
    genesis_config.poh_config.hashes_per_tick = Some(DEFAULT_HASHES_PER_TICK);
    let replay_tx_thread_pool = create_replay_thread_pool();
    let scheduler_pool = DefaultSchedulerPool::new_for_verification(None, None, None, None, None);
    let (root_bank, bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
    bank_forks
        .write()
        .expect("bank_forks lock poisoned")
        .install_scheduler_pool(scheduler_pool);
    let total_slot_transactions = entries_per_slot.saturating_mul(txs_per_entry);
    eprintln!("total_slot_transactions: {total_slot_transactions}");
    let sender_count = PARALLEL_SENDERS.min(total_slot_transactions.max(1));
    let sender_keypairs: Vec<Keypair> = replay_tx_thread_pool.install(|| {
        (0..sender_count)
            .into_par_iter()
            .map(|_| Keypair::new())
            .collect()
    });
    for sender in &sender_keypairs {
        root_bank
            .process_transaction(&transfer(
                &mint_keypair,
                &sender.pubkey(),
                LAMPORTS_PER_SOL,
                root_bank.last_blockhash(),
            ))
            .expect("failed to fund parallel sender");
    }
    goto_end_of_slot(root_bank.clone());
    let hashes_per_tick = root_bank.hashes_per_tick().unwrap_or(0);
    let ticks_per_slot = root_bank.ticks_per_slot();

    let parent_bank = root_bank;
    let parent_last_entry = parent_bank.last_blockhash();
    let slot_entries = build_slot_entries(
        parent_last_entry,
        parent_last_entry,
        hashes_per_tick,
        ticks_per_slot,
        entries_per_slot,
        txs_per_entry,
        &sender_keypairs,
    );
    let slot_entry_windows = build_slot_windows(slot_entries, window_size);
    let mut child_slot = parent_bank.slot().saturating_add(1);

    b.iter(|| {
        let now = std::time::Instant::now();
        parent_bank.set_tick_height(child_slot.saturating_mul(ticks_per_slot));
        let bank = bank_forks
            .write()
            .expect("bank_forks lock poisoned")
            .insert(Bank::new_from_parent(
                parent_bank.clone(),
                &Pubkey::default(),
                child_slot,
            ));
        child_slot = child_slot.saturating_add(1);
        assert!(bank.has_installed_scheduler());
        bank.write_cost_tracker()
            .expect("cost tracker lock poisoned")
            .set_limits(u64::MAX, u64::MAX, u64::MAX);
        let mut timing = ConfirmationTiming::default();
        let mut progress = ConfirmationProgress::new(parent_last_entry);

        for (window_index, entry_window) in slot_entry_windows.iter().enumerate() {
            let slot_full = window_index + 1 == slot_entry_windows.len();
            confirm_slot_entries(
                &bank,
                &replay_tx_thread_pool,
                (entry_window.clone(), 0, slot_full),
                &mut timing,
                &mut progress,
                skip_verification,
                None,
                None,
                None,
                None,
                None,
                &MigrationStatus::default(),
            )
            .expect("confirm_slot_entries failed");
        }

        if let Some((result, _timings)) = bank.wait_for_completed_scheduler() {
            result.expect("scheduler returned error");
        }
        // progress
        //     .wait_for_async_verification_results()
        //     .expect("async verification returned error");
        assert!(bank.is_complete(), "bank was not completed");
        eprintln!(
            "confirm_slot_entries streamed {} entries ({} transactions) with \
             skip_verification={skip_verification} took {:?}",
            entries_per_slot,
            total_slot_transactions,
            now.elapsed(),
        );
    });
}

#[bench]
#[ignore]
fn bench_confirm_slot_entries_streamed_1k_verify_on(b: &mut Bencher) {
    bench_streamed_confirm_slot_entries(b, false, 1000, 1000, 1000);
}

#[bench]
#[ignore]
fn bench_confirm_slot_entries_streamed_1k_verify_off(b: &mut Bencher) {
    bench_streamed_confirm_slot_entries(b, true, 500, 50, 64);
}

// #[bench]
// #[ignore]
// fn bench_confirm_slot_entries_streamed_10k_verify_on(b: &mut Bencher) {
//     bench_streamed_confirm_slot_entries(b, false, 10_000);
// }

// #[bench]
// #[ignore]
// fn bench_confirm_slot_entries_streamed_10k_verify_off(b: &mut Bencher) {
//     bench_streamed_confirm_slot_entries(b, true, 10_000);
// }

// #[bench]
// #[ignore]
// fn bench_confirm_slot_entries_streamed_100k_verify_on(b: &mut Bencher) {
//     bench_streamed_confirm_slot_entries(b, false, 100_000);
// }

// #[bench]
// #[ignore]
// fn bench_confirm_slot_entries_streamed_100k_verify_off(b: &mut Bencher) {
//     bench_streamed_confirm_slot_entries(b, true, 100_000);
// }
