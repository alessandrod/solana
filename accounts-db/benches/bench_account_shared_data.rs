use {
    agave_memory::jemalloc::{Arena, ArenaStats, Jemalloc, Stats},
    clap::Parser,
    solana_account::{AccountSharedData, WritableAccount},
    solana_clock::Epoch,
    solana_pubkey::Pubkey,
    std::{
        fmt,
        hint::black_box,
        process,
        str::FromStr,
        sync::{
            Arc, Barrier,
            mpsc::{Receiver, Sender, channel},
        },
        thread::{self, JoinHandle},
        time::{Duration, Instant},
    },
};

#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

const THREAD_COUNT: usize = 12;
const DEFAULT_MANUAL_ARENAS: usize = THREAD_COUNT;
const KB: usize = 1024;
const MB: usize = 1_048_576;
const GB: usize = 1_073_741_824;
const BYTE_UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];

#[derive(Debug, Clone, Copy)]
struct DataSize(usize);

impl fmt::Display for DataSize {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl FromStr for DataSize {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim().to_ascii_lowercase().replace('_', "");
        if value.is_empty() {
            return Err("data size must not be empty".to_string());
        }

        let suffix_start = value
            .find(|character: char| !character.is_ascii_digit())
            .unwrap_or(value.len());
        let (number, suffix) = value.split_at(suffix_start);
        if number.is_empty() {
            return Err(format!("data size must start with a number: {value}"));
        }

        let number = number
            .parse::<usize>()
            .map_err(|error| format!("invalid data size number {number}: {error}"))?;
        let multiplier = match suffix {
            "" | "b" => 1,
            "k" | "kb" | "kib" => KB,
            "m" | "mb" | "mib" => MB,
            "g" | "gb" | "gib" => GB,
            _ => {
                return Err(format!(
                    "invalid data size suffix {suffix}; use b, k, kb, m, mb, g, or gb"
                ));
            }
        };

        number
            .checked_mul(multiplier)
            .map(Self)
            .ok_or_else(|| format!("data size overflows usize: {value}"))
    }
}

#[derive(Debug, Parser)]
#[command(about = "Break AccountSharedData Arc<Vec<u8>> copy-on-write from 12 threads")]
struct Args {
    /// Account data sizes to copy on write. Accepts raw bytes or k/m/g suffixes.
    #[arg(
        long = "data-size",
        value_delimiter = ',',
        default_values = ["200", "1k", "1m", "10m"]
    )]
    data_sizes: Vec<DataSize>,

    /// Number of measured rounds per data size.
    #[arg(long, default_value_t = 10)]
    rounds: usize,

    /// Copy-on-write iterations to run inside each measured round.
    #[arg(long, default_value_t = 1)]
    iters_per_round: usize,

    /// Untimed copy-on-write iterations to run before measured rounds.
    #[arg(long, default_value_t = 1)]
    warmup_iters: usize,

    /// Sleep before each measured round so freed allocations can decay.
    #[arg(long, default_value_t = 0)]
    sleep_ms: u64,

    /// Number of manual jemalloc arenas to create for workers. Use 0 for automatic arenas.
    #[arg(long = "arenas", default_value_t = DEFAULT_MANUAL_ARENAS)]
    arenas: usize,

    /// Do not print jemalloc stats snapshots.
    #[arg(long, default_value_t = false)]
    no_jemalloc_stats: bool,

    /// Accepted because `cargo bench` appends libtest's `--bench` argument.
    #[arg(long = "bench", hide = true, default_value_t = false)]
    _bench: bool,
}

#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
#[derive(Clone, Copy)]
struct JemallocStats {
    stats: Stats,
    arena_stats: ArenaStats,
}

#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
impl JemallocStats {
    fn read() -> Result<Self, String> {
        Jemalloc::advance_epoch()
            .map_err(|error| format!("failed to advance jemalloc epoch: {error}"))?;
        Ok(Self {
            stats: Jemalloc::stats()
                .map_err(|error| format!("failed to read jemalloc stats: {error}"))?,
            arena_stats: Jemalloc::merged_arena_stats()
                .map_err(|error| format!("failed to read merged jemalloc arena stats: {error}"))?,
        })
    }
}

struct WorkerPool {
    start_barrier: Arc<Barrier>,
    finish_barrier: Arc<Barrier>,
    result_receiver: Receiver<u8>,
    account_senders: Vec<Sender<AccountSharedData>>,
    thread_handles: Vec<JoinHandle<()>>,
}

type WorkerChannels = (Vec<Sender<AccountSharedData>>, Vec<JoinHandle<()>>);

struct RoundDurationSummary {
    data_size_label: String,
    durations: Vec<Duration>,
}

impl WorkerPool {
    fn new(worker_arenas: Option<Arc<Vec<Arena>>>) -> Result<Self, String> {
        let barrier_participants = THREAD_COUNT
            .checked_add(1)
            .expect("barrier participant count must not overflow");
        let start_barrier = Arc::new(Barrier::new(barrier_participants));
        let finish_barrier = Arc::new(Barrier::new(barrier_participants));
        let (result_sender, result_receiver) = channel();
        let (account_senders, thread_handles) = spawn_workers(
            Arc::clone(&start_barrier),
            Arc::clone(&finish_barrier),
            result_sender,
            worker_arenas,
        )?;

        Ok(Self {
            start_barrier,
            finish_barrier,
            result_receiver,
            account_senders,
            thread_handles,
        })
    }

    fn break_cow_once(&self, shared_data: &Arc<Vec<u8>>) -> u8 {
        for account_sender in &self.account_senders {
            account_sender
                .send(shared_account(Arc::clone(shared_data)))
                .expect("worker threads must receive accounts");
        }

        self.start_barrier.wait();

        let mut checksum = 0;
        for _ in 0..THREAD_COUNT {
            checksum ^= self
                .result_receiver
                .recv()
                .expect("worker threads must send results");
        }

        self.finish_barrier.wait();
        checksum
    }

    fn break_cow_iterations(&self, shared_data: &Arc<Vec<u8>>, iterations: usize) -> u8 {
        let mut checksum = 0;
        for _ in 0..iterations {
            checksum ^= self.break_cow_once(shared_data);
        }
        checksum
    }

    fn shutdown(self) {
        let Self {
            account_senders,
            thread_handles,
            ..
        } = self;
        drop(account_senders);

        join_worker_threads(thread_handles).expect("worker thread must not panic during benchmark");
    }
}

fn spawn_workers(
    start_barrier: Arc<Barrier>,
    finish_barrier: Arc<Barrier>,
    result_sender: Sender<u8>,
    worker_arenas: Option<Arc<Vec<Arena>>>,
) -> Result<WorkerChannels, String> {
    let mut account_senders = Vec::with_capacity(THREAD_COUNT);
    let mut thread_handles = Vec::with_capacity(THREAD_COUNT);
    let (setup_sender, setup_receiver) = channel::<Result<(), String>>();

    for thread_index in 0..THREAD_COUNT {
        let (account_sender, account_receiver) = channel::<AccountSharedData>();
        let start_barrier = Arc::clone(&start_barrier);
        let finish_barrier = Arc::clone(&finish_barrier);
        let result_sender = result_sender.clone();
        let setup_sender = setup_sender.clone();
        let worker_arena = worker_arenas.as_ref().map(|arena_ids| {
            let arena_index = thread_index
                .checked_rem(arena_ids.len())
                .expect("manual arena list must not be empty");
            arena_ids[arena_index]
        });

        let thread_handle = thread::spawn(move || {
            if let Err(error) = configure_worker_thread_arena(thread_index, worker_arena) {
                let _ = setup_sender.send(Err(error));
                return;
            }
            if setup_sender.send(Ok(())).is_err() {
                return;
            }

            let thread_value = u8::try_from(
                thread_index
                    .checked_add(1)
                    .expect("thread index must not overflow"),
            )
            .expect("thread count must fit in u8");

            while let Ok(mut account) = account_receiver.recv() {
                start_barrier.wait();

                let result = {
                    let data = account.data_as_mut_slice();
                    data[thread_index] = data[thread_index].wrapping_add(thread_value);
                    data[thread_index]
                };

                result_sender
                    .send(result)
                    .expect("main benchmark thread must receive worker results");

                drop(account);
                finish_barrier.wait();
            }
        });

        account_senders.push(account_sender);
        thread_handles.push(thread_handle);
    }
    drop(setup_sender);

    let mut setup_error = None;
    for _ in 0..THREAD_COUNT {
        match setup_receiver.recv() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                setup_error.get_or_insert(error);
            }
            Err(error) => {
                setup_error.get_or_insert(format!(
                    "worker thread setup channel closed before all workers reported: {error}"
                ));
            }
        };
    }

    if let Some(error) = setup_error {
        drop(account_senders);
        join_worker_threads(thread_handles)?;
        return Err(error);
    }

    Ok((account_senders, thread_handles))
}

fn join_worker_threads(thread_handles: Vec<JoinHandle<()>>) -> Result<(), String> {
    for thread_handle in thread_handles {
        thread_handle
            .join()
            .map_err(|_| "worker thread panicked during benchmark".to_string())?;
    }
    Ok(())
}

fn configure_worker_thread_arena(
    thread_index: usize,
    worker_arena: Option<Arena>,
) -> Result<(), String> {
    if let Some(worker_arena) = worker_arena {
        worker_arena
            .bind_current_thread_permanently()
            .map_err(|error| {
                format!(
                    "worker {thread_index} failed to use jemalloc arena {}: {error}",
                    worker_arena.id()
                )
            })?;
    }

    Ok(())
}

fn shared_account(data: Arc<Vec<u8>>) -> AccountSharedData {
    AccountSharedData::create_from_existing_shared_data(
        1,
        data,
        Pubkey::default(),
        false,
        Epoch::default(),
    )
}

fn bytes_copied(data_size: usize, iterations: usize) -> usize {
    THREAD_COUNT
        .checked_mul(data_size)
        .and_then(|bytes| bytes.checked_mul(iterations))
        .expect("byte count must not overflow")
}

fn format_bytes(bytes: usize) -> String {
    format_bytes_f64(bytes as f64)
}

fn format_bytes_f64(bytes: f64) -> String {
    if bytes < KB as f64 {
        return format!("{bytes:.0} B");
    }

    let mut value = bytes;
    let mut unit_index: usize = 0;
    while value >= KB as f64 {
        let Some(next_unit_index) = unit_index.checked_add(1) else {
            break;
        };
        if next_unit_index >= BYTE_UNITS.len() {
            break;
        }

        value /= KB as f64;
        unit_index = next_unit_index;
    }

    let precision = if value >= 100.0 {
        0
    } else if value >= 10.0 {
        1
    } else {
        2
    };
    format!("{value:.precision$} {}", BYTE_UNITS[unit_index])
}

fn format_data_size(bytes: usize) -> String {
    let readable = format_bytes(bytes);
    if readable == format!("{bytes} B") {
        readable
    } else {
        format!("{readable} ({bytes} bytes)")
    }
}

fn format_duration(duration: Duration) -> String {
    format_duration_ns(duration.as_nanos() as f64)
}

fn format_duration_ns(nanos: f64) -> String {
    if nanos < 1_000.0 {
        format!("{nanos:.0} ns")
    } else if nanos < 1_000_000.0 {
        format!("{:.2} us", nanos / 1_000.0)
    } else if nanos < 1_000_000_000.0 {
        format!("{:.2} ms", nanos / 1_000_000.0)
    } else {
        format!("{:.2} s", nanos / 1_000_000_000.0)
    }
}

fn format_throughput(bytes: usize, elapsed: Duration) -> String {
    let seconds = elapsed.as_secs_f64();
    if seconds == 0.0 {
        "n/a".to_string()
    } else {
        format!("{}/s", format_bytes_f64(bytes as f64 / seconds))
    }
}

fn iteration_unit(iterations: usize) -> &'static str {
    if iterations == 1 {
        "iteration"
    } else {
        "iterations"
    }
}

fn round_unit(rounds: usize) -> &'static str {
    if rounds == 1 { "round" } else { "rounds" }
}

fn percentile_nearest_rank(sorted_durations: &[Duration], percentile: usize) -> Duration {
    debug_assert!(!sorted_durations.is_empty());
    debug_assert!((1..=100).contains(&percentile));

    let rank = percentile
        .checked_mul(sorted_durations.len())
        .and_then(|rank| rank.checked_add(99))
        .and_then(|rank| rank.checked_div(100))
        .expect("percentile rank must not overflow");
    let index = rank
        .checked_sub(1)
        .expect("percentile rank must be at least one");
    sorted_durations[index]
}

fn print_round_duration_percentiles(summaries: &[RoundDurationSummary]) {
    println!();
    println!("round duration percentiles:");

    for summary in summaries {
        let mut sorted_durations = summary.durations.clone();
        sorted_durations.sort_unstable();

        println!(
            "  {} ({} {}): p50 {}, p75 {}, p90 {}, p95 {}, p99 {}, p100 {}",
            summary.data_size_label,
            summary.durations.len(),
            round_unit(summary.durations.len()),
            format_duration(percentile_nearest_rank(&sorted_durations, 50)),
            format_duration(percentile_nearest_rank(&sorted_durations, 75)),
            format_duration(percentile_nearest_rank(&sorted_durations, 90)),
            format_duration(percentile_nearest_rank(&sorted_durations, 95)),
            format_duration(percentile_nearest_rank(&sorted_durations, 99)),
            format_duration(percentile_nearest_rank(&sorted_durations, 100))
        );
    }
}

#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
fn format_bytes_with_delta(bytes: usize, previous_bytes: Option<usize>) -> String {
    let formatted_bytes = format_bytes(bytes);
    match previous_bytes {
        Some(previous_bytes) => format!(
            "{formatted_bytes} ({})",
            format_usize_delta(bytes, previous_bytes, format_bytes)
        ),
        None => formatted_bytes,
    }
}

#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
fn format_pages_with_delta(pages: usize, previous_pages: Option<usize>) -> String {
    match previous_pages {
        Some(previous_pages) => format!(
            "{pages} pages ({})",
            format_usize_delta(pages, previous_pages, |delta| format!("{delta} pages"))
        ),
        None => format!("{pages} pages"),
    }
}

#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
fn format_u64_with_delta(value: u64, previous_value: Option<u64>) -> String {
    match previous_value {
        Some(previous_value) => format!("{value} ({})", format_u64_delta(value, previous_value)),
        None => value.to_string(),
    }
}

#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
fn format_usize_delta(
    current: usize,
    previous: usize,
    format_delta: impl FnOnce(usize) -> String,
) -> String {
    if current >= previous {
        let delta = current
            .checked_sub(previous)
            .expect("usize delta must not underflow");
        format!("+{}", format_delta(delta))
    } else {
        let delta = previous
            .checked_sub(current)
            .expect("usize delta must not underflow");
        format!("-{}", format_delta(delta))
    }
}

#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
fn format_u64_delta(current: u64, previous: u64) -> String {
    if current >= previous {
        let delta = current
            .checked_sub(previous)
            .expect("u64 delta must not underflow");
        format!("+{delta}")
    } else {
        let delta = previous
            .checked_sub(current)
            .expect("u64 delta must not underflow");
        format!("-{delta}")
    }
}

#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
fn print_jemalloc_stats(
    label: &str,
    previous: Option<&JemallocStats>,
) -> Result<JemallocStats, String> {
    let stats = JemallocStats::read()?;

    println!("jemalloc stats {label}:");
    println!(
        "  allocated {}, active {}, resident {}, mapped {}",
        format_bytes_with_delta(
            stats.stats.allocated,
            previous.map(|stats| stats.stats.allocated)
        ),
        format_bytes_with_delta(stats.stats.active, previous.map(|stats| stats.stats.active)),
        format_bytes_with_delta(
            stats.stats.resident,
            previous.map(|stats| stats.stats.resident)
        ),
        format_bytes_with_delta(stats.stats.mapped, previous.map(|stats| stats.stats.mapped))
    );
    println!(
        "  retained {}, metadata {}, active {}",
        format_bytes_with_delta(
            stats.stats.retained,
            previous.map(|stats| stats.stats.retained)
        ),
        format_bytes_with_delta(
            stats.stats.metadata,
            previous.map(|stats| stats.stats.metadata)
        ),
        format_pages_with_delta(
            stats.arena_stats.active_pages,
            previous.map(|stats| stats.arena_stats.active_pages)
        )
    );
    println!(
        "  dirty {} / {}, muzzy {} / {}",
        format_bytes_with_delta(
            stats.arena_stats.dirty,
            previous.map(|stats| stats.arena_stats.dirty)
        ),
        format_pages_with_delta(
            stats.arena_stats.dirty_pages,
            previous.map(|stats| stats.arena_stats.dirty_pages)
        ),
        format_bytes_with_delta(
            stats.arena_stats.muzzy,
            previous.map(|stats| stats.arena_stats.muzzy)
        ),
        format_pages_with_delta(
            stats.arena_stats.muzzy_pages,
            previous.map(|stats| stats.arena_stats.muzzy_pages)
        )
    );
    println!(
        "  decay dirty {}, muzzy {}",
        stats.arena_stats.dirty_decay, stats.arena_stats.muzzy_decay
    );
    println!(
        "  dirty purge sweeps {}, madvise {}, purged {} pages",
        format_u64_with_delta(
            stats.arena_stats.dirty_purges.sweeps,
            previous.map(|stats| stats.arena_stats.dirty_purges.sweeps)
        ),
        format_u64_with_delta(
            stats.arena_stats.dirty_purges.madvise,
            previous.map(|stats| stats.arena_stats.dirty_purges.madvise)
        ),
        format_u64_with_delta(
            stats.arena_stats.dirty_purges.purged_pages,
            previous.map(|stats| stats.arena_stats.dirty_purges.purged_pages)
        )
    );
    println!(
        "  muzzy purge sweeps {}, madvise {}, purged {} pages",
        format_u64_with_delta(
            stats.arena_stats.muzzy_purges.sweeps,
            previous.map(|stats| stats.arena_stats.muzzy_purges.sweeps)
        ),
        format_u64_with_delta(
            stats.arena_stats.muzzy_purges.madvise,
            previous.map(|stats| stats.arena_stats.muzzy_purges.madvise)
        ),
        format_u64_with_delta(
            stats.arena_stats.muzzy_purges.purged_pages,
            previous.map(|stats| stats.arena_stats.muzzy_purges.purged_pages)
        )
    );

    Ok(stats)
}

#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
fn print_jemalloc_stats_or_exit(label: &str, previous: Option<&JemallocStats>) -> JemallocStats {
    match print_jemalloc_stats(label, previous) {
        Ok(stats) => stats,
        Err(error) => {
            eprintln!("error: {error}");
            process::exit(2);
        }
    }
}

#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
fn maybe_print_jemalloc_stats(label: &str, previous_stats: &mut Option<JemallocStats>) {
    if previous_stats.is_some() {
        let next_stats = print_jemalloc_stats_or_exit(label, previous_stats.as_ref());
        *previous_stats = Some(next_stats);
    }
}

fn print_run_summary(args: &Args, worker_arenas: Option<&[Arena]>) {
    let data_sizes = args
        .data_sizes
        .iter()
        .map(|data_size| format_data_size(data_size.0))
        .collect::<Vec<_>>()
        .join(", ");
    #[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
    let jemalloc_stats = if args.no_jemalloc_stats {
        "disabled"
    } else {
        "enabled"
    };
    #[cfg(any(target_env = "msvc", target_os = "freebsd"))]
    let jemalloc_stats = "unavailable on this target";

    println!("AccountSharedData Arc<Vec<u8>> copy-on-write");
    println!("threads: {THREAD_COUNT}");
    println!("data sizes: {data_sizes}");
    println!("rounds per data size: {}", args.rounds);
    println!("warmup iterations per data size: {}", args.warmup_iters);
    println!("measured iterations per round: {}", args.iters_per_round);
    println!("sleep before each measured round: {} ms", args.sleep_ms);
    println!(
        "jemalloc worker arenas: {}",
        format_worker_arenas(worker_arenas)
    );
    println!("jemalloc stats: {jemalloc_stats}");
}

fn format_worker_arenas(worker_arenas: Option<&[Arena]>) -> String {
    match worker_arenas {
        Some(worker_arenas) => {
            let arena_ids = worker_arenas
                .iter()
                .map(|arena| arena.id().to_string())
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "{} manual arenas, ids [{arena_ids}], round-robin across {THREAD_COUNT} workers",
                worker_arenas.len()
            )
        }
        None => "automatic thread arenas".to_string(),
    }
}

fn validate_args(args: &Args) -> Result<(), String> {
    if args.data_sizes.is_empty() {
        return Err("at least one --data-size is required".to_string());
    }
    if args.rounds == 0 {
        return Err("--rounds must be greater than 0".to_string());
    }
    if args.iters_per_round == 0 {
        return Err("--iters-per-round must be greater than 0".to_string());
    }
    if args.arenas > THREAD_COUNT {
        return Err(format!(
            "--arenas must be no more than {THREAD_COUNT}; extra arenas would be unused"
        ));
    }
    for data_size in &args.data_sizes {
        if data_size.0 < THREAD_COUNT {
            return Err(format!(
                "--data-size must be at least {THREAD_COUNT} bytes: {}",
                data_size.0
            ));
        }
    }

    Ok(())
}

fn main() {
    let args = Args::parse();
    if let Err(error) = validate_args(&args) {
        eprintln!("error: {error}");
        process::exit(2);
    }

    let worker_arenas = if args.arenas == 0 {
        None
    } else {
        let mut arenas = Vec::with_capacity(args.arenas);
        for _ in 0..args.arenas {
            let arena = match Jemalloc::create_arena() {
                Ok(arena) => arena,
                Err(error) => {
                    eprintln!("error: failed to create jemalloc arena: {error}");
                    process::exit(2);
                }
            };
            arenas.push(arena);
        }
        Some(Arc::new(arenas))
    };

    print_run_summary(
        &args,
        worker_arenas
            .as_ref()
            .map(|worker_arena_ids| worker_arena_ids.as_slice()),
    );

    let sleep_duration = Duration::from_millis(args.sleep_ms);
    let worker_pool = match WorkerPool::new(worker_arenas) {
        Ok(worker_pool) => worker_pool,
        Err(error) => {
            eprintln!("error: {error}");
            process::exit(2);
        }
    };
    #[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
    let mut previous_jemalloc_stats = if args.no_jemalloc_stats {
        None
    } else {
        Some(print_jemalloc_stats_or_exit("at start", None))
    };
    let mut round_duration_summaries = Vec::with_capacity(args.data_sizes.len());

    for data_size in &args.data_sizes {
        let data_size_label = format_data_size(data_size.0);
        let shared_data = Arc::new(vec![0; data_size.0]);
        let bytes_copied = bytes_copied(data_size.0, args.iters_per_round);
        let mut round_durations = Vec::with_capacity(args.rounds);

        println!();
        println!("data size: {data_size_label}");
        println!("bytes copied per round: {}", format_bytes(bytes_copied));
        #[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
        maybe_print_jemalloc_stats(
            &format!("after allocating shared data for {data_size_label}"),
            &mut previous_jemalloc_stats,
        );

        if args.warmup_iters > 0 {
            let checksum = worker_pool.break_cow_iterations(&shared_data, args.warmup_iters);
            black_box(checksum);
            println!(
                "warmup: {} {}, checksum {checksum}",
                args.warmup_iters,
                iteration_unit(args.warmup_iters)
            );
            #[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
            maybe_print_jemalloc_stats(
                &format!("after warmup for {data_size_label}"),
                &mut previous_jemalloc_stats,
            );
        } else {
            println!("warmup: disabled");
        }

        for round in 0..args.rounds {
            let round_number = round
                .checked_add(1)
                .expect("round number must not overflow");
            if !sleep_duration.is_zero() {
                #[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
                maybe_print_jemalloc_stats(
                    &format!("before round {round_number} sleep for {data_size_label}"),
                    &mut previous_jemalloc_stats,
                );
                thread::sleep(sleep_duration);
                #[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
                maybe_print_jemalloc_stats(
                    &format!("after round {round_number} sleep for {data_size_label}"),
                    &mut previous_jemalloc_stats,
                );
            }

            let start = Instant::now();
            let checksum = worker_pool.break_cow_iterations(&shared_data, args.iters_per_round);
            let elapsed = start.elapsed();
            black_box(checksum);
            round_durations.push(elapsed);

            let elapsed_ns = elapsed.as_nanos();
            let ns_per_iter = elapsed_ns as f64 / args.iters_per_round as f64;

            println!(
                "round {:>3}/{}: elapsed {}, per iter {}, throughput {}, checksum {}",
                round_number,
                args.rounds,
                format_duration(elapsed),
                format_duration_ns(ns_per_iter),
                format_throughput(bytes_copied, elapsed),
                checksum
            );
            #[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
            maybe_print_jemalloc_stats(
                &format!("after round {round_number} COW for {data_size_label}"),
                &mut previous_jemalloc_stats,
            );
        }

        round_duration_summaries.push(RoundDurationSummary {
            data_size_label,
            durations: round_durations,
        });
    }

    worker_pool.shutdown();
    print_round_duration_percentiles(&round_duration_summaries);
}
