//! The `retransmit_stage` retransmits shreds between validators

use {
    crate::{
        addr_cache::AddrCache,
        cluster_nodes::{self, ClusterNodes, ClusterNodesCache, Error, MAX_NUM_TURBINE_HOPS},
    },
    agave_io_uring::{io_uring_supported, Ring, RingCtx, RingOp},
    bytes::Bytes,
    crossbeam_channel::{Receiver, RecvError, Sender, TryRecvError},
    io_uring::{cqueue, opcode, squeue, types::Fd, IoUring},
    libc::{iovec, msghdr, sockaddr_in, sockaddr_in6, AF_INET, AF_INET6},
    lru::LruCache,
    rand::Rng,
    rayon::{prelude::*, ThreadPool, ThreadPoolBuilder},
    solana_gossip::{cluster_info::ClusterInfo, contact_info::Protocol},
    solana_ledger::{
        leader_schedule_cache::LeaderScheduleCache,
        shred::{self, ShredFlags, ShredId, ShredType},
    },
    solana_measure::measure::Measure,
    solana_perf::deduper::Deduper,
    solana_rayon_threadlimit::get_thread_count,
    solana_rpc::{
        max_slots::MaxSlots, rpc_subscriptions::RpcSubscriptions,
        slot_status_notifier::SlotStatusNotifier,
    },
    solana_rpc_client_api::response::SlotUpdate,
    solana_runtime::{
        bank::{Bank, MAX_LEADER_SCHEDULE_STAKES},
        bank_forks::BankForks,
    },
    solana_sdk::{clock::Slot, pubkey::Pubkey, timing::timestamp},
    solana_streamer::{
        sendmmsg::{multi_target_send, SendPktsError},
        socket::SocketAddrSpace,
    },
    static_assertions::const_assert_eq,
    std::{
        borrow::Cow,
        collections::{HashMap, HashSet},
        io,
        mem::{self, MaybeUninit},
        net::{SocketAddr, UdpSocket},
        ops::AddAssign,
        os::fd::{AsFd as _, AsRawFd as _, BorrowedFd},
        ptr,
        sync::{
            atomic::{AtomicU64, AtomicUsize, Ordering},
            Arc, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
    tokio::sync::mpsc::Sender as AsyncSender,
};

const MAX_DUPLICATE_COUNT: usize = 2;
const DEDUPER_FALSE_POSITIVE_RATE: f64 = 0.001;
const DEDUPER_NUM_BITS: u64 = 637_534_199; // 76MB
const DEDUPER_RESET_CYCLE: Duration = Duration::from_secs(5 * 60);
// Minimum number of shreds to use rayon parallel iterators.
const PAR_ITER_MIN_NUM_SHREDS: usize = 2;

const_assert_eq!(CLUSTER_NODES_CACHE_NUM_EPOCH_CAP, 5);
const CLUSTER_NODES_CACHE_NUM_EPOCH_CAP: usize = MAX_LEADER_SCHEDULE_STAKES as usize;
const CLUSTER_NODES_CACHE_TTL: Duration = Duration::from_secs(5);

// Output of fn retransmit_shred(...).
struct RetransmitShredOutput {
    shred: ShredId,
    // If the shred has ShredFlags::LAST_SHRED_IN_SLOT.
    last_shred_in_slot: bool,
    // This node's distance from the turbine root.
    root_distance: u8,
    // Number of nodes the shred was retransmitted to.
    num_nodes: usize,
    // Addresses the shred was sent to if there was a cache miss.
    addrs: Option<Box<[SocketAddr]>>,
}

#[derive(Default)]
pub(crate) struct RetransmitSlotStats {
    asof: u64,   // Latest timestamp struct was updated.
    outset: u64, // 1st shred retransmit timestamp.
    // Maximum code and data indices observed.
    pub(crate) max_index_code: u32,
    pub(crate) max_index_data: u32,
    // If any of the shreds had ShredFlags::LAST_SHRED_IN_SLOT.
    pub(crate) last_shred_in_slot: bool,
    // Number of shreds sent and received at different
    // distances from the turbine broadcast root.
    pub(crate) num_shreds_received: [usize; MAX_NUM_TURBINE_HOPS],
    num_shreds_sent: [usize; MAX_NUM_TURBINE_HOPS],
    // Root distance and socket-addresses the shreds were sent to if there was
    // a cache miss.
    pub(crate) addrs: Vec<(ShredId, /*root_distance:*/ u8, Box<[SocketAddr]>)>,
}

struct RetransmitStats {
    since: Instant,
    addr_cache_hit: AtomicUsize,
    addr_cache_miss: AtomicUsize,
    num_nodes: AtomicUsize,
    num_addrs_failed: AtomicUsize,
    num_loopback_errs: AtomicUsize,
    num_shreds: usize,
    num_shreds_skipped: AtomicUsize,
    num_small_batches: usize,
    total_batches: usize,
    total_time: u64,
    epoch_fetch: u64,
    epoch_cache_update: u64,
    retransmit_total: AtomicU64,
    compute_turbine_peers_total: AtomicU64,
    slot_stats: LruCache<Slot, RetransmitSlotStats>,
    unknown_shred_slot_leader: usize,
}

impl RetransmitStats {
    fn maybe_submit(
        &mut self,
        root_bank: &Bank,
        working_bank: &Bank,
        cluster_info: &ClusterInfo,
        cluster_nodes_cache: &ClusterNodesCache<RetransmitStage>,
    ) {
        const SUBMIT_CADENCE: Duration = Duration::from_secs(2);
        if self.since.elapsed() < SUBMIT_CADENCE {
            return;
        }
        cluster_nodes_cache
            .get(root_bank.slot(), root_bank, working_bank, cluster_info)
            .submit_metrics("cluster_nodes_retransmit", timestamp());
        datapoint_info!(
            "retransmit-stage",
            ("total_time", self.total_time, i64),
            ("epoch_fetch", self.epoch_fetch, i64),
            ("epoch_cache_update", self.epoch_cache_update, i64),
            ("total_batches", self.total_batches, i64),
            ("num_small_batches", self.num_small_batches, i64),
            ("num_nodes", *self.num_nodes.get_mut(), i64),
            ("num_addrs_failed", *self.num_addrs_failed.get_mut(), i64),
            ("num_loopback_errs", *self.num_loopback_errs.get_mut(), i64),
            ("num_shreds", self.num_shreds, i64),
            (
                "num_shreds_skipped",
                *self.num_shreds_skipped.get_mut(),
                i64
            ),
            ("retransmit_total", *self.retransmit_total.get_mut(), i64),
            ("addr_cache_hit", *self.addr_cache_hit.get_mut(), i64),
            ("addr_cache_miss", *self.addr_cache_miss.get_mut(), i64),
            (
                "compute_turbine",
                *self.compute_turbine_peers_total.get_mut(),
                i64
            ),
            (
                "unknown_shred_slot_leader",
                self.unknown_shred_slot_leader,
                i64
            ),
        );
        // slot_stats are submited at a different cadence.
        let old = std::mem::replace(self, Self::new(Instant::now()));
        self.slot_stats = old.slot_stats;
    }
}

struct ShredDeduper<const K: usize = 2> {
    deduper: Deduper<K, /*shred:*/ [u8]>,
    shred_id_filter: Deduper<K, (ShredId, /*0..MAX_DUPLICATE_COUNT:*/ usize)>,
}

impl<const K: usize> ShredDeduper<K> {
    fn new<R: Rng>(rng: &mut R, num_bits: u64) -> Self {
        Self {
            deduper: Deduper::new(rng, num_bits),
            shred_id_filter: Deduper::new(rng, num_bits),
        }
    }

    fn maybe_reset<R: Rng>(
        &mut self,
        rng: &mut R,
        false_positive_rate: f64,
        reset_cycle: Duration,
    ) {
        self.deduper
            .maybe_reset(rng, false_positive_rate, reset_cycle);
        self.shred_id_filter
            .maybe_reset(rng, false_positive_rate, reset_cycle);
    }

    // Returns true if the shred is duplicate and should be discarded.
    #[must_use]
    fn dedup(&self, key: ShredId, shred: &[u8], max_duplicate_count: usize) -> bool {
        // Shreds in the retransmit stage:
        //   * don't have repair nonce (repaired shreds are not retransmitted).
        //   * are already resigned by this node as the retransmitter.
        //   * have their leader's signature verified.
        // Therefore in order to dedup shreds, it suffices to compare:
        //    (signature, slot, shred-index, shred-type)
        // Because ShredCommonHeader already includes all of the above tuple,
        // the rest of the payload can be skipped.
        // In order to detect duplicate blocks across cluster, we retransmit
        // max_duplicate_count different shreds for each ShredId.
        shred::layout::get_common_header_bytes(shred)
            .map(|header| self.deduper.dedup(header))
            .unwrap_or(true)
            || (0..max_duplicate_count).all(|i| self.shred_id_filter.dedup(&(key, i)))
    }
}

// pull the shreds from the shreds_receiver until empty, then retransmit them.
// uses a thread_pool to parallelize work if there are enough shreds to justify that
#[allow(clippy::too_many_arguments)]
fn retransmit(
    thread_pool: &ThreadPool,
    bank_forks: &RwLock<BankForks>,
    leader_schedule_cache: &LeaderScheduleCache,
    cluster_info: &ClusterInfo,
    retransmit_receiver: &Receiver<Vec<shred::Payload>>,
    retransmit_sockets: &[UdpSocket],
    quic_endpoint_sender: &AsyncSender<(SocketAddr, Bytes)>,
    udp_sender: &Option<Sender<(Vec<SocketAddr>, (usize, shred::Payload))>>,
    stats: &mut RetransmitStats,
    cluster_nodes_cache: &ClusterNodesCache<RetransmitStage>,
    addr_cache: &mut AddrCache,
    shred_deduper: &mut ShredDeduper,
    max_slots: &MaxSlots,
    rpc_subscriptions: Option<&RpcSubscriptions>,
    slot_status_notifier: Option<&SlotStatusNotifier>,
) -> Result<(), RecvError> {
    // Try to receive shreds from the channel without blocking. If the channel
    // is empty precompute turbine trees speculatively. If no cache updates are
    // made then block on the channel until some shreds are received.
    let mut shreds = match retransmit_receiver.try_recv() {
        Ok(shreds) => shreds,
        Err(TryRecvError::Disconnected) => return Err(RecvError),
        Err(TryRecvError::Empty) => {
            if cache_retransmit_addrs(
                thread_pool,
                addr_cache,
                bank_forks,
                leader_schedule_cache,
                cluster_info,
                cluster_nodes_cache,
            ) {
                return Ok(());
            }
            retransmit_receiver.recv()?
        }
    };
    // now the batch has started
    let mut timer_start = Measure::start("retransmit");
    // drain the channel until it is empty to form a batch
    shreds.extend(retransmit_receiver.try_iter().flatten());
    stats.num_shreds += shreds.len();
    stats.total_batches += 1;

    let mut epoch_fetch = Measure::start("retransmit_epoch_fetch");
    let (working_bank, root_bank) = {
        let bank_forks = bank_forks.read().unwrap();
        (bank_forks.working_bank(), bank_forks.root_bank())
    };
    epoch_fetch.stop();
    stats.epoch_fetch += epoch_fetch.as_us();

    let mut epoch_cache_update = Measure::start("retransmit_epoch_cache_update");
    shred_deduper.maybe_reset(
        &mut rand::thread_rng(),
        DEDUPER_FALSE_POSITIVE_RATE,
        DEDUPER_RESET_CYCLE,
    );
    epoch_cache_update.stop();
    stats.epoch_cache_update += epoch_cache_update.as_us();
    // Lookup slot leader and cluster nodes for each slot.
    let cache: HashMap<Slot, _> = shreds
        .iter()
        .filter_map(|shred| shred::layout::get_slot(shred))
        .collect::<HashSet<Slot>>()
        .into_iter()
        .filter_map(|slot: Slot| {
            max_slots.retransmit.fetch_max(slot, Ordering::Relaxed);
            // TODO: consider using root-bank here for leader lookup!
            // Shreds' signatures should be verified before they reach here,
            // and if the leader is unknown they should fail signature check.
            // So here we should expect to know the slot leader and otherwise
            // skip the shred.
            let Some(slot_leader) = leader_schedule_cache.slot_leader_at(slot, Some(&working_bank))
            else {
                stats.unknown_shred_slot_leader += shreds.len();
                return None;
            };
            let cluster_nodes =
                cluster_nodes_cache.get(slot, &root_bank, &working_bank, cluster_info);
            Some((slot, (slot_leader, cluster_nodes)))
        })
        .collect();
    let socket_addr_space = cluster_info.socket_addr_space();
    let record = |mut stats: HashMap<Slot, RetransmitSlotStats>, out: RetransmitShredOutput| {
        let now = timestamp();
        let entry = stats.entry(out.shred.slot()).or_default();
        entry.record(now, out);
        stats
    };
    let retransmit_shred = |shred, socket, stats| {
        retransmit_shred(
            shred,
            &root_bank,
            shred_deduper,
            &cache,
            addr_cache,
            socket_addr_space,
            socket,
            quic_endpoint_sender,
            udp_sender,
            stats,
        )
    };
    let slot_stats = if shreds.len() < PAR_ITER_MIN_NUM_SHREDS {
        stats.num_small_batches += 1;
        shreds
            .into_iter()
            .enumerate()
            .filter_map(|(index, shred)| {
                let socket = &retransmit_sockets[index % retransmit_sockets.len()];
                retransmit_shred(shred, socket, stats)
            })
            .fold(HashMap::new(), record)
    } else {
        thread_pool.install(|| {
            shreds
                .into_par_iter()
                .filter_map(|shred| {
                    let index = thread_pool.current_thread_index().unwrap();
                    let socket = &retransmit_sockets[index % retransmit_sockets.len()];
                    retransmit_shred(shred, socket, stats)
                })
                .fold(HashMap::new, record)
                .reduce(HashMap::new, RetransmitSlotStats::merge)
        })
    };
    stats.upsert_slot_stats(
        slot_stats,
        root_bank.slot(),
        addr_cache,
        rpc_subscriptions,
        slot_status_notifier,
    );
    timer_start.stop();
    stats.total_time += timer_start.as_us();
    stats.maybe_submit(&root_bank, &working_bank, cluster_info, cluster_nodes_cache);
    Ok(())
}

// Retransmit a single shred to all downstream nodes
fn retransmit_shred(
    shred: shred::Payload,
    root_bank: &Bank,
    shred_deduper: &ShredDeduper,
    cache: &HashMap<Slot, (/*leader:*/ Pubkey, Arc<ClusterNodes<RetransmitStage>>)>,
    addr_cache: &AddrCache,
    socket_addr_space: &SocketAddrSpace,
    socket: &UdpSocket,
    quic_endpoint_sender: &AsyncSender<(SocketAddr, Bytes)>,
    udp_sender: &Option<Sender<(Vec<SocketAddr>, (usize, shred::Payload))>>,
    stats: &RetransmitStats,
) -> Option<RetransmitShredOutput> {
    let key = shred::layout::get_shred_id(shred.as_ref())?;
    if key.slot() < root_bank.slot()
        || shred_deduper.dedup(key, shred.as_ref(), MAX_DUPLICATE_COUNT)
    {
        stats.num_shreds_skipped.fetch_add(1, Ordering::Relaxed);
        return None;
    }
    let mut compute_turbine_peers = Measure::start("turbine_start");
    let (root_distance, addrs) =
        get_retransmit_addrs(&key, root_bank, cache, addr_cache, socket_addr_space, stats)?;
    compute_turbine_peers.stop();
    stats
        .compute_turbine_peers_total
        .fetch_add(compute_turbine_peers.as_us(), Ordering::Relaxed);
    let last_shred_in_slot = shred::wire::get_flags(shred.as_ref())
        .map(|flags| flags.contains(ShredFlags::LAST_SHRED_IN_SLOT))
        .unwrap_or_default();
    let mut retransmit_time = Measure::start("retransmit_to");
    let num_addrs = addrs.len();
    let num_nodes = match cluster_nodes::get_broadcast_protocol(&key) {
        Protocol::QUIC => {
            let shred = Bytes::from(shred::Payload::unwrap_or_clone(shred));
            addrs
                .iter()
                .filter_map(|&addr| quic_endpoint_sender.try_send((addr, shred.clone())).ok())
                .count()
        }
        Protocol::UDP => {
            if let Some(udp_sender) = udp_sender {
                let count = addrs.len();
                udp_sender
                    .send((addrs.iter().cloned().collect(), (42, shred)))
                    .unwrap();
                count
            } else {
                match multi_target_send(socket, shred, &addrs) {
                    Ok(()) => addrs.len(),
                    Err(SendPktsError::IoError(ioerr, num_failed)) => {
                        error!(
                    "retransmit_to multi_target_send error: {ioerr:?}, {num_failed}/{} packets failed",
                    addrs.len(),
                );
                        addrs.len() - num_failed
                    }
                }
            }
        }
    };
    retransmit_time.stop();
    stats
        .num_addrs_failed
        .fetch_add(num_addrs - num_nodes, Ordering::Relaxed);
    stats.num_nodes.fetch_add(num_nodes, Ordering::Relaxed);
    stats
        .retransmit_total
        .fetch_add(retransmit_time.as_us(), Ordering::Relaxed);
    Some(RetransmitShredOutput {
        shred: key,
        last_shred_in_slot,
        root_distance,
        num_nodes,
        addrs: match addrs {
            Cow::Owned(addrs) => Some(addrs.into_boxed_slice()),
            Cow::Borrowed(_) => None,
        },
    })
}

fn get_retransmit_addrs<'a>(
    shred: &ShredId,
    root_bank: &Bank,
    cache: &HashMap<Slot, (/*leader:*/ Pubkey, Arc<ClusterNodes<RetransmitStage>>)>,
    addr_cache: &'a AddrCache,
    socket_addr_space: &SocketAddrSpace,
    stats: &RetransmitStats,
) -> Option<(/*root_distance:*/ u8, Cow<'a, [SocketAddr]>)> {
    if let Some((root_distance, addrs)) = addr_cache.get(shred) {
        stats.addr_cache_hit.fetch_add(1, Ordering::Relaxed);
        return Some((root_distance, Cow::Borrowed(addrs)));
    }
    let (slot_leader, cluster_nodes) = cache.get(&shred.slot())?;
    let data_plane_fanout = cluster_nodes::get_data_plane_fanout(shred.slot(), root_bank);
    let (root_distance, addrs) = cluster_nodes
        .get_retransmit_addrs(slot_leader, shred, data_plane_fanout, socket_addr_space)
        .inspect_err(|err| match err {
            Error::Loopback { .. } => {
                stats.num_loopback_errs.fetch_add(1, Ordering::Relaxed);
            }
        })
        .ok()?;
    stats.addr_cache_miss.fetch_add(1, Ordering::Relaxed);
    Some((root_distance, Cow::Owned(addrs)))
}

// Speculatively precomputes turbine tree and caches retranmsit addresses.
// Returns false if no new addresses were cached.
#[must_use]
fn cache_retransmit_addrs(
    thread_pool: &ThreadPool,
    addr_cache: &mut AddrCache,
    bank_forks: &RwLock<BankForks>,
    leader_schedule_cache: &LeaderScheduleCache,
    cluster_info: &ClusterInfo,
    cluster_nodes_cache: &ClusterNodesCache<RetransmitStage>,
) -> bool {
    let shreds = addr_cache.get_shreds(thread_pool.current_num_threads() * 4);
    if shreds.is_empty() {
        return false;
    }
    let (working_bank, root_bank) = {
        let bank_forks = bank_forks.read().unwrap();
        (bank_forks.working_bank(), bank_forks.root_bank())
    };
    let cache: HashMap<Slot, _> = shreds
        .iter()
        .map(ShredId::slot)
        .collect::<HashSet<Slot>>()
        .into_iter()
        .filter_map(|slot: Slot| {
            let slot_leader = leader_schedule_cache.slot_leader_at(slot, Some(&working_bank))?;
            let cluster_nodes =
                cluster_nodes_cache.get(slot, &root_bank, &working_bank, cluster_info);
            Some((slot, (slot_leader, cluster_nodes)))
        })
        .collect();
    if cache.is_empty() {
        return false;
    }
    let socket_addr_space = cluster_info.socket_addr_space();
    let get_retransmit_addrs = |shred: ShredId| {
        let data_plane_fanout = cluster_nodes::get_data_plane_fanout(shred.slot(), &root_bank);
        let (slot_leader, cluster_nodes) = cache.get(&shred.slot())?;
        let (root_distance, addrs) = cluster_nodes
            .get_retransmit_addrs(slot_leader, &shred, data_plane_fanout, socket_addr_space)
            .ok()?;
        Some((shred, (root_distance, addrs.into_boxed_slice())))
    };
    let mut out = false;
    if shreds.len() < PAR_ITER_MIN_NUM_SHREDS {
        for (shred, entry) in shreds.into_iter().filter_map(get_retransmit_addrs) {
            addr_cache.put(&shred, entry);
            out = true;
        }
    } else {
        let entries: Vec<_> = thread_pool.install(|| {
            shreds
                .into_par_iter()
                .filter_map(get_retransmit_addrs)
                .collect()
        });
        for (shred, entry) in entries {
            addr_cache.put(&shred, entry);
            out = true;
        }
    }
    out
}

/// Service to retransmit messages received from other peers in turbine.
pub struct RetransmitStage {
    retransmit_thread_handle: JoinHandle<()>,
    retransmit_io_thread_handle: Option<JoinHandle<()>>,
}

impl RetransmitStage {
    /// Construct the RetransmitStage.
    ///
    /// Key arguments:
    /// * `retransmit_sockets` - Sockets to use for transmission of shreds
    /// * `max_slots` - Structure to keep track of the Turbine progress
    /// * `bank_forks` - Reference to the BankForks structure
    /// * `leader_schedule_cache` - The leader schedule to verify shreds
    /// * `cluster_info` - This structure needs to be updated and populated by the bank and via gossip.
    /// * `retransmit_receiver` - Receive channel for batches of shreds to be retransmitted.
    pub fn new(
        bank_forks: Arc<RwLock<BankForks>>,
        leader_schedule_cache: Arc<LeaderScheduleCache>,
        cluster_info: Arc<ClusterInfo>,
        retransmit_sockets: Arc<Vec<UdpSocket>>,
        quic_endpoint_sender: AsyncSender<(SocketAddr, Bytes)>,
        retransmit_receiver: Receiver<Vec<shred::Payload>>,
        max_slots: Arc<MaxSlots>,
        rpc_subscriptions: Option<Arc<RpcSubscriptions>>,
        slot_status_notifier: Option<SlotStatusNotifier>,
    ) -> Self {
        let cluster_nodes_cache = ClusterNodesCache::<RetransmitStage>::new(
            CLUSTER_NODES_CACHE_NUM_EPOCH_CAP,
            CLUSTER_NODES_CACHE_TTL,
        );
        let mut rng = rand::thread_rng();
        let mut stats = RetransmitStats::new(Instant::now());
        let mut addr_cache = AddrCache::with_capacity(/*capacity:*/ 4);
        let mut shred_deduper = ShredDeduper::new(&mut rng, DEDUPER_NUM_BITS);

        let thread_pool = {
            // Using clamp will panic if max < min.
            #[allow(clippy::manual_clamp)]
            let num_threads = get_thread_count().min(12).max(retransmit_sockets.len());
            ThreadPoolBuilder::new()
                .num_threads(num_threads)
                .thread_name(|i| format!("solRetransmit{i:02}"))
                .build()
                .unwrap()
        };

        let (udp_sender, udp_receiver) = if io_uring_supported() {
            let (a, b) = crossbeam_channel::unbounded();
            (Some(a), Some(b))
        } else {
            (None, None)
        };

        let retransmit_thread_handle = Builder::new()
            .name("solRetransmittr".to_string())
            .spawn({
                let retransmit_sockets = retransmit_sockets.clone();
                move || {
                    while retransmit(
                        &thread_pool,
                        &bank_forks,
                        &leader_schedule_cache,
                        &cluster_info,
                        &retransmit_receiver,
                        &retransmit_sockets,
                        &quic_endpoint_sender,
                        &udp_sender,
                        &mut stats,
                        &cluster_nodes_cache,
                        &mut addr_cache,
                        &mut shred_deduper,
                        &max_slots,
                        rpc_subscriptions.as_deref(),
                        slot_status_notifier.as_ref(),
                    )
                    .is_ok()
                    {}
                }
            })
            .unwrap();

        let retransmit_io_thread_handle = if let Some(udp_receiver) = udp_receiver {
            Some(
                Builder::new()
                    .name("solRetransmitIO".to_string())
                    .spawn(move || {
                        let ring = IoUring::builder().setup_sqpoll(10).build(64).unwrap();
                        ring.submitter()
                            .register_iowq_max_workers(&mut [4, 4])
                            .unwrap();
                        let ring = Ring::<RingState, Op>::new(ring, RingState::new());
                        rtx_loop(ring, udp_receiver, &retransmit_sockets)
                    })
                    .unwrap(),
            )
        } else {
            None
        };

        Self {
            retransmit_thread_handle,
            retransmit_io_thread_handle,
        }
    }

    pub fn join(self) -> thread::Result<()> {
        if let Some(handle) = self.retransmit_io_thread_handle {
            handle.join()?;
        }
        self.retransmit_thread_handle.join()
    }
}

fn rtx_loop<'a>(
    mut ring: Ring<RingState, Op<'a>>,
    receiver: Receiver<(Vec<SocketAddr>, (usize, shred::Payload))>,
    sockets: &'a Arc<Vec<UdpSocket>>,
) {
    loop {
        match receiver.try_recv() {
            Ok((addrs, (buf_index, payload))) => {
                for (addr, socket) in addrs.into_iter().zip(sockets.iter().cycle()) {
                    unsafe {
                        ring.push(Op::SendMsg(SendMsgOp::new(
                            socket,
                            addr,
                            buf_index,
                            payload.clone(),
                        )))
                        .unwrap()
                    }
                }
            }
            Err(TryRecvError::Empty) => {
                ring.submit().unwrap();
                let e = ring.process_completions();
                if let Err(e) = e {
                    eprintln!("ERROR: {e}");
                }
                thread::sleep(Duration::from_nanos(500));
            }
            Err(TryRecvError::Disconnected) => break,
        }
    }
    ring.drain().unwrap();
}

impl AddAssign for RetransmitSlotStats {
    fn add_assign(&mut self, other: Self) {
        let Self {
            asof,
            outset,
            max_index_code,
            max_index_data,
            last_shred_in_slot,
            num_shreds_received,
            num_shreds_sent,
            mut addrs,
        } = other;
        self.asof = self.asof.max(asof);
        self.max_index_code = self.max_index_code.max(max_index_code);
        self.max_index_data = self.max_index_data.max(max_index_data);
        self.last_shred_in_slot |= last_shred_in_slot;
        self.outset = if self.outset == 0 {
            outset
        } else {
            self.outset.min(outset)
        };
        if self.addrs.len() < addrs.len() {
            std::mem::swap(&mut self.addrs, &mut addrs);
        }
        self.addrs.append(&mut addrs);
        for k in 0..MAX_NUM_TURBINE_HOPS {
            self.num_shreds_received[k] += num_shreds_received[k];
            self.num_shreds_sent[k] += num_shreds_sent[k];
        }
    }
}

impl RetransmitStats {
    const SLOT_STATS_CACHE_CAPACITY: usize = 750;

    fn new(now: Instant) -> Self {
        Self {
            since: now,
            addr_cache_hit: AtomicUsize::default(),
            addr_cache_miss: AtomicUsize::default(),
            num_nodes: AtomicUsize::default(),
            num_addrs_failed: AtomicUsize::default(),
            num_loopback_errs: AtomicUsize::default(),
            num_shreds: 0usize,
            num_shreds_skipped: AtomicUsize::default(),
            total_batches: 0usize,
            num_small_batches: 0usize,
            total_time: 0u64,
            epoch_fetch: 0u64,
            epoch_cache_update: 0u64,
            retransmit_total: AtomicU64::default(),
            compute_turbine_peers_total: AtomicU64::default(),
            // Cache capacity is manually enforced by `SLOT_STATS_CACHE_CAPACITY`
            slot_stats: LruCache::<Slot, RetransmitSlotStats>::unbounded(),
            unknown_shred_slot_leader: 0usize,
        }
    }

    fn upsert_slot_stats(
        &mut self,
        feed: impl IntoIterator<Item = (Slot, RetransmitSlotStats)>,
        root: Slot,
        addr_cache: &mut AddrCache,
        rpc_subscriptions: Option<&RpcSubscriptions>,
        slot_status_notifier: Option<&SlotStatusNotifier>,
    ) {
        for (slot, mut slot_stats) in feed {
            addr_cache.record(slot, &mut slot_stats);
            match self.slot_stats.get_mut(&slot) {
                None => {
                    if slot > root {
                        notify_subscribers(
                            slot,
                            slot_stats.outset,
                            rpc_subscriptions,
                            slot_status_notifier,
                        );
                    }
                    self.slot_stats.put(slot, slot_stats);
                }
                Some(entry) => {
                    *entry += slot_stats;
                }
            }
        }
        while self.slot_stats.len() > Self::SLOT_STATS_CACHE_CAPACITY {
            // Pop and submit metrics for the slot which was updated least
            // recently. At this point the node most likely will not receive
            // and retransmit any more shreds for this slot.
            match self.slot_stats.pop_lru() {
                Some((slot, stats)) => stats.submit(slot),
                None => break,
            }
        }
    }
}

impl RetransmitSlotStats {
    fn record(&mut self, now: u64, out: RetransmitShredOutput) {
        self.outset = if self.outset == 0 {
            now
        } else {
            self.outset.min(now)
        };
        self.asof = self.asof.max(now);
        let max_index = match out.shred.shred_type() {
            ShredType::Code => &mut self.max_index_code,
            ShredType::Data => &mut self.max_index_data,
        };
        *max_index = (*max_index).max(out.shred.index());
        self.last_shred_in_slot |= out.last_shred_in_slot;
        self.num_shreds_received[usize::from(out.root_distance)] += 1;
        self.num_shreds_sent[usize::from(out.root_distance)] += out.num_nodes;
        if let Some(addrs) = out.addrs {
            self.addrs.push((out.shred, out.root_distance, addrs));
        }
    }

    fn merge(mut acc: HashMap<Slot, Self>, other: HashMap<Slot, Self>) -> HashMap<Slot, Self> {
        if acc.len() < other.len() {
            return Self::merge(other, acc);
        }
        for (key, value) in other {
            *acc.entry(key).or_default() += value;
        }
        acc
    }

    fn submit(&self, slot: Slot) {
        let num_shreds: usize = self.num_shreds_received.iter().sum();
        let num_nodes: usize = self.num_shreds_sent.iter().sum();
        let elapsed_millis = self.asof.saturating_sub(self.outset);
        datapoint_info!(
            "retransmit-stage-slot-stats",
            ("slot", slot, i64),
            ("outset_timestamp", self.outset, i64),
            ("elapsed_millis", elapsed_millis, i64),
            ("num_shreds", num_shreds, i64),
            ("num_nodes", num_nodes, i64),
            ("num_shreds_received_root", self.num_shreds_received[0], i64),
            (
                "num_shreds_received_1st_layer",
                self.num_shreds_received[1],
                i64
            ),
            (
                "num_shreds_received_2nd_layer",
                self.num_shreds_received[2],
                i64
            ),
            (
                "num_shreds_received_3rd_layer",
                self.num_shreds_received[3],
                i64
            ),
            ("num_shreds_sent_root", self.num_shreds_sent[0], i64),
            ("num_shreds_sent_1st_layer", self.num_shreds_sent[1], i64),
            ("num_shreds_sent_2nd_layer", self.num_shreds_sent[2], i64),
            ("num_shreds_sent_3rd_layer", self.num_shreds_sent[3], i64),
        );
    }
}

// Notifies subscribers of shreds received from a new slot.
fn notify_subscribers(
    slot: Slot,
    timestamp: u64, // When the first shred in the slot was received.
    rpc_subscriptions: Option<&RpcSubscriptions>,
    slot_status_notifier: Option<&SlotStatusNotifier>,
) {
    if let Some(rpc_subscriptions) = rpc_subscriptions {
        let slot_update = SlotUpdate::FirstShredReceived { slot, timestamp };
        rpc_subscriptions.notify_slot_update(slot_update);
        datapoint_info!("retransmit-first-shred", ("slot", slot, i64));
    }
    if let Some(slot_status_notifier) = slot_status_notifier {
        slot_status_notifier
            .read()
            .unwrap()
            .notify_first_shred_received(slot);
    }
}

struct RingState {}

impl RingState {
    fn new() -> Self {
        Self {}
    }
}

enum Op<'a> {
    SendMsg(SendMsgOp<'a>),
}

unsafe impl<'a> Send for Op<'a> {}
unsafe impl<'a> Send for SendMsgOp<'a> {}

impl RingOp<RingState> for Op<'_> {
    fn entry(&mut self) -> io_uring::squeue::Entry {
        match self {
            Op::SendMsg(op) => op.entry(),
        }
    }

    fn complete(
        &mut self,
        entry: &cqueue::Entry,
        res: io::Result<i32>,
        state: &mut RingCtx<RingState, Self>,
    ) -> io::Result<()> {
        match self {
            Op::SendMsg(op) => op.complete(entry, res, state),
        }
    }
}

#[repr(C)]
union SockAddr {
    v4: sockaddr_in,
    v6: sockaddr_in6,
}

struct SendMsgOp<'a> {
    socket: BorrowedFd<'a>,
    addr: SockAddr,
    addr_len: u32,
    iov: iovec,
    msg: msghdr,
    buf_index: usize,
    payload: shred::Payload,
}

impl<'a> SendMsgOp<'a> {
    fn new(
        socket: &'a UdpSocket,
        addr: SocketAddr,
        buf_index: usize,
        payload: shred::Payload,
    ) -> Self {
        let (addr, addr_len) = socketaddr_to_sockaddr(addr);
        Self {
            socket: socket.as_fd(),
            addr,
            addr_len,
            iov: unsafe { MaybeUninit::zeroed().assume_init() },
            msg: unsafe { MaybeUninit::zeroed().assume_init() },
            buf_index,
            payload,
        }
    }

    fn entry(&mut self) -> squeue::Entry {
        self.iov = iovec {
            iov_base: self.payload.as_ptr() as *mut _,
            iov_len: self.payload.len(),
        };
        self.msg = msghdr {
            msg_name: &mut self.addr as *mut _ as *mut _,
            msg_namelen: self.addr_len,
            msg_iov: &mut self.iov as *mut _ as *mut _,
            msg_iovlen: 1,
            msg_control: ptr::null_mut(),
            msg_controllen: 0,
            msg_flags: 0,
        };
        opcode::SendMsg::new(Fd(self.socket.as_raw_fd()), &self.msg as *const _)
            .build()
            .flags(squeue::Flags::ASYNC)
    }

    fn complete(
        &mut self,
        entry: &cqueue::Entry,
        res: io::Result<i32>,
        state: &mut RingCtx<RingState, Op>,
    ) -> io::Result<()> {
        if res.is_err() {
            eprintln!("LMAO {res:?}");
        }
        res.map(|_| ())
    }
}

fn socketaddr_to_sockaddr(addr: SocketAddr) -> (SockAddr, u32) {
    match addr {
        SocketAddr::V4(addr) => {
            let len = mem::size_of::<sockaddr_in>() as u32;
            (
                SockAddr {
                    v4: sockaddr_in {
                        sin_family: AF_INET as u16,
                        sin_port: addr.port().to_be(),
                        sin_addr: libc::in_addr {
                            s_addr: u32::from_ne_bytes(addr.ip().octets()),
                        },
                        sin_zero: [0; 8],
                    },
                },
                len,
            )
        }
        SocketAddr::V6(addr) => {
            let len = mem::size_of::<sockaddr_in6>() as u32;
            (
                SockAddr {
                    v6: sockaddr_in6 {
                        sin6_family: AF_INET6 as u16,
                        sin6_port: addr.port().to_be(),
                        sin6_flowinfo: addr.flowinfo(),
                        sin6_addr: libc::in6_addr {
                            s6_addr: addr.ip().octets(),
                        },
                        sin6_scope_id: addr.scope_id(),
                    },
                },
                len,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        rand::SeedableRng,
        rand_chacha::ChaChaRng,
        solana_ledger::shred::{Shred, ShredFlags},
        solana_sdk::signature::Keypair,
    };

    fn get_keypair() -> Keypair {
        const KEYPAIR: &str = "Fcc2HUvRC7Dv4GgehTziAremzRvwDw5miYu8Ahuu1rsGjA\
            5eCn55pXiSkEPcuqviV41rJxrFpZDmHmQkZWfoYYS";
        bs58::decode(KEYPAIR)
            .into_vec()
            .as_deref()
            .map(Keypair::from_bytes)
            .unwrap()
            .unwrap()
    }

    #[test]
    fn test_already_received() {
        let slot = 1;
        let index = 5;
        let version = 0x40;
        let keypair = get_keypair();
        let mut shred = Shred::new_from_data(
            slot,
            index,
            0,
            &[],
            ShredFlags::LAST_SHRED_IN_SLOT,
            0,
            version,
            0,
        );
        shred.sign(&keypair);
        let mut rng = ChaChaRng::from_seed([0xa5; 32]);
        let shred_deduper = ShredDeduper::<2>::new(&mut rng, /*num_bits:*/ 640_007);
        // unique shred for (1, 5) should pass
        assert!(!shred_deduper.dedup(shred.id(), shred.payload(), MAX_DUPLICATE_COUNT));
        // duplicate shred for (1, 5) blocked
        assert!(shred_deduper.dedup(shred.id(), shred.payload(), MAX_DUPLICATE_COUNT));
        let mut shred = Shred::new_from_data(
            slot,
            index,
            2,
            &[],
            ShredFlags::LAST_SHRED_IN_SLOT,
            0,
            version,
            0,
        );
        shred.sign(&keypair);
        // first duplicate shred for (1, 5) passed
        assert!(!shred_deduper.dedup(shred.id(), shred.payload(), MAX_DUPLICATE_COUNT));
        // then blocked
        assert!(shred_deduper.dedup(shred.id(), shred.payload(), MAX_DUPLICATE_COUNT));

        let mut shred = Shred::new_from_data(
            slot,
            index,
            8,
            &[],
            ShredFlags::LAST_SHRED_IN_SLOT,
            0,
            version,
            0,
        );
        shred.sign(&keypair);
        // 2nd duplicate shred for (1, 5) blocked
        assert!(shred_deduper.dedup(shred.id(), shred.payload(), MAX_DUPLICATE_COUNT));
        assert!(shred_deduper.dedup(shred.id(), shred.payload(), MAX_DUPLICATE_COUNT));

        let shred = Shred::new_from_parity_shard(slot, index, &[], 0, 1, 1, 0, version);
        // Coding at (1, 5) passes
        assert!(!shred_deduper.dedup(shred.id(), shred.payload(), MAX_DUPLICATE_COUNT));
        // then blocked
        assert!(shred_deduper.dedup(shred.id(), shred.payload(), MAX_DUPLICATE_COUNT));

        let shred = Shred::new_from_parity_shard(slot, index, &[], 2, 1, 1, 0, version);
        // 2nd unique coding at (1, 5) passes
        assert!(!shred_deduper.dedup(shred.id(), shred.payload(), MAX_DUPLICATE_COUNT));
        // same again is blocked
        assert!(shred_deduper.dedup(shred.id(), shred.payload(), MAX_DUPLICATE_COUNT));

        let shred = Shred::new_from_parity_shard(slot, index, &[], 3, 1, 1, 0, version);
        // Another unique coding at (1, 5) always blocked
        assert!(shred_deduper.dedup(shred.id(), shred.payload(), MAX_DUPLICATE_COUNT));
        assert!(shred_deduper.dedup(shred.id(), shred.payload(), MAX_DUPLICATE_COUNT));
    }

    #[test]
    fn test_ring_loop_empty_receiver() {
        let (sender, receiver) = crossbeam_channel::unbounded();
        let sockets = Arc::new(vec![UdpSocket::bind("127.0.0.1:0").unwrap()]);
        let ring = IoUring::builder().setup_sqpoll(10).build(64).unwrap();
        ring.submitter()
            .register_iowq_max_workers(&mut [4, 4])
            .unwrap();
        let mut ring = Ring::new(ring, RingState::new());
        let handle = thread::spawn(move || rtx_loop(ring, receiver, &sockets));
        thread::sleep(Duration::from_millis(100));
        drop(sender);
        handle.join().unwrap();
    }

    #[test]
    fn test_ring_loop_with_data() {
        let (sender, receiver) = crossbeam_channel::unbounded();
        let send_sockets = Arc::new(vec![
            UdpSocket::bind("127.0.0.1:0").unwrap(),
            UdpSocket::bind("127.0.0.1:0").unwrap(),
        ]);
        let recv_sockets = vec![
            UdpSocket::bind("127.0.0.1:1234").unwrap(),
            UdpSocket::bind("127.0.0.1:1235").unwrap(),
            UdpSocket::bind("127.0.0.1:1236").unwrap(),
        ];
        let addrs: Vec<_> = recv_sockets
            .iter()
            .map(|s| s.local_addr().unwrap())
            .collect();
        let payload = shred::Payload::Shared(Arc::new(vec![1, 2, 3, 4]));
        sender.send((addrs.clone(), (42, payload.clone()))).unwrap();
        let ring = IoUring::builder().setup_sqpoll(1000).build(1024).unwrap();
        ring.submitter()
            .register_iowq_max_workers(&mut [4, 4])
            .unwrap();
        let mut ring = Ring::new(ring, RingState::new());
        let handle = thread::spawn(move || rtx_loop(ring, receiver, &send_sockets));

        for socket in recv_sockets {
            let mut buf = [0; 4];
            let (len, _) = socket.recv_from(&mut buf).unwrap();
            assert_eq!(len, payload.len());
            assert_eq!(&buf, &payload[..]);
        }

        drop(sender);
        handle.join().unwrap();
    }

    #[test]
    fn test_ring_send_a_lot() {
        // Get number of rings/threads from environment variable
        let num_rings = std::env::var("NUM_RINGS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1);

        let send_sockets = Arc::new(vec![
            UdpSocket::bind("147.28.156.23:0").unwrap(),
            UdpSocket::bind("147.28.156.23:0").unwrap(),
            UdpSocket::bind("147.28.156.23:0").unwrap(),
            UdpSocket::bind("147.28.156.23:0").unwrap(),
            UdpSocket::bind("147.28.156.23:0").unwrap(),
            UdpSocket::bind("147.28.156.23:0").unwrap(),
            UdpSocket::bind("147.28.156.23:0").unwrap(),
            UdpSocket::bind("147.28.156.23:0").unwrap(),
        ]);

        let ip = std::env::var("IP").unwrap_or("86.109.14.143:8101".to_string());
        let recv_addrs = vec![ip.parse().unwrap()];

        let num_threads = std::env::var("NUM_THREADS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let sq_entries = std::env::var("SQ_ENTRIES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1024);
        let cq_entries = std::env::var("CQ_ENTRIES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1024);

        // Create collections to store our dynamic resources
        let mut channels = Vec::with_capacity(num_rings);
        let mut io_threads = Vec::with_capacity(num_rings);
        let mut sender_threads = Vec::with_capacity(num_rings);

        // Create channel pairs
        for _ in 0..num_rings {
            let (sender, receiver) = crossbeam_channel::bounded(100_000);
            channels.push((sender, receiver));
        }

        let mut fd = 0;
        // Create and spawn RTX threads
        for i in 0..num_rings {
            // Clone the sockets for this thread
            let sockets_clone = send_sockets.clone();

            // Get receiver for this thread
            let receiver = channels[i].1.clone();

            let mut builder = IoUring::builder();
            // Create a ring attached to the base ring's work queue
            builder.setup_cqsize(cq_entries);
            if fd != 0 {
                builder.setup_attach_wq(fd);
            }

            let ring = builder.build(sq_entries).unwrap();
            if fd == 0 {
                fd = ring.as_raw_fd();
            }

            if num_threads != 0 {
                ring.submitter()
                    .register_iowq_max_workers(&mut [num_threads, num_threads])
                    .unwrap();
            }

            let ring = Ring::new(ring, RingState::new());

            // Spawn rtx_loop thread
            let handle = thread::spawn(move || rtx_loop(ring, receiver, &sockets_clone));

            io_threads.push(handle);
        }

        // Create sender threads
        let data_size = std::env::var("DATA_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1400);

        for i in 0..num_rings {
            let sender = channels[i].0.clone();
            let addrs = recv_addrs.clone();
            let data = Arc::new(vec![42; data_size]);

            let handle = thread::spawn(move || loop {
                sender.send((
                    addrs.clone(),
                    (42, shred::Payload::Shared(Arc::clone(&data))),
                ));
            });

            sender_threads.push(handle);
        }

        // Wait for all IO threads to complete
        for handle in io_threads {
            handle.join().unwrap();
        }
    }
}

use jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;
