use {
    agave_xdp::{
        device::{NetworkDevice, QueueId},
        load_xdp_program,
        netlink::MacAddress,
        packet::{
            write_eth_header, write_ip_header, write_udp_header, ETH_HEADER_SIZE, IP_HEADER_SIZE,
        },
        route::Router,
        set_cpu_affinity,
        socket::{Socket, Tx, TxRing},
        umem::{Frame as _, PageAlignedMemory, SliceUmem, SliceUmemFrame, Umem as _},
    },
    caps::{CapSet, Capability},
    clap::Parser,
    core_affinity::CoreId,
    std::{
        hint,
        net::Ipv4Addr,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
        thread,
        time::{Duration, Instant},
    },
};

#[derive(Parser, Debug)]
#[command(author, version, about = "AF_XDP UDP sender", long_about = None)]
struct Opt {
    #[arg(short, long, default_value = "eth0")]
    interface: String,

    #[arg(long, default_value = "127.0.0.1")]
    dest_ip: String,

    #[arg(long, default_value = "9999")]
    dest_port: u16,

    #[arg(short, long, default_value = "64")]
    payload_size: usize,

    #[arg(short, long, default_value = "1000")]
    batch_size: usize,

    #[arg(long, default_value = "0")]
    idle_sleep_us: u64,

    #[arg(long, default_value = "0")]
    churn_threads: usize,

    #[arg(short, long)]
    zero_copy: bool,
}

// Metrics structure to share between threads
struct Metrics {
    tx_packets: AtomicUsize,
    tx_bytes: AtomicUsize,
}

// Helper function to format bitrate with appropriate units
fn format_bitrate(bits_per_second: f64) -> String {
    if bits_per_second < 1000.0 {
        format!("{:.2} bps", bits_per_second)
    } else if bits_per_second < 1_000_000.0 {
        format!("{:.2} Kbps", bits_per_second / 1000.0)
    } else if bits_per_second < 1_000_000_000.0 {
        format!("{:.2} Mbps", bits_per_second / 1_000_000.0)
    } else {
        format!("{:.2} Gbps", bits_per_second / 1_000_000_000.0)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::parse();

    let exit = Arc::new(AtomicBool::new(false));

    ctrlc::set_handler({
        let exit = Arc::clone(&exit);
        move || {
            println!("exiting...");
            exit.store(true, Ordering::Relaxed);
        }
    })?;

    for cap in [
        Capability::CAP_NET_ADMIN,
        Capability::CAP_NET_RAW,
        Capability::CAP_BPF,
    ] {
        caps::raise(None, CapSet::Effective, cap).unwrap();
    }

    let dev = NetworkDevice::new(&opt.interface).unwrap();
    let _ebpf = if opt.zero_copy {
        Some(load_xdp_program(dev.if_index()).unwrap())
    } else {
        None
    };
    let dest_ip = opt.dest_ip.parse::<Ipv4Addr>()?;

    let router = Router::new().unwrap();
    let next_hop = router.route(dest_ip.into()).unwrap();
    assert_eq!(next_hop.if_index, dev.if_index());

    let src_ip = dev.ipv4_addr().unwrap();
    let src_port = 12345;
    let src_mac = dev.mac_addr().unwrap();
    let dest_mac = next_hop.mac_addr.unwrap_or(MacAddress([0; 6])).0;

    let frame_size = 4096;
    let frame_count = 4096;

    let mut memory =
        PageAlignedMemory::alloc_with_page_size(frame_size, frame_count, 2 * 1024 * 1024, true)
            .or_else(|_| {
                println!("huge page alloc failed, falling back to regular page size");
                PageAlignedMemory::alloc(frame_size, frame_count)
            })
            .unwrap();

    let umem = SliceUmem::new(&mut memory, frame_size as u32).unwrap();
    let (mut socket, tx) =
        Socket::tx(dev.open_queue(QueueId(0)), umem, opt.zero_copy, 2048, 2048).unwrap();

    for cap in [
        Capability::CAP_NET_ADMIN,
        Capability::CAP_NET_RAW,
        Capability::CAP_BPF,
    ] {
        caps::drop(None, CapSet::Effective, cap).unwrap();
    }

    let metrics = Arc::new(Metrics {
        tx_packets: AtomicUsize::new(0),
        tx_bytes: AtomicUsize::new(0),
    });

    let metrics_thread = thread::spawn({
        let metrics = metrics.clone();
        let exit = Arc::clone(&exit);
        move || {
            let mut last_time = Instant::now();
            let mut last_packets = 0;
            let mut last_bytes = 0;

            while exit.load(Ordering::SeqCst) == false {
                thread::sleep(Duration::from_secs(1));

                let current_time = Instant::now();
                let elapsed = current_time.duration_since(last_time).as_secs_f64();

                // Get current metrics from shared metrics
                let packets = metrics.tx_packets.load(Ordering::SeqCst);
                let bytes = metrics.tx_bytes.load(Ordering::SeqCst);

                let pps = (packets - last_packets) as f64 / elapsed;
                let bps = ((bytes - last_bytes) as f64 * 8.0) / elapsed;

                println!("throughput: {:.2} pps | {}", pps, format_bitrate(bps));

                last_time = current_time;
                last_packets = packets;
                last_bytes = bytes;
            }
        }
    });

    let udp_payload_size = opt.payload_size;
    let packet_size = 14 + 20 + 8 + udp_payload_size; // Eth + IP + UDP + payload

    println!("sending UDP packets to {}:{}", dest_ip, opt.dest_port);

    let mut packet_data = vec![0xFEu8; packet_size];
    for i in 0..udp_payload_size {
        packet_data[14 + 20 + 8 + i] = (i % 256) as u8;
    }

    write_eth_header(&mut packet_data, &src_mac, &dest_mac);
    write_ip_header(
        &mut packet_data[ETH_HEADER_SIZE..],
        &src_ip,
        &dest_ip,
        udp_payload_size as u16 + 8,
    );
    write_udp_header(
        &mut packet_data[ETH_HEADER_SIZE + IP_HEADER_SIZE..],
        &src_ip,
        src_port,
        &dest_ip,
        opt.dest_port,
        udp_payload_size as u16,
        false,
    );

    // fill the whole umem with the same payload
    let mut frames = vec![];
    while let Some(mut frame) = socket.umem().reserve() {
        frame.set_len(packet_size);
        let buf = socket.umem().map_frame_mut(&frame);
        buf[..packet_size].copy_from_slice(&packet_data);
        frames.push(frame);
    }
    for frame in frames {
        socket.umem().release(frame.offset());
    }

    let cores = core_affinity::get_core_ids().expect("Failed to get core IDs");

    // Create a mask for all cores except CPU 0
    let cores_except_cpu0: Vec<CoreId> =
        cores.iter().filter(|core| core.id != 0).cloned().collect();

    unsafe {
        let mut cpu_set = std::mem::zeroed();
        for core in &cores_except_cpu0 {
            libc::CPU_SET(core.id, &mut cpu_set);
        }

        let result = libc::sched_setaffinity(
            0,
            std::mem::size_of::<libc::cpu_set_t>(),
            &cpu_set as *const libc::cpu_set_t,
        );
        if result != 0 {
            eprintln!("Failed to set process affinity");
        }
    }

    for _ in 0..opt.churn_threads {
        thread::spawn(|| loop {
            hint::black_box(())
        });
    }

    set_cpu_affinity([1]).unwrap();

    let Tx {
        mut completion,
        ring,
    } = tx;
    let mut ring = ring.unwrap();
    let umem = socket.umem();

    let kick = |ring: &TxRing<SliceUmemFrame<'_>>| {
        if !ring.needs_wakeup() {
            return;
        }

        if let Err(e) = ring.wake() {
            match e.raw_os_error() {
                // these are non-fatal errors
                Some(libc::EBUSY | libc::ENOBUFS | libc::EAGAIN) => {}
                // this can temporarily happen with some drivers when changing
                // settings (eg with ethtool)
                Some(libc::ENETDOWN) => {
                    eprintln!("network interface is down")
                }
                // we should never get here, hopefully the driver recovers?
                _ => {
                    eprintln!("network interface driver error: {e:?}");
                }
            }
        }
    };

    // drive the NIC
    let mut chunk_remaining = opt.batch_size;
    while exit.load(Ordering::Relaxed) == false {
        // wait until opt.batch_size slots are available to minimize wakeups if
        // the NIC requires them
        loop {
            completion.sync(true);
            // we haven't written any frames so we only need to sync the consumer position
            ring.sync(false);

            // check if any frames were completed
            while let Some(frame_offset) = completion.read() {
                umem.release(frame_offset);
            }

            if ring.available() > 0 && umem.available() > 0 {
                // we have a frame and a slot in the ring
                break;
            }

            // queues are full, if NEEDS_WAKEUP is set kick the driver so hopefully it'll
            // complete some work
            kick(&ring);
        }

        let mut frame = umem.reserve().unwrap();
        frame.set_len(packet_size);
        ring.write(frame, 0).map_err(|_| "ring full").unwrap();

        chunk_remaining -= 1;
        // check if it's time to commit the ring and kick the driver
        if chunk_remaining == 0 {
            chunk_remaining = opt.batch_size;

            // commit new frames
            ring.commit();
            kick(&ring);
        }

        metrics.tx_packets.fetch_add(1, Ordering::SeqCst);
        metrics.tx_bytes.fetch_add(packet_size, Ordering::SeqCst);
    }

    metrics_thread.join().unwrap();

    println!("terminated");
    Ok(())
}
