use {
    agave_xdp::{
        device::{NetworkDevice, QueueId},
        tx_loop::tx_loop,
    },
    clap::Parser,
    crossbeam_channel::{bounded, Receiver, Sender},
    std::{
        fs::File,
        io::{self, BufReader, Read},
        net::SocketAddr,
        thread,
    },
};

/// Command-line arguments for the file sender.
#[derive(Parser, Debug)]
struct Args {
    /// network interface to use
    #[arg(short, long)]
    interface: Option<String>,

    /// queue ID
    #[arg(short, long, default_value = "0")]
    queue_id: u64,

    /// destination address
    dest: SocketAddr,

    /// path to the file to send
    filename: String,
}

fn send_file(
    dev: NetworkDevice,
    queue_id: u64,
    dest_addr: SocketAddr,
    filename: &str,
) -> io::Result<()> {
    let file = File::open(filename)?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);

    let (sender, receiver): (
        Sender<(Vec<SocketAddr>, Vec<u8>)>,
        Receiver<(Vec<SocketAddr>, Vec<u8>)>,
    ) = bounded(1_000_000);
    let (drop_sender, _drop_receiver) = bounded(1_000_000);

    let tx_handle = thread::spawn(move || {
        tx_loop(
            &dev,
            12345,
            QueueId(queue_id),
            false,
            0,
            receiver,
            drop_sender,
        );
    });

    let mut buffer = [0u8; 1400];
    while let Ok(bytes_read) = reader.read(&mut buffer) {
        if bytes_read == 0 {
            break;
        }
        sender
            .send((vec![dest_addr], buffer[..bytes_read].to_vec()))
            .unwrap();
    }

    drop(sender);

    tx_handle.join().unwrap();

    Ok(())
}

fn main() -> io::Result<()> {
    let args = Args::parse();

    let dev = if let Some(interface) = args.interface {
        NetworkDevice::new(interface).unwrap()
    } else {
        NetworkDevice::new_from_default_route().unwrap()
    };

    send_file(dev, args.queue_id, args.dest, &args.filename)
}
