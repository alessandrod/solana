use {
    clap::Parser,
    std::{
        fs,
        fs::File,
        io::{self, BufRead, BufReader, ErrorKind, Write},
        num::ParseIntError,
        path::Path,
    },
};

#[derive(Debug)]
enum QueueType {
    Rx,
    Tx,
    Combined,
}

struct Queue {
    num: usize,
    irq: u32,
    ty: QueueType,
}

fn nic_driver(iface: &str) -> io::Result<NetworkDriver> {
    if iface.contains('/') || iface.contains('\\') {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "Invalid interface name",
        ));
    }

    let path = format!("/sys/class/net/{}/device/driver", iface);

    let path = fs::read_link(path).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("Failed to read driver link for interface {}: {}", iface, e),
        )
    })?;

    Ok(path.file_name().unwrap().to_str().unwrap().into())
}

pub fn nic_irqs(interface: &str) -> io::Result<Vec<u32>> {
    let irq_path = format!("/sys/class/net/{}/device/msi_irqs", interface);

    if !Path::new(&irq_path).exists() {
        return Err(io::Error::new(
            ErrorKind::NotFound,
            format!(
                "Interface '{}' not found or doesn't support MSI-X IRQs",
                interface
            ),
        ));
    }

    let entries = fs::read_dir(&irq_path)?;

    let mut irqs = Vec::new();
    for entry in entries {
        let entry = entry?;

        // Get the filename (which is the IRQ number)
        if let Some(irq_str) = entry.file_name().to_str() {
            // Parse the IRQ number to u32
            match irq_str.parse::<u32>() {
                Ok(irq) => irqs.push(irq),
                Err(_) => {
                    // Skip entries that can't be parsed as integers
                    continue;
                }
            }
        }
    }

    irqs.sort();

    Ok(irqs)
}

enum NetworkDriver {
    Mlx5,
    Other(String),
}

impl From<&str> for NetworkDriver {
    fn from(driver: &str) -> Self {
        match driver {
            "mlx5_core" => NetworkDriver::Mlx5,
            other => NetworkDriver::Other(other.to_string()),
        }
    }
}

struct NetworkInterface {
    name: String,
    driver: NetworkDriver,
}

impl NetworkInterface {
    fn new(name: impl Into<String>) -> io::Result<Self> {
        let name = name.into();
        let driver = nic_driver(&name)?;
        Ok(NetworkInterface { name, driver })
    }

    fn queues(&self) -> io::Result<Vec<Queue>> {
        let irqs = nic_irqs(&self.name)?;
        let queues: Vec<Queue> = irqs
            .into_iter()
            .enumerate()
            .map(|(num, irq)| {
                let ty = match self.driver {
                    NetworkDriver::Mlx5 => QueueType::Combined,
                    NetworkDriver::Other(_) => todo!(),
                };
                Queue { num, irq, ty }
            })
            .collect();

        Ok(queues)
    }
}

fn irq_affinity(irq: u32) -> io::Result<Vec<usize>> {
    let path = format!("/proc/irq/{}/smp_affinity", irq);
    let affinity = fs::read_to_string(&path)?;

    let affinity_mask =
        u64::from_str_radix(&affinity.trim().replace(",", ""), 16).map_err(|e| {
            io::Error::new(
                ErrorKind::InvalidData,
                format!("Failed to parse affinity mask for IRQ {}: {}", irq, e),
            )
        })?;

    let mut cpus = Vec::new();
    let mut cpu = 0;
    let mut mask = affinity_mask;
    while mask != 0 {
        if mask & 1 != 0 {
            cpus.push(cpu);
        }
        mask >>= 1;
        cpu += 1;
    }
    Ok(cpus)
}

pub fn parse_list(data: &str) -> Result<Vec<u32>, io::Error> {
    data.split(',')
        .map(|range| {
            let mut iter = range
                .split('-')
                .map(|s| s.parse::<u32>().map_err(|ParseIntError { .. }| range));
            let start = iter.next().unwrap()?; // str::split always returns at least one element.
            let end = match iter.next() {
                None => start,
                Some(end) => {
                    if iter.next().is_some() {
                        return Err(range);
                    }
                    end?
                }
            };
            Ok(start..=end)
        })
        .try_fold(Vec::new(), |mut cpus, range| {
            let range = range.map_err(|range| io::Error::new(io::ErrorKind::InvalidData, range))?;
            cpus.extend(range);
            Ok(cpus)
        })
}

fn set_irq_affinity(irqs: &str, cpu_list: &str) -> io::Result<()> {
    let irq_list = parse_list(irqs)?;

    for irq in irq_list {
        let path = format!("/proc/irq/{}/smp_affinity_list", irq);
        let mut file = File::create(&path)?;
        write!(file, "{cpu_list}")?;
    }

    Ok(())
}

#[derive(Debug, Parser)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(subcommand)]
    command: Command,
}

fn main() {
    let args = Args::parse();

    match args.command {
        Command::ShowQueues { interface } => {
            let nic = match NetworkInterface::new(&interface) {
                Ok(nic) => nic,
                Err(e) => {
                    eprintln!("Failed to open network interface: {}", e);
                    return;
                }
            };

            let queues = match nic.queues() {
                Ok(queues) => queues,
                Err(e) => {
                    eprintln!("Failed to get queues: {}", e);
                    return;
                }
            };
            for queue in queues {
                println!(
                    "Queue {}: IRQ {}, Type: {:?}",
                    queue.num, queue.irq, queue.ty
                );
            }
        }
        Command::ShowAffinity { interface } => {
            let nic = match NetworkInterface::new(&interface) {
                Ok(nic) => nic,
                Err(e) => {
                    eprintln!("Failed to open network interface: {}", e);
                    return;
                }
            };

            let queues = match nic.queues() {
                Ok(queues) => queues,
                Err(e) => {
                    eprintln!("Failed to get queues: {}", e);
                    return;
                }
            };

            for queue in queues {
                match irq_affinity(queue.irq) {
                    Ok(cpus) => {
                        println!("Queue {}: IRQ {}, CPUs: {:?}", queue.num, queue.irq, cpus);
                    }
                    Err(e) => {
                        eprintln!("Failed to get affinity for IRQ {}: {}", queue.irq, e);
                    }
                }
            }
        }
        Command::SetAffinity { irq, cpu } => match set_irq_affinity(&irq, &cpu) {
            Ok(_) => println!("Successfully set affinity for IRQs {} to CPUs {}", irq, cpu),
            Err(e) => eprintln!("Failed to set affinity for IRQs {}: {}", irq, e),
        },
    }
}

#[derive(Debug, Parser)]
enum Command {
    ShowQueues {
        #[clap(short, long)]
        interface: String,
    },
    ShowAffinity {
        #[clap(short, long)]
        interface: String,
    },
    SetAffinity {
        #[clap(short, long)]
        irq: String,
        #[clap(short, long)]
        cpu: String,
    },
}
