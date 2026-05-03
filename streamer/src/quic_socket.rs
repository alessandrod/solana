//! This module defines [`QuicSocket`], which allows selecting between kernel UDP and AF_XDP-backed
//! QUIC socket configurations.
use {
    agave_xdp::{
        ecn_codepoint::EcnCodepoint as XdpEcnCodepoint,
        transmitter::{BytesTxPacket, XdpSender},
    },
    bytes::Bytes,
    crossbeam_channel::TrySendError,
    nix::ifaddrs::getifaddrs,
    quinn::{
        AsyncUdpSocket, UdpPoller,
        udp::{EcnCodepoint as QuinnEcnCodepoint, RecvMeta, Transmit, UdpSocketState},
    },
    std::{
        fmt::{self, Debug},
        future::Future,
        io::{self, IoSliceMut},
        net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4},
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, ready},
    },
    tokio::io::Interest,
};

/// [`QuicSocket`] is an enum for selecting between a kernel UDP socket and an AF_XDP-backed
/// socket for QUIC communication.
#[derive(Debug)]
pub enum QuicSocket {
    /// A QUIC socket that uses AF_XDP for sending and a kernel UDP socket for receiving.
    Xdp(QuicXdpSocketBundle),
    /// A QUIC socket that uses kernel UDP socket for both sending and receiving.
    Kernel(std::net::UdpSocket),
}

impl From<std::net::UdpSocket> for QuicSocket {
    fn from(socket: std::net::UdpSocket) -> Self {
        QuicSocket::Kernel(socket)
    }
}

impl QuicSocket {
    pub fn with_xdp(
        socket: std::net::UdpSocket,
        fallback_src_ip: Ipv4Addr,
        xdp_sender: XdpSender,
    ) -> Self {
        Self::Xdp(QuicXdpSocketBundle {
            socket,
            fallback_src_ip,
            xdp_sender,
        })
    }

    #[cfg(feature = "dev-context-only-utils")]
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        match self {
            QuicSocket::Xdp(bundle) => bundle.socket.local_addr(),
            QuicSocket::Kernel(socket) => socket.local_addr(),
        }
    }
}

/// [`QuicXdpSocketBundle`] bundles the resources required to construct an AF_XDP-backed QUIC socket.
///
/// It carries both an [`XdpSender`] and a [`std::net::UdpSocket`], rather than constructing an
/// [`QuicXdpTxSocket`] directly, because the underlying sockets can only be created when a Tokio
/// runtime is present. `fallback_src_ip` is used when the local address of `socket` is a
/// wildcard address.
pub struct QuicXdpSocketBundle {
    socket: std::net::UdpSocket,
    fallback_src_ip: Ipv4Addr,
    xdp_sender: XdpSender,
}

impl Debug for QuicXdpSocketBundle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicXdpSocketBundle")
            .field("socket", &self.socket)
            .finish()
    }
}

/// [`QuicXdpTxSocket`] uses AF_XDP for egress traffic and `UdpSocket` for ingress traffic.
///
/// For egress traffic, it employs an underlying `QuicXdpSender` for non-local destinations. For
/// destinations owned by the local host (routed via `lo`, including loopback and local interface
/// IPs), it falls back to a kernel `UdpSocket`.
pub(crate) struct QuicXdpTxSocket {
    udp_socket: Arc<UdpSocket>,
    xdp_sender: QuicXdpSender,
    local_ips: Vec<Ipv4Addr>,
}

impl QuicXdpTxSocket {
    pub fn new(
        QuicXdpSocketBundle {
            socket,
            fallback_src_ip,
            xdp_sender,
        }: QuicXdpSocketBundle,
    ) -> io::Result<Self> {
        let src_addr = socket.local_addr()?;
        let SocketAddr::V4(src_addr) = src_addr else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Only IPv4 addresses are supported",
            ));
        };
        // if local address is wildcard, override it with fallback_src_ip.
        let src_addr = if src_addr.ip().is_unspecified() {
            SocketAddrV4::new(fallback_src_ip, src_addr.port())
        } else {
            src_addr
        };

        // Collect local interface IPs once at construction time. We do not refresh them if
        // interface addresses change later. This is a low-risk tradeoff because local-destination
        // egress is expected to be rare: only RPC sendTransaction traffic or local testing.
        let local_ips = collect_local_ipv4_ips()?;

        Ok(Self {
            udp_socket: Arc::new(UdpSocket::new(socket)?),
            xdp_sender: QuicXdpSender::new(xdp_sender, src_addr),
            local_ips,
        })
    }

    fn should_use_kernel_udp(&self, dst: SocketAddr) -> bool {
        dst.ip().is_loopback() || matches!(dst.ip(), IpAddr::V4(ip) if self.local_ips.contains(&ip))
    }
}

impl fmt::Debug for QuicXdpTxSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicXdpTxSocket")
            .field("local_addr", &self.udp_socket.local_addr())
            .finish_non_exhaustive()
    }
}

impl AsyncUdpSocket for QuicXdpTxSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        // The kernel UDP socket poller is always returned here, ignoring the XDP sender. This
        // implementation is correct under the following assumptions:
        // 1. When egress AF_XDP is enabled, the kernel UDP socket is used rarely and only for local
        //    destinations, so it should almost always be writable.
        // 2. `QuicXdpSender` can almost always enqueue.
        //
        // A rare mismatch is still possible: if the UDP socket is not writable while
        // `QuicXdpSender` could enqueue, throughput may be temporarily suboptimal until the UDP
        // socket becomes writable. The reverse mismatch is also possible: the UDP poller is ready
        // but the selected `QuicXdpSender` channel is full. In this case `try_send` fails with
        // `WouldBlock`, and the caller invokes `poll_writable` again, which can select another
        // channel in the next round.
        self.udp_socket.clone().create_io_poller()
    }

    /// Attempts to send the given [`Transmit`].
    ///
    /// For non-local destinations uses AF_XDP, otherwise kernel UDP.
    ///
    /// If enqueueing fails after some datagrams were already enqueued, this method returns
    /// `Err(WouldBlock)`. The caller may retry the whole transmit, which can cause duplicate
    /// datagrams to be sent for the already enqueued chunks. QUIC packet numbers make this
    /// protocol-safe, but duplicates can still degrade throughput and congestion behavior. This
    /// implementation therefore assumes the AF_XDP channel is rarely (ideally never) full.
    fn try_send(&self, t: &Transmit<'_>) -> io::Result<()> {
        if self.should_use_kernel_udp(t.destination) {
            return self.udp_socket.try_send(t);
        }
        let src_ip = match t.src_ip {
            Some(IpAddr::V4(ip)) => Some(ip),
            Some(IpAddr::V6(_)) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "IPv6 source addresses are not supported",
                ));
            }
            None => None,
        };

        debug_assert!(
            t.segment_size.is_none(),
            "GSO segmentation is disabled for AF_XDP sends, but segment_size is {:?}",
            t.segment_size
        );

        let payload = Bytes::copy_from_slice(t.contents);
        match self
            .xdp_sender
            .try_send(src_ip, t.destination, t.ecn, payload)
        {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(io::ErrorKind::WouldBlock.into()),
            Err(TrySendError::Disconnected(_)) => Err(io::ErrorKind::BrokenPipe.into()),
        }
    }

    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        self.udp_socket.poll_recv(cx, bufs, meta)
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.udp_socket.local_addr()
    }

    fn max_transmit_segments(&self) -> usize {
        // no GSO batches, so each transmit describes exactly one datagram
        1
    }

    fn max_receive_segments(&self) -> usize {
        self.udp_socket.max_receive_segments()
    }

    fn may_fragment(&self) -> bool {
        self.udp_socket.may_fragment()
    }
}

/// [`QuicXdpSender`] wraps [`XdpSender`] and provides round-robin sender selection.
///
/// This wrapper provides a simple round-robin sender index for each packet
/// sent. It is required because `AsyncUdpSocket::try_send` does not provide a way to specify the
/// sender index. If the `XdpSender` has only one sender, the index is always 0 and the atomic is
/// not used.
struct QuicXdpSender {
    xdp_sender: XdpSender,
    src_addr: SocketAddrV4,
    next_sender_index: Option<AtomicUsize>,
}

impl QuicXdpSender {
    fn new(xdp_sender: XdpSender, src_addr: SocketAddrV4) -> Self {
        let next_sender_index = (xdp_sender.len() > 1).then(|| AtomicUsize::new(0));
        Self {
            xdp_sender,
            src_addr,
            next_sender_index,
        }
    }

    fn try_send(
        &self,
        src_ip: Option<Ipv4Addr>,
        destination: SocketAddr,
        ecn: Option<QuinnEcnCodepoint>,
        payload: Bytes,
    ) -> Result<(), TrySendError<BytesTxPacket>> {
        let sender_idx = self
            .next_sender_index
            .as_ref()
            .map_or(0, |idx| idx.fetch_add(1, Ordering::Relaxed));

        // For wildcard or multihoming cases, `src_ip` may be overridden. In that case, use the
        // source port from `self.src_addr`.
        let src_ip = src_ip.unwrap_or(*self.src_addr.ip());
        let src_addr = SocketAddrV4::new(src_ip, self.src_addr.port());
        let ecn = ecn.map(quinn_ecn_to_xdp);

        self.xdp_sender.try_send(
            sender_idx,
            BytesTxPacket::new(src_addr, destination, ecn, payload),
        )
    }
}

/// [`UdpSocket`] adapts a Tokio [`tokio::net::UdpSocket`] and its [`UdpSocketState`] to implement
/// [`AsyncUdpSocket`].
#[derive(Debug)]
struct UdpSocket {
    io: tokio::net::UdpSocket,
    inner: UdpSocketState,
}

impl UdpSocket {
    fn new(sock: std::net::UdpSocket) -> io::Result<Self> {
        Ok(Self {
            inner: UdpSocketState::new((&sock).into())?,
            io: tokio::net::UdpSocket::from_std(sock)?,
        })
    }
}

impl AsyncUdpSocket for UdpSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(UdpPollHelper::new(move || {
            let socket = self.clone();
            async move { socket.io.writable().await }
        }))
    }

    fn try_send(&self, transmit: &Transmit) -> io::Result<()> {
        self.io.try_io(Interest::WRITABLE, || {
            self.inner.send((&self.io).into(), transmit)
        })
    }

    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [std::io::IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        loop {
            ready!(self.io.poll_recv_ready(cx))?;
            match self.io.try_io(Interest::READABLE, || {
                self.inner.recv((&self.io).into(), bufs, meta)
            }) {
                Ok(res) => return Poll::Ready(Ok(res)),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }

    fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.io.local_addr()
    }

    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }

    fn max_transmit_segments(&self) -> usize {
        self.inner.max_gso_segments()
    }

    fn max_receive_segments(&self) -> usize {
        self.inner.gro_segments()
    }
}

pin_project_lite::pin_project! {
    /// Helper adapting a function `MakeFut` that constructs a single-use future `Fut` into a
    /// [`UdpPoller`] that may be reused indefinitely.
    struct UdpPollHelper<MakeFut, Fut> {
        make_fut: MakeFut,
        #[pin]
        fut: Option<Fut>,
    }
}

impl<MakeFut, Fut> UdpPollHelper<MakeFut, Fut> {
    /// Construct a [`UdpPoller`] that calls `make_fut` to get the future to poll, storing it until
    /// it yields [`Poll::Ready`], then creating a new one on the next
    /// [`poll_writable`](UdpPoller::poll_writable).
    fn new(make_fut: MakeFut) -> Self {
        Self {
            make_fut,
            fut: None,
        }
    }
}

impl<MakeFut, Fut> UdpPoller for UdpPollHelper<MakeFut, Fut>
where
    MakeFut: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = io::Result<()>> + Send + Sync + 'static,
{
    fn poll_writable(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        let mut this = self.project();
        if this.fut.is_none() {
            this.fut.set(Some((this.make_fut)()));
        }
        // We're forced to `unwrap` here because `Fut` may be `!Unpin`, which means we can't safely
        // obtain an `&mut Fut` after storing it in `self.fut` when `self` is already behind `Pin`,
        // and if we didn't store it then we wouldn't be able to keep it alive between
        // `poll_writable` calls.
        let result = this.fut.as_mut().as_pin_mut().unwrap().poll(cx);
        if result.is_ready() {
            // Polling an arbitrary `Future` after it becomes ready is a logic error, so arrange for
            // a new `Future` to be created on the next call.
            this.fut.set(None);
        }
        result
    }
}

impl<MakeFut, Fut> Debug for UdpPollHelper<MakeFut, Fut> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdpPollHelper").finish_non_exhaustive()
    }
}

/// Collects IPv4 addresses assigned to local network interfaces.
fn collect_local_ipv4_ips() -> io::Result<Vec<Ipv4Addr>> {
    let mut ips = Vec::new();
    for ifa in getifaddrs().map_err(io::Error::other)? {
        let Some(addr) = ifa.address else { continue };
        if let Some(v4) = addr.as_sockaddr_in() {
            let ip = v4.ip();
            if !ips.contains(&ip) {
                ips.push(ip);
            }
        }
    }
    Ok(ips)
}

#[inline]
const fn quinn_ecn_to_xdp(ecn: QuinnEcnCodepoint) -> XdpEcnCodepoint {
    match ecn {
        QuinnEcnCodepoint::Ect0 => XdpEcnCodepoint::Ect0,
        QuinnEcnCodepoint::Ect1 => XdpEcnCodepoint::Ect1,
        QuinnEcnCodepoint::Ce => XdpEcnCodepoint::Ce,
    }
}
