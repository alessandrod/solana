use {
    crate::streamer::ChannelSend,
    crossbeam_channel::{Receiver, Sender, TryRecvError, TrySendError, bounded},
};

/// A sender implementation that evicts the oldest message when the channel is full.
pub struct EvictingSender<T> {
    sender: Sender<T>,
    receiver: Receiver<T>,
}

// Manual implementation of Clone since `T` is not required to implement Clone.
impl<T> Clone for EvictingSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            receiver: self.receiver.clone(),
        }
    }
}

impl<T> EvictingSender<T> {
    /// Create a new evicting sender with provided sender, receiver.
    #[inline]
    pub fn new(sender: Sender<T>, receiver: Receiver<T>) -> Self {
        Self { sender, receiver }
    }

    /// Attempts to send, passing any evicted item to `on_evict` after the retry.
    /// A failed incoming item is returned, even if an older item was evicted.
    pub fn try_send_with_eviction(
        &self,
        msg: T,
        on_evict: impl FnOnce(T),
    ) -> Result<(), TrySendError<T>> {
        let msg = match self.sender.try_send(msg) {
            Ok(()) => return Ok(()),
            Err(TrySendError::Full(msg)) => msg,
            Err(TrySendError::Disconnected(_)) => unreachable!(),
        };

        match self.receiver.try_recv() {
            Ok(older) => {
                let result = self.sender.try_send(msg);
                on_evict(older);
                result
            }
            Err(TryRecvError::Empty) => self.sender.try_send(msg),
            Err(TryRecvError::Disconnected) => unreachable!(),
        }
    }

    /// Create a new `EvictingSender` with a bounded channel of the specified capacity.
    #[inline]
    pub fn new_bounded(capacity: usize) -> (Self, Receiver<T>) {
        let (sender, receiver) = bounded(capacity);
        (Self::new(sender, receiver.clone()), receiver)
    }
}

impl<T> ChannelSend<T> for EvictingSender<T>
where
    T: Send + 'static,
{
    fn try_send(&self, msg: T) -> std::result::Result<(), TrySendError<T>> {
        let mut evicted = None;
        self.try_send_with_eviction(msg, |older| evicted = Some(older))?;

        match evicted {
            Some(older) => Err(TrySendError::Full(older)),
            None => Ok(()),
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.receiver.is_empty()
    }

    #[inline]
    fn len(&self) -> usize {
        self.receiver.len()
    }
}
