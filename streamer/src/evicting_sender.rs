use {
    crate::streamer::{ChannelSend, ChannelTryRecv},
    crossbeam_channel::{Receiver, Sender, TryRecvError, TrySendError, bounded},
    std::marker::PhantomData,
};

/// A sender implementation that evicts the oldest message when the channel is full.
///
/// Generic over the two channel ends, defaulting to crossbeam's sender and receiver.
pub struct EvictingSender<T, S = Sender<T>, R = Receiver<T>> {
    sender: S,
    receiver: R,
    _message: PhantomData<fn() -> T>,
}

// Manual implementation of Clone since `T` is not required to implement Clone.
impl<T, S: Clone, R: Clone> Clone for EvictingSender<T, S, R> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            receiver: self.receiver.clone(),
            _message: PhantomData,
        }
    }
}

impl<T, S, R> EvictingSender<T, S, R>
where
    S: ChannelSend<T>,
    R: ChannelTryRecv<T>,
{
    /// Create a new evicting sender with provided sender, receiver.
    #[inline]
    pub fn new(sender: S, receiver: R) -> Self {
        Self {
            sender,
            receiver,
            _message: PhantomData,
        }
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
}

impl<T: Send + 'static> EvictingSender<T> {
    /// Create a new `EvictingSender` with a bounded channel of the specified capacity.
    #[inline]
    pub fn new_bounded(capacity: usize) -> (Self, Receiver<T>) {
        let (sender, receiver) = bounded(capacity);
        (Self::new(sender, receiver.clone()), receiver)
    }
}

impl<T, S, R> ChannelSend<T> for EvictingSender<T, S, R>
where
    T: Send + 'static,
    S: ChannelSend<T>,
    R: ChannelTryRecv<T>,
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
        self.sender.is_empty()
    }

    #[inline]
    fn len(&self) -> usize {
        self.sender.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evicts_oldest_on_crossbeam_channel() {
        let (sender, receiver) = bounded(2);
        let sender = EvictingSender::new(sender, receiver.clone());
        sender.try_send(1).unwrap();
        sender.try_send(2).unwrap();
        assert_eq!(sender.try_send(3), Err(TrySendError::Full(1)));
        assert_eq!(receiver.try_recv(), Ok(2));
        assert_eq!(receiver.try_recv(), Ok(3));
        assert!(sender.is_empty());
    }
}
