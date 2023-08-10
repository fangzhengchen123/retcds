use futures::SinkExt;
use async_channel::{bounded, Sender, Receiver, SendError, RecvError, TryRecvError};

#[derive(Debug)]
pub struct Channel<T> {
    rx: Option<Receiver<T>>,
    tx: Option<Sender<T>>,
}

impl<T> Clone for Channel<T> {
    fn clone(&self) -> Self {
        Self {
            rx: self.rx.clone(),
            tx: self.tx.clone(),
        }
    }
}

impl<T> Channel<T> {
    pub(crate) fn new(n: usize) -> Self {
        let (tx, rx) = bounded(n);
        Channel {
            rx: Some(rx),
            tx: Some(tx),
        }
    }
    pub async fn try_send(&self, msg: T) -> Result<(), SendError<T>> {
        if let Some(tx) = &self.tx {
            if tx.is_full(){
                return Err(SendError(msg));
            }
            return tx.send(msg).await;
        }
        Ok(())
    }

    pub(crate) async fn try_recv(&self) -> Result<T, TryRecvError> {
        if let Some(rx) = &self.rx {
            return rx.try_recv();
        }
        Err(TryRecvError::Empty)
    }

    pub(crate) async fn recv(&self) -> Result<T, async_channel::RecvError> {
        let rx = self.rx.as_ref().unwrap();
        rx.recv().await
    }

    pub(crate) async fn send(&self, msg: T) -> Result<(), SendError<T>> {
        let tx = self.tx.as_ref().unwrap();
        tx.send(msg).await
    }

    pub(crate) fn tx(&self) -> Sender<T> {
        self.tx.as_ref().unwrap().clone()
    }

    pub(crate) fn take_tx(&mut self) -> Option<Sender<T>> {
        self.tx.take()
    }

    pub fn is_empty(&self) -> bool {
        if let Some(rx) = &self.rx {
            return rx.is_empty();
        }
        true
    }

    pub fn is_full(&self) -> bool {
        if let Some(tx) = &self.tx {
            return tx.is_full();
        }
        true
    }
}

