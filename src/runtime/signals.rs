use std::{collections::HashMap, sync::Arc};

use tokio::sync::{Mutex, watch};

use crate::runtime::model::ActorId;

#[derive(Clone, Default)]
pub struct ActorSignals {
    channels: Arc<Mutex<HashMap<ActorId, watch::Sender<i64>>>>,
}

#[derive(Clone)]
pub struct ActorDirectorySignals {
    sender: watch::Sender<u64>,
}

impl Default for ActorDirectorySignals {
    fn default() -> Self {
        Self {
            sender: watch::channel(0).0,
        }
    }
}

impl ActorDirectorySignals {
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.sender.subscribe()
    }

    pub fn notify(&self) {
        self.sender
            .send_modify(|value| *value = value.wrapping_add(1));
    }
}

impl ActorSignals {
    pub async fn subscribe(&self, actor: &ActorId) -> watch::Receiver<i64> {
        self.sender(actor).await.subscribe()
    }

    pub async fn notify(&self, actor: &ActorId, sequence: i64) {
        let sender = self.sender(actor).await;
        sender.send_modify(|current| {
            *current = (*current).max(sequence);
        });
    }

    async fn sender(&self, actor: &ActorId) -> watch::Sender<i64> {
        let mut channels = self.channels.lock().await;
        channels
            .entry(actor.clone())
            .or_insert_with(|| watch::channel(0).0)
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime::{model::ActorId, signals::ActorSignals};

    #[tokio::test]
    async fn subscriber_receives_newer_actor_sequence() {
        let signals = ActorSignals::default();
        let actor = ActorId::from_string("actor-1");
        let mut receiver = signals.subscribe(&actor).await;

        signals.notify(&actor, 2).await;
        receiver.changed().await.unwrap();

        assert_eq!(*receiver.borrow(), 2);

        signals.notify(&actor, 1).await;
        assert_eq!(*receiver.borrow(), 2);
    }
}
