//! Runtime commands and source identity for managed feeds.
use super::{EventCounters, FeedEvent, FeedStatus};
use crate::exchange::ExchangeFeed;
use cryptofeed_core::{
    error::{Error, Result},
    exchange::ExchangeId,
};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::{broadcast, mpsc, oneshot};

/// Process-local identity of one logical feed, retained across replacements.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct FeedId(u64);

impl FeedId {
    pub fn as_u64(self) -> u64 {
        self.0
    }
    pub(crate) fn allocate() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct FeedIdentity {
    pub id: FeedId,
    pub generation: u64,
}

/// Registry entry; identity is not a promise of remote subscription readiness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct FeedInfo {
    pub identity: FeedIdentity,
    pub exchange: ExchangeId,
}

/// An event tagged with its logical source and configuration-generation scope.
/// Already buffered events are not removed on replacement; compare identity.
#[derive(Clone, Debug)]
pub struct FeedEnvelope {
    pub identity: FeedIdentity,
    pub event: FeedEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FeedState {
    Preparing,
    /// Configuration is validated and session tasks launched, not remote-ready.
    Started,
    Stopping,
    Stopped {
        forced: bool,
    },
    Failed {
        error: String,
    },
    Degraded {
        error: String,
    },
    Cancelled {
        reason: String,
    },
}

pub(crate) type Reply<T> = oneshot::Sender<Result<T>>;
pub(crate) enum Command {
    Add {
        identity: FeedIdentity,
        feed: Box<ExchangeFeed>,
        reply: Reply<FeedIdentity>,
    },
    Remove {
        id: FeedId,
        reply: Reply<()>,
    },
    Replace {
        id: FeedId,
        feed: Box<ExchangeFeed>,
        reply: Reply<FeedIdentity>,
    },
    Shutdown {
        reply: Reply<()>,
    },
    List {
        reply: Reply<Vec<FeedInfo>>,
    },
}

/// Cloneable control handle. Commands use a bounded queue. Add/replace replies
/// acknowledge validated task startup, not exchange subscription readiness.
#[derive(Clone)]
pub struct RuntimeControl {
    pub(crate) sender: mpsc::Sender<Command>,
}

impl RuntimeControl {
    async fn request<T>(&self, build: impl FnOnce(Reply<T>) -> Command) -> Result<T> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(build(reply))
            .await
            .map_err(|_| Error::Transport("runtime control is closed".to_owned()))?;
        receive.await.map_err(|_| {
            Error::Transport("runtime stopped before completing the command".to_owned())
        })?
    }

    pub async fn add_feed(&self, feed: ExchangeFeed) -> Result<FeedIdentity> {
        let identity = FeedIdentity {
            id: FeedId::allocate(),
            generation: 1,
        };
        self.request(|reply| Command::Add {
            identity,
            feed: Box::new(feed),
            reply,
        })
        .await
    }
    pub async fn remove_feed(&self, id: FeedId) -> Result<()> {
        self.request(|reply| Command::Remove { id, reply }).await
    }
    pub async fn replace_feed(&self, id: FeedId, feed: ExchangeFeed) -> Result<FeedIdentity> {
        self.request(|reply| Command::Replace {
            id,
            feed: Box::new(feed),
            reply,
        })
        .await
    }
    /// Lists registered logical feeds, including preparing or failed workers.
    pub async fn feeds(&self) -> Result<Vec<FeedInfo>> {
        self.request(|reply| Command::List { reply }).await
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.request(|reply| Command::Shutdown { reply }).await
    }
}

#[derive(Clone)]
pub(crate) struct ControlContext {
    pub event_sender: Option<broadcast::Sender<FeedEvent>>,
    pub envelope_sender: Option<broadcast::Sender<FeedEnvelope>>,
    pub status_sender: Option<broadcast::Sender<FeedStatus>>,
    pub counters: Arc<EventCounters>,
}

impl ControlContext {
    pub fn attach(&self, feed: &mut ExchangeFeed, identity: FeedIdentity) {
        feed.identity = Some(identity);
        feed.managed = false;
        feed.event_sender = self.event_sender.clone();
        feed.envelope_sender = self.envelope_sender.clone();
        feed.status_sender = self.status_sender.clone();
        feed.event_counts = Some(self.counters.clone());
        feed.fresh_runtime_state();
    }
    pub fn emit(&self, identity: FeedIdentity, exchange: ExchangeId, state: FeedState) {
        if let FeedState::Failed { error } = &state {
            tracing::error!(?identity, ?exchange, %error, "managed feed failed");
        }
        if let Some(sender) = &self.status_sender {
            let _ = sender.send(FeedStatus::Lifecycle {
                identity,
                exchange,
                state,
            });
        }
    }
}
