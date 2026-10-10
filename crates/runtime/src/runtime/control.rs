use crate::{
    exchange::ExchangeFeed,
    feed::{
        FeedHandler,
        control::{Command, ControlContext, FeedId, FeedIdentity, FeedInfo, FeedState, Reply},
    },
};
use cryptofeed_core::{
    error::{Error, Result},
    exchange::ExchangeId,
};
use futures::{FutureExt, future::BoxFuture};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::{
    sync::{mpsc, watch},
    task::{JoinHandle, JoinSet},
    time::Instant,
};

type Prepare = fn(ExchangeFeed) -> BoxFuture<'static, Result<Vec<ExchangeFeed>>>;
type Consume = fn(ExchangeFeed, watch::Receiver<bool>) -> BoxFuture<'static, Result<()>>;

// Private boundaries let actor tests drive lifecycle without live exchanges.
#[derive(Clone, Copy)]
struct Services {
    prepare: Prepare,
    consume: Consume,
}

fn prepare(feed: ExchangeFeed) -> BoxFuture<'static, Result<Vec<ExchangeFeed>>> {
    super::hydrate_feed_symbols(vec![feed]).boxed()
}
fn consume(feed: ExchangeFeed, shutdown: watch::Receiver<bool>) -> BoxFuture<'static, Result<()>> {
    super::consume_feed(feed, shutdown).boxed()
}

#[derive(Default)]
struct Admission {
    rows: Mutex<HashMap<FeedId, HashMap<ExchangeId, usize>>>,
}

impl Admission {
    fn reserve(
        &self,
        id: FeedId,
        old: Option<(ExchangeId, usize)>,
        next: Option<(ExchangeId, usize)>,
    ) -> Result<()> {
        let mut replacement = HashMap::new();
        if let Some((exchange, count)) = old {
            replacement.insert(exchange, count);
        }
        if let Some((exchange, count)) = next {
            let entry = replacement.entry(exchange).or_insert(0);
            *entry = (*entry).max(count);
        }
        let mut rows = self.rows.lock().expect("admission lock");
        let mut totals = replacement.clone();
        for (other, counts) in rows.iter() {
            if *other == id {
                continue;
            }
            for (exchange, count) in counts {
                *totals.entry(*exchange).or_insert(0) += count;
            }
        }
        if totals
            .values()
            .any(|count| *count > super::budget::MAX_CONNECTIONS_PER_EXCHANGE)
        {
            return Err(Error::InvalidConfiguration(
                "managed runtime connection plan exceeds the per-exchange budget".to_owned(),
            ));
        }
        if replacement.is_empty() {
            rows.remove(&id);
        } else {
            rows.insert(id, replacement);
        }
        Ok(())
    }
    fn release(&self, id: FeedId) {
        self.rows.lock().expect("admission lock").remove(&id);
    }
}

struct Resources {
    services: Services,
    admission: Arc<Admission>,
}

enum FeedCommand {
    Remove,
    Replace {
        feed: Box<ExchangeFeed>,
        reply: Reply<FeedIdentity>,
    },
}

impl FeedCommand {
    fn reject(self, message: &str) {
        match self {
            Self::Remove => {}
            Self::Replace { reply, .. } => {
                let _ = reply.send(Err(Error::InvalidConfiguration(message.to_owned())));
            }
        }
    }
}

type WorkerInfo = Arc<Mutex<Arc<super::readiness::FeedMonitor>>>;

struct Entry {
    removal_reply: Option<Reply<()>>,
    sender: mpsc::Sender<FeedCommand>,
    info: WorkerInfo,
}

struct Preparing {
    monitor: Arc<super::readiness::FeedMonitor>,
    identity: FeedIdentity,
    exchange: ExchangeId,
    future: BoxFuture<'static, Result<Vec<ExchangeFeed>>>,
    reply: Option<Reply<FeedIdentity>>,
    initial: bool,
}

impl Preparing {
    fn new(
        mut feed: ExchangeFeed,
        monitor: Arc<super::readiness::FeedMonitor>,
        reply: Option<Reply<FeedIdentity>>,
        initial: bool,
        context: &ControlContext,
        services: Services,
    ) -> Self {
        let exchange = feed.exchange;
        let identity = monitor.identity;
        context.attach(&mut feed, identity);
        feed.managed = true;
        feed.monitor = Some(monitor.clone());
        monitor.lifecycle(FeedState::Preparing);
        Self {
            monitor,
            identity,
            exchange,
            future: (services.prepare)(feed),
            reply,
            initial,
        }
    }
    fn cancel(self, message: &str) {
        self.monitor.lifecycle(FeedState::Cancelled {
            reason: message.to_owned(),
        });
        if let Some(reply) = self.reply {
            let _ = reply.send(Err(Error::Transport(message.to_owned())));
        }
    }
}

struct Prepared {
    monitor: Arc<super::readiness::FeedMonitor>,
    connections: usize,
    initial: bool,
    identity: FeedIdentity,
    exchange: ExchangeId,
    feeds: Vec<ExchangeFeed>,
    reply: Option<Reply<FeedIdentity>>,
}

impl Prepared {
    fn cancel(self, message: &str) {
        self.monitor.lifecycle(FeedState::Cancelled {
            reason: message.to_owned(),
        });
        if let Some(reply) = self.reply {
            let _ = reply.send(Err(Error::Transport(message.to_owned())));
        }
    }
}

struct Running {
    monitor: Arc<super::readiness::FeedMonitor>,
    connections: usize,
    exchange: ExchangeId,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<Result<()>>,
    stop_at: Option<Instant>,
    forced: Arc<std::sync::atomic::AtomicBool>,
}

impl Running {
    fn start(prepared: Prepared, services: Services) -> Self {
        let (shutdown, receiver) = watch::channel(false);
        let forced = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let books = prepared
            .feeds
            .iter()
            .filter(|feed| {
                feed.channels
                    .contains(&cryptofeed_core::exchange::Channel::L2Book)
            })
            .flat_map(|feed| feed.symbols.iter().map(|symbol| symbol.as_str().to_owned()))
            .collect();
        prepared.monitor.configure(prepared.connections, books);
        prepared.monitor.lifecycle(FeedState::Started);
        let task = tokio::spawn(super::run_feeds_until_shutdown_tracked(
            prepared.feeds,
            receiver,
            services.consume,
            Some(forced.clone()),
        ));
        if let Some(reply) = prepared.reply {
            let _ = reply.send(Ok(prepared.identity));
        }
        Self {
            monitor: prepared.monitor,
            connections: prepared.connections,
            exchange: prepared.exchange,
            shutdown,
            task,
            stop_at: None,
            forced,
        }
    }

    fn request_stop(&mut self) {
        if self.stop_at.is_none() {
            self.monitor.lifecycle(FeedState::Stopping);
            let _ = self.shutdown.send(true);
            self.stop_at = Some(
                Instant::now()
                    + super::SHUTDOWN_GRACE_PERIOD
                    + std::time::Duration::from_millis(100),
            );
        }
    }
    async fn stop(&mut self) -> std::result::Result<Result<()>, tokio::task::JoinError> {
        self.request_stop();
        match tokio::time::timeout_at(self.stop_at.expect("stop deadline"), &mut self.task).await {
            Ok(result) => result,
            Err(_) => {
                self.forced
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                self.task.abort();
                (&mut self.task).await
            }
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        self.task.abort();
    }
}

fn record_exit(
    running: &Running,
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
    errors: &mut Vec<String>,
) {
    match result {
        Ok(Ok(())) => running.monitor.lifecycle(FeedState::Stopped {
            forced: running.forced.load(std::sync::atomic::Ordering::Relaxed),
        }),
        Err(error) if running.stop_at.is_some() && error.is_cancelled() => running
            .monitor
            .lifecycle(FeedState::Stopped { forced: true }),
        result => {
            let error = match result {
                Ok(Err(error)) => error.to_string(),
                Err(error) => error.to_string(),
                _ => unreachable!(),
            };
            running.monitor.lifecycle(FeedState::Failed {
                error: error.clone(),
            });
            errors.push(error);
            if running.stop_at.is_some() {
                running.monitor.lifecycle(FeedState::Stopped {
                    forced: running.forced.load(std::sync::atomic::Ordering::Relaxed),
                });
            }
        }
    }
}

async fn worker(
    feed: ExchangeFeed,
    reply: Option<Reply<FeedIdentity>>,
    context: ControlContext,
    mut commands: mpsc::Receiver<FeedCommand>,
    mut shutdown: watch::Receiver<bool>,
    info: WorkerInfo,
    resources: Resources,
) -> Result<()> {
    let services = resources.services;
    let initial_monitor = info.lock().expect("feed info lock").clone();
    let identity = initial_monitor.identity;
    let mut preparing = Some(Preparing::new(
        feed,
        initial_monitor,
        reply,
        true,
        &context,
        services,
    ));
    let mut prepared: Option<Prepared> = None;
    let mut running: Option<Running> = None;
    let mut removing = false;
    let mut errors = Vec::new();
    let mut generation = identity.generation;
    loop {
        if running.is_none() {
            if let Some(candidate) = prepared.take() {
                if candidate.initial
                    && candidate
                        .reply
                        .as_ref()
                        .is_some_and(|reply| reply.is_closed())
                {
                    candidate.cancel("command caller disappeared before startup");
                    return super::terminal_result(errors);
                } else {
                    resources
                        .admission
                        .reserve(
                            identity.id,
                            None,
                            Some((candidate.exchange, candidate.connections)),
                        )
                        .expect("candidate already admitted");
                    generation = candidate.identity.generation;
                    *info.lock().expect("feed info lock") = candidate.monitor.clone();
                    running = Some(Running::start(candidate, services));
                }
            }
            if removing {
                return super::terminal_result(errors);
            }
        }
        let deadline = running
            .as_ref()
            .filter(|running| !running.forced.load(std::sync::atomic::Ordering::Relaxed))
            .and_then(|running| running.stop_at);
        let (future, reply) = match preparing.as_mut() {
            Some(preparing) => (Some(&mut preparing.future), preparing.reply.as_mut()),
            None => (None, None),
        };
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_ok() && !*shutdown.borrow() { continue; }
                if let Some(candidate) = preparing.take() { candidate.cancel("runtime is shutting down"); }
                if let Some(candidate) = prepared.take() { candidate.cancel("runtime is shutting down"); }
                if let Some(mut active) = running.take() {
                    let result = active.stop().await;
                    record_exit(&active, result, &mut errors);
                }
                return super::terminal_result(errors);
            }
            _ = async { match reply { Some(reply) => reply.closed().await, None => std::future::pending().await } } => {
                let candidate = preparing.take().expect("closed preparation reply");
                let initial = candidate.initial;
                candidate.cancel("command cancelled");
                if initial && running.is_none() { return super::terminal_result(errors); }
            }
            result = async { match future { Some(future) => future.await, None => std::future::pending().await } } => {
                let candidate = preparing.take().expect("completed preparation");
                let result = result.and_then(|feeds| {
                    let connections = feeds.iter().try_fold(0, |count, feed| super::planning::physical_connection_count(feed).map(|next| count + next))?;
                    resources.admission.reserve(identity.id, running.as_ref().map(|active| (active.exchange, active.connections)), Some((candidate.exchange, connections)))?;
                    Ok((feeds, connections))
                });
                match result {
                    Ok((feeds, connections)) => {
                        prepared = Some(Prepared { monitor: candidate.monitor, connections, initial: candidate.initial, identity: candidate.identity, exchange: candidate.exchange, feeds, reply: candidate.reply });
                        if let Some(active) = running.as_mut() { active.request_stop(); }
                    }
                    Err(error) => {
                        candidate.monitor.lifecycle(FeedState::Failed { error: error.to_string() });
                        let dynamic = candidate.reply.is_some();
                        if let Some(reply) = candidate.reply { let _ = reply.send(Err(error)); }
                        else { errors.push(error.to_string()); }
                        // Initial configured IDs remain queryable/recoverable.
                        // A rejected dynamic add was never committed.
                        if candidate.initial && dynamic { return super::terminal_result(errors); }
                    }
                }
            }
            _ = tokio::time::sleep_until(deadline.unwrap_or_else(Instant::now)), if deadline.is_some() => {
                let active = running.as_mut().expect("stopping feed");
                active.forced.store(true, std::sync::atomic::Ordering::Relaxed);
                active.task.abort();

            }
            result = async { match running.as_mut() { Some(active) => (&mut active.task).await, None => std::future::pending().await } } => {
                let active = running.take().expect("completed feed task");
                record_exit(&active, result, &mut errors);
                resources.admission.reserve(identity.id, None, prepared.as_ref().map(|candidate| (candidate.exchange, candidate.connections))).expect("releasing old admission");
            }
            command = commands.recv() => {
                match command {
                    Some(FeedCommand::Replace { feed, reply }) => {
                        if preparing.is_some() || prepared.is_some() || removing {
                            let _ = reply.send(Err(Error::InvalidConfiguration("another feed update is in progress".to_owned())));
                        } else {
                            generation += 1;
                            let monitor = context.monitor(FeedIdentity { id: identity.id, generation }, feed.exchange);
                            preparing = Some(Preparing::new(*feed, monitor, Some(reply), false, &context, services));
                        }
                    }
                    Some(FeedCommand::Remove) => {
                        if removing { continue; }
                        if let Some(candidate) = preparing.take() { candidate.cancel("feed removed during preparation"); }
                        if let Some(candidate) = prepared.take() { candidate.cancel("feed removed before replacement startup"); }
                        if let Some(active) = running.as_mut() { active.request_stop(); }
                        else {
                            info.lock().expect("feed info lock").clone().lifecycle(FeedState::Stopped { forced: false });
                        }
                        resources.admission.reserve(identity.id, running.as_ref().map(|active| (active.exchange, active.connections)), None).expect("releasing candidate admission");
                        removing = true;
                    }
                    None => return super::terminal_result(errors),
                }
            }
        }
    }
}

struct Manager {
    admission: Arc<Admission>,
    context: ControlContext,
    shutdown: watch::Receiver<bool>,
    services: Services,
    workers: JoinSet<(FeedId, Result<()>)>,
    entries: HashMap<FeedId, Entry>,
    tasks: HashMap<tokio::task::Id, FeedId>,
    errors: Vec<String>,
}

impl Manager {
    fn launch(
        &mut self,
        feed: ExchangeFeed,
        identity: FeedIdentity,
        reply: Option<Reply<FeedIdentity>>,
    ) {
        let info = Arc::new(Mutex::new(self.context.monitor(identity, feed.exchange)));
        let (sender, receiver) = mpsc::channel(4);
        let context = self.context.clone();
        let shutdown = self.shutdown.clone();
        let worker_info = info.clone();
        let resources = Resources {
            services: self.services,
            admission: self.admission.clone(),
        };
        let task = self.workers.spawn(async move {
            (
                identity.id,
                worker(
                    feed,
                    reply,
                    context,
                    receiver,
                    shutdown,
                    worker_info,
                    resources,
                )
                .await,
            )
        });
        self.tasks.insert(task.id(), identity.id);
        self.entries.insert(
            identity.id,
            Entry {
                sender,
                info,
                removal_reply: None,
            },
        );
    }

    fn completed(
        &mut self,
        result: std::result::Result<
            (tokio::task::Id, (FeedId, Result<()>)),
            tokio::task::JoinError,
        >,
    ) {
        match result {
            Ok((task, (id, result))) => {
                self.tasks.remove(&task);
                let entry = self.entries.remove(&id);
                self.admission.release(id);
                if let Some(reply) = entry.and_then(|entry| entry.removal_reply) {
                    let _ = reply.send(Ok(()));
                }
                if let Err(error) = result {
                    self.errors.push(error.to_string());
                }
            }
            Err(error) => {
                if let Some(id) = self.tasks.remove(&error.id()) {
                    self.admission.release(id);
                    if let Some(entry) = self.entries.remove(&id) {
                        if let Some(reply) = entry.removal_reply {
                            let _ = reply.send(Err(Error::Transport(error.to_string())));
                        }
                        entry
                            .info
                            .lock()
                            .expect("feed info lock")
                            .clone()
                            .lifecycle(FeedState::Failed {
                                error: error.to_string(),
                            });
                    }
                }
                self.errors.push(error.to_string());
            }
        }
    }

    fn remove(&mut self, id: FeedId, reply: Reply<()>) {
        let Some(entry) = self.entries.get_mut(&id) else {
            let _ = reply.send(Err(Error::InvalidConfiguration(
                "unknown or removed feed ID".to_owned(),
            )));
            return;
        };
        if entry.removal_reply.is_some() {
            let _ = reply.send(Err(Error::InvalidConfiguration(
                "feed removal is already in progress".to_owned(),
            )));
        } else if entry.sender.try_send(FeedCommand::Remove).is_err() {
            let _ = reply.send(Err(Error::InvalidConfiguration(
                "feed command queue is full or feed has stopped".to_owned(),
            )));
        } else {
            // The registry owner acknowledges only after joining/removing the
            // worker, so list() cannot observe a removed entry after its ack.
            entry.removal_reply = Some(reply);
        }
    }

    fn route(&self, id: FeedId, command: FeedCommand) {
        match self.entries.get(&id) {
            Some(entry) if entry.removal_reply.is_some() => command.reject("feed is being removed"),
            Some(entry) => {
                if let Err(error) = entry.sender.try_send(command) {
                    error
                        .into_inner()
                        .reject("feed command queue is full or feed has stopped");
                }
            }
            None => command.reject("unknown or removed feed ID"),
        }
    }

    async fn drain(&mut self) {
        while let Some(result) = self.workers.join_next_with_id().await {
            self.completed(result);
        }
    }
}

pub(super) async fn run(handler: FeedHandler, shutdown: watch::Receiver<bool>) -> Result<()> {
    run_with(handler, shutdown, Services { prepare, consume }).await
}

async fn run_with(
    handler: FeedHandler,
    mut shutdown: watch::Receiver<bool>,
    services: Services,
) -> Result<()> {
    let (feeds, context, mut commands) = handler.into_control_parts();
    let (stop, stopped) = watch::channel(false);
    let mut manager = Manager {
        admission: Arc::new(Admission::default()),
        context,
        shutdown: stopped,
        services,
        workers: JoinSet::new(),
        entries: HashMap::new(),
        tasks: HashMap::new(),
        errors: Vec::new(),
    };
    for feed in feeds {
        let identity = feed.identity.expect("registered feed identity");
        manager.launch(feed, identity, None);
    }
    let mut open = true;
    let mut shutdown_reply = None;
    while open || !manager.workers.is_empty() {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_ok() && !*shutdown.borrow() { continue; }
                break;
            }
            result = manager.workers.join_next_with_id(), if !manager.workers.is_empty() => {
                if let Some(result) = result { manager.completed(result); }
            }
            command = commands.recv(), if open => {
                match command {
                    Some(Command::Add { identity, feed, reply }) => manager.launch(*feed, identity, Some(reply)),
                    Some(Command::Remove { id, reply }) => manager.remove(id, reply),
                    Some(Command::Replace { id, feed, reply }) => manager.route(id, FeedCommand::Replace { feed, reply }),
                    Some(Command::Shutdown { reply }) => { shutdown_reply = Some(reply); break; }
                    Some(Command::State { id, reply }) => {
                        let result = manager.entries.get(&id)
                            .map(|entry| entry.info.lock().expect("feed info lock").clone().snapshot())
                            .ok_or_else(|| Error::InvalidConfiguration("unknown or removed feed ID".to_owned()));
                        let _ = reply.send(result);
                    }
                    Some(Command::List { reply }) => {
                        let mut feeds: Vec<_> = manager.entries.values().map(|entry| {
                            let snapshot = entry.info.lock().expect("feed info lock").clone().snapshot();
                            FeedInfo { identity: snapshot.identity, exchange: snapshot.exchange }
                        }).collect();
                        feeds.sort_by_key(|feed| feed.identity.id.as_u64());
                        let _ = reply.send(Ok(feeds));
                    }
                    None => { open = false; if manager.workers.is_empty() { break; } }
                }
            }
        }
    }
    commands.close();
    let _ = stop.send(true);
    if tokio::time::timeout(
        super::SHUTDOWN_GRACE_PERIOD + std::time::Duration::from_millis(200),
        manager.drain(),
    )
    .await
    .is_err()
    {
        manager.workers.abort_all();
        manager.drain().await;
    }
    let result = super::terminal_result(manager.errors);
    if let Some(reply) = shutdown_reply {
        let _ = reply.send(
            result
                .as_ref()
                .map(|_| ())
                .map_err(|error| Error::Transport(error.to_string())),
        );
    }
    result
}

#[cfg(all(test, feature = "trade"))]
mod tests {
    use super::*;
    use crate::prelude::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::broadcast;

    static STUCK_DROPPED: AtomicBool = AtomicBool::new(false);
    struct DropFlag;
    impl Drop for DropFlag {
        fn drop(&mut self) {
            STUCK_DROPPED.store(true, Ordering::SeqCst);
        }
    }

    fn input(base: &str) -> ExchangeFeed {
        Binance::new()
            .trade()
            .symbol(&format!("{base}-USDT"))
            .exchange_symbol(&format!("{base}USDT"))
            .build()
    }
    fn fake_prepare(feed: ExchangeFeed) -> BoxFuture<'static, Result<Vec<ExchangeFeed>>> {
        async move {
            if feed
                .symbols
                .first()
                .is_some_and(|symbol| symbol.as_str() == "WAIT-USDT")
            {
                std::future::pending::<()>().await;
            }
            prepare(feed).await
        }
        .boxed()
    }
    fn emit(feed: &ExchangeFeed) {
        feed.publish_event(FeedEvent::Trade(Trade {
            exchange: feed.exchange,
            symbol: feed.symbols[0].clone(),
            side: Side::Buy,
            amount: rust_decimal::Decimal::ONE,
            price: rust_decimal::Decimal::ONE,
            exchange_ts: 1.0,
            received_ts: 2.0,
            id: None,
            implied_volatility: None,
        }));
    }
    fn fake_consume(
        feed: ExchangeFeed,
        mut shutdown: watch::Receiver<bool>,
    ) -> BoxFuture<'static, Result<()>> {
        async move {
            if feed.symbols[0].as_str() == "PANIC-USDT" {
                panic!("fake session panic");
            }
            if !feed.channels.contains(&Channel::Trade) {
                return Err(Error::Protocol("fake book failure".to_owned()));
            }
            if feed.symbols[0].as_str() == "STUCK-USDT" {
                let _flag = DropFlag;
                emit(&feed);
                std::future::pending::<()>().await;
            }
            loop {
                emit(&feed);
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { return Ok(()); }
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_millis(2)) => {}
                }
            }
        }
        .boxed()
    }
    type TestRuntime = (
        RuntimeControl,
        broadcast::Receiver<FeedEnvelope>,
        broadcast::Receiver<FeedStatus>,
        JoinHandle<Result<()>>,
        watch::Sender<bool>,
    );

    fn start(mut handler: FeedHandler) -> TestRuntime {
        let control = handler.control_handle();
        let events = handler.subscribe_identified();
        let statuses = handler.subscribe_status();
        let (stop, receiver) = watch::channel(false);
        let running = tokio::spawn(run_with(
            handler,
            receiver,
            Services {
                prepare: fake_prepare,
                consume: fake_consume,
            },
        ));
        (control, events, statuses, running, stop)
    }
    async fn event_for(
        events: &mut broadcast::Receiver<FeedEnvelope>,
        identity: FeedIdentity,
    ) -> FeedEnvelope {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let event = events.recv().await.unwrap();
                if event.identity == identity {
                    return event;
                }
            }
        })
        .await
        .unwrap()
    }
    async fn state_for(
        statuses: &mut broadcast::Receiver<FeedStatus>,
        id: FeedId,
        predicate: impl Fn(&FeedState) -> bool,
    ) -> FeedIdentity {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if let FeedStatus::Lifecycle {
                    identity, state, ..
                } = statuses.recv().await.unwrap()
                {
                    if identity.id == id && predicate(&state) {
                        return identity;
                    }
                }
            }
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn add_replace_remove_preserves_identity_and_rejects_invalid_candidate() {
        let (control, mut events, mut statuses, running, _stop) = start(FeedHandler::new());
        let old = control.add_feed(input("BTC")).await.unwrap();
        event_for(&mut events, old).await;
        assert!(
            control
                .replace_feed(old.id, Binance::new().build())
                .await
                .is_err()
        );
        event_for(&mut events, old).await;
        let new = control.replace_feed(old.id, input("ETH")).await.unwrap();
        assert_eq!(new.id, old.id);
        assert_eq!(new.generation, 3); // Failed attempts also get a unique generation.
        assert_eq!(control.feeds().await.unwrap()[0].identity, new);
        let event = event_for(&mut events, new).await;
        assert_eq!(event.event.symbol().as_str(), "ETH-USDT");
        let mut stopped_old = false;
        loop {
            if let FeedStatus::Lifecycle {
                identity, state, ..
            } = statuses.recv().await.unwrap()
            {
                if identity == old && matches!(state, FeedState::Stopped { .. }) {
                    stopped_old = true;
                }
                if identity == new && state == FeedState::Started {
                    assert!(stopped_old);
                    break;
                }
            }
        }
        control.remove_feed(new.id).await.unwrap();
        while events.try_recv().is_ok() {}
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), events.recv())
                .await
                .is_err()
        );
        assert!(control.remove_feed(new.id).await.is_err());
        assert!(control.feeds().await.unwrap().is_empty());
        control.shutdown().await.unwrap();
        running.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn cancelling_preparation_keeps_old_generation_running() {
        let (control, mut events, mut statuses, running, _stop) = start(FeedHandler::new());
        let old = control.add_feed(input("BTC")).await.unwrap();
        let update = {
            let control = control.clone();
            tokio::spawn(async move { control.replace_feed(old.id, input("WAIT")).await })
        };
        let attempted = state_for(&mut statuses, old.id, |state| {
            *state == FeedState::Preparing
        })
        .await;
        // Initial Preparing may still be queued; wait for the replacement scope.
        if attempted == old {
            state_for(&mut statuses, old.id, |state| {
                *state == FeedState::Preparing
            })
            .await;
        }
        while events.try_recv().is_ok() {}
        event_for(&mut events, old).await;
        update.abort();
        assert!(update.await.unwrap_err().is_cancelled());
        state_for(&mut statuses, old.id, |state| {
            matches!(state, FeedState::Cancelled { .. })
        })
        .await;
        while events.try_recv().is_ok() {}
        event_for(&mut events, old).await;
        let new = control.replace_feed(old.id, input("ETH")).await.unwrap();
        assert_eq!(new.generation, 3);
        control.shutdown().await.unwrap();
        running.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn shutdown_cancels_preparation_without_waiting_for_http() {
        let (control, _events, mut statuses, running, _stop) = start(FeedHandler::new());
        let old = control.add_feed(input("BTC")).await.unwrap();
        let update = {
            let control = control.clone();
            tokio::spawn(async move { control.replace_feed(old.id, input("WAIT")).await })
        };
        loop {
            if state_for(&mut statuses, old.id, |state| {
                *state == FeedState::Preparing
            })
            .await
            .generation
                > old.generation
            {
                break;
            }
        }
        tokio::time::timeout(std::time::Duration::from_secs(1), control.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert!(update.await.unwrap().is_err());
        running.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remove_acknowledgement_follows_registry_removal() {
        let (control, _events, _statuses, running, _stop) = start(FeedHandler::new());
        for _ in 0..32 {
            let identity = control.add_feed(input("BTC")).await.unwrap();
            control.remove_feed(identity.id).await.unwrap();
            assert!(control.feeds().await.unwrap().is_empty());
        }
        control.shutdown().await.unwrap();
        running.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn removal_cancels_pending_replacement_and_busy_updates_are_rejected() {
        let (control, _events, mut statuses, running, _stop) = start(FeedHandler::new());
        let old = control.add_feed(input("BTC")).await.unwrap();
        let update = {
            let control = control.clone();
            tokio::spawn(async move { control.replace_feed(old.id, input("WAIT")).await })
        };
        loop {
            if state_for(&mut statuses, old.id, |state| {
                *state == FeedState::Preparing
            })
            .await
            .generation
                > old.generation
            {
                break;
            }
        }
        assert!(control.replace_feed(old.id, input("ETH")).await.is_err());
        control.remove_feed(old.id).await.unwrap();
        assert!(update.await.unwrap().is_err());
        assert!(control.feeds().await.unwrap().is_empty());
        control.shutdown().await.unwrap();
        running.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn dropping_external_shutdown_sender_closes_managed_runtime() {
        let (control, mut events, _statuses, running, stop) = start(FeedHandler::new());
        let identity = control.add_feed(input("BTC")).await.unwrap();
        event_for(&mut events, identity).await;
        drop(stop);
        tokio::time::timeout(std::time::Duration::from_secs(1), running)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(control.feeds().await.is_err());
    }

    #[tokio::test]
    async fn cancelled_add_does_not_leave_an_unreachable_feed() {
        let (control, _events, mut statuses, running, _stop) = start(FeedHandler::new());
        let add = {
            let control = control.clone();
            tokio::spawn(async move { control.add_feed(input("WAIT")).await })
        };
        let identity = loop {
            if let FeedStatus::Lifecycle {
                identity,
                state: FeedState::Preparing,
                ..
            } = statuses.recv().await.unwrap()
            {
                break identity;
            }
        };
        add.abort();
        assert!(add.await.unwrap_err().is_cancelled());
        state_for(&mut statuses, identity.id, |state| {
            matches!(state, FeedState::Cancelled { .. })
        })
        .await;
        while !control.feeds().await.unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
        let next = control.add_feed(input("BTC")).await.unwrap();
        assert_ne!(next.id, identity.id);
        control.shutdown().await.unwrap();
        running.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn removal_waits_for_forced_child_cancellation() {
        STUCK_DROPPED.store(false, Ordering::SeqCst);
        let (control, mut events, mut statuses, running, _stop) = start(FeedHandler::new());
        let identity = control.add_feed(input("STUCK")).await.unwrap();
        event_for(&mut events, identity).await;
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            control.remove_feed(identity.id),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(STUCK_DROPPED.load(Ordering::SeqCst));
        state_for(&mut statuses, identity.id, |state| {
            *state == FeedState::Stopped { forced: true }
        })
        .await;
        control.shutdown().await.unwrap();
        running.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn state_queries_survive_lifecycle_lag_and_observed_data_is_not_ready() {
        let (control, mut events, mut statuses, running, _stop) = start(FeedHandler::new());
        let mut current = control.add_feed(input("BTC")).await.unwrap();
        for _ in 0..20 {
            current = control
                .replace_feed(current.id, input("ETH"))
                .await
                .unwrap();
        }
        event_for(&mut events, current).await;
        assert!(matches!(
            statuses.try_recv(),
            Err(broadcast::error::TryRecvError::Lagged(_))
        ));
        let state = control.state(current.id).await.unwrap();
        assert_eq!(state.identity, current);
        assert_eq!(state.state, FeedState::Started);
        assert!(state.observed_events > 0);
        assert!(!state.is_ready()); // Private fake transport emitted data, not remote acknowledgement.
        control.remove_feed(current.id).await.unwrap();
        assert!(control.state(current.id).await.is_err());
        control.shutdown().await.unwrap();
        running.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn independent_initial_startup_keeps_healthy_feeds_running() {
        let mut handler = FeedHandler::new();
        let bad = handler.add_feed_with_id(Binance::new().build());
        let good = handler.add_feed_with_id(input("BTC"));
        let (control, mut events, mut statuses, running, _stop) = start(handler);
        state_for(&mut statuses, bad, |state| {
            matches!(state, FeedState::Failed { .. })
        })
        .await;
        event_for(
            &mut events,
            FeedIdentity {
                id: good,
                generation: 1,
            },
        )
        .await;
        assert!(control.shutdown().await.is_err());
        assert!(running.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn initial_validation_failure_remains_queryable_and_replaceable() {
        let mut handler = FeedHandler::new();
        let id = handler.add_feed_with_id(Binance::new().build());
        let (control, mut events, mut statuses, running, _stop) = start(handler);
        state_for(&mut statuses, id, |state| {
            matches!(state, FeedState::Failed { .. })
        })
        .await;
        let state = control.state(id).await.unwrap();
        assert!(matches!(state.state, FeedState::Failed { .. }));
        assert!(!state.is_ready());
        let replacement = control.replace_feed(id, input("BTC")).await.unwrap();
        assert_eq!(replacement.id, id);
        event_for(&mut events, replacement).await;
        assert_eq!(control.state(id).await.unwrap().identity, replacement);
        assert!(control.shutdown().await.is_err());
        assert!(running.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn panicked_session_is_scoped_and_can_be_replaced() {
        let (control, mut events, mut statuses, running, _stop) = start(FeedHandler::new());
        let identity = control.add_feed(input("PANIC")).await.unwrap();
        state_for(&mut statuses, identity.id, |state| {
            matches!(state, FeedState::Degraded { .. })
        })
        .await;
        state_for(&mut statuses, identity.id, |state| {
            matches!(state, FeedState::Failed { .. })
        })
        .await;
        let healthy = control
            .replace_feed(identity.id, input("BTC"))
            .await
            .unwrap();
        event_for(&mut events, healthy).await;
        assert!(control.shutdown().await.is_err());
        assert!(running.await.unwrap().is_err());
    }

    #[test]
    fn admission_reserves_maximum_during_replacement_and_releases_old_exchange() {
        let admission = Admission::default();
        let first = FeedId::allocate();
        let second = FeedId::allocate();
        admission
            .reserve(first, None, Some((ExchangeId::Binance, 100)))
            .unwrap();
        admission
            .reserve(
                first,
                Some((ExchangeId::Binance, 100)),
                Some((ExchangeId::Binance, 100)),
            )
            .unwrap();
        assert!(
            admission
                .reserve(second, None, Some((ExchangeId::Binance, 1)))
                .is_err()
        );
        admission
            .reserve(
                first,
                Some((ExchangeId::Binance, 100)),
                Some((ExchangeId::Bybit, 100)),
            )
            .unwrap();
        assert!(
            admission
                .reserve(second, None, Some((ExchangeId::Bybit, 1)))
                .is_err()
        );
        admission
            .reserve(first, None, Some((ExchangeId::Bybit, 100)))
            .unwrap();
        admission
            .reserve(second, None, Some((ExchangeId::Binance, 100)))
            .unwrap();
        admission.release(first);
        admission
            .reserve(FeedId::allocate(), None, Some((ExchangeId::Bybit, 100)))
            .unwrap();
    }

    #[test]
    fn late_subscribers_receive_tagged_events_without_double_counting() {
        let mut handler = FeedHandler::new();
        let id = handler.add_feed_with_id(input("BTC"));
        let mut raw = handler.subscribe();
        let mut tagged = handler.subscribe_identified();
        emit(&handler.feeds()[0]);
        assert!(raw.try_recv().is_ok());
        assert_eq!(
            tagged.try_recv().unwrap().identity,
            FeedIdentity { id, generation: 1 }
        );
        assert_eq!(handler.event_count(Channel::Trade), 1);
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn reconnect_clears_only_the_books_owned_by_that_connection() {
        let feed = input("BTC");
        for base in ["BTC", "ETH"] {
            feed.orderbook_states.lock().unwrap().insert(
                format!("{base}-USDT"),
                cryptofeed_orderbook::L2BookState::new(Symbol::spot(base, "USDT")),
            );
        }
        feed.clear_connection_books(&[Symbol::spot("BTC", "USDT")]);
        let books = feed.orderbook_states.lock().unwrap();
        assert!(!books.contains_key("BTC-USDT"));
        assert!(books.contains_key("ETH-USDT"));
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn attachment_uses_fresh_runtime_book_state_without_mutating_old_clone() {
        let mut handler = FeedHandler::new();
        let mut feed = input("BTC");
        feed.orderbook_states.lock().unwrap().insert(
            "BTC-USDT".to_owned(),
            cryptofeed_orderbook::L2BookState::new(Symbol::spot("BTC", "USDT")),
        );
        let old = feed.orderbook_states.clone();
        let id = handler.add_feed_with_id(feed.clone());
        feed = handler.feeds()[0].clone();
        assert_eq!(feed.identity().unwrap().id, id);
        assert!(!Arc::ptr_eq(&old, &feed.orderbook_states));
        assert!(feed.orderbook_states.lock().unwrap().is_empty());
        assert!(old.lock().unwrap().contains_key("BTC-USDT"));
    }
}
