//! Offline native normalization, with fresh per-connection parser/book state.
use super::{RawObservationKind, RawRecordingReader, RawSessionInfo};
use crate::{
    feed::{FeedEvent, FeedIdentity},
    recording::{RecordingSummary, ReplayOptions, ReplayTiming},
};
use cryptofeed_core::error::{Error, Result};
use futures::FutureExt;
use std::{collections::HashMap, future::Future, sync::Arc};
use tokio::{io::AsyncBufRead, sync::watch};
#[derive(Clone, Debug)]
pub struct RawReplayEvent {
    pub session_id: u64,
    pub identity: Option<FeedIdentity>,
    pub observation_sequence: u64,
    pub event: FeedEvent,
}
#[derive(Clone, Debug)]
#[non_exhaustive]
// Market models dominate the stream; keep them inline instead of a heap allocation per event.
#[allow(clippy::large_enum_variant)]
pub enum RawReplayItem {
    SessionStarted(Arc<RawSessionInfo>),
    Market(RawReplayEvent),
    SessionEnded {
        session: Arc<RawSessionInfo>,
        clean: bool,
    },
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawReplaySummary {
    pub recording: RecordingSummary,
    pub models: u64,
}
#[derive(Clone, Copy, Debug)]
pub struct RawReplayOptions {
    delivery: ReplayOptions,
    maximum_batch: usize,
    maximum_models: u64,
}
impl Default for RawReplayOptions {
    fn default() -> Self {
        Self {
            delivery: ReplayOptions::default(),
            maximum_batch: 1024,
            maximum_models: 1_000_000,
        }
    }
}
impl RawReplayOptions {
    pub fn delivery(mut self, options: ReplayOptions) -> Self {
        self.delivery = options;
        self
    }
    pub fn output_limits(mut self, maximum_batch: usize, maximum_models: u64) -> Result<Self> {
        if !(1..=65_536).contains(&maximum_batch) || maximum_models == 0 {
            return Err(Error::InvalidConfiguration(
                "raw replay requires batch 1..65536 and positive model budget".into(),
            ));
        }
        self.maximum_batch = maximum_batch;
        self.maximum_models = maximum_models;
        Ok(self)
    }
}
impl<R: AsyncBufRead + Unpin> RawRecordingReader<R> {
    /// Uses native parsers/dispatch with only local state, never live HTTP/WS.
    /// Complete EOF certifies file validation, not source/native retained completeness.
    pub async fn replay<F, Fut>(
        &mut self,
        options: RawReplayOptions,
        shutdown: watch::Receiver<bool>,
        callback: F,
    ) -> Result<Option<RawReplaySummary>>
    where
        F: FnMut(RawReplayItem) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        self.begin_replay()?;
        let result = run(self, options, shutdown, callback).await;
        self.end_replay(matches!(result, Ok(Some(_))));
        result
    }
}
async fn run<R, F, Fut>(
    reader: &mut RawRecordingReader<R>,
    options: RawReplayOptions,
    mut shutdown: watch::Receiver<bool>,
    mut callback: F,
) -> Result<Option<RawReplaySummary>>
where
    R: AsyncBufRead + Unpin,
    F: FnMut(RawReplayItem) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut sessions = HashMap::new();
    let mut models = 0u64;
    let started = tokio::time::Instant::now();
    loop {
        if *shutdown.borrow() {
            return Ok(None);
        }
        let next = tokio::select! {biased;_=super::super::stopped(&mut shutdown)=>return Ok(None),result=reader.read_observation()=>result?};
        let Some(record) = next else {
            return Ok(reader
                .summary()
                .map(|recording| RawReplaySummary { recording, models }));
        };
        if options.delivery.timing == ReplayTiming::Recorded {
            let deadline = started
                .checked_add(std::time::Duration::from_nanos(record.elapsed_ns))
                .ok_or_else(|| {
                    Error::MalformedData("raw replay elapsed deadline overflow".into())
                })?;
            tokio::select! {biased;_=super::super::stopped(&mut shutdown)=>return Ok(None),_=tokio::time::sleep_until(deadline)=>{}}
        }
        let id = record.session.session_id;
        let mut output = Vec::new();
        match record.kind {
            RawObservationKind::Connected => {
                let session = crate::runtime::RawParserSession::new(
                    &record.session.feed,
                    options.maximum_batch,
                )?;
                sessions.insert(id, session);
                output.push(RawReplayItem::SessionStarted(record.session));
            }
            RawObservationKind::Received(payload) => {
                let session = sessions
                    .get_mut(&id)
                    .ok_or_else(|| Error::MalformedData("raw parser session absent".into()))?;
                let parsed = session
                    .process(&payload, record.observed_ts, options.maximum_batch)
                    .await?;
                if parsed.len() as u64 > options.maximum_models.saturating_sub(models) {
                    return Err(Error::Protocol("raw replay total model budget".into()));
                }
                models += parsed.len() as u64;
                for event in parsed {
                    output.push(RawReplayItem::Market(RawReplayEvent {
                        session_id: id,
                        identity: record.session.feed.identity,
                        observation_sequence: record.sequence,
                        event,
                    }));
                }
            }
            RawObservationKind::Closed { clean } => {
                sessions.remove(&id);
                output.push(RawReplayItem::SessionEnded {
                    session: record.session,
                    clean,
                });
            }
            RawObservationKind::Sent(_) => {}
        }
        for item in output {
            let future =
                std::panic::AssertUnwindSafe(async { callback(item).await }).catch_unwind();
            tokio::select! {biased;
                _=super::super::stopped(&mut shutdown)=>return Ok(None),
                result=tokio::time::timeout(options.delivery.callback_deadline,future)=>result.map_err(|_|Error::Protocol("raw replay callback deadline".into()))?.map_err(|_|Error::Protocol("raw replay callback panicked".into()))??,
            }
        }
    }
}

#[cfg(all(test, feature = "trade"))]
mod tests {
    use super::*;
    use crate::{
        exchange::ExchangeFeedBuilder,
        recording::{
            RecordingEnd,
            raw::{RawFeedInfo, RawRecordingLimits, RawRecordingWriter, raw_capture_channel},
        },
    };
    use cryptofeed_core::{
        exchange::{Channel, ExchangeId},
        symbol::Symbol,
    };
    use serde_json::{Value, json};
    fn feed(exchange: ExchangeId) -> RawFeedInfo {
        let native = match exchange {
            ExchangeId::Okx => "BTC-USDT",
            ExchangeId::Gateio => "BTC_USDT",
            _ => "BTCUSDT",
        };
        RawFeedInfo::from_feed(
            &ExchangeFeedBuilder::new(exchange)
                .trade()
                .symbol("BTC-USDT")
                .exchange_symbol(native)
                .build(),
        )
    }
    fn trade(exchange: ExchangeId) -> Value {
        match exchange {
            ExchangeId::Binance => {
                json!({"e":"aggTrade","s":"BTCUSDT","a":1,"p":"100.01","q":"0.25","T":1000,"m":false})
            }
            ExchangeId::Bitget => {
                json!({"arg":{"instType":"spot","topic":"publicTrade","symbol":"BTCUSDT"},"data":[{"T":"1000","p":"100.01","v":"0.25","S":"buy","i":"1"}]})
            }
            ExchangeId::Bybit => {
                json!({"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1000,"data":[{"T":1000,"s":"BTCUSDT","S":"Buy","v":"0.25","p":"100.01","i":"1"}]})
            }
            ExchangeId::Okx => {
                json!({"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"tradeId":"1","px":"100.01","sz":"0.25","side":"buy","ts":"1000"}]})
            }
            ExchangeId::Gateio => {
                json!({"channel":"spot.trades","event":"update","result":{"id":1,"currency_pair":"BTC_USDT","side":"buy","price":"100.01","amount":"0.25","create_time_ms":"1000"}})
            }
            _ => unreachable!(),
        }
    }
    async fn segment(info: RawFeedInfo, messages: Vec<Value>, reconnect: bool) -> Vec<u8> {
        let (capture, mut input) = raw_capture_channel(64, 64 * 1024).unwrap();
        let mut session = capture.session(info.clone()).unwrap();
        for (i, message) in messages.into_iter().enumerate() {
            if reconnect && i == 1 {
                session.close(true);
                drop(session);
                session = capture.session(info.clone()).unwrap();
            }
            session.text(
                &serde_json::to_string(&message).unwrap(),
                Some(&message),
                true,
                2.0 + i as f64,
            );
        }
        session.close(true);
        drop(session);
        drop(capture);
        let mut bytes = Vec::new();
        let mut writer = RawRecordingWriter::new(&mut bytes, RawRecordingLimits::default())
            .await
            .unwrap();
        while let Some(record) = input.recv().await.unwrap() {
            writer.append(record).await.unwrap();
        }
        writer.finish(RecordingEnd::Complete).await.unwrap();
        bytes
    }
    #[tokio::test]
    async fn five_native_trade_families_replay_with_original_clocks_and_lifecycle() {
        for exchange in [
            ExchangeId::Binance,
            ExchangeId::Bitget,
            ExchangeId::Bybit,
            ExchangeId::Okx,
            ExchangeId::Gateio,
        ] {
            let bytes = segment(feed(exchange), vec![trade(exchange)], false).await;
            let mut reader =
                RawRecordingReader::new(bytes.as_slice(), RawRecordingLimits::default());
            let (_tx, stop) = watch::channel(false);
            let mut lifecycle = Vec::new();
            let mut output = Vec::new();
            let result = reader
                .replay(RawReplayOptions::default(), stop, |item| {
                    match item {
                        RawReplayItem::SessionStarted(_) => lifecycle.push("start"),
                        RawReplayItem::SessionEnded { .. } => lifecycle.push("end"),
                        RawReplayItem::Market(record) => output.push(record),
                    }
                    std::future::ready(Ok(()))
                })
                .await
                .unwrap()
                .unwrap();
            assert_eq!(lifecycle, vec!["start", "end"]);
            assert_eq!(result.models, 1, "{exchange:?}");
            assert_eq!(output.len(), 1);
            let FeedEvent::Trade(trade) = &output[0].event else {
                panic!("trade")
            };
            assert_eq!(trade.symbol, Symbol::spot("BTC", "USDT"));
            assert_eq!(trade.price.to_string(), "100.01");
            assert_eq!(trade.amount.to_string(), "0.25");
            assert_eq!(trade.exchange_ts, 1.0);
            assert_eq!(trade.received_ts, 2.0);
            assert_eq!(trade.id.as_deref(), Some("1"));
        }
    }
    #[tokio::test]
    async fn dense_batch_limits_and_total_limits_fail_without_silent_model_loss() {
        let mut message = trade(ExchangeId::Bybit);
        let mut second = message["data"][0].clone();
        second["i"] = json!("2");
        message["data"].as_array_mut().unwrap().push(second);
        let bytes = segment(feed(ExchangeId::Bybit), vec![message], false).await;
        for options in [
            RawReplayOptions::default().output_limits(1, 10).unwrap(),
            RawReplayOptions::default().output_limits(1024, 1).unwrap(),
        ] {
            let mut reader =
                RawRecordingReader::new(bytes.as_slice(), RawRecordingLimits::default());
            let (_tx, stop) = watch::channel(false);
            let mut models = 0;
            assert!(
                reader
                    .replay(options, stop, |item| {
                        if matches!(item, RawReplayItem::Market(_)) {
                            models += 1;
                        }
                        std::future::ready(Ok(()))
                    })
                    .await
                    .is_err()
            );
            assert_eq!(models, 0);
            assert!(reader.next_observation().await.is_err());
        }
    }
    #[cfg(feature = "ticker")]
    #[tokio::test]
    async fn bybit_ticker_deltas_merge_and_reconnect_cannot_reuse_snapshot_fields() {
        let mut info = feed(ExchangeId::Bybit);
        info.channels = vec![Channel::Ticker];
        info.symbols = vec![Symbol::perpetual("BTC", "USDT")];
        let snapshot = json!({"topic":"tickers.BTCUSDT","type":"snapshot","ts":1000,"data":{"symbol":"BTCUSDT","bid1Price":"100","ask1Price":"101"}});
        let delta = json!({"topic":"tickers.BTCUSDT","type":"delta","ts":1001,"data":{"symbol":"BTCUSDT","bid1Price":"99"}});
        let bytes = segment(info.clone(), vec![snapshot.clone(), delta.clone()], false).await;
        let mut reader = RawRecordingReader::new(bytes.as_slice(), RawRecordingLimits::default());
        let (_tx, stop) = watch::channel(false);
        let mut values = Vec::new();
        reader
            .replay(RawReplayOptions::default(), stop, |item| {
                if let RawReplayItem::Market(RawReplayEvent {
                    event: FeedEvent::Ticker(value),
                    ..
                }) = item
                {
                    values.push(value);
                }
                std::future::ready(Ok(()))
            })
            .await
            .unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values[1].bid.to_string(), "99");
        assert_eq!(values[1].ask.to_string(), "101");
        assert_eq!(values[1].received_ts, 3.0);
        let bytes = segment(info, vec![snapshot, delta], true).await;
        let mut reader = RawRecordingReader::new(bytes.as_slice(), RawRecordingLimits::default());
        let (_tx, stop) = watch::channel(false);
        let mut models = 0;
        assert!(
            reader
                .replay(RawReplayOptions::default(), stop, |item| {
                    if matches!(item, RawReplayItem::Market(_)) {
                        models += 1;
                    }
                    std::future::ready(Ok(()))
                })
                .await
                .is_err()
        );
        assert_eq!(models, 1);
    }
    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn native_bybit_l2_snapshot_delta_and_reset_are_replayed_locally() {
        let mut info = feed(ExchangeId::Bybit);
        info.channels = vec![Channel::L2Book];
        info.l2_book_depth = Some(50);
        let snapshot = json!({"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1000,"data":{"s":"BTCUSDT","b":[["100","2"]],"a":[["101","1"]],"u":10,"seq":20}});
        let delta = json!({"topic":"orderbook.50.BTCUSDT","type":"delta","ts":1001,"data":{"s":"BTCUSDT","b":[["100","3"]],"a":[],"u":11,"seq":21}});
        let bytes = segment(info.clone(), vec![snapshot.clone(), delta.clone()], false).await;
        let mut reader = RawRecordingReader::new(bytes.as_slice(), RawRecordingLimits::default());
        let (_tx, stop) = watch::channel(false);
        let mut books = Vec::new();
        reader
            .replay(RawReplayOptions::default(), stop, |item| {
                if let RawReplayItem::Market(RawReplayEvent {
                    event: FeedEvent::L2Book(book),
                    ..
                }) = item
                {
                    books.push(book);
                }
                std::future::ready(Ok(()))
            })
            .await
            .unwrap();
        assert_eq!(books.len(), 2);
        assert_eq!(books[0].bids()[0].amount.to_string(), "2");
        assert_eq!(books[1].bids()[0].amount.to_string(), "3");
        let bytes = segment(info, vec![snapshot, delta], true).await;
        let mut reader = RawRecordingReader::new(bytes.as_slice(), RawRecordingLimits::default());
        let (_tx, stop) = watch::channel(false);
        assert!(
            reader
                .replay(RawReplayOptions::default(), stop, |_| std::future::ready(
                    Ok(())
                ))
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn missing_http_bootstrap_contexts_fail_at_start_not_online_fallback() {
        for exchange in [ExchangeId::Binance, ExchangeId::Gateio] {
            let mut info = feed(exchange);
            info.channels = vec![Channel::L2Book];
            let bytes = segment(info, vec![], false).await;
            let mut reader =
                RawRecordingReader::new(bytes.as_slice(), RawRecordingLimits::default());
            let (_tx, stop) = watch::channel(false);
            let mut calls = 0;
            assert!(
                reader
                    .replay(RawReplayOptions::default(), stop, |_| {
                        calls += 1;
                        std::future::ready(Ok(()))
                    })
                    .await
                    .is_err()
            );
            assert_eq!(calls, 0);
        }
    }
    #[tokio::test(start_paused = true)]
    async fn callback_error_panic_deadline_and_cancellation_make_reader_terminal() {
        let bytes = segment(feed(ExchangeId::Okx), vec![trade(ExchangeId::Okx)], false).await;
        for mode in 0..4 {
            let mut reader =
                RawRecordingReader::new(bytes.as_slice(), RawRecordingLimits::default());
            let (_tx, stop) = watch::channel(false);
            assert!(
                reader
                    .replay(RawReplayOptions::default(), stop, |_| {
                        if mode == 3 {
                            panic!("callback construction");
                        }
                        async move {
                            match mode {
                                0 => Err(Error::Protocol("business".into())),
                                1 => panic!("callback"),
                                _ => std::future::pending::<Result<()>>().await,
                            }
                        }
                    })
                    .await
                    .is_err()
            );
            assert!(reader.next_observation().await.is_err());
        }
        let mut reader = RawRecordingReader::new(bytes.as_slice(), RawRecordingLimits::default());
        let (tx, stop) = watch::channel(false);
        let mut replay = Box::pin(reader.replay(RawReplayOptions::default(), stop, |_| {
            std::future::pending::<Result<()>>()
        }));
        assert!(futures::poll!(replay.as_mut()).is_pending());
        tx.send(true).unwrap();
        assert!(replay.await.unwrap().is_none());
        assert!(reader.next_observation().await.is_err());
        let mut reader = RawRecordingReader::new(bytes.as_slice(), RawRecordingLimits::default());
        let (_tx, stop) = watch::channel(false);
        let mut replay = Box::pin(reader.replay(RawReplayOptions::default(), stop, |_| {
            std::future::pending::<Result<()>>()
        }));
        assert!(futures::poll!(replay.as_mut()).is_pending());
        drop(replay);
        assert!(reader.next_observation().await.is_err());
    }
    #[cfg(feature = "orderbook")]
    #[tokio::test]
    async fn okx_and_bitget_l2_reuse_native_sequence_and_checksum_validation() {
        for exchange in [ExchangeId::Okx, ExchangeId::Bitget] {
            let mut info = feed(exchange);
            info.channels = vec![Channel::L2Book];
            let (snapshot, delta) = if exchange == ExchangeId::Okx {
                (
                    json!({"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[{"bids":[["100","2","0","1"]],"asks":[["101","1","0","1"]],"ts":"1000","seqId":10,"prevSeqId":-1,"checksum":1996849324}]}),
                    json!({"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[{"bids":[["100","3","0","1"]],"asks":[],"ts":"1001","seqId":11,"prevSeqId":10,"checksum":-781022440}]}),
                )
            } else {
                (
                    json!({"arg":{"instType":"spot","topic":"books","symbol":"BTCUSDT"},"action":"snapshot","data":[{"b":[["100","2"]],"a":[["101","1"]],"ts":"1000","seq":10,"pseq":0}]}),
                    json!({"arg":{"instType":"spot","topic":"books","symbol":"BTCUSDT"},"action":"update","data":[{"b":[["100","3"]],"a":[],"ts":"1001","seq":11,"pseq":10}]}),
                )
            };
            let bytes = segment(info.clone(), vec![snapshot.clone(), delta.clone()], false).await;
            let mut reader =
                RawRecordingReader::new(bytes.as_slice(), RawRecordingLimits::default());
            let (_tx, stop) = watch::channel(false);
            let mut books = Vec::new();
            reader
                .replay(RawReplayOptions::default(), stop, |item| {
                    if let RawReplayItem::Market(RawReplayEvent {
                        event: FeedEvent::L2Book(book),
                        ..
                    }) = item
                    {
                        books.push(book);
                    }
                    std::future::ready(Ok(()))
                })
                .await
                .unwrap();
            assert_eq!(books.len(), 2, "{exchange:?}");
            assert_eq!(books[1].bids()[0].amount.to_string(), "3");
            let mut bad = delta;
            if exchange == ExchangeId::Okx {
                bad["data"][0]["checksum"] = json!(1);
            } else {
                // First bridge must include snapshot seq=10; [12,13] does not.
                bad["data"][0]["pseq"] = json!(12);
                bad["data"][0]["seq"] = json!(13);
            }
            let bytes = segment(info, vec![snapshot, bad], false).await;
            let mut reader =
                RawRecordingReader::new(bytes.as_slice(), RawRecordingLimits::default());
            let (_tx, stop) = watch::channel(false);
            assert!(
                reader
                    .replay(RawReplayOptions::default(), stop, |_| std::future::ready(
                        Ok(())
                    ))
                    .await
                    .is_err(),
                "{exchange:?}"
            );
        }
    }
    #[cfg(all(feature = "ticker", feature = "candles"))]
    #[tokio::test]
    async fn sparse_rules_and_candle_completion_policy_are_not_bypassed() {
        let mut info = feed(ExchangeId::Binance);
        info.channels.push(Channel::Ticker);
        info.symbols.push(Symbol::spot("ETH", "USDT"));
        info.exchange_symbols.push("ETHUSDT".into());
        info.channel_subscriptions = vec![
            (Channel::Trade, vec![Symbol::spot("BTC", "USDT")]),
            (Channel::Ticker, vec![Symbol::spot("ETH", "USDT")]),
        ];
        let mut ignored = trade(ExchangeId::Binance);
        ignored["s"] = json!("ETHUSDT");
        let bytes = segment(info, vec![ignored, trade(ExchangeId::Binance)], false).await;
        let mut reader = RawRecordingReader::new(bytes.as_slice(), RawRecordingLimits::default());
        let (_tx, stop) = watch::channel(false);
        let summary = reader
            .replay(RawReplayOptions::default(), stop, |_| {
                std::future::ready(Ok(()))
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(summary.models, 1);
        let mut info = feed(ExchangeId::Bybit);
        info.channels = vec![Channel::Candles];
        info.candle_policy = Some("ClosedOnly".into());
        let open = json!({"topic":"kline.1.BTCUSDT","type":"snapshot","ts":1000,"data":[{"start":0,"end":59999,"interval":"1","open":"100","close":"101","high":"102","low":"99","volume":"1","confirm":false}]});
        let mut closed = open.clone();
        closed["data"][0]["confirm"] = json!(true);
        let bytes = segment(info, vec![open, closed], false).await;
        let mut reader = RawRecordingReader::new(bytes.as_slice(), RawRecordingLimits::default());
        let (_tx, stop) = watch::channel(false);
        let mut clocks = Vec::new();
        let summary = reader
            .replay(RawReplayOptions::default(), stop, |item| {
                if let RawReplayItem::Market(RawReplayEvent {
                    event: FeedEvent::Candle(value),
                    ..
                }) = item
                {
                    assert_eq!(value.closed, Some(true));
                    clocks.push(value.received_ts);
                }
                std::future::ready(Ok(()))
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(summary.models, 1);
        assert_eq!(clocks, vec![3.0]);
    }
    #[tokio::test(start_paused = true)]
    async fn recorded_timing_is_absolute_and_keeps_lifecycle_order() {
        let original = segment(feed(ExchangeId::Okx), vec![trade(ExchangeId::Okx)], false).await;
        let mut reader =
            RawRecordingReader::new(original.as_slice(), RawRecordingLimits::default());
        let mut records = Vec::new();
        while let Some(record) = reader.next_observation().await.unwrap() {
            records.push(record);
        }
        records[0].elapsed_ns = 0;
        records[1].elapsed_ns = 1_000_000_000;
        records[2].elapsed_ns = 3_000_000_000;
        let mut bytes = Vec::new();
        let mut writer = RawRecordingWriter::new(&mut bytes, RawRecordingLimits::default())
            .await
            .unwrap();
        for record in records {
            writer.append(record).await.unwrap();
        }
        writer.finish(RecordingEnd::Complete).await.unwrap();
        let mut reader = RawRecordingReader::new(bytes.as_slice(), RawRecordingLimits::default());
        let (_tx, stop) = watch::channel(false);
        let start = tokio::time::Instant::now();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let log = seen.clone();
        reader
            .replay(
                RawReplayOptions::default()
                    .delivery(ReplayOptions::default().timing(ReplayTiming::Recorded)),
                stop,
                move |item| {
                    let log = log.clone();
                    async move {
                        let tag = match item {
                            RawReplayItem::SessionStarted(_) => "start",
                            RawReplayItem::Market(_) => "market",
                            RawReplayItem::SessionEnded { .. } => "end",
                        };
                        log.lock().unwrap().push((tag, start.elapsed()));
                        if tag == "market" {
                            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        }
                        Ok(())
                    }
                },
            )
            .await
            .unwrap();
        assert_eq!(
            *seen.lock().unwrap(),
            vec![
                ("start", std::time::Duration::ZERO),
                ("market", std::time::Duration::from_secs(1)),
                ("end", std::time::Duration::from_secs(3))
            ]
        );
    }
    #[tokio::test]
    async fn native_derivative_and_dated_mappings_are_not_reparsed_as_spot() {
        for exchange in [ExchangeId::Binance, ExchangeId::Bitget, ExchangeId::Gateio] {
            let mut info = feed(exchange);
            let mut message = trade(exchange);
            let expected = if exchange == ExchangeId::Bitget {
                info.symbols = vec![Symbol::perpetual("BTC", "USDT")];
                message["arg"]["instType"] = json!("usdt-futures");
                Symbol::perpetual("BTC", "USDT")
            } else if exchange == ExchangeId::Binance {
                info.symbols = vec![Symbol::futures("BTC", "USD", "241227")];
                info.exchange_symbols = vec!["BTCUSD_241227".into()];
                message["s"] = json!("BTCUSD_241227");
                Symbol::futures("BTC", "USD", "241227")
            } else {
                info.symbols = vec![Symbol::futures("BTC", "USDT", "241227")];
                info.exchange_symbols = vec!["BTC_USDT_20241227".into()];
                message = json!({"channel":"futures.trades","event":"update","result":[{"id":1,"contract":"BTC_USDT_20241227","size":2,"price":"100.01","create_time":1}]});
                Symbol::futures("BTC", "USDT", "241227")
            };
            let bytes = segment(info, vec![message], false).await;
            let mut reader =
                RawRecordingReader::new(bytes.as_slice(), RawRecordingLimits::default());
            let (_tx, stop) = watch::channel(false);
            let mut symbols = Vec::new();
            reader
                .replay(RawReplayOptions::default(), stop, |item| {
                    if let RawReplayItem::Market(RawReplayEvent {
                        event: FeedEvent::Trade(trade),
                        ..
                    }) = item
                    {
                        symbols.push(trade.symbol);
                    }
                    std::future::ready(Ok(()))
                })
                .await
                .unwrap();
            assert_eq!(symbols, vec![expected], "{exchange:?}");
        }
    }
}

#[cfg(all(test, not(feature = "trade")))]
#[tokio::test]
async fn recorded_disabled_channel_is_rejected_before_any_replay_callback() {
    use crate::{
        exchange::okx::Okx,
        recording::{
            RecordingEnd,
            raw::{RawFeedInfo, RawRecordingLimits, RawRecordingWriter, raw_capture_channel},
        },
    };
    let (capture, mut input) = raw_capture_channel(8, 4096).unwrap();
    let feed = Okx::new()
        .trade()
        .symbol("BTC-USDT")
        .exchange_symbol("BTC-USDT")
        .build();
    let mut session = capture.session(RawFeedInfo::from_feed(&feed)).unwrap();
    session.close(true);
    drop(session);
    drop(capture);
    let mut bytes = Vec::new();
    let mut writer = RawRecordingWriter::new(&mut bytes, RawRecordingLimits::default())
        .await
        .unwrap();
    while let Some(record) = input.recv().await.unwrap() {
        writer.append(record).await.unwrap();
    }
    writer.finish(RecordingEnd::Complete).await.unwrap();
    let mut reader = RawRecordingReader::new(bytes.as_slice(), RawRecordingLimits::default());
    let (_tx, stop) = watch::channel(false);
    let mut calls = 0;
    assert!(
        reader
            .replay(RawReplayOptions::default(), stop, |_| {
                calls += 1;
                std::future::ready(Ok(()))
            })
            .await
            .is_err()
    );
    assert_eq!(calls, 0);
}
