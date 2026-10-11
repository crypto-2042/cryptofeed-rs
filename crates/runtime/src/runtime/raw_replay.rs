use super::{
    binance_instrument_for_message, gateio_error_detail, merge_bybit_ticker_message,
    process_binance_text_message_for_plan, process_bitget_text_message, process_bybit_message,
    process_gateio_text_message_for_plan, process_okx_text_message,
};
use crate::exchange::{
    ExchangeFeed,
    binance::adapter::{BinanceAdapter, BinanceConnectionPlan},
    gateio::adapter::{GateioAdapter, GateioConnectionPlan},
};
use cryptofeed_core::{
    error::{Error, Result},
    exchange::{Channel, ExchangeId},
};
use serde_json::Value;

/// Offline parser state: owns only a fresh feed/local caches and bounded output queue.
/// No transport, hydration, supervisor, callback handler or HTTP bootstrap is attached.
pub(crate) struct RawParserSession {
    feed: ExchangeFeed,
    output: tokio::sync::broadcast::Receiver<crate::feed::FeedEvent>,
    binance: Vec<BinanceConnectionPlan>,
    gateio: Vec<GateioConnectionPlan>,
    #[cfg(feature = "orderbook")]
    binance_receivers: super::SnapshotReceivers,
    #[cfg(feature = "orderbook")]
    gateio_receivers: super::GateioSnapshotReceivers,
    #[cfg(feature = "orderbook")]
    binance_pending: std::collections::HashMap<String, Vec<super::BinanceSequencedDepthDelta>>,
    #[cfg(feature = "orderbook")]
    gateio_pending: std::collections::HashMap<String, Vec<super::GateioDepthDelta>>,
    #[cfg(feature = "orderbook")]
    binance_resnapshots: std::collections::HashMap<String, u32>,
    #[cfg(feature = "orderbook")]
    gateio_resnapshots: std::collections::HashMap<String, u32>,
    bybit_tickers: std::collections::HashMap<String, serde_json::Map<String, Value>>,
}
impl RawParserSession {
    pub(crate) fn new(
        info: &crate::recording::raw::RawFeedInfo,
        maximum_batch: usize,
        allow_http: bool,
    ) -> Result<Self> {
        use crate::exchange::ExchangeFeedBuilder;
        if !allow_http
            && info.channels.contains(&Channel::L2Book)
            && matches!(info.exchange, ExchangeId::Binance | ExchangeId::Gateio)
        {
            return Err(Error::UnsupportedCapability(
                "raw L2 replay needs captured HTTP bootstrap for Binance/Gate".into(),
            ));
        }
        let mut feed = ExchangeFeedBuilder::new(info.exchange).build();
        feed.replay_offline = true;
        feed.channels = info.channels.clone();
        feed.symbols = info.symbols.clone();
        feed.exchange_symbols = info.exchange_symbols.clone();
        feed.channel_subscriptions = info.channel_subscriptions.clone();
        feed.candle_interval = info.candle_interval.clone();
        feed.l2_book_depth = info.l2_book_depth;
        feed.l2_book_interval = info.l2_book_interval.clone();
        #[cfg(feature = "candles")]
        {
            feed.candle_policy = match info.candle_policy.as_deref() {
                Some("All") => crate::exchange::CandlePolicy::All,
                Some("ClosedOnly") => crate::exchange::CandlePolicy::ClosedOnly,
                Some("ClosedOrUnknown") => crate::exchange::CandlePolicy::ClosedOrUnknown,
                None if !feed.channels.contains(&Channel::Candles) => {
                    crate::exchange::CandlePolicy::All
                }
                _ => {
                    return Err(Error::InvalidConfiguration(
                        "missing/unknown recorded candle policy".into(),
                    ));
                }
            };
        }
        crate::markets::validate_feed(&feed)?;
        let mut registry = crate::markets::SymbolRegistry::default();
        for (symbol, native) in feed.symbols.iter().zip(&feed.exchange_symbols) {
            registry.insert(symbol.clone(), native)?;
        }
        let binance = if feed.exchange == ExchangeId::Binance {
            BinanceAdapter::connection_plans(&feed)?
        } else {
            Vec::new()
        };
        let gateio = if feed.exchange == ExchangeId::Gateio {
            GateioAdapter::connection_plans(&feed)?
        } else {
            Vec::new()
        };
        #[cfg(feature = "orderbook")]
        let mut binance_receivers = std::collections::HashMap::new();
        #[cfg(feature = "orderbook")]
        for plan in &binance {
            for instrument in &plan.instruments {
                if feed.subscribes(Channel::L2Book, &instrument.symbol) {
                    binance_receivers.insert(
                        instrument.symbol.as_str().to_owned(),
                        super::spawn_binance_snapshot_fetch(
                            instrument.clone(),
                            plan.l2_book_depth.unwrap_or(1000),
                            feed.transport.clone(),
                            super::snapshot::SnapshotMode::Replay,
                        ),
                    );
                }
            }
        }
        let (sender, output) = tokio::sync::broadcast::channel(maximum_batch);
        feed.event_sender = Some(sender);
        Ok(Self {
            feed,
            output,
            binance,
            gateio,
            #[cfg(feature = "orderbook")]
            binance_receivers,
            #[cfg(feature = "orderbook")]
            gateio_receivers: Default::default(),
            #[cfg(feature = "orderbook")]
            binance_pending: Default::default(),
            #[cfg(feature = "orderbook")]
            gateio_pending: Default::default(),
            #[cfg(feature = "orderbook")]
            binance_resnapshots: Default::default(),
            #[cfg(feature = "orderbook")]
            gateio_resnapshots: Default::default(),
            bybit_tickers: std::collections::HashMap::new(),
        })
    }
    pub(crate) async fn process(
        &mut self,
        payload: &crate::recording::raw::RawPayload,
        received_ts: f64,
        maximum_batch: usize,
    ) -> Result<Vec<crate::feed::FeedEvent>> {
        use crate::recording::raw::RawPayload;
        let RawPayload::Json { value, .. } = payload else {
            return Ok(Vec::new());
        };
        if value.get("op").and_then(Value::as_str) == Some("pong")
            || value.get("ret_msg").and_then(Value::as_str) == Some("pong")
            || value
                .get("channel")
                .and_then(Value::as_str)
                .is_some_and(|channel| channel.ends_with(".pong"))
        {
            return Ok(Vec::new());
        }
        let text = serde_json::to_string(value)
            .map_err(|_| Error::MalformedData("cannot encode recorded protocol JSON".into()))?;
        match self.feed.exchange {
            ExchangeId::Binance => {
                if value.get("code").is_some() {
                    return Err(Error::Subscription("recorded Binance control error".into()));
                }
                if value.get("id").is_some() && value.get("result").is_some() {
                    if !value["result"].is_null() {
                        return Err(Error::Subscription(
                            "invalid recorded Binance subscribe result".into(),
                        ));
                    }
                    return Ok(Vec::new());
                }
                let plan = self
                    .binance
                    .iter()
                    .find(|plan| {
                        binance_instrument_for_message(plan, value).is_ok()
                            || value
                                .get("stream")
                                .and_then(Value::as_str)
                                .is_some_and(|stream| {
                                    BinanceAdapter::instrument_for_stream(plan, stream).is_some()
                                })
                    })
                    .ok_or_else(|| {
                        Error::Parse("recorded Binance instrument absent from mapping".into())
                    })?;
                #[cfg(feature = "orderbook")]
                let handled = super::process_binance_orderbook_message(
                    &self.feed,
                    plan,
                    &text,
                    received_ts,
                    &mut self.binance_receivers,
                    &mut self.binance_pending,
                )
                .await?;
                #[cfg(not(feature = "orderbook"))]
                let handled = false;
                if !handled {
                    process_binance_text_message_for_plan(&self.feed, plan, &text, received_ts)
                        .await?;
                }
            }
            ExchangeId::Bitget => {
                process_bitget_text_message(&self.feed, &text, received_ts).await?
            }
            ExchangeId::Bybit => {
                let mut value = value.clone();
                merge_bybit_ticker_message(&self.feed, &mut self.bybit_tickers, &mut value)?;
                process_bybit_message(&self.feed, &value, received_ts).await?;
            }
            ExchangeId::Okx => process_okx_text_message(&self.feed, &text, received_ts).await?,
            ExchangeId::Gateio => {
                let control = value.get("event").and_then(Value::as_str) == Some("subscribe")
                    || gateio_error_detail(value).is_some();
                let plan = self
                    .gateio
                    .iter()
                    .find(|plan| {
                        control
                            || GateioAdapter::instrument_for_message(value, &plan.instruments)
                                .is_some()
                    })
                    .ok_or_else(|| {
                        Error::Parse("recorded Gate instrument absent from product plans".into())
                    })?;
                #[cfg(feature = "orderbook")]
                let handled = super::process_gateio_orderbook_message_for_plan(
                    &self.feed,
                    plan,
                    &text,
                    received_ts,
                    &mut self.gateio_receivers,
                    &mut self.gateio_pending,
                )
                .await?;
                #[cfg(not(feature = "orderbook"))]
                let handled = false;
                if !handled {
                    process_gateio_text_message_for_plan(&self.feed, plan, &text, received_ts)
                        .await?;
                }
            }
            _ => return Err(Error::UnsupportedExchange("raw replay exchange".into())),
        }
        self.drain(maximum_batch)
    }
    pub(crate) async fn snapshot(
        &mut self,
        symbol: &cryptofeed_core::symbol::Symbol,
        depth: Option<u16>,
        payload: &crate::recording::raw::RawPayload,
        received_ts: f64,
        maximum_batch: usize,
    ) -> Result<Vec<crate::feed::FeedEvent>> {
        #[cfg(feature = "orderbook")]
        {
            let crate::recording::raw::RawPayload::Json { value, .. } = payload else {
                return Err(Error::MalformedData("snapshot body must be JSON".into()));
            };
            match self.feed.exchange {
                ExchangeId::Binance => {
                    let plan = self
                        .binance
                        .iter()
                        .find(|plan| {
                            plan.instruments
                                .iter()
                                .any(|instrument| &instrument.symbol == symbol)
                        })
                        .ok_or_else(|| {
                            Error::Parse("recorded snapshot instrument absent".into())
                        })?;
                    let instrument = plan
                        .instruments
                        .iter()
                        .find(|instrument| &instrument.symbol == symbol)
                        .expect("snapshot instrument");
                    let limit = depth.ok_or_else(|| {
                        Error::Parse("recorded Binance snapshot depth missing".into())
                    })?;
                    if limit != plan.l2_book_depth.unwrap_or(1000) {
                        return Err(Error::Parse(
                            "recorded snapshot depth differs from plan".into(),
                        ));
                    }
                    if !self.binance_receivers.contains_key(symbol.as_str()) {
                        return Err(Error::MalformedData(
                            "recorded Binance snapshot has no pending bootstrap".into(),
                        ));
                    }
                    let result = super::binance_parser::parse_l2_book_snapshot_for_instrument(
                        value,
                        instrument,
                        received_ts,
                    )
                    .ok_or_else(|| Error::Parse("invalid recorded Binance snapshot".into()));
                    self.binance_receivers.insert(
                        symbol.as_str().to_owned(),
                        super::snapshot::SnapshotReceiver::ready(result),
                    );
                    super::poll_binance_snapshot_bootstraps(
                        &self.feed,
                        &plan.instruments,
                        &mut self.binance_receivers,
                        &mut self.binance_pending,
                        &mut self.binance_resnapshots,
                        limit,
                        plan.l2_book_depth.is_some(),
                    )
                    .await?;
                }
                ExchangeId::Gateio => {
                    let plan = self
                        .gateio
                        .iter()
                        .find(|plan| {
                            plan.instruments
                                .iter()
                                .any(|instrument| &instrument.symbol == symbol)
                        })
                        .ok_or_else(|| {
                            Error::Parse("recorded snapshot instrument absent".into())
                        })?;
                    let instrument = plan
                        .instruments
                        .iter()
                        .find(|instrument| &instrument.symbol == symbol)
                        .expect("snapshot instrument");
                    if !self.gateio_receivers.contains_key(symbol.as_str())
                        || self
                            .gateio_pending
                            .get(symbol.as_str())
                            .is_none_or(Vec::is_empty)
                    {
                        return Err(Error::MalformedData(
                            "recorded Gate snapshot has no pending buffered bootstrap".into(),
                        ));
                    }
                    let result = super::gateio_parser::parse_l2_book_snapshot_for_instrument(
                        value,
                        instrument,
                        received_ts,
                    )
                    .ok_or_else(|| Error::Parse("invalid recorded Gate snapshot".into()));
                    self.gateio_receivers.insert(
                        symbol.as_str().to_owned(),
                        super::snapshot::SnapshotReceiver::ready(result),
                    );
                    super::poll_gateio_snapshot_bootstraps_for_plan(
                        &self.feed,
                        plan,
                        &mut self.gateio_receivers,
                        &mut self.gateio_pending,
                        &mut self.gateio_resnapshots,
                    )
                    .await?;
                }
                _ => {
                    return Err(Error::UnsupportedCapability(
                        "recorded HTTP book snapshot exchange".into(),
                    ));
                }
            }
            self.drain(maximum_batch)
        }
        #[cfg(not(feature = "orderbook"))]
        {
            let _ = (symbol, depth, payload, received_ts, maximum_batch);
            Err(Error::UnsupportedCapability(
                "orderbook replay feature disabled".into(),
            ))
        }
    }
    fn drain(&mut self, maximum_batch: usize) -> Result<Vec<crate::feed::FeedEvent>> {
        let mut events = Vec::new();
        loop {
            match self.output.try_recv() {
                Ok(event) => {
                    if events.len() >= maximum_batch {
                        return Err(Error::Protocol("raw replay output batch limit".into()));
                    }
                    events.push(event);
                }
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => {
                    return Err(Error::Protocol("raw replay output queue overflow".into()));
                }
                Err(
                    tokio::sync::broadcast::error::TryRecvError::Empty
                    | tokio::sync::broadcast::error::TryRecvError::Closed,
                ) => break,
            }
        }
        Ok(events)
    }
}
