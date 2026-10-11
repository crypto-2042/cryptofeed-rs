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
    bybit_tickers: std::collections::HashMap<String, serde_json::Map<String, Value>>,
}
impl RawParserSession {
    pub(crate) fn new(
        info: &crate::recording::raw::RawFeedInfo,
        maximum_batch: usize,
    ) -> Result<Self> {
        use crate::exchange::ExchangeFeedBuilder;
        if info.channels.contains(&Channel::L2Book)
            && matches!(info.exchange, ExchangeId::Binance | ExchangeId::Gateio)
        {
            return Err(Error::UnsupportedCapability(
                "raw L2 replay needs captured HTTP bootstrap for Binance/Gate".into(),
            ));
        }
        let mut feed = ExchangeFeedBuilder::new(info.exchange).build();
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
        let (sender, output) = tokio::sync::broadcast::channel(maximum_batch);
        feed.event_sender = Some(sender);
        Ok(Self {
            feed,
            output,
            binance,
            gateio,
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
                    .find(|plan| binance_instrument_for_message(plan, value).is_ok())
                    .ok_or_else(|| {
                        Error::Parse("recorded Binance instrument absent from mapping".into())
                    })?;
                process_binance_text_message_for_plan(&self.feed, plan, &text, received_ts).await?;
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
                process_gateio_text_message_for_plan(&self.feed, plan, &text, received_ts).await?;
            }
            _ => return Err(Error::UnsupportedExchange("raw replay exchange".into())),
        }
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
