//! Public, normalized REST market-data queries.
#[cfg(any(
    feature = "ticker",
    feature = "orderbook",
    feature = "funding",
    feature = "candles"
))]
mod adapter;
#[cfg(feature = "candles")]
mod candles;
#[cfg(feature = "funding")]
mod history;
use crate::{catalog::MarketCatalog, transport::TransportConfig};
#[cfg(feature = "candles")]
pub use candles::{CandleHistory, CandleHistoryCursor, CandleHistoryQuery, CandleHistoryStop};
#[cfg(any(
    feature = "ticker",
    feature = "orderbook",
    feature = "funding",
    feature = "candles"
))]
use cryptofeed_core::symbol::Symbol;
use cryptofeed_core::{
    error::Result,
    exchange::{Channel, ExchangeId},
    symbol::InstrumentKind,
};
#[cfg(feature = "funding")]
pub use history::{FundingHistory, FundingHistoryCursor, FundingHistoryQuery, HistoryStop};
use std::sync::Arc;

/// A standalone query result, not a WS synchronization/recovery anchor.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct RestSnapshot<T> {
    pub data: T,
    /// Native payload/response time where supplied. Model exchange_ts uses
    /// received_ts as a compatibility fallback only when this is None.
    pub exchange_ts: Option<f64>,
    pub received_ts: f64,
    pub sequence: Option<u64>,
}
#[derive(Clone)]
pub struct PublicRestClient {
    catalog: Arc<MarketCatalog>,
    #[cfg(any(
        feature = "ticker",
        feature = "orderbook",
        feature = "funding",
        feature = "candles"
    ))]
    transport: TransportConfig,
}
impl PublicRestClient {
    pub async fn load(exchange: ExchangeId, product: InstrumentKind) -> Result<Self> {
        Self::load_with_transport(exchange, product, TransportConfig::direct()).await
    }
    pub async fn load_with_transport(
        exchange: ExchangeId,
        product: InstrumentKind,
        transport: TransportConfig,
    ) -> Result<Self> {
        let catalog = MarketCatalog::load_with_transport(exchange, product, &transport).await?;
        Ok(Self::from_catalog(catalog, transport))
    }
    pub fn from_catalog(catalog: MarketCatalog, transport: TransportConfig) -> Self {
        #[cfg(not(any(
            feature = "ticker",
            feature = "orderbook",
            feature = "funding",
            feature = "candles"
        )))]
        let _ = transport;
        Self {
            catalog: Arc::new(catalog),
            #[cfg(any(
                feature = "ticker",
                feature = "orderbook",
                feature = "funding",
                feature = "candles"
            ))]
            transport,
        }
    }
    pub fn catalog(&self) -> &MarketCatalog {
        &self.catalog
    }
    /// Implemented public REST methods for this build, distinct from WS channels.
    pub fn supported_channels(&self) -> Vec<Channel> {
        [
            Channel::Ticker,
            Channel::L2Book,
            Channel::Funding,
            Channel::Candles,
        ]
        .into_iter()
        .filter(|channel| self.catalog.supported_channels().contains(channel))
        .filter(|channel| {
            *channel != Channel::Candles
                || self.catalog.exchange() != ExchangeId::Gateio
                || self.catalog.product() != InstrumentKind::Futures
        })
        .collect()
    }
    #[cfg(feature = "candles")]
    pub async fn candle_history(
        &self,
        symbol: &Symbol,
        query: CandleHistoryQuery,
    ) -> Result<CandleHistory> {
        let info = self.catalog.market(symbol)?;
        candles::collect(info, query, |url| async move {
            crate::runtime::snapshot::fetch_json(url.as_str(), &self.transport, info.exchange).await
        })
        .await
    }
    #[cfg(feature = "funding")]
    pub async fn funding_history(
        &self,
        symbol: &Symbol,
        query: FundingHistoryQuery,
    ) -> Result<FundingHistory> {
        let info = self.catalog.market(symbol)?;
        history::collect(info, query, |url| async move {
            crate::runtime::snapshot::fetch_json(url.as_str(), &self.transport, info.exchange).await
        })
        .await
    }

    #[cfg(feature = "ticker")]
    pub async fn ticker(&self, symbol: &Symbol) -> Result<RestSnapshot<cryptofeed_ticker::Ticker>> {
        let info = self.catalog.market(symbol)?;
        let plan = adapter::plan(info, None)?;
        let payload =
            crate::runtime::snapshot::fetch_json(plan.url.as_str(), &self.transport, info.exchange)
                .await?;
        adapter::ticker(info, &plan, &payload, received_time())
    }
    #[cfg(feature = "orderbook")]
    pub async fn l2_book(
        &self,
        symbol: &Symbol,
        depth: u16,
    ) -> Result<RestSnapshot<cryptofeed_orderbook::L2BookSnapshot>> {
        let info = self.catalog.market(symbol)?;
        let plan = adapter::plan(info, Some(depth))?;
        let payload =
            crate::runtime::snapshot::fetch_json(plan.url.as_str(), &self.transport, info.exchange)
                .await?;
        adapter::book(info, &plan, &payload, depth, received_time())
    }
}
#[cfg(any(
    feature = "ticker",
    feature = "orderbook",
    feature = "funding",
    feature = "candles"
))]
fn received_time() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |time| time.as_secs_f64())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cryptofeed_core::symbol::Symbol;
    fn client() -> PublicRestClient {
        let mut registry = crate::markets::SymbolRegistry::default();
        registry
            .insert(Symbol::spot("BTC", "USDT"), "BTCUSDT")
            .unwrap();
        PublicRestClient::from_catalog(
            MarketCatalog::from_registry(ExchangeId::Binance, InstrumentKind::Spot, registry),
            TransportConfig::direct(),
        )
    }
    #[test]
    fn rest_capabilities_are_an_implemented_build_specific_subset() {
        let channels = client().supported_channels();
        assert_eq!(
            channels.contains(&Channel::Ticker),
            cfg!(feature = "ticker")
        );
        assert_eq!(
            channels.contains(&Channel::L2Book),
            cfg!(feature = "orderbook")
        );
        assert!(!channels.contains(&Channel::Trade));
        assert_eq!(
            channels.contains(&Channel::Candles),
            cfg!(feature = "candles")
        );
    }
    #[test]
    fn candle_rest_capabilities_exclude_undocumented_gate_delivery() {
        for exchange in [
            ExchangeId::Binance,
            ExchangeId::Bitget,
            ExchangeId::Bybit,
            ExchangeId::Okx,
            ExchangeId::Gateio,
        ] {
            let catalog = MarketCatalog::from_registry(
                exchange,
                InstrumentKind::Spot,
                crate::markets::SymbolRegistry::default(),
            );
            let client = PublicRestClient::from_catalog(catalog, TransportConfig::direct());
            assert_eq!(
                client.supported_channels().contains(&Channel::Candles),
                cfg!(feature = "candles")
            );
        }
        let catalog = MarketCatalog::from_registry(
            ExchangeId::Gateio,
            InstrumentKind::Futures,
            crate::markets::SymbolRegistry::default(),
        );
        let client = PublicRestClient::from_catalog(catalog, TransportConfig::direct());
        assert!(!client.supported_channels().contains(&Channel::Candles));
    }
    #[cfg(all(feature = "ticker", feature = "orderbook"))]
    #[tokio::test]
    async fn unknown_symbols_and_invalid_depth_fail_before_http() {
        assert!(
            client()
                .ticker(&Symbol::spot("MISSING", "USDT"))
                .await
                .is_err()
        );
        assert!(
            client()
                .l2_book(&Symbol::spot("BTC", "USDT"), 0)
                .await
                .is_err()
        );
    }
}
