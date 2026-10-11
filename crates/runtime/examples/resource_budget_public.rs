use cryptofeed_rs::prelude::*;
use std::time::Duration;
fn feed(symbol: &str, transport: &TransportConfig) -> ExchangeFeed {
    Okx::new()
        .trade()
        .symbol(symbol)
        .exchange_symbol(symbol)
        .transport(transport.clone())
        .build()
}
async fn event(
    events: &mut tokio::sync::broadcast::Receiver<FeedEnvelope>,
    id: FeedIdentity,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let envelope = events.recv().await?;
        if envelope.identity == id && matches!(envelope.event, FeedEvent::Trade(_)) {
            return Ok(());
        }
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let policy = ResourcePolicy::default()
        .http(1, Duration::from_secs(2))?
        .websocket(1, Duration::from_secs(2))?;
    let budget = ResourceBudget::new(policy);
    let transport = TransportConfig::direct().with_resource_budget(budget.clone());
    let mut handler = FeedHandler::new();
    let mut events = handler.subscribe_identified();
    let control = handler.control_handle();
    let runtime = tokio::spawn(handler.run());
    let result=tokio::time::timeout(Duration::from_secs(90),async {
  let first=control.add_feed(feed("BTC-USDT",&transport)).await?;event(&mut events,first).await?;
  if control.add_feed(feed("ETH-USDT",&transport)).await.is_ok() {return Err("shared WS capacity was not enforced".into());}
  if control.feeds().await?.len()!=1 {return Err("failed add disturbed healthy feed".into());}
  let next=control.replace_feed(first.id,feed("ETH-USDT",&transport)).await?;event(&mut events,next).await?;control.remove_feed(next.id).await?;
  let reused=control.add_feed(feed("BTC-USDT",&transport)).await?;event(&mut events,reused).await?;control.remove_feed(reused.id).await?;
  let client=PublicRestClient::load_with_transport(ExchangeId::Okx,InstrumentKind::Spot,transport.clone()).await?;
  let symbol=Symbol::spot("BTC","USDT");client.ticker(&symbol).await?;client.l2_book(&symbol,5).await?;
  println!("resource budget shared=true rejected_add=true replaced_same_id={} live_slot_reused=true rest_queries=3",next.id==first.id);
  Ok::<(),Box<dyn std::error::Error>>(())
 }).await;
    let stopped = control.shutdown().await;
    let finished = runtime.await?;
    stopped?;
    finished?;
    result??;
    Ok(())
}
