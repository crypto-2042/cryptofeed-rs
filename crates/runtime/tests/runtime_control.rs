use cryptofeed_rs::prelude::*;

#[tokio::test]
async fn empty_controlled_runtime_can_be_stopped_without_signals() {
    let mut handler = FeedHandler::new();
    let control = handler.control_handle();
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let running = tokio::spawn(handler.run_with_shutdown(rx));
    control.shutdown().await.unwrap();
    running.await.unwrap().unwrap();
    assert!(control.shutdown().await.is_err());
}

#[tokio::test]
async fn rejected_dynamic_feed_does_not_close_the_control_plane() {
    let mut handler = FeedHandler::new();
    let control = handler.control_handle();
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let running = tokio::spawn(handler.run_with_shutdown(rx));
    assert!(control.add_feed(Binance::new().build()).await.is_err());
    control.shutdown().await.unwrap();
    running.await.unwrap().unwrap();
}

#[test]
fn initial_feed_ids_are_unique_and_can_be_retained() {
    let mut handler = FeedHandler::new();
    let first = handler.add_feed_with_id(Binance::new().build());
    let second = handler.add_feed_with_id(Binance::new().build());
    assert_ne!(first, second);
    assert_eq!(handler.feed_count(), 2);
}
