//! Synthetic failure paths; these are not captured issue #6 traffic.
use super::*;
use crate::feed::{FailureDiagnostic, FeedIdentity, FeedState};
use serde_json::json;

#[tokio::test]
async fn bitget_gap_and_crossed_book_withdraw_then_recover_in_fresh_epoch() {
    for crossed in [false, true] {
        let mut handler = FeedHandler::new();
        let books = handler.l2_book_handle();
        let id = handler.add_feed_with_id(
            crate::exchange::bitget::Bitget::new()
                .l2_book()
                .symbol("BTC-USDT-PERP")
                .l2_book_cross_check(true)
                .build(),
        );
        let (mut feeds, context, _commands) = handler.into_control_parts();
        let mut feed = feeds.pop().unwrap();
        let identity = FeedIdentity { id, generation: 1 };
        let monitor = context.monitor(identity, ExchangeId::Bitget);
        monitor.configure(1, vec!["BTC-USDT-PERP".into()]);
        monitor.lifecycle(FeedState::Started);
        feed.monitor = Some(monitor.clone());
        readiness::track(
            &mut feed,
            BitgetAdapter::websocket_url(),
            vec!["BTC-USDT-PERP".into()],
        );
        let mut attempt = readiness::begin(&mut feed).unwrap();
        attempt.connected();
        let messages =
            attempt.prepare_messages([BitgetAdapter::subscription_message(&feed)].into());
        attempt.sent(&messages[0], tokio::time::Instant::now());
        let arg = json!({"instType":"usdt-futures","topic":"books","symbol":"BTCUSDT"});
        attempt
            .control(&json!({"event":"subscribe","arg":arg}))
            .unwrap();
        let snapshot = json!({"arg":arg,"action":"snapshot","data":[{"b":[["100","1"]],"a":[["101","1"]],"seq":100,"pseq":0,"ts":"1746698732562"}]});
        process_bitget_text_message(&feed, &snapshot.to_string(), 1.0)
            .await
            .unwrap();
        assert!(monitor.snapshot().is_ready());
        let symbol = "BTC-USDT-PERP".parse().unwrap();
        assert!(books.recover(identity, &symbol).snapshot.is_some());
        let update = json!({"arg":arg,"action":"update","data":[{"b":[[if crossed {"102"} else {"100"},"1"]],"a":[],"seq":102,"pseq":if crossed {100} else {999},"ts":"1746698732563"}]});
        let error = process_bitget_text_message(&feed, &update.to_string(), 2.0)
            .await
            .unwrap_err();
        assert!(!supervisor::is_permanent(&error));
        attempt.failed(&error, "session");
        let state = monitor.snapshot();
        let diagnostic = state.connections[0].last_failure.unwrap();
        assert_eq!(
            diagnostic.reason,
            if crossed {
                "crossed_book"
            } else {
                "book_sequence_gap"
            }
        );
        assert!(!state.is_ready());
        assert_eq!(state.books_synchronized, 0);
        assert!(books.recover(identity, &symbol).snapshot.is_none());
        // The production session resets books before reconnecting.
        reset_connection_books(&feed, true, &feed.symbols);
        let mut next = readiness::begin(&mut feed).unwrap();
        next.connected();
        let messages = next.prepare_messages([BitgetAdapter::subscription_message(&feed)].into());
        next.sent(&messages[0], tokio::time::Instant::now());
        next.control(&json!({"event":"subscribe","arg":arg}))
            .unwrap();
        process_bitget_text_message(&feed, &snapshot.to_string(), 3.0)
            .await
            .unwrap();
        // A late result from an older epoch must not overwrite diagnostics/readiness.
        attempt.failed(&Error::Subscription("SECRET".into()), "session");
        assert!(monitor.snapshot().is_ready());
        assert!(monitor.snapshot().connections[0].last_failure.is_none());
        assert_eq!(
            books
                .recover(identity, &symbol)
                .snapshot
                .unwrap()
                .anchor
                .epoch,
            2
        );
        next.disconnected(None);
        assert!(books.recover(identity, &symbol).snapshot.is_none());
    }
}

#[tokio::test]
async fn bitget_rejection_is_permanent_while_gap_and_crossed_book_retry() {
    for error in [
        Error::Subscription("SECRET".into()),
        Error::Parse("bitget book sequence gap detected".into()),
        Error::MalformedData("crossed L2 book: BTC-USDT-PERP".into()),
    ] {
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let (_stop, shutdown) = watch::channel(false);
        let result = supervisor::retry_with_backoff_until_shutdown(
            Some(1),
            supervisor::Backoff::new(0, 0),
            shutdown,
            || async {
                if calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    Err(error.clone())
                } else {
                    Ok(())
                }
            },
        )
        .await;
        if FailureDiagnostic::from(&error).permanent {
            assert!(matches!(result, Err(Error::Subscription(_))));
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        } else {
            assert!(matches!(result, Ok(Some(()))));
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        }
    }
}
