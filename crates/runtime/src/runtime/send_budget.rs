//! Per-session admission. Counts SDK starts, not remote receipt times.
use cryptofeed_core::{
    error::{Error, Result},
    exchange::ExchangeId,
};
use std::{collections::VecDeque, time::Duration};
use tokio::time::Instant;

pub(super) fn is_control(text: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(text)
        .ok()
        .and_then(|value| {
            value
                .get("op")
                .and_then(|op| op.as_str())
                .map(|op| matches!(op, "subscribe" | "unsubscribe" | "login"))
        })
        .unwrap_or(false)
}

pub(super) struct SendBudget {
    next_text: Instant,
    frame_limit: Option<usize>,
    frames: VecDeque<Instant>,
    control_limit: Option<usize>,
    controls: VecDeque<Instant>,
}
impl SendBudget {
    #[cfg(test)]
    pub(super) fn control_count_for_test(&self) -> usize {
        self.controls.len()
    }
    pub(super) fn new(exchange: Option<ExchangeId>) -> Self {
        Self {
            next_text: Instant::now(),
            // Use Spot's stricter five-frame limit for all Binance products.
            frame_limit: match exchange {
                Some(ExchangeId::Binance) => Some(5),
                Some(ExchangeId::Bitget) => Some(10),
                _ => None,
            },
            frames: VecDeque::new(),
            control_limit: match exchange {
                Some(ExchangeId::Okx) => Some(480),
                Some(ExchangeId::Bitget) => Some(240),
                _ => None,
            },
            controls: VecDeque::new(),
        }
    }
    fn prune(&mut self, now: Instant) {
        while self
            .frames
            .front()
            .is_some_and(|at| *at + Duration::from_secs(1) <= now)
        {
            self.frames.pop_front();
        }
        while self
            .controls
            .front()
            .is_some_and(|at| *at + Duration::from_secs(3600) <= now)
        {
            self.controls.pop_front();
        }
    }
    pub(super) fn text_ready(&mut self, now: Instant, control: bool) -> Instant {
        self.prune(now);
        let mut deadline = self.next_text.max(now);
        // Leave one immediate Pong slot. Multiple server pings consume it and
        // dynamically postpone text until enough actual starts expire.
        if let Some(limit) = self.frame_limit {
            if self.frames.len() >= limit - 1 {
                deadline = deadline
                    .max(self.frames[self.frames.len() - (limit - 1)] + Duration::from_secs(1));
            }
        }
        if let Some(limit) = self.control_limit.filter(|_| control) {
            if self.controls.len() >= limit {
                deadline = deadline
                    .max(self.controls[self.controls.len() - limit] + Duration::from_secs(3600));
            }
        }
        deadline
    }
    pub(super) fn text_started(&mut self, now: Instant, control: bool) {
        self.next_text = now + Duration::from_millis(250);
        if self.frame_limit.is_some() {
            self.frames.push_back(now);
        }
        if control && self.control_limit.is_some() {
            self.controls.push_back(now);
        }
    }
    pub(super) fn pong_started(&mut self, now: Instant) -> Result<()> {
        self.prune(now);
        if let Some(limit) = self.frame_limit {
            if self.frames.len() >= limit {
                // Do not poll/flush the stream again: tungstenite has queued an
                // automatic Pong. Returning an error lets the caller drop it.
                return Err(Error::Transport(
                    "server ping burst exceeds websocket send budget".into(),
                ));
            }
            self.frames.push_back(now);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn pongs_dynamically_reduce_text_capacity_and_exhaustion_never_reserves_extra() {
        let start = Instant::now();
        let mut budget = SendBudget::new(Some(ExchangeId::Binance));
        let mut writes = Vec::new();
        for index in 0..4 {
            let now = start + Duration::from_millis(index * 250);
            assert_eq!(budget.text_ready(now, false), now);
            budget.text_started(now, false);
            writes.push(now);
        }
        let now = start + Duration::from_millis(800);
        budget.pong_started(now).unwrap();
        writes.push(now);
        assert!(budget.pong_started(now).is_err());
        assert_eq!(budget.frames.len(), 5);
        assert_eq!(
            budget.text_ready(now, false),
            start + Duration::from_millis(1250)
        );
        let later = start + Duration::from_millis(1250);
        assert_eq!(budget.text_ready(later, false), later);
        budget.text_started(later, false);
        writes.push(later);
        for at in &writes {
            assert!(
                writes
                    .iter()
                    .filter(|other| **other >= *at && **other < *at + Duration::from_secs(1))
                    .count()
                    <= 5
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn hourly_limits_are_connection_local_and_heartbeats_do_not_consume_them() {
        for (exchange, limit) in [(ExchangeId::Okx, 480), (ExchangeId::Bitget, 240)] {
            let start = Instant::now();
            let mut budget = SendBudget::new(Some(exchange));
            for index in 0..limit {
                let now = start + Duration::from_secs(index);
                assert_eq!(budget.text_ready(now, true), now);
                budget.text_started(now, true);
            }
            let now = start + Duration::from_secs(limit);
            assert_eq!(
                budget.text_ready(now, true),
                start + Duration::from_secs(3600)
            );
            assert_eq!(budget.text_ready(now, false), now);
            budget.text_started(now, false);
            assert_eq!(budget.controls.len(), limit as usize);
            assert_eq!(
                budget.text_ready(start + Duration::from_secs(3600), true),
                start + Duration::from_secs(3600)
            );
            assert_eq!(budget.controls.len(), limit as usize - 1);
            assert_eq!(SendBudget::new(Some(exchange)).text_ready(now, true), now);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn later_time_does_not_accumulate_burst_credit_and_queries_do_not_charge() {
        let mut budget = SendBudget::new(None);
        let now = Instant::now();
        for _ in 0..20 {
            assert_eq!(budget.text_ready(now, true), now);
        }
        assert!(budget.frames.is_empty() && budget.controls.is_empty());
        let later = now + Duration::from_secs(3600);
        budget.text_started(later, false);
        assert_eq!(
            budget.text_ready(later, false),
            later + Duration::from_millis(250)
        );
        assert_eq!(
            budget.text_ready(later + Duration::from_millis(250), false),
            later + Duration::from_millis(250)
        );
    }

    #[test]
    fn only_exact_public_control_operations_consume_hourly_requests() {
        for op in ["subscribe", "unsubscribe", "login"] {
            assert!(is_control(&format!(r#"{{"op":"{op}","args":[]}}"#)));
        }
        for text in [
            "ping",
            r#"{"op":"ping"}"#,
            r#"{"op":"Subscribe"}"#,
            r#"{"op":1}"#,
            "invalid",
            "[]",
        ] {
            assert!(!is_control(text));
        }
    }
}
