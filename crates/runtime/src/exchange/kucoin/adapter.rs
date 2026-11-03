//! KuCoin UTA WebSocket V2 routing and subscription framing.
//!
//! Implemented in the phase-1 increment described in
//! `docs/cex-expansion-plan-2026-11-03.md`; capability preflight keeps KuCoin
//! feeds unreachable until the adapter, parsers and book sync land together.
pub struct KucoinAdapter;
