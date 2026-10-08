use serde::{Deserialize, Serialize};

/// The aggressor side of a trade or liquidation. Shared by the trade and
/// liquidation models so both normalize to the same vocabulary; serializes
/// lowercase (`"buy"`/`"sell"`).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}
