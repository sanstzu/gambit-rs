use std::{collections::BTreeMap, fmt, sync::Arc};

use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type RawFields = BTreeMap<String, Value>;

/// A poker action accepted by Gambit's gameplay endpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    Fold,
    Check,
    Call,
    Bet,
    Raise,
    #[serde(rename = "all-in")]
    AllIn,
}

impl Action {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fold => "fold",
            Self::Check => "check",
            Self::Call => "call",
            Self::Bet => "bet",
            Self::Raise => "raise",
            Self::AllIn => "all-in",
        }
    }
}

/// A broad table mode inferred from authoritative state.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum GameMode {
    Bot,
    Live,
    Friends,
    #[default]
    Unknown,
}

impl<'de> Deserialize<'de> for GameMode {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Option::<String>::deserialize(deserializer)?;
        Ok(match value.as_deref() {
            Some("bot") => Self::Bot,
            Some("live") => Self::Live,
            Some("friends") => Self::Friends,
            _ => Self::Unknown,
        })
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct User {
    #[serde(deserialize_with = "deserialize_stringish")]
    pub id: String,
    pub username: String,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub avatar_url: Option<String>,
    #[serde(flatten)]
    pub raw: RawFields,
}

/// An authenticated identity. Its bearer token is redacted from debug output.
#[derive(Clone)]
pub struct AuthSession {
    pub(crate) token: SecretString,
    pub user: User,
    pub expires_in: Option<String>,
    pub is_new_user: bool,
}

impl fmt::Debug for AuthSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthSession")
            .field("token", &"[REDACTED]")
            .field("user", &self.user.username)
            .field("expires_in", &self.expires_in)
            .field("is_new_user", &self.is_new_user)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BetRaiseBounds {
    pub min_to: u64,
    pub max_to: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AllowedActions {
    #[serde(default)]
    pub fold: bool,
    #[serde(default)]
    pub check: bool,
    #[serde(default)]
    pub call: Option<u64>,
    #[serde(default)]
    pub bet_raise: Option<BetRaiseBounds>,
    #[serde(default)]
    pub all_in: Option<u64>,
    #[serde(flatten)]
    pub raw: RawFields,
}

impl AllowedActions {
    #[must_use]
    pub fn permits(&self, action: Action, amount: Option<u64>) -> bool {
        match action {
            Action::Fold => self.fold,
            Action::Check => self.check,
            Action::Call => self.call.is_some(),
            Action::Bet | Action::Raise => self.bet_raise.is_some_and(|bounds| {
                amount.is_some_and(|amount| (bounds.min_to..=bounds.max_to).contains(&amount))
            }),
            Action::AllIn => self.all_in.is_some(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Seat {
    pub seat: u8,
    #[serde(default, deserialize_with = "deserialize_nullable_string")]
    pub name: String,
    #[serde(default)]
    pub stack: u64,
    #[serde(default)]
    pub bet: u64,
    #[serde(default = "unknown_status")]
    pub status: String,
    #[serde(default)]
    pub is_bot: bool,
    #[serde(default)]
    pub is_turn: bool,
    #[serde(default)]
    pub cards: Option<Vec<String>>,
    #[serde(default)]
    pub last_action: Option<Value>,
    #[serde(flatten)]
    pub raw: RawFields,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableSnapshot {
    pub table_id: String,
    #[serde(default)]
    pub hand_id: Option<String>,
    #[serde(default)]
    pub phase: Option<String>,
    #[serde(default)]
    pub street: Option<String>,
    #[serde(default)]
    pub pot: u64,
    #[serde(default)]
    pub board: Vec<String>,
    #[serde(default)]
    pub to_act_seat: Option<u8>,
    #[serde(default)]
    pub turn_ends_at_ms: Option<u64>,
    #[serde(default)]
    pub you_seat: Option<u8>,
    #[serde(default)]
    pub seats: Vec<Seat>,
    #[serde(default)]
    pub allowed: Option<AllowedActions>,
    #[serde(default)]
    pub hand_result: Option<Value>,
    #[serde(default)]
    pub mode: GameMode,
    #[serde(default)]
    pub is_friend_table: bool,
    #[serde(default)]
    pub is_observer: bool,
    #[serde(default)]
    pub hero_position: Option<u8>,
    #[serde(default)]
    pub waiting_for_players: bool,
    #[serde(default)]
    pub seated_humans: Option<u8>,
    #[serde(flatten)]
    pub raw: RawFields,
}

impl TableSnapshot {
    #[must_use]
    pub fn is_our_turn(&self) -> bool {
        self.you_seat.is_some() && self.you_seat == self.to_act_seat
    }

    #[must_use]
    pub fn hero(&self) -> Option<&Seat> {
        self.seats
            .iter()
            .find(|seat| Some(seat.seat) == self.you_seat)
    }

    #[must_use]
    pub fn is_seated(&self) -> bool {
        self.you_seat.is_some() && !self.is_observer && self.hero().is_some()
    }

    #[must_use]
    pub fn available_seats(&self) -> Vec<u8> {
        self.seats
            .iter()
            .filter(|seat| seat.status == "empty")
            .map(|seat| seat.seat)
            .collect()
    }

    pub(crate) fn turn_key(&self) -> TurnKey {
        TurnKey {
            hand_id: self.hand_id.clone(),
            street: self.street.clone(),
            to_act_seat: self.to_act_seat,
            turn_ends_at_ms: self.turn_ends_at_ms,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TurnKey {
    pub hand_id: Option<String>,
    pub street: Option<String>,
    pub to_act_seat: Option<u8>,
    pub turn_ends_at_ms: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct LiveTableMatch {
    pub slug: String,
    pub created: bool,
    /// Zero-based wire position suggested by Gambit.
    pub open_seat: Option<u8>,
    pub raw: Arc<Value>,
}

impl LiveTableMatch {
    #[must_use]
    pub fn suggested_seat(&self) -> Option<u8> {
        self.open_seat.and_then(|seat| seat.checked_add(1))
    }
}

#[derive(Clone, Debug)]
pub struct SeatRejected {
    /// Zero-based position returned on the wire.
    pub position: Option<u8>,
    pub reason: String,
    pub raw: Arc<Value>,
}

impl SeatRejected {
    #[must_use]
    pub fn seat_number(&self) -> Option<u8> {
        self.position.and_then(|seat| seat.checked_add(1))
    }
}

#[derive(Clone, Debug)]
pub struct AfkWarning {
    pub hands_remaining: u64,
    pub raw: Arc<Value>,
}

#[derive(Clone, Debug)]
pub struct LiveBust {
    pub slug: String,
    pub reserved_until: u64,
    pub raw: Arc<Value>,
}

#[derive(Clone, Debug)]
pub struct LiveRejoinResult {
    pub slug: String,
    pub same_table: bool,
    pub raw: Arc<Value>,
}

fn unknown_status() -> String {
    "unknown".to_owned()
}

fn deserialize_nullable_string<'de, D>(deserializer: D) -> std::result::Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)?.unwrap_or_default())
}

fn deserialize_stringish<'de, D>(deserializer: D) -> std::result::Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::String(value) => Ok(value),
        Value::Number(value) => Ok(value.to_string()),
        _ => Err(serde::de::Error::custom("expected a string or number")),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn snapshot_preserves_unknown_fields_and_checks_bounds() {
        let snapshot: TableSnapshot = serde_json::from_value(json!({
            "tableId": "default",
            "handId": "hand-1",
            "street": "preflop",
            "toActSeat": 1,
            "youSeat": 1,
            "seats": [{"seat": 1, "name": "tester", "status": "active"}],
            "allowed": {
                "fold": true,
                "call": 2,
                "betRaise": {"minTo": 4, "maxTo": 200},
                "allIn": 200
            },
            "futureField": {"preserved": true}
        }))
        .unwrap();

        assert!(snapshot.is_our_turn());
        assert!(snapshot.is_seated());
        let allowed = snapshot.allowed.unwrap();
        assert!(allowed.permits(Action::Raise, Some(4)));
        assert!(!allowed.permits(Action::Raise, Some(3)));
        assert_eq!(snapshot.raw["futureField"]["preserved"], true);
    }
}
