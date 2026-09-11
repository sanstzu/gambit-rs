use std::{collections::BTreeMap, fmt, sync::Arc};

use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type RawFields = BTreeMap<String, Value>;

/// Number of one-based table slots in the observed protocol.
pub const MAX_SEATS: u8 = 6;

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
    /// A null or absent wire value means the minimum is unknown.
    /// No normal bet or raise is permitted locally when the minimum is unknown.
    #[serde(default)]
    pub min_to: Option<u64>,
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
                bounds.min_to.is_some_and(|minimum| {
                    amount.is_some_and(|amount| (minimum..=bounds.max_to).contains(&amount))
                })
            }),
            Action::AllIn => self.all_in.is_some(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(clippy::struct_excessive_bools)] // Independent flags mirror the wire schema.
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
    pub last_action: Option<LastAction>,
    #[serde(default)]
    pub is_dealer: bool,
    #[serde(default, rename = "isSB")]
    pub is_small_blind: bool,
    #[serde(default, rename = "isBB")]
    pub is_big_blind: bool,
    #[serde(default, deserialize_with = "deserialize_optional_stringish")]
    pub user_id: Option<String>,
    #[serde(default)]
    pub rating: Option<f64>,
    #[serde(default)]
    pub tournament_points: Option<f64>,
    #[serde(default)]
    pub avatar_url: Option<String>,
    #[serde(default)]
    pub is_dealt_out: bool,
    #[serde(default)]
    pub check_raised_this_street: bool,
    #[serde(flatten)]
    pub raw: RawFields,
}

impl Seat {
    #[must_use]
    pub fn is_open(&self) -> bool {
        self.status == "empty"
    }

    #[must_use]
    pub fn is_folded(&self) -> bool {
        self.status == "folded"
    }

    #[must_use]
    pub fn is_all_in(&self) -> bool {
        matches!(self.status.as_str(), "allin" | "all-in")
    }
}

/// The last explicit action. Unknown action kinds retain their wire string.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LastAction {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub amount: Option<u64>,
    #[serde(default)]
    pub to: Option<u64>,
    #[serde(flatten)]
    pub raw: RawFields,
}

impl LastAction {
    /// Target total when supplied, otherwise the observed amount.
    #[must_use]
    pub fn target_total(&self) -> Option<u64> {
        self.to.or(self.amount)
    }

    /// Calls use chips called; bets and raises use the target total.
    #[must_use]
    pub fn display_amount(&self) -> Option<u64> {
        match self.kind.as_str() {
            "call" => self.amount,
            "bet" | "raise" | "betraise" | "allin" | "all-in" => self.target_total(),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SeatEquity {
    pub seat: u8,
    pub equity: f64,
    #[serde(flatten)]
    pub raw: RawFields,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BlindLevel {
    pub small_blind: u64,
    pub big_blind: u64,
    #[serde(flatten)]
    pub raw: RawFields,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeroDecision {
    pub equity: f64,
    pub ev_fold: f64,
    pub ev_call: f64,
    #[serde(default)]
    pub ev_raise: Option<f64>,
    pub best_action: String,
    #[serde(default)]
    pub best_amount: Option<u64>,
    #[serde(flatten)]
    pub raw: RawFields,
}

/// Observed completion data, not a stable published server contract.
/// Unknown variants and malformed known results retain the original JSON.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum HandResult {
    FoldWin(FoldWinResult),
    Showdown(ShowdownResult),
    Unknown(Value),
}

impl<'de> Deserialize<'de> for HandResult {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let parsed = match value.get("type").and_then(Value::as_str) {
            Some("foldWin") => serde_json::from_value(value.clone()).map(Self::FoldWin),
            Some("showdown") => serde_json::from_value(value.clone()).map(Self::Showdown),
            _ => return Ok(Self::Unknown(value)),
        };
        Ok(parsed.unwrap_or(Self::Unknown(value)))
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FoldWinResult {
    pub winner_seat: u8,
    /// Gross pot awarded, not net stack change.
    pub pot: u64,
    /// Net stack changes indexed by one-based seat minus one.
    #[serde(default)]
    pub chips_won_by_seat: Vec<i64>,
    #[serde(flatten)]
    pub raw: RawFields,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShowdownResult {
    /// Main pot first, followed by side pots in wire order.
    #[serde(default)]
    pub pots: Vec<PotResult>,
    #[serde(default)]
    pub payouts: Vec<Payout>,
    /// Returned uncalled chips, not winnings.
    #[serde(default)]
    pub refunds: Vec<Refund>,
    #[serde(default)]
    pub hands: Vec<EvaluatedHand>,
    #[serde(default)]
    pub board: Vec<String>,
    #[serde(default)]
    pub pot: u64,
    #[serde(default)]
    pub winner_seats: Vec<u8>,
    #[serde(default)]
    pub river_call_occurred: bool,
    #[serde(default)]
    pub all_in_runout: bool,
    /// Net stack changes indexed by one-based seat minus one.
    #[serde(default)]
    pub chips_won_by_seat: Vec<i64>,
    #[serde(flatten)]
    pub raw: RawFields,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PotResult {
    pub amount: u64,
    #[serde(default)]
    pub eligible_seats: Vec<u8>,
    #[serde(flatten)]
    pub raw: RawFields,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Payout {
    pub seat: u8,
    /// Gross chips awarded from this pot, excluding refunds.
    pub amount: u64,
    /// Source pot index when provided. Never inferred from the pot amount.
    #[serde(default)]
    pub pot_index: Option<usize>,
    /// Observed source-pot amount; production semantics are not yet verified.
    #[serde(default)]
    pub pot: Option<u64>,
    #[serde(flatten)]
    pub raw: RawFields,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Refund {
    pub seat: u8,
    pub amount: u64,
    /// Observed source-pot amount; production semantics are not yet verified.
    #[serde(default)]
    pub pot: Option<u64>,
    #[serde(flatten)]
    pub raw: RawFields,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EvaluatedHand {
    pub seat: u8,
    #[serde(default, deserialize_with = "deserialize_nullable_string")]
    pub name: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub cards: Vec<String>,
    #[serde(flatten)]
    pub raw: RawFields,
}

impl HandResult {
    /// Declared winners plus payout recipients, deduplicated in observed order.
    #[must_use]
    pub fn winner_seats(&self) -> Vec<u8> {
        match self {
            Self::FoldWin(result) => vec![result.winner_seat],
            Self::Showdown(result) => {
                let mut seats = Vec::new();
                for seat in result
                    .winner_seats
                    .iter()
                    .copied()
                    .chain(result.payouts.iter().map(|payout| payout.seat))
                {
                    if !seats.contains(&seat) {
                        seats.push(seat);
                    }
                }
                seats
            }
            Self::Unknown(_) => Vec::new(),
        }
    }

    /// Net stack change, or `None` for an invalid seat or missing data.
    #[must_use]
    pub fn net_chips_for(&self, seat: u8) -> Option<i64> {
        if !(1..=MAX_SEATS).contains(&seat) {
            return None;
        }
        let chips = match self {
            Self::FoldWin(result) => &result.chips_won_by_seat,
            Self::Showdown(result) => &result.chips_won_by_seat,
            Self::Unknown(_) => return None,
        };
        chips.get(usize::from(seat - 1)).copied()
    }

    /// Sum of explicit gross awards, excluding refunds. Returns `None` when
    /// no award is recorded, the seat is invalid, or the sum overflows.
    /// Missing awards are never inferred from winners or divided from a pot.
    #[must_use]
    pub fn gross_payout_for(&self, seat: u8) -> Option<u64> {
        if !(1..=MAX_SEATS).contains(&seat) {
            return None;
        }
        match self {
            Self::FoldWin(result) => (seat == result.winner_seat).then_some(result.pot),
            Self::Showdown(result) => {
                let mut payouts = result.payouts_for_seat(seat);
                let first = payouts.next()?.amount;
                payouts.try_fold(first, |sum, payout| sum.checked_add(payout.amount))
            }
            Self::Unknown(_) => None,
        }
    }

    #[must_use]
    pub fn is_all_in_runout(&self) -> bool {
        matches!(self, Self::Showdown(result) if result.all_in_runout)
    }
}

impl ShowdownResult {
    pub fn payouts_for_pot(&self, pot_index: usize) -> impl Iterator<Item = &Payout> {
        self.payouts
            .iter()
            .filter(move |payout| payout.pot_index == Some(pot_index))
    }

    pub fn payouts_for_seat(&self, seat: u8) -> impl Iterator<Item = &Payout> {
        self.payouts
            .iter()
            .filter(move |payout| payout.seat == seat)
    }

    /// Whether explicit awards for this pot identify distinct recipients.
    /// A false result does not prove that omitted payout data was unsplit.
    #[must_use]
    pub fn is_split_pot(&self, pot_index: usize) -> bool {
        let mut payouts = self.payouts_for_pot(pot_index);
        payouts
            .next()
            .is_some_and(|first| payouts.any(|payout| payout.seat != first.seat))
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(clippy::struct_excessive_bools)] // Independent flags mirror the wire schema.
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
    pub hand_result: Option<HandResult>,
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
    #[serde(default)]
    pub hero_equity: Option<f64>,
    #[serde(default)]
    pub hero_decision: Option<HeroDecision>,
    #[serde(default)]
    pub seat_equities: Option<Vec<SeatEquity>>,
    #[serde(default)]
    pub skip_hand_paused: bool,
    #[serde(default)]
    pub unrated_hand: bool,
    #[serde(default)]
    pub unrated_practice_pending: bool,
    #[serde(default)]
    pub hero_is_dealt_out: bool,
    #[serde(default)]
    pub blind_level: Option<BlindLevel>,
    #[serde(default)]
    pub auto_all_in: bool,
    #[serde(default)]
    pub auto_check_raise: bool,
    #[serde(default)]
    pub is_debug: bool,
    #[serde(flatten)]
    pub raw: RawFields,
}

impl TableSnapshot {
    #[must_use]
    pub fn seat(&self, number: u8) -> Option<&Seat> {
        self.seats.iter().find(|seat| seat.seat == number)
    }

    #[must_use]
    pub fn next_seat_number(number: u8) -> Option<u8> {
        (1..=MAX_SEATS)
            .contains(&number)
            .then_some(number % MAX_SEATS + 1)
    }

    /// Visits every other visual slot, including absent or empty seats.
    /// This is clockwise display order, not poker action order.
    #[must_use]
    pub fn clockwise_seat_numbers_after(number: u8) -> Option<impl ExactSizeIterator<Item = u8>> {
        (1..=MAX_SEATS)
            .contains(&number)
            .then(|| (1..MAX_SEATS).map(move |offset| (number - 1 + offset) % MAX_SEATS + 1))
    }

    #[must_use]
    pub fn is_hand_complete(&self) -> bool {
        self.phase.as_deref() == Some("handComplete") && self.hand_result.is_some()
    }

    /// Whether this seat is identified as a winner of the completed hand.
    ///
    /// Includes fold wins and recipients of main, side, or split pots. Refunds
    /// and net stack changes do not identify winners. Returns false before hand
    /// completion, for invalid seats, or when no known result identifies a win.
    /// False does not prove a loss when result data is missing or unknown.
    #[must_use]
    pub fn is_winner(&self, seat: u8) -> bool {
        if !(1..=MAX_SEATS).contains(&seat) || !self.is_hand_complete() {
            return false;
        }
        match self.hand_result.as_ref() {
            Some(HandResult::FoldWin(result)) => result.winner_seat == seat,
            Some(HandResult::Showdown(result)) => {
                result.winner_seats.contains(&seat)
                    || result.payouts.iter().any(|payout| payout.seat == seat)
            }
            _ => false,
        }
    }

    #[must_use]
    pub fn is_all_in_runout(&self) -> bool {
        self.phase.as_deref() == Some("runout")
            || self
                .hand_result
                .as_ref()
                .is_some_and(HandResult::is_all_in_runout)
    }

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

fn deserialize_optional_stringish<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    match value {
        None => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        Some(Value::Number(value)) => Ok(Some(value.to_string())),
        _ => Err(serde::de::Error::custom(
            "expected a string, number, or null",
        )),
    }
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
    #[test]
    fn occupied_seat_promotes_metadata_and_preserves_extensions() {
        let seat: Seat = serde_json::from_value(json!({
            "seat": 1, "name": "tester", "stack": 160, "bet": 40,
            "status": "active", "isBot": true, "isTurn": true,
            "isDealer": true, "isSB": true, "isBB": false,
            "cards": ["7d", "Ad"], "userId": 123,
            "rating": 1400.5, "tournamentPoints": 2.5, "avatarUrl": "avatar",
            "isDealtOut": true, "checkRaisedThisStreet": true,
            "lastAction": {"type": "raise", "amount": 20, "to": 40, "future": 1},
            "future": {"kept": true}
        }))
        .unwrap();
        assert!(seat.is_dealer && seat.is_small_blind && !seat.is_big_blind);
        assert!(seat.is_bot && seat.is_turn && seat.is_dealt_out && seat.check_raised_this_street);
        assert_eq!(seat.user_id.as_deref(), Some("123"));
        assert_eq!(seat.rating, Some(1400.5));
        assert_eq!(seat.tournament_points, Some(2.5));
        assert_eq!(seat.avatar_url.as_deref(), Some("avatar"));
        assert!(!seat.is_open() && !seat.is_folded() && !seat.is_all_in());
        assert_eq!(seat.raw["future"]["kept"], true);
        assert!(!seat.raw.contains_key("isDealer"));
        let action = seat.last_action.unwrap();
        assert_eq!(action.display_amount(), Some(40));
        assert_eq!(action.raw["future"], 1);
    }

    #[test]
    fn empty_seat_defaults_and_nullable_user_ids() {
        let seat: Seat =
            serde_json::from_value(json!({"seat": 6, "name": null, "status": "empty"})).unwrap();
        assert!(seat.is_open());
        assert!(seat.name.is_empty());
        assert!(!seat.is_dealer && !seat.is_small_blind && !seat.is_big_blind);
        assert!(seat.last_action.is_none() && seat.cards.is_none() && seat.user_id.is_none());
        for (wire, expected) in [
            (json!(null), None),
            (json!("123"), Some("123")),
            (json!(123), Some("123")),
        ] {
            let seat: Seat = serde_json::from_value(json!({"seat": 1, "userId": wire})).unwrap();
            assert_eq!(seat.user_id.as_deref(), expected);
        }
        let folded: Seat = serde_json::from_value(json!({"seat": 2, "status": "folded"})).unwrap();
        assert!(folded.is_folded());
        assert!(folded.last_action.is_none());
        for status in ["allin", "all-in"] {
            let seat: Seat = serde_json::from_value(json!({"seat": 2, "status": status})).unwrap();
            assert!(seat.is_all_in());
        }
    }

    #[test]
    fn last_actions_keep_kind_and_amount_semantics() {
        for kind in [
            "fold",
            "check",
            "call",
            "bet",
            "raise",
            "betraise",
            "allin",
            "all-in",
            "blind",
            "action",
            "future-action",
        ] {
            let action: LastAction =
                serde_json::from_value(json!({"type": kind, "amount": 40, "to": 120})).unwrap();
            assert_eq!(action.kind, kind);
            assert_eq!(action.target_total(), Some(120));
            let expected = match kind {
                "call" => Some(40),
                "bet" | "raise" | "betraise" | "allin" | "all-in" => Some(120),
                _ => None,
            };
            assert_eq!(action.display_amount(), expected, "{kind}");
        }
        for kind in ["bet", "raise", "betraise", "allin", "all-in"] {
            let action: LastAction =
                serde_json::from_value(json!({"type": kind, "amount": 40})).unwrap();
            assert_eq!(action.display_amount(), Some(40));
        }
        let call: LastAction = serde_json::from_value(json!({"type": "call", "to": 120})).unwrap();
        assert_eq!(call.display_amount(), None);
        let unknown: TableSnapshot = serde_json::from_value(
            json!({"tableId": "t", "seats": [{"seat": 1, "lastAction": {"type": "future"}}]}),
        )
        .unwrap();
        assert_eq!(
            unknown.seat(1).unwrap().last_action.as_ref().unwrap().kind,
            "future"
        );
    }

    #[test]
    fn snapshot_with_unknown_raise_minimum_never_guesses_a_legal_raise() {
        for bounds in [json!({"minTo": null, "maxTo": 200}), json!({"maxTo": 200})] {
            let snapshot: TableSnapshot = serde_json::from_value(json!({
                "tableId": "default",
                "allowed": {"betRaise": bounds, "allIn": 200}
            }))
            .unwrap();
            let allowed = snapshot.allowed.unwrap();
            assert_eq!(
                allowed.bet_raise,
                Some(BetRaiseBounds {
                    min_to: None,
                    max_to: 200,
                })
            );
            for action in [Action::Bet, Action::Raise] {
                for amount in [None, Some(0), Some(100), Some(200), Some(201)] {
                    assert!(!allowed.permits(action, amount));
                }
            }
            assert!(allowed.permits(Action::AllIn, None));
        }
    }

    #[test]
    fn snapshot_with_known_raise_minimum_checks_inclusive_bounds() {
        let snapshot: TableSnapshot = serde_json::from_value(json!({
            "tableId": "default",
            "allowed": {"betRaise": {"minTo": 4, "maxTo": 200}}
        }))
        .unwrap();
        let allowed = snapshot.allowed.unwrap();
        assert_eq!(
            allowed.bet_raise,
            Some(BetRaiseBounds {
                min_to: Some(4),
                max_to: 200,
            })
        );
        for action in [Action::Bet, Action::Raise] {
            for amount in [4, 100, 200] {
                assert!(allowed.permits(action, Some(amount)));
            }
            for amount in [None, Some(3), Some(201)] {
                assert!(!allowed.permits(action, amount));
            }
        }
    }

    #[test]
    fn fold_win_separates_gross_and_net() {
        let result: HandResult = serde_json::from_value(json!({
            "type": "foldWin", "winnerSeat": 2, "pot": 300,
            "chipsWonBySeat": [-100, 200], "lastFolder": 1, "participants": [1, 2]
        }))
        .unwrap();
        assert_eq!(result.winner_seats(), vec![2]);
        assert_eq!(result.net_chips_for(1), Some(-100));
        assert_eq!(result.net_chips_for(2), Some(200));
        assert_eq!(result.net_chips_for(3), None);
        assert_eq!(result.gross_payout_for(2), Some(300));
        assert_eq!(result.gross_payout_for(1), None);
        assert!(!result.is_all_in_runout());
        let HandResult::FoldWin(fold) = result else {
            panic!("expected fold win")
        };
        assert_eq!(fold.raw["lastFolder"], 1);
        assert_eq!(fold.raw["participants"], json!([1, 2]));
    }

    fn side_pot_result(split: bool) -> HandResult {
        let (payouts, winners, chips) = if split {
            (
                json!([
                    {"seat": 1, "amount": 150, "potIndex": 0},
                    {"seat": 2, "amount": 150, "potIndex": 0},
                    {"seat": 3, "amount": 400, "potIndex": 1}
                ]),
                json!([1, 2, 3]),
                json!([50, -150, 100]),
            )
        } else {
            (
                json!([
                    {"seat": 1, "amount": 300, "potIndex": 0},
                    {"seat": 2, "amount": 400, "potIndex": 1}
                ]),
                json!([1, 2]),
                json!([200, 100, -300]),
            )
        };
        serde_json::from_value(json!({
            "type": "showdown", "pot": 700,
            "pots": [{"amount": 300, "eligibleSeats": [1, 2, 3]}, {"amount": 400, "eligibleSeats": [2, 3]}],
            "payouts": payouts, "winnerSeats": winners, "chipsWonBySeat": chips
        })).unwrap()
    }

    fn assert_complete_result_invariants(result: &ShowdownResult) {
        assert_eq!(
            result.payouts.iter().map(|p| p.amount).sum::<u64>()
                + result.refunds.iter().map(|r| r.amount).sum::<u64>(),
            result.pots.iter().map(|p| p.amount).sum::<u64>()
        );
        for payout in &result.payouts {
            assert!((1..=MAX_SEATS).contains(&payout.seat));
            let pot = &result.pots[payout.pot_index.unwrap()];
            assert!(pot.eligible_seats.contains(&payout.seat));
            assert!(result.winner_seats.contains(&payout.seat));
        }
        assert!(result.chips_won_by_seat.len() <= usize::from(MAX_SEATS));
    }

    #[test]
    fn main_and_side_pots_keep_award_identity() {
        let result = side_pot_result(false);
        assert_eq!(result.gross_payout_for(1), Some(300));
        assert_eq!(result.gross_payout_for(2), Some(400));
        let HandResult::Showdown(showdown) = result else {
            panic!("expected showdown")
        };
        assert_eq!(showdown.payouts_for_pot(0).next().unwrap().seat, 1);
        assert_eq!(showdown.payouts_for_pot(1).next().unwrap().seat, 2);
        assert!(!showdown.is_split_pot(0));
        assert_complete_result_invariants(&showdown);
    }

    #[test]
    fn split_main_pot_keeps_both_recipients_and_side_pot_winner() {
        let result = side_pot_result(true);
        assert_eq!(result.gross_payout_for(1), Some(150));
        assert_eq!(result.gross_payout_for(2), Some(150));
        assert_eq!(result.gross_payout_for(3), Some(400));
        let HandResult::Showdown(showdown) = result else {
            panic!("expected showdown")
        };
        assert_eq!(showdown.payouts_for_pot(0).count(), 2);
        assert!(showdown.is_split_pot(0));
        assert!(!showdown.is_split_pot(1));
        assert_complete_result_invariants(&showdown);
    }

    #[test]
    fn multiple_awards_sum_without_counting_refunds() {
        let result: HandResult = serde_json::from_value(json!({
            "type": "showdown", "pots": [
                {"amount": 300, "eligibleSeats": [1, 2, 3]},
                {"amount": 400, "eligibleSeats": [2, 3]},
                {"amount": 50, "eligibleSeats": [2]}
            ],
            "payouts": [{"seat": 2, "amount": 300, "potIndex": 0}, {"seat": 2, "amount": 400, "potIndex": 1}],
            "refunds": [{"seat": 2, "amount": 50}], "winnerSeats": [2],
            "chipsWonBySeat": [-100, 400, -300]
        })).unwrap();
        assert_eq!(result.gross_payout_for(2), Some(700));
        assert_eq!(result.net_chips_for(2), Some(400));
        assert_eq!(result.winner_seats(), vec![2]);
        let HandResult::Showdown(showdown) = result else {
            panic!("expected showdown")
        };
        assert_eq!(showdown.payouts_for_seat(2).count(), 2);
        assert_complete_result_invariants(&showdown);
    }

    #[test]
    fn one_pot_showdown_preserves_nested_fields_and_evaluated_cards() {
        let result: HandResult = serde_json::from_value(json!({
            "type": "showdown", "pot": 200, "allInRunout": true, "riverCallOccurred": true,
            "pots": [{"amount": 200, "eligibleSeats": [1, 2], "future": 1}],
            "payouts": [{"seat": 1, "amount": 200, "potIndex": 0, "pot": 200, "future": 2}],
            "refunds": [{"seat": 2, "amount": 0, "pot": 200, "future": 3}],
            "hands": [{"seat": 1, "name": null, "label": "Flush", "cards": ["Ad", "Kd", "Qd", "Jd", "9d"], "future": 4}],
            "board": ["Kd", "Qd", "Jd", "9d", "2s"], "winnerSeats": [1], "future": 5
        })).unwrap();
        assert!(result.is_all_in_runout());
        let HandResult::Showdown(showdown) = result else {
            panic!("expected showdown")
        };
        assert_eq!(showdown.pots[0].raw["future"], 1);
        assert_eq!(showdown.payouts[0].raw["future"], 2);
        assert_eq!(showdown.refunds[0].raw["future"], 3);
        assert_eq!(showdown.hands[0].raw["future"], 4);
        assert_eq!(showdown.raw["future"], 5);
        assert!(showdown.hands[0].name.is_empty());
        assert_eq!(showdown.hands[0].label, "Flush");
        assert_eq!(showdown.hands[0].cards.len(), 5);
        assert_complete_result_invariants(&showdown);
    }

    #[test]
    fn malformed_and_future_results_preserve_original_without_snapshot_failure() {
        for value in [
            json!({"type": "future", "winnerSeat": 1, "pot": 200}),
            json!({"type": "foldWin", "winnerSeat": "new", "pot": 200}),
            json!({"type": "foldWin", "winnerSeat": 1}),
            json!({"type": "showdown", "payouts": [{"seat": 1, "amount": "new"}]}),
            json!({"type": "showdown", "chipsWonBySeat": [1.5]}),
            json!({"winnerSeat": 1, "pot": 200}),
            json!([1, 2]),
            json!("future"),
            json!(42),
            json!(true),
        ] {
            let snapshot: TableSnapshot =
                serde_json::from_value(json!({"tableId": "t", "handResult": value})).unwrap();
            let HandResult::Unknown(original) = snapshot.hand_result.unwrap() else {
                panic!("expected unknown for {value}")
            };
            assert_eq!(original, value);
        }
        let result: HandResult = serde_json::from_value(Value::Null).unwrap();
        assert!(matches!(result, HandResult::Unknown(Value::Null)));
        let snapshot: TableSnapshot =
            serde_json::from_value(json!({"tableId": "t", "handResult": null})).unwrap();
        assert!(snapshot.hand_result.is_none());
    }

    #[test]
    fn missing_result_details_never_invent_awards() {
        let result: HandResult =
            serde_json::from_value(json!({"type": "showdown", "winnerSeats": [1, 2], "pot": 201}))
                .unwrap();
        assert_eq!(result.winner_seats(), vec![1, 2]);
        assert_eq!(result.gross_payout_for(1), None);
        assert_eq!(result.net_chips_for(1), None);
        let HandResult::Showdown(showdown) = result else {
            panic!("expected showdown")
        };
        assert!(showdown.pots.is_empty() && showdown.hands.is_empty());
        assert!(!showdown.is_split_pot(0));
        let fold: HandResult =
            serde_json::from_value(json!({"type": "foldWin", "winnerSeat": 1, "pot": 0})).unwrap();
        assert_eq!(fold.gross_payout_for(1), Some(0));
        assert_eq!(fold.net_chips_for(1), None);
    }

    #[test]
    fn payout_helpers_handle_duplicates_absent_identity_overflow_and_invalid_seats() {
        let result: HandResult = serde_json::from_value(json!({
            "type": "showdown", "winnerSeats": [2, 2],
            "payouts": [
                {"seat": 1, "amount": u64::MAX, "potIndex": 0},
                {"seat": 1, "amount": 1, "potIndex": 0},
                {"seat": 2, "amount": 40, "pot": 40}
            ], "chipsWonBySeat": [0, 0, 0, 0, 0, 0, 99]
        }))
        .unwrap();
        assert_eq!(result.winner_seats(), vec![2, 1]);
        assert_eq!(result.gross_payout_for(1), None);
        assert_eq!(result.gross_payout_for(2), Some(40));
        for seat in [0, 7, u8::MAX] {
            assert_eq!(result.net_chips_for(seat), None);
            assert_eq!(result.gross_payout_for(seat), None);
        }
        let HandResult::Showdown(showdown) = result else {
            panic!("expected showdown")
        };
        assert!(!showdown.is_split_pot(0));
        assert_eq!(showdown.payouts_for_pot(40).count(), 0);
        let unknown = HandResult::Unknown(json!({"pot": 100}));
        assert!(unknown.winner_seats().is_empty());
        assert_eq!(unknown.gross_payout_for(1), None);
        assert_eq!(unknown.net_chips_for(1), None);
    }

    #[test]
    fn snapshot_analysis_fields_and_extensions_decode() {
        let snapshot: TableSnapshot = serde_json::from_value(json!({
            "tableId": "t", "heroEquity": 0.5,
            "heroDecision": {"equity": 0.5, "evFold": 0.0, "evCall": -2.5, "evRaise": null, "bestAction": "future", "bestAmount": null, "future": 1},
            "seatEquities": [{"seat": 1, "equity": 0.5, "future": 2}],
            "blindLevel": {"smallBlind": 1, "bigBlind": 2, "future": 3},
            "skipHandPaused": true, "unratedHand": true, "unratedPracticePending": true,
            "heroIsDealtOut": true, "autoAllIn": true, "autoCheckRaise": true, "isDebug": true
        })).unwrap();
        assert_eq!(snapshot.hero_equity, Some(0.5));
        assert!(
            snapshot.skip_hand_paused && snapshot.unrated_hand && snapshot.unrated_practice_pending
        );
        assert!(
            snapshot.hero_is_dealt_out
                && snapshot.auto_all_in
                && snapshot.auto_check_raise
                && snapshot.is_debug
        );
        let decision = snapshot.hero_decision.unwrap();
        assert_eq!(decision.best_action, "future");
        assert!(decision.ev_raise.is_none() && decision.best_amount.is_none());
        assert_eq!(decision.raw["future"], 1);
        assert_eq!(snapshot.seat_equities.unwrap()[0].raw["future"], 2);
        assert_eq!(snapshot.blind_level.unwrap().raw["future"], 3);
        let minimal: TableSnapshot = serde_json::from_value(json!({"tableId": "t"})).unwrap();
        assert!(minimal.hero_equity.is_none() && minimal.hero_decision.is_none());
        assert!(minimal.seat_equities.is_none() && minimal.blind_level.is_none());
        assert!(
            !minimal.skip_hand_paused && !minimal.unrated_hand && !minimal.unrated_practice_pending
        );
        assert!(
            !minimal.hero_is_dealt_out
                && !minimal.auto_all_in
                && !minimal.auto_check_raise
                && !minimal.is_debug
        );
    }

    #[test]
    fn seat_topology_wraps_and_preserves_empty_slots() {
        let snapshot: TableSnapshot = serde_json::from_value(json!({"tableId": "t", "youSeat": 4, "seats": [{"seat": 4}, {"seat": 6, "status": "empty"}]})).unwrap();
        assert_eq!(snapshot.hero().unwrap().seat, 4);
        assert!(snapshot.seat(5).is_none());
        assert!(snapshot.seat(6).unwrap().is_open());
        let clockwise = TableSnapshot::clockwise_seat_numbers_after(4).unwrap();
        assert_eq!(clockwise.len(), 5);
        assert_eq!(clockwise.collect::<Vec<_>>(), vec![5, 6, 1, 2, 3]);
        assert_eq!(TableSnapshot::next_seat_number(6), Some(1));
        for seat in 1..=MAX_SEATS {
            let clockwise: Vec<_> = TableSnapshot::clockwise_seat_numbers_after(seat)
                .unwrap()
                .collect();
            assert_eq!(clockwise.len(), usize::from(MAX_SEATS - 1));
            assert!(!clockwise.contains(&seat));
            for other in 1..=MAX_SEATS {
                assert_eq!(clockwise.contains(&other), seat != other);
            }
        }
        for invalid in [0, 7, u8::MAX] {
            assert!(TableSnapshot::next_seat_number(invalid).is_none());
            assert!(TableSnapshot::clockwise_seat_numbers_after(invalid).is_none());
        }
    }

    #[test]
    fn completion_and_runout_predicates_require_authoritative_state() {
        for (phase, result, complete, runout) in [
            ("betting", Value::Null, false, false),
            ("runout", Value::Null, false, true),
            ("handComplete", Value::Null, false, false),
            (
                "handComplete",
                json!({"type": "foldWin", "winnerSeat": 1, "pot": 100}),
                true,
                false,
            ),
            (
                "handComplete",
                json!({"type": "showdown", "allInRunout": true}),
                true,
                true,
            ),
            ("future", json!({"type": "future"}), false, false),
        ] {
            let snapshot: TableSnapshot = serde_json::from_value(
                json!({"tableId": "t", "phase": phase, "handResult": result}),
            )
            .unwrap();
            assert_eq!(snapshot.is_hand_complete(), complete);
            assert_eq!(snapshot.is_all_in_runout(), runout);
        }
    }

    #[test]
    fn snapshot_winners_include_fold_split_and_side_pot_awards() {
        let mut snapshot: TableSnapshot = serde_json::from_value(json!({
            "tableId": "t", "phase": "handComplete",
            "handResult": {"type": "foldWin", "winnerSeat": 2, "pot": 100}
        }))
        .unwrap();
        assert!(!snapshot.is_winner(1));
        assert!(snapshot.is_winner(2));
        for split in [false, true] {
            snapshot.hand_result = Some(side_pot_result(split));
            assert!(snapshot.is_winner(1));
            assert!(snapshot.is_winner(2));
            assert_eq!(snapshot.is_winner(3), split);
            assert!(!snapshot.is_winner(4));
        }
        // A negative net result does not negate a split-pot win.
        assert_eq!(
            snapshot.hand_result.as_ref().unwrap().net_chips_for(2),
            Some(-150)
        );
        assert!(snapshot.is_winner(2));
        for invalid in [0, 7, u8::MAX] {
            assert!(!snapshot.is_winner(invalid));
        }
    }

    #[test]
    fn snapshot_winners_use_result_evidence_not_refunds_or_stale_state() {
        let mut snapshot: TableSnapshot = serde_json::from_value(json!({
            "tableId": "t", "phase": "handComplete",
            "handResult": {
                "type": "showdown", "winnerSeats": [1],
                "payouts": [{"seat": 2, "amount": 100}],
                "refunds": [{"seat": 3, "amount": 50}],
                "chipsWonBySeat": [0, 0, 50]
            }
        }))
        .unwrap();
        assert!(snapshot.is_winner(1));
        assert!(snapshot.is_winner(2));
        assert!(!snapshot.is_winner(3));
        for phase in [
            None,
            Some("waiting"),
            Some("betting"),
            Some("runout"),
            Some("future"),
        ] {
            snapshot.phase = phase.map(str::to_owned);
            assert!(!snapshot.is_winner(1));
            assert!(!snapshot.is_winner(2));
        }
        snapshot.phase = Some("handComplete".to_owned());
        for result in [
            Value::Null,
            json!({"type": "future", "winnerSeat": 1}),
            json!({"type": "foldWin", "winnerSeat": 1}),
            json!({"type": "showdown"}),
        ] {
            snapshot.hand_result = serde_json::from_value(result).unwrap();
            assert!(!snapshot.is_winner(1));
        }
    }
}
