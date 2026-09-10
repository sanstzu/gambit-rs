#![doc = include_str!("../README.md")]

mod client;
mod config;
mod error;
mod models;
mod protocol;
mod session;
mod table_code;

pub use client::GambitClient;
pub use config::{ClientConfig, Credentials, load_credentials};
pub use error::{Error, Result};
pub use models::{
    Action, AfkWarning, AllowedActions, AuthSession, BetRaiseBounds, BlindLevel, EvaluatedHand,
    FoldWinResult, GameMode, HandResult, HeroDecision, LastAction, LiveBust, LiveRejoinResult,
    LiveTableMatch, MAX_SEATS, Payout, PotResult, RawFields, Refund, Seat, SeatEquity,
    SeatRejected, ShowdownResult, TableSnapshot, User,
};
pub use session::{GameEvent, GameSession};
pub use table_code::normalize_table_code;
