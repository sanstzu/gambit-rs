use std::time::Duration;

/// Result type returned by Gambit client operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors produced by configuration, transport, protocol, and gameplay operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("invalid Gambit client configuration: {0}")]
    Configuration(String),

    #[error("Gambit authentication failed: {0}")]
    Authentication(String),

    #[error("Gambit transport failed: {0}")]
    Transport(String),

    #[error("Gambit protocol error: {0}")]
    Protocol(String),

    #[error("Gambit WebSocket authentication failed")]
    WebSocketAuthentication,

    #[error("Gambit operation timed out after {0:?}")]
    Timeout(Duration),

    #[error("illegal action: {0}")]
    IllegalAction(String),

    #[error("an action was already sent for the latest authoritative turn")]
    StaleTurn,

    #[error("seating operation failed: {0}")]
    Seating(String),

    #[error("Gambit rejected seat {seat:?}: {reason}")]
    SeatRejected { seat: Option<u8>, reason: String },

    #[error("game session is closed")]
    Closed,
}

impl From<serde_json::Error> for Error {
    fn from(_: serde_json::Error) -> Self {
        Self::Protocol("server returned invalid JSON".to_owned())
    }
}
