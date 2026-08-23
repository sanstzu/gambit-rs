use std::{path::Path, sync::Arc};

use reqwest::StatusCode;
use secrecy::SecretString;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    AuthSession, ClientConfig, Credentials, Error, GameMode, GameSession, LiveTableMatch, Result,
    User, load_credentials, normalize_table_code, session::SessionInner,
};

const LOGIN_PATH: &str = "api/twirp/gambit.v1.AuthService/Login";

struct ClientInner {
    credentials: Credentials,
    config: ClientConfig,
    http: reqwest::Client,
    auth: Mutex<Option<AuthSession>>,
    games: Mutex<Vec<std::sync::Weak<SessionInner>>>,
}

/// Reusable asynchronous client for Gambit's observed web protocol.
#[derive(Clone)]
pub struct GambitClient {
    inner: Arc<ClientInner>,
}

impl std::fmt::Debug for GambitClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GambitClient")
            .finish_non_exhaustive()
    }
}

impl GambitClient {
    /// Creates a client without making a network request.
    pub fn new(credentials: Credentials, config: ClientConfig) -> Result<Self> {
        config.validate()?;
        let http = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|error| {
                Error::Configuration(format!("could not build HTTP client: {error}"))
            })?;
        Ok(Self {
            inner: Arc::new(ClientInner {
                credentials,
                config,
                http,
                auth: Mutex::new(None),
                games: Mutex::new(Vec::new()),
            }),
        })
    }

    /// Loads `GAMBIT_USERNAME` and `GAMBIT_PASSWORD` from the environment and dotenv fallback.
    pub fn from_env(dotenv_path: impl AsRef<Path>, config: ClientConfig) -> Result<Self> {
        Self::new(load_credentials(dotenv_path)?, config)
    }

    /// Logs in through the observed Twirp endpoint and retains the token in memory.
    pub async fn authenticate(&self) -> Result<AuthSession> {
        let url = self
            .inner
            .config
            .http_origin
            .join(LOGIN_PATH)
            .map_err(|_| Error::Configuration("login endpoint is invalid".to_owned()))?;
        let response = self
            .inner
            .http
            .post(url)
            .json(&serde_json::json!({
                "email": self.inner.credentials.email(),
                "password": self.inner.credentials.password(),
            }))
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    Error::Timeout(self.inner.config.request_timeout)
                } else {
                    Error::Authentication(format!(
                        "could not reach login endpoint: {}",
                        transport_kind(&error)
                    ))
                }
            })?;
        let status = response.status();
        if status == StatusCode::UNAUTHORIZED {
            return Err(Error::Authentication(
                "Gambit rejected the configured credentials".to_owned(),
            ));
        }
        if !status.is_success() {
            return Err(Error::Authentication(format!(
                "login failed with HTTP {}",
                status.as_u16()
            )));
        }
        let body: LoginResponse = response
            .json()
            .await
            .map_err(|_| Error::Protocol("login returned invalid JSON".to_owned()))?;
        let session = AuthSession {
            token: SecretString::new(body.token),
            user: body.user,
            expires_in: body.expires_in,
            is_new_user: body.is_new_user,
        };
        *self.inner.auth.lock().await = Some(session.clone());
        Ok(session)
    }

    /// Opens an authenticated WebSocket without joining a table.
    pub async fn open_game(&self) -> Result<GameSession> {
        let auth = self.inner.auth.lock().await.clone().ok_or_else(|| {
            Error::Authentication("call authenticate before opening a game".to_owned())
        })?;
        let game = GameSession::open(auth, self.inner.config.clone()).await?;
        self.inner
            .games
            .lock()
            .await
            .push(Arc::downgrade(&game.inner));
        Ok(game)
    }

    pub async fn join_table(&self, table_id: &str, buy_in: Option<u64>) -> Result<GameSession> {
        let game = self.open_game().await?;
        if let Err(error) = game.join_table(table_id, buy_in).await {
            let _ = game.leave().await;
            return Err(error);
        }
        Ok(game)
    }

    /// Joins a friend/live table code as an observer, with optional explicit seating.
    pub async fn join_game_by_code(
        &self,
        code: &str,
        auto_seat: bool,
        seat_number: Option<u8>,
    ) -> Result<GameSession> {
        let table_id = normalize_table_code(code)?;
        let game = self.open_game().await?;
        let result = async {
            let snapshot = game.join_table(&table_id, None).await?;
            if auto_seat || seat_number.is_some() {
                let selected = seat_number
                    .or_else(|| snapshot.available_seats().into_iter().next())
                    .ok_or_else(|| Error::Seating("the table has no open seat".to_owned()))?;
                game.request_seat(selected, None).await?;
            }
            Ok(())
        }
        .await;
        if let Err(error) = result {
            let _ = game.leave().await;
            return Err(error);
        }
        Ok(game)
    }

    pub async fn create_bot_game(
        &self,
        buy_in: u64,
        bots_auto_all_in: bool,
    ) -> Result<GameSession> {
        let game = self.open_game().await?;
        if let Err(error) = game.create_bot_game(buy_in, bots_auto_all_in).await {
            let _ = game.leave().await;
            return Err(error);
        }
        Ok(game)
    }

    pub async fn create_friend_game(
        &self,
        small_blind: u64,
        big_blind: u64,
        starting_chips: u64,
    ) -> Result<GameSession> {
        let game = self.open_game().await?;
        if let Err(error) = game
            .create_friend_game(small_blind, big_blind, starting_chips)
            .await
        {
            let _ = game.leave().await;
            return Err(error);
        }
        Ok(game)
    }

    /// Finds one live table without joining it or requesting a seat.
    pub async fn find_live_table(&self) -> Result<LiveTableMatch> {
        let game = self.open_game().await?;
        let result = game.find_live_table().await;
        let _ = game.leave().await;
        result
    }

    /// Matches and joins one live table, initially as an observer.
    pub async fn create_live_game(
        &self,
        auto_seat: bool,
        seat_number: Option<u8>,
    ) -> Result<GameSession> {
        let matcher = self.open_game().await?;
        let table_match = matcher.find_live_table().await;
        let _ = matcher.leave().await;
        let table_match = table_match?;

        let game = self.open_game().await?;
        game.set_table_match(table_match.clone()).await;
        let result = async {
            game.join_table_with_mode(&table_match.slug, None, Some(GameMode::Live))
                .await?;
            if auto_seat || seat_number.is_some() {
                let selected = seat_number
                    .or_else(|| table_match.suggested_seat())
                    .ok_or_else(|| {
                        Error::Seating("matched table suggested no open seat".to_owned())
                    })?;
                game.request_seat(selected, None).await?;
            }
            Ok(())
        }
        .await;
        if let Err(error) = result {
            let _ = game.leave().await;
            return Err(error);
        }
        Ok(game)
    }

    /// Closes all game sessions currently reachable through this client.
    pub async fn close(&self) {
        let games = std::mem::take(&mut *self.inner.games.lock().await);
        for game in games {
            if let Some(game) = game.upgrade() {
                GameSession::close_from_client(game).await;
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoginResponse {
    token: String,
    user: User,
    #[serde(default)]
    expires_in: Option<String>,
    #[serde(default)]
    is_new_user: bool,
}

fn transport_kind(error: &reqwest::Error) -> &'static str {
    if error.is_connect() {
        "connection error"
    } else if error.is_redirect() {
        "redirect error"
    } else if error.is_request() {
        "request error"
    } else {
        "transport error"
    }
}
