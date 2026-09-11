use std::{
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use http::header::ORIGIN;
use secrecy::ExposeSecret;
use serde_json::{Value, json};
use tokio::{
    net::TcpStream,
    sync::{Mutex, broadcast, watch},
    task::JoinHandle,
    time::{Instant, interval, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};

use crate::{
    Action, AfkWarning, AuthSession, ClientConfig, Error, GameMode, LiveBust, LiveRejoinResult,
    LiveTableMatch, MAX_SEATS, Result, SeatRejected, TableSnapshot,
    protocol::{
        ActionIdGenerator, ServerMessage, decode_server_message, envelope, now_ms,
        player_action_payload,
    },
};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type SocketWriter = SplitSink<Socket, Message>;

/// A typed notification emitted by a game connection.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum GameEvent {
    Authenticated,
    Snapshot(Arc<TableSnapshot>),
    SeatRejected(SeatRejected),
    AfkWarning(AfkWarning),
    LiveBust(LiveBust),
    LiveRejoin(LiveRejoinResult),
    Raw(Arc<Value>),
    ConnectionError(String),
    Closed,
}

#[derive(Clone, Debug)]
struct VersionedSnapshot {
    version: u64,
    snapshot: Arc<TableSnapshot>,
}

#[derive(Clone, Debug)]
struct JoinState {
    payload: Value,
    mode: GameMode,
}

// Deadlines and legal-action metadata can refresh within a turn. Only observed
// table/hand/street/actor progression establishes another turn.
#[derive(Debug, Eq, PartialEq)]
struct TurnPosition {
    table_id: String,
    hand_id: Option<String>,
    street: Option<String>,
    seat: u8,
}

#[derive(Debug)]
struct SessionState {
    closed: bool,
    connection_generation: u64,
    ids: ActionIdGenerator,
    join: Option<JoinState>,
    mode: GameMode,
    table_match: Option<LiveTableMatch>,
    turn_position: Option<TurnPosition>,
    turn_generation: u64,
    acted_turn: Option<u64>,
    snapshot_version: u64,
    seating_pending: bool,
    stand_up_pending: bool,
}

pub(crate) struct SessionInner {
    public_handles: AtomicUsize,
    auth: AuthSession,
    config: ClientConfig,
    lifecycle: Mutex<()>,
    writer: Mutex<Option<SocketWriter>>,
    state: Mutex<SessionState>,
    snapshots: watch::Sender<Option<VersionedSnapshot>>,
    events: broadcast::Sender<GameEvent>,
    raw_messages: broadcast::Sender<Arc<Value>>,
    receiver_task: StdMutex<Option<JoinHandle<()>>>,
    heartbeat_task: StdMutex<Option<JoinHandle<()>>>,
}

/// One authenticated WebSocket and its latest authoritative table state.
pub struct GameSession {
    pub(crate) inner: Arc<SessionInner>,
}

impl std::fmt::Debug for GameSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GameSession")
            .finish_non_exhaustive()
    }
}

impl GameSession {
    pub(crate) async fn open(auth: AuthSession, config: ClientConfig) -> Result<Self> {
        let (snapshots, _) = watch::channel(None);
        let (events, _) = broadcast::channel(128);
        let (raw_messages, _) = broadcast::channel(128);
        let session = Self {
            inner: Arc::new(SessionInner {
                public_handles: AtomicUsize::new(1),
                auth,
                config,
                lifecycle: Mutex::new(()),
                writer: Mutex::new(None),
                state: Mutex::new(SessionState {
                    closed: true,
                    connection_generation: 0,
                    ids: ActionIdGenerator::create(),
                    join: None,
                    mode: GameMode::Unknown,
                    table_match: None,
                    turn_position: None,
                    turn_generation: 0,
                    acted_turn: None,
                    snapshot_version: 0,
                    seating_pending: false,
                    stand_up_pending: false,
                }),
                snapshots,
                events,
                raw_messages,
                receiver_task: StdMutex::new(None),
                heartbeat_task: StdMutex::new(None),
            }),
        };
        session.connect().await?;
        Ok(session)
    }

    async fn connect(&self) -> Result<()> {
        let result = self.connect_inner().await;
        if result.is_err() {
            self.stop_connection().await;
        }
        result
    }

    async fn connect_inner(&self) -> Result<()> {
        let mut request = self
            .inner
            .config
            .websocket_url
            .as_str()
            .into_client_request()
            .map_err(|_| Error::Configuration("WebSocket URL cannot form a request".to_owned()))?;
        request.headers_mut().insert(
            ORIGIN,
            self.inner
                .config
                .origin_header
                .parse()
                .map_err(|_| Error::Configuration("Origin header is invalid".to_owned()))?,
        );
        let connect = connect_async(request);
        let (socket, _) = timeout(self.inner.config.websocket_timeout, connect)
            .await
            .map_err(|_| Error::Timeout(self.inner.config.websocket_timeout))?
            .map_err(|error| Error::Transport(format!("could not open WebSocket: {error}")))?;
        let (writer, reader) = socket.split();
        *self.inner.writer.lock().await = Some(writer);
        let generation = {
            let mut state = self.inner.state.lock().await;
            state.connection_generation += 1;
            state.closed = false;
            state.connection_generation
        };

        let inner = Arc::clone(&self.inner);
        replace_task(
            &self.inner.receiver_task,
            tokio::spawn(async move {
                receive_loop(inner, reader, generation).await;
            }),
        );

        let mut events = self.subscribe_events();
        let session_id = {
            let state = self.inner.state.lock().await;
            state.ids.session_id().to_owned()
        };
        self.send(
            "client_capabilities",
            json!({"invisibleDeployHandoff": true}),
            false,
        )
        .await?;
        self.send(
            "authenticate",
            json!({
                "token": self.inner.auth.token.expose_secret(),
                "sessionId": session_id,
            }),
            false,
        )
        .await?;
        if let Err(error) =
            wait_for_event(&mut events, self.inner.config.websocket_timeout, |event| {
                matches!(event, GameEvent::Authenticated)
            })
            .await
        {
            self.stop_connection().await;
            return Err(error);
        }

        let inner = Arc::clone(&self.inner);
        replace_task(
            &self.inner.heartbeat_task,
            tokio::spawn(async move {
                heartbeat_loop(inner, generation).await;
            }),
        );
        Ok(())
    }

    #[must_use]
    pub fn subscribe_events(&self) -> broadcast::Receiver<GameEvent> {
        self.inner.events.subscribe()
    }

    /// Subscribes to complete decoded inbound JSON envelopes, including snapshots.
    ///
    /// This bounded stream starts at subscription time and can report lag. It does
    /// not replay earlier messages or include outbound authentication payloads.
    /// Values are untrusted and may contain personal data. Do not log credentials
    /// or bearer tokens. JSON values preserve fields, not wire whitespace or bytes.
    /// Typed subscribers still receive one event per snapshot.
    #[must_use]
    pub fn subscribe_raw_messages(&self) -> broadcast::Receiver<Arc<Value>> {
        self.inner.raw_messages.subscribe()
    }

    /// Returns the latest authoritative table snapshot, if one has arrived.
    #[must_use]
    pub fn snapshot(&self) -> Option<Arc<TableSnapshot>> {
        self.inner
            .snapshots
            .borrow()
            .as_ref()
            .map(|versioned| Arc::clone(&versioned.snapshot))
    }

    #[must_use]
    pub fn snapshot_version(&self) -> u64 {
        self.inner
            .snapshots
            .borrow()
            .as_ref()
            .map_or(0, |versioned| versioned.version)
    }

    pub async fn mode(&self) -> GameMode {
        self.inner.state.lock().await.mode
    }

    pub async fn table_match(&self) -> Option<LiveTableMatch> {
        self.inner.state.lock().await.table_match.clone()
    }

    pub(crate) async fn set_table_match(&self, table_match: LiveTableMatch) {
        self.inner.state.lock().await.table_match = Some(table_match);
    }

    pub async fn is_closed(&self) -> bool {
        self.inner.state.lock().await.closed
    }

    pub async fn join_table(
        &self,
        table_id: &str,
        buy_in: Option<u64>,
    ) -> Result<Arc<TableSnapshot>> {
        self.join_table_with_mode(table_id, buy_in, None).await
    }

    pub(crate) async fn join_table_with_mode(
        &self,
        table_id: &str,
        buy_in: Option<u64>,
        mode: Option<GameMode>,
    ) -> Result<Arc<TableSnapshot>> {
        self.require_open().await?;
        let mut payload = json!({
            "tableId": table_id,
            "userId": self.inner.auth.user.id,
            "username": self.inner.auth.user.username,
            "avatarUrl": self.inner.auth.user.avatar_url,
            "timezoneOffset": self.inner.config.timezone_offset,
        });
        if let Some(buy_in) = buy_in {
            payload["buyInAmount"] = json!(buy_in);
        }
        let baseline = self.snapshot_version();
        {
            let mut state = self.inner.state.lock().await;
            if let Some(mode) = mode {
                state.mode = mode;
            }
            state.join = Some(JoinState {
                payload: payload.clone(),
                mode: state.mode,
            });
        }
        self.send("join_table", payload, false).await?;
        self.wait_for_snapshot(baseline, None).await
    }

    pub async fn create_bot_game(
        &self,
        buy_in: u64,
        bots_auto_all_in: bool,
    ) -> Result<Arc<TableSnapshot>> {
        self.require_open().await?;
        self.send("set_buyin", json!({"amount": buy_in}), true)
            .await?;
        self.send(
            "set_bot_auto_all_in",
            json!({"enabled": bots_auto_all_in}),
            false,
        )
        .await?;
        self.join_table_with_mode("default", Some(buy_in), Some(GameMode::Bot))
            .await
    }

    pub async fn create_friend_game(
        &self,
        small_blind: u64,
        big_blind: u64,
        starting_chips: u64,
    ) -> Result<Arc<TableSnapshot>> {
        self.require_open().await?;
        let mut events = self.subscribe_events();
        self.send(
            "create_friend_table",
            json!({
                "userId": self.inner.auth.user.id,
                "username": self.inner.auth.user.username,
                "avatarUrl": self.inner.auth.user.avatar_url,
                "timezoneOffset": self.inner.config.timezone_offset,
                "settings": {
                    "smallBlind": small_blind,
                    "bigBlind": big_blind,
                    "startingChips": starting_chips,
                }
            }),
            false,
        )
        .await?;
        let raw = wait_for_raw_types(
            &mut events,
            self.inner.config.websocket_timeout,
            &["friend_table_created", "friend_table_create_rejected"],
        )
        .await?;
        let slug = raw
            .get("payload")
            .and_then(|payload| payload.get("slug"))
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Protocol("friend table response has no slug".to_owned()))?;
        self.join_table_with_mode(slug, None, Some(GameMode::Friends))
            .await
    }

    pub async fn find_live_table(&self) -> Result<LiveTableMatch> {
        self.require_open().await?;
        let mut events = self.subscribe_events();
        self.send("find_live_table", json!({}), false).await?;
        let raw = wait_for_raw_types(
            &mut events,
            self.inner.config.websocket_timeout,
            &["live_table_matched"],
        )
        .await?;
        let payload = raw
            .get("payload")
            .ok_or_else(|| Error::Protocol("live table match has no payload".to_owned()))?;
        let result = LiveTableMatch {
            slug: payload
                .get("slug")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::Protocol("live table match has no slug".to_owned()))?
                .to_owned(),
            created: payload
                .get("created")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            open_seat: optional_u8(payload.get("openSeat")),
            raw: Arc::clone(&raw),
        };
        self.inner.state.lock().await.table_match = Some(result.clone());
        Ok(result)
    }

    pub async fn request_seat(
        &self,
        seat_number: u8,
        wait_timeout: Option<Duration>,
    ) -> Result<Arc<TableSnapshot>> {
        if !(1..=MAX_SEATS).contains(&seat_number) {
            return Err(Error::Seating(format!(
                "seat number must be between 1 and {MAX_SEATS}"
            )));
        }
        let snapshot = self
            .snapshot()
            .ok_or_else(|| Error::Seating("a joined table snapshot is required".to_owned()))?;
        if snapshot.is_seated() {
            return Err(Error::Seating("already seated at this table".to_owned()));
        }
        if !snapshot.available_seats().contains(&seat_number) {
            return Err(Error::Seating(format!(
                "seat {seat_number} is not empty in the latest snapshot"
            )));
        }
        {
            let mut state = self.inner.state.lock().await;
            if !matches!(state.mode, GameMode::Live | GameMode::Friends) {
                return Err(Error::Seating(
                    "seat requests require a human table".to_owned(),
                ));
            }
            if state.seating_pending {
                return Err(Error::Seating(
                    "a seat request is already pending".to_owned(),
                ));
            }
            state.seating_pending = true;
        }

        let result = self.request_seat_inner(seat_number, wait_timeout).await;
        self.inner.state.lock().await.seating_pending = false;
        result
    }

    async fn request_seat_inner(
        &self,
        seat_number: u8,
        wait_timeout: Option<Duration>,
    ) -> Result<Arc<TableSnapshot>> {
        let mut events = self.subscribe_events();
        self.send("request_seat", json!({"position": seat_number - 1}), true)
            .await?;
        let duration = wait_timeout.unwrap_or(self.inner.config.websocket_timeout);
        let deadline = Instant::now() + duration;
        loop {
            if let Some(snapshot) = self.snapshot()
                && snapshot.is_seated()
                && snapshot.you_seat == Some(seat_number)
            {
                return Ok(snapshot);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::Timeout(duration));
            }
            match timeout(remaining, events.recv()).await {
                Ok(Ok(GameEvent::Snapshot(snapshot)))
                    if snapshot.is_seated() && snapshot.you_seat == Some(seat_number) =>
                {
                    return Ok(snapshot);
                }
                Ok(Ok(GameEvent::SeatRejected(rejection))) => {
                    return Err(Error::SeatRejected {
                        seat: rejection.seat_number(),
                        reason: rejection.reason,
                    });
                }
                Ok(Ok(GameEvent::Closed)) => return Err(Error::Closed),
                Ok(Ok(GameEvent::ConnectionError(error))) => {
                    return Err(Error::Transport(error));
                }
                Ok(Ok(_)) => {}
                Ok(Err(error)) => return Err(channel_error(&error)),
                Err(_) => return Err(Error::Timeout(duration)),
            }
        }
    }

    pub async fn stand_up(&self, wait_timeout: Option<Duration>) -> Result<Arc<TableSnapshot>> {
        let snapshot = self
            .snapshot()
            .ok_or_else(|| Error::Seating("a joined table snapshot is required".to_owned()))?;
        if !snapshot.is_seated() {
            return Err(Error::Seating(
                "cannot stand up while not seated".to_owned(),
            ));
        }
        {
            let mut state = self.inner.state.lock().await;
            if state.stand_up_pending {
                return Err(Error::Seating(
                    "a stand-up request is already pending".to_owned(),
                ));
            }
            state.stand_up_pending = true;
        }
        let result = async {
            let mut snapshots = self.inner.snapshots.subscribe();
            self.send("stand_up", json!({}), true).await?;
            let duration = wait_timeout.unwrap_or(self.inner.config.websocket_timeout);
            timeout(duration, async {
                loop {
                    self.require_open().await?;
                    if let Some(snapshot) = self.snapshot()
                        && !snapshot.is_seated()
                    {
                        return Ok(snapshot);
                    }
                    snapshots.changed().await.map_err(|_| Error::Closed)?;
                }
            })
            .await
            .map_err(|_| Error::Timeout(duration))?
        }
        .await;
        self.inner.state.lock().await.stand_up_pending = false;
        result
    }

    pub async fn wait_for_snapshot(
        &self,
        after_version: u64,
        wait_timeout: Option<Duration>,
    ) -> Result<Arc<TableSnapshot>> {
        let duration = wait_timeout.unwrap_or(self.inner.config.websocket_timeout);
        let mut snapshots = self.inner.snapshots.subscribe();
        timeout(duration, async {
            loop {
                self.require_open().await?;
                if let Some(versioned) = snapshots.borrow().as_ref()
                    && versioned.version > after_version
                {
                    return Ok(Arc::clone(&versioned.snapshot));
                }
                snapshots.changed().await.map_err(|_| Error::Closed)?;
            }
        })
        .await
        .map_err(|_| Error::Timeout(duration))?
    }

    pub async fn wait_for_turn(
        &self,
        wait_timeout: Option<Duration>,
    ) -> Result<Arc<TableSnapshot>> {
        let duration = wait_timeout.unwrap_or(self.inner.config.websocket_timeout);
        let deadline = Instant::now() + duration;
        let mut snapshots = self.inner.snapshots.subscribe();
        loop {
            {
                let state = self.inner.state.lock().await;
                if state.closed {
                    return Err(Error::Closed);
                }
                if let Some(snapshot) = self.snapshot()
                    && snapshot.is_seated()
                    && snapshot.is_our_turn()
                    && snapshot.allowed.is_some()
                    && state.acted_turn != Some(state.turn_generation)
                {
                    return Ok(snapshot);
                }
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::Timeout(duration));
            }
            timeout(remaining, snapshots.changed())
                .await
                .map_err(|_| Error::Timeout(duration))?
                .map_err(|_| Error::Closed)?;
        }
    }

    pub async fn act(&self, action: Action, amount: Option<u64>) -> Result<()> {
        // Acquire the writer first so queued actions validate the latest state.
        // Keep the state lock through the send: publication, validation and the
        // reservation must not race with each other or with disconnection.
        let mut writer = self.inner.writer.lock().await;
        let mut state = self.inner.state.lock().await;
        if state.closed || writer.is_none() {
            return Err(Error::Closed);
        }
        let snapshot = self.snapshot().ok_or_else(|| {
            Error::IllegalAction("no authoritative snapshot is available".to_owned())
        })?;
        if !snapshot.is_seated() || !snapshot.is_our_turn() {
            return Err(Error::IllegalAction("it is not our seated turn".to_owned()));
        }
        let allowed = snapshot.allowed.as_ref().ok_or_else(|| {
            Error::IllegalAction("the latest snapshot has no legal actions".to_owned())
        })?;
        if !allowed.permits(action, amount) {
            return Err(Error::IllegalAction(format!(
                "{} is not allowed by the latest snapshot",
                action.as_str()
            )));
        }
        if action == Action::Bet && allowed.call.is_some() {
            return Err(Error::IllegalAction(
                "use raise_to when facing a wager".to_owned(),
            ));
        }
        if action == Action::Raise && allowed.call.is_none() {
            return Err(Error::IllegalAction(
                "use bet when no wager is being faced".to_owned(),
            ));
        }

        if state.acted_turn == Some(state.turn_generation) {
            return Err(Error::StaleTurn);
        }
        let stamp = now_ms();
        let id = state.ids.next(stamp);
        let text = serde_json::to_string(&envelope(
            "player_action",
            player_action_payload(
                action,
                &snapshot.table_id,
                snapshot.hand_id.as_deref(),
                amount,
            ),
            stamp,
            Some(&id),
        ))?;
        // Reserve before I/O, including cancellation and uncertain delivery.
        // Neither repeated snapshots nor reconnect may erase this reservation.
        state.acted_turn = Some(state.turn_generation);
        send_locked(&self.inner, &mut writer, &mut state, text).await
    }

    pub async fn fold(&self) -> Result<()> {
        self.act(Action::Fold, None).await
    }

    pub async fn check(&self) -> Result<()> {
        self.act(Action::Check, None).await
    }

    pub async fn call(&self) -> Result<()> {
        self.act(Action::Call, None).await
    }

    pub async fn bet(&self, amount: u64) -> Result<()> {
        self.act(Action::Bet, Some(amount)).await
    }

    pub async fn raise_to(&self, amount: u64) -> Result<()> {
        self.act(Action::Raise, Some(amount)).await
    }

    pub async fn all_in(&self) -> Result<()> {
        self.act(Action::AllIn, None).await
    }

    /// Sends the observed rebuy command for a seated, busted player outside a hand.
    ///
    /// Success means the frame was sent, not that the server granted chips. The
    /// amount is server-controlled. Observe later snapshots for the outcome.
    /// This does not replace live reservation rejoin or bot-game restart flows.
    /// No retry or replay occurs, including after an ambiguous transport failure.
    pub async fn rebuy(&self) -> Result<()> {
        self.require_open().await?;
        let snapshot = self.snapshot().ok_or_else(|| {
            Error::IllegalAction("rebuy requires an authoritative table snapshot".to_owned())
        })?;
        let joined = self
            .inner
            .state
            .lock()
            .await
            .join
            .as_ref()
            .is_some_and(|join| {
                join.payload.get("tableId").and_then(Value::as_str)
                    == Some(snapshot.table_id.as_str())
            });
        if !joined {
            return Err(Error::IllegalAction(
                "rebuy requires the current joined table".to_owned(),
            ));
        }
        let hero = snapshot
            .hero()
            .filter(|hero| snapshot.is_seated() && !hero.is_open())
            .ok_or_else(|| Error::IllegalAction("rebuy requires a seated player".to_owned()))?;
        if hero.stack != 0 {
            return Err(Error::IllegalAction(
                "rebuy requires an empty stack".to_owned(),
            ));
        }
        if hero.is_all_in()
            || (hero.cards.as_ref().is_some_and(|cards| !cards.is_empty()) && !hero.is_folded())
        {
            return Err(Error::IllegalAction(
                "cannot rebuy while holding an active hand".to_owned(),
            ));
        }
        self.send("rebuy", json!({}), true).await
    }

    pub async fn live_rejoin(&self, slug: Option<&str>) -> Result<LiveRejoinResult> {
        let mut events = self.subscribe_events();
        let payload = slug.map_or_else(|| json!({}), |slug| json!({"slug": slug}));
        self.send("live_rejoin", payload, false).await?;
        wait_for_typed_event(
            &mut events,
            self.inner.config.websocket_timeout,
            |event| match event {
                GameEvent::LiveRejoin(result) => Some(result),
                _ => None,
            },
        )
        .await
    }

    pub async fn reconnect(&self) -> Result<Arc<TableSnapshot>> {
        let _lifecycle = self.inner.lifecycle.lock().await;
        let join =
            self.inner.state.lock().await.join.clone().ok_or_else(|| {
                Error::Protocol("cannot reconnect before joining a table".to_owned())
            })?;
        let baseline = self.snapshot_version();
        self.stop_connection().await;
        self.connect().await?;
        self.inner.state.lock().await.mode = join.mode;
        let result = async {
            self.send("join_table", join.payload, false).await?;
            self.wait_for_snapshot(baseline, None).await
        }
        .await;
        if result.is_err() {
            self.stop_connection().await;
        }
        result
    }

    /// Gracefully leaves the table and closes the underlying socket. This operation is idempotent.
    pub async fn leave(&self) -> Result<()> {
        let _lifecycle = self.inner.lifecycle.lock().await;
        let (seated, joined) = {
            let mut state = self.inner.state.lock().await;
            let seated =
                !state.closed && self.snapshot().is_some_and(|snapshot| snapshot.is_seated());
            let joined = !state.closed && state.join.is_some();
            invalidate_connection(&self.inner, &mut state, None);
            (seated, joined)
        };
        if seated {
            let _ = self.send_inner("stand_up", json!({}), true, true).await;
        }
        if joined {
            let _ = self.send_inner("sit_out", json!({}), false, true).await;
            let _ = self.send_inner("leave_table", json!({}), false, true).await;
        }
        self.stop_connection().await;
        Ok(())
    }

    pub(crate) async fn close_from_client(inner: Arc<SessionInner>) {
        inner.public_handles.fetch_add(1, Ordering::Relaxed);
        let _ = Self { inner }.leave().await;
    }

    async fn send(&self, message_type: &str, payload: Value, action_id: bool) -> Result<()> {
        self.send_inner(message_type, payload, action_id, false)
            .await
    }

    async fn send_inner(
        &self,
        message_type: &str,
        payload: Value,
        action_id: bool,
        closing: bool,
    ) -> Result<()> {
        let mut writer = self.inner.writer.lock().await;
        let mut state = self.inner.state.lock().await;
        if state.closed && !closing {
            return Err(Error::Closed);
        }
        let stamp = now_ms();
        let generated = action_id.then(|| state.ids.next(stamp));
        let text = serde_json::to_string(&envelope(
            message_type,
            payload,
            stamp,
            generated.as_deref(),
        ))
        .map_err(|_| Error::Protocol("could not encode an outbound message".to_owned()))?;
        send_locked(&self.inner, &mut writer, &mut state, text).await
    }

    async fn require_open(&self) -> Result<()> {
        if self.inner.state.lock().await.closed {
            return Err(Error::Closed);
        }
        Ok(())
    }

    async fn stop_connection(&self) {
        {
            let mut state = self.inner.state.lock().await;
            abort_task(&self.inner.heartbeat_task);
            abort_task(&self.inner.receiver_task);
            invalidate_connection(&self.inner, &mut state, None);
        }
        if let Some(mut writer) = self.inner.writer.lock().await.take() {
            let _ = timeout(self.inner.config.websocket_timeout, writer.close()).await;
        }
    }
}

impl Clone for GameSession {
    fn clone(&self) -> Self {
        self.inner.public_handles.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Drop for GameSession {
    fn drop(&mut self) {
        if self.inner.public_handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            abort_task(&self.inner.heartbeat_task);
            abort_task(&self.inner.receiver_task);
        }
    }
}

// A cancelled send can leave bytes buffered in the sink. Never restore that
// sink or close/flush it; invalidate while the caller still holds the state lock.
struct PendingSend<'a> {
    inner: &'a SessionInner,
    state: &'a mut SessionState,
    completed: bool,
}

impl Drop for PendingSend<'_> {
    fn drop(&mut self) {
        if !self.completed {
            invalidate_connection(
                self.inner,
                self.state,
                Some("WebSocket send cancelled; delivery is unknown".to_owned()),
            );
            abort_task(&self.inner.receiver_task);
            abort_task(&self.inner.heartbeat_task);
        }
    }
}

async fn send_locked(
    inner: &SessionInner,
    slot: &mut Option<SocketWriter>,
    state: &mut SessionState,
    text: String,
) -> Result<()> {
    let mut writer = slot.take().ok_or(Error::Closed)?;
    let mut pending = PendingSend {
        inner,
        state,
        completed: false,
    };
    let result = timeout(
        inner.config.websocket_timeout,
        writer.send(Message::Text(text.into())),
    )
    .await
    .map_err(|_| Error::Timeout(inner.config.websocket_timeout))
    .and_then(|result| {
        result.map_err(|error| Error::Transport(format!("WebSocket send failed: {error}")))
    });
    if let Err(error) = &result {
        invalidate_connection(inner, pending.state, Some(error.to_string()));
        abort_task(&inner.receiver_task);
        abort_task(&inner.heartbeat_task);
    } else {
        *slot = Some(writer);
    }
    pending.completed = true;
    result
}

async fn receive_loop(
    inner: Arc<SessionInner>,
    mut reader: futures_util::stream::SplitStream<Socket>,
    generation: u64,
) {
    let mut failure = None;
    while let Some(frame) = reader.next().await {
        let result = match frame {
            Ok(Message::Text(text)) => decode_server_message(&text),
            Ok(Message::Binary(bytes)) => std::str::from_utf8(&bytes)
                .map_err(|_| Error::Protocol("server sent non-UTF-8 binary data".to_owned()))
                .and_then(decode_server_message),
            Ok(Message::Close(_)) => break,
            Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_)) => continue,
            Err(error) => {
                failure = Some(format!("WebSocket receive failed: {error}"));
                break;
            }
        };
        match result {
            Ok(message) => handle_server_message(&inner, message, generation).await,
            Err(error) => {
                failure = Some(error.to_string());
                break;
            }
        }
    }
    finish_receive(&inner, generation, failure).await;
}

async fn finish_receive(inner: &SessionInner, generation: u64, failure: Option<String>) {
    let mut state = inner.state.lock().await;
    if state.connection_generation != generation {
        return;
    }
    invalidate_connection(inner, &mut state, failure);
    abort_task(&inner.heartbeat_task);
}

// Called with the same lock used for snapshot publication and action reservation.
// Keep turn history even when the actionable snapshot is discarded.
fn invalidate_connection(inner: &SessionInner, state: &mut SessionState, error: Option<String>) {
    let notify = !state.closed || inner.snapshots.borrow().is_some();
    state.closed = true;
    // Aborted tasks can still finish their current poll on another runtime thread.
    // Reject all later publications and cleanup from this connection generation.
    state.connection_generation += 1;
    inner.snapshots.send_replace(None);
    if let Some(error) = error {
        let _ = inner.events.send(GameEvent::ConnectionError(error));
    }
    if notify {
        let _ = inner.events.send(GameEvent::Closed);
    }
}

async fn handle_server_message(inner: &Arc<SessionInner>, message: ServerMessage, generation: u64) {
    let mut state = inner.state.lock().await;
    if state.closed || state.connection_generation != generation {
        return;
    }
    let raw = Arc::new(message.raw);
    let _ = inner.raw_messages.send(Arc::clone(&raw));
    match message.message_type.as_str() {
        "authenticated" => {
            let _ = inner.events.send(GameEvent::Authenticated);
        }
        "snapshot" => {
            if let Some(snapshot) = message.snapshot {
                publish_snapshot(inner, &mut state, snapshot);
            }
        }
        "seat_rejected" => {
            let rejection = SeatRejected {
                position: optional_u8(message.payload.get("position")),
                reason: message
                    .payload
                    .get("reason")
                    .and_then(Value::as_str)
                    .unwrap_or("seat_not_available")
                    .to_owned(),
                raw: Arc::new(message.payload),
            };
            let _ = inner.events.send(GameEvent::SeatRejected(rejection));
        }
        "afk_warning" => {
            let warning = AfkWarning {
                hands_remaining: message
                    .payload
                    .get("handsRemaining")
                    .and_then(Value::as_u64)
                    .unwrap_or(1),
                raw: Arc::new(message.payload),
            };
            let _ = inner.events.send(GameEvent::AfkWarning(warning));
        }
        "live_bust" => {
            if let (Some(slug), Some(reserved_until)) = (
                message.payload.get("slug").and_then(Value::as_str),
                message.payload.get("reservedUntil").and_then(Value::as_u64),
            ) {
                let event = LiveBust {
                    slug: slug.to_owned(),
                    reserved_until,
                    raw: Arc::new(message.payload),
                };
                let _ = inner.events.send(GameEvent::LiveBust(event));
            }
        }
        "live_rejoin_ok" => {
            if let Some(slug) = message.payload.get("slug").and_then(Value::as_str) {
                let event = LiveRejoinResult {
                    slug: slug.to_owned(),
                    same_table: message
                        .payload
                        .get("sameTable")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    raw: Arc::new(message.payload),
                };
                let _ = inner.events.send(GameEvent::LiveRejoin(event));
            }
        }
        "error"
            if message
                .payload
                .get("message")
                .and_then(Value::as_str)
                .is_some_and(|text| text.to_ascii_lowercase().contains("auth")) =>
        {
            let _ = inner.events.send(GameEvent::ConnectionError(
                Error::WebSocketAuthentication.to_string(),
            ));
        }
        _ => {
            let _ = inner.events.send(GameEvent::Raw(raw));
        }
    }
}

fn publish_snapshot(inner: &SessionInner, state: &mut SessionState, mut snapshot: TableSnapshot) {
    if snapshot.mode == GameMode::Unknown && snapshot.is_friend_table {
        snapshot.mode = GameMode::Friends;
    }
    if snapshot.mode != GameMode::Unknown {
        state.mode = snapshot.mode;
    }
    if let Some(seat) = snapshot.to_act_seat {
        let position = TurnPosition {
            table_id: snapshot.table_id.clone(),
            hand_id: snapshot.hand_id.clone(),
            street: snapshot.street.clone(),
            seat,
        };
        if state.turn_position.as_ref() != Some(&position) {
            state.turn_generation += 1;
            state.turn_position = Some(position);
        }
    }
    state.snapshot_version += 1;
    let version = state.snapshot_version;
    let snapshot = Arc::new(snapshot);
    inner.snapshots.send_replace(Some(VersionedSnapshot {
        version,
        snapshot: Arc::clone(&snapshot),
    }));
    let _ = inner.events.send(GameEvent::Snapshot(snapshot));
}

async fn heartbeat_loop(inner: Arc<SessionInner>, generation: u64) {
    let mut ticker = interval(inner.config.heartbeat_interval);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let Ok(text) = serde_json::to_string(&envelope("ping", json!({}), now_ms(), None)) else {
            return;
        };
        let mut writer = inner.writer.lock().await;
        let mut state = inner.state.lock().await;
        if state.closed || state.connection_generation != generation {
            return;
        }
        if send_locked(&inner, &mut writer, &mut state, text)
            .await
            .is_err()
        {
            return;
        }
    }
}

async fn wait_for_event(
    events: &mut broadcast::Receiver<GameEvent>,
    duration: Duration,
    predicate: impl Fn(&GameEvent) -> bool,
) -> Result<()> {
    timeout(duration, async {
        loop {
            match events.recv().await {
                Ok(event) if predicate(&event) => return Ok(()),
                Ok(GameEvent::ConnectionError(error)) => return Err(Error::Transport(error)),
                Ok(GameEvent::Closed) => return Err(Error::Closed),
                Ok(_) => {}
                Err(error) => return Err(channel_error(&error)),
            }
        }
    })
    .await
    .map_err(|_| Error::Timeout(duration))?
}

async fn wait_for_raw_types(
    events: &mut broadcast::Receiver<GameEvent>,
    duration: Duration,
    wanted: &[&str],
) -> Result<Arc<Value>> {
    wait_for_typed_event(events, duration, |event| match event {
        GameEvent::Raw(raw)
            if raw
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| wanted.contains(&kind)) =>
        {
            Some(raw)
        }
        _ => None,
    })
    .await
}

async fn wait_for_typed_event<T>(
    events: &mut broadcast::Receiver<GameEvent>,
    duration: Duration,
    extract: impl Fn(GameEvent) -> Option<T>,
) -> Result<T> {
    timeout(duration, async {
        loop {
            match events.recv().await {
                Ok(GameEvent::ConnectionError(error)) => return Err(Error::Transport(error)),
                Ok(GameEvent::Closed) => return Err(Error::Closed),
                Ok(event) => {
                    if let Some(value) = extract(event) {
                        return Ok(value);
                    }
                }
                Err(error) => return Err(channel_error(&error)),
            }
        }
    })
    .await
    .map_err(|_| Error::Timeout(duration))?
}

fn optional_u8(value: Option<&Value>) -> Option<u8> {
    value
        .and_then(Value::as_u64)
        .and_then(|value| u8::try_from(value).ok())
}

fn channel_error(error: &broadcast::error::RecvError) -> Error {
    Error::Protocol(format!("event stream failed: {error}"))
}

fn replace_task(slot: &StdMutex<Option<JoinHandle<()>>>, task: JoinHandle<()>) {
    let mut slot = slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(previous) = slot.replace(task) {
        previous.abort();
    }
}

fn abort_task(slot: &StdMutex<Option<JoinHandle<()>>>) {
    if let Some(task) = slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
    {
        task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> GameSession {
        let (snapshots, _) = watch::channel(None);
        let (events, _) = broadcast::channel(128);
        let (raw_messages, _) = broadcast::channel(128);
        GameSession {
            inner: Arc::new(SessionInner {
                public_handles: AtomicUsize::new(1),
                auth: AuthSession {
                    token: secrecy::SecretString::new("test-token".to_owned()),
                    user: serde_json::from_value(json!({"id": "hero", "username": "hero"}))
                        .unwrap(),
                    expires_in: None,
                    is_new_user: false,
                },
                config: ClientConfig::default(),
                lifecycle: Mutex::new(()),
                writer: Mutex::new(None),
                state: Mutex::new(SessionState {
                    closed: false,
                    connection_generation: 1,
                    ids: ActionIdGenerator::create(),
                    join: None,
                    mode: GameMode::Bot,
                    table_match: None,
                    turn_position: None,
                    turn_generation: 0,
                    acted_turn: None,
                    snapshot_version: 0,
                    seating_pending: false,
                    stand_up_pending: false,
                }),
                snapshots,
                events,
                raw_messages,
                receiver_task: StdMutex::new(None),
                heartbeat_task: StdMutex::new(None),
            }),
        }
    }

    fn snapshot(actor: u8) -> ServerMessage {
        decode_server_message(
            &json!({
                "type": "snapshot",
                "snapshot": {
                    "tableId": "table", "handId": "hand", "street": "preflop",
                    "youSeat": 1, "toActSeat": actor,
                    "seats": [{"seat": 1, "status": "active", "stack": 100}],
                    "allowed": {"call": 2}
                }
            })
            .to_string(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn stale_receiver_cannot_publish_or_close_replacement_connection() {
        let game = session();
        handle_server_message(&game.inner, snapshot(1), 1).await;
        {
            let mut state = game.inner.state.lock().await;
            state.acted_turn = Some(state.turn_generation);
            invalidate_connection(&game.inner, &mut state, None);
            state.connection_generation = 3;
            state.closed = false;
        }
        let mut events = game.subscribe_events();
        handle_server_message(&game.inner, snapshot(2), 1).await;
        finish_receive(&game.inner, 1, Some("old socket failed".to_owned())).await;
        assert!(game.snapshot().is_none());
        assert!(!game.is_closed().await);
        assert!(events.try_recv().is_err());
        handle_server_message(&game.inner, snapshot(1), 3).await;
        let state = game.inner.state.lock().await;
        assert_eq!(state.acted_turn, Some(state.turn_generation));
        assert_eq!(game.snapshot_version(), 2);
    }

    #[tokio::test]
    async fn cancelled_send_guard_invalidates_state_without_clearing_reservation() {
        let game = session();
        handle_server_message(&game.inner, snapshot(1), 1).await;
        let mut events = game.subscribe_events();
        {
            let mut state = game.inner.state.lock().await;
            state.acted_turn = Some(state.turn_generation);
            drop(PendingSend {
                inner: &game.inner,
                state: &mut state,
                completed: false,
            });
            assert_eq!(state.acted_turn, Some(state.turn_generation));
        }
        assert!(game.is_closed().await);
        assert!(game.snapshot().is_none());
        assert!(matches!(
            events.try_recv(),
            Ok(GameEvent::ConnectionError(_))
        ));
        assert!(matches!(events.try_recv(), Ok(GameEvent::Closed)));
    }

    #[tokio::test]
    async fn dropping_clone_does_not_abort_lone_receiver() {
        let game = session();
        let receiver = tokio::spawn(std::future::pending::<()>());
        let abort = receiver.abort_handle();
        replace_task(&game.inner.receiver_task, receiver);
        drop(game.clone());
        tokio::task::yield_now().await;
        assert!(!abort.is_finished());
        drop(game);
        tokio::task::yield_now().await;
        assert!(abort.is_finished());
    }

    #[tokio::test]
    async fn queued_action_validates_snapshot_after_acquiring_writer() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            tokio_tungstenite::accept_async(stream).await.unwrap()
        });
        let (socket, _) = connect_async(format!("ws://{address}")).await.unwrap();
        let mut peer = server.await.unwrap();
        let (writer, _reader) = socket.split();
        let game = session();
        *game.inner.writer.lock().await = Some(writer);
        handle_server_message(&game.inner, snapshot(1), 1).await;

        let writer = game.inner.writer.lock().await;
        let action = game.call();
        tokio::pin!(action);
        assert!(futures_util::poll!(&mut action).is_pending());
        handle_server_message(&game.inner, snapshot(2), 1).await;
        drop(writer);
        assert!(matches!(action.await, Err(Error::IllegalAction(_))));
        assert!(
            timeout(Duration::from_millis(30), peer.next())
                .await
                .is_err()
        );
        assert!(game.inner.state.lock().await.acted_turn.is_none());
    }
}
