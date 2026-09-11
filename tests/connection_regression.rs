use std::time::Duration;

use axum::{
    Json, Router,
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    response::Response,
    routing::{any, post},
};
use gambit_rs::{ClientConfig, Credentials, Error, GambitClient, GameEvent, GameSession};
use serde_json::{Value, json};
use tokio::{
    net::TcpListener,
    sync::{broadcast, mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
use url::Url;

const DEADLINE: Duration = Duration::from_secs(2);
const QUIET: Duration = Duration::from_millis(50);

// Each connection has its own controls and inbox, so reconnect traffic cannot be
// mistaken for traffic from the old socket. Joining never sends a snapshot.
#[derive(Clone)]
struct ServerState {
    connections: mpsc::UnboundedSender<ControlledSocket>,
}

struct TestServer {
    config: ClientConfig,
    connections: mpsc::UnboundedReceiver<ControlledSocket>,
    task: JoinHandle<()>,
}

struct ControlledSocket {
    commands: mpsc::UnboundedSender<(Command, oneshot::Sender<()>)>,
    messages: mpsc::UnboundedReceiver<Value>,
}

enum Command {
    Text(String),
    Close,
    Drop,
}

impl TestServer {
    async fn spawn() -> Self {
        let (connections, incoming) = mpsc::unbounded_channel();
        let app = Router::new()
            .route("/api/twirp/gambit.v1.AuthService/Login", post(login))
            .route("/ws", any(websocket))
            .with_state(ServerState { connections });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            config: ClientConfig {
                http_origin: Url::parse(&format!("http://{address}")).unwrap(),
                websocket_url: Url::parse(&format!("ws://{address}/ws")).unwrap(),
                origin_header: "https://www.gambit.com".to_owned(),
                request_timeout: DEADLINE,
                websocket_timeout: DEADLINE,
                heartbeat_interval: Duration::from_secs(60),
                timezone_offset: Some(480),
            },
            connections: incoming,
            task,
        }
    }

    async fn accept_join(&mut self) -> ControlledSocket {
        let mut socket = timeout(DEADLINE, self.connections.recv())
            .await
            .expect("client did not connect")
            .expect("mock server stopped");
        socket.expect_type("client_capabilities").await;
        let authenticate = socket.expect_type("authenticate").await;
        assert_eq!(authenticate["payload"]["token"], "secret-token");
        let join = socket.expect_type("join_table").await;
        assert_eq!(join["payload"]["tableId"], "default");
        socket
    }

    async fn joined(&mut self) -> (GambitClient, GameSession, ControlledSocket) {
        let client = GambitClient::new(
            Credentials::new("person@example.com", "password"),
            self.config.clone(),
        )
        .unwrap();
        client.authenticate().await.unwrap();
        let joining = tokio::spawn({
            let client = client.clone();
            async move { client.join_table("default", Some(200)).await }
        });
        let socket = self.accept_join().await;
        socket.snapshot("hand-1").await;
        let game = timeout(DEADLINE, joining).await.unwrap().unwrap().unwrap();
        assert!(game.snapshot().unwrap().is_our_turn());
        (client, game, socket)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl ControlledSocket {
    async fn command(&self, command: Command) {
        let (acknowledge, acknowledged) = oneshot::channel();
        self.commands.send((command, acknowledge)).unwrap();
        timeout(DEADLINE, acknowledged).await.unwrap().unwrap();
    }

    async fn snapshot(&self, hand_id: &str) {
        self.command(Command::Text(snapshot(hand_id).to_string()))
            .await;
    }

    async fn expect_type(&mut self, wanted: &str) -> Value {
        let message = timeout(DEADLINE, self.messages.recv())
            .await
            .expect("expected client message did not arrive")
            .expect("socket closed before expected client message");
        assert_eq!(
            message["type"], wanted,
            "unexpected client message: {message}"
        );
        message
    }

    async fn assert_no_messages(&mut self) {
        assert!(
            timeout(QUIET, self.messages.recv()).await.is_err(),
            "unexpected client traffic or socket closure"
        );
    }
}

async fn login() -> Json<Value> {
    Json(json!({
        "token": "secret-token",
        "expiresIn": "86400",
        "isNewUser": false,
        "user": {"id": "user-1", "username": "tester", "email": "person@example.com"}
    }))
}

async fn websocket(ws: WebSocketUpgrade, State(state): State<ServerState>) -> Response {
    ws.on_upgrade(move |socket| serve_socket(socket, state))
}

async fn serve_socket(mut socket: WebSocket, state: ServerState) {
    let (commands, mut incoming) = mpsc::unbounded_channel();
    let (messages, inbox) = mpsc::unbounded_channel();
    if state
        .connections
        .send(ControlledSocket {
            commands,
            messages: inbox,
        })
        .is_err()
    {
        return;
    }
    loop {
        tokio::select! {
            command = incoming.recv() => {
                let Some((command, acknowledge)) = command else { return };
                match command {
                    Command::Text(text) => {
                        socket.send(Message::Text(text.into())).await.unwrap();
                        let _ = acknowledge.send(());
                    }
                    Command::Close => {
                        socket.send(Message::Close(None)).await.unwrap();
                        let _ = acknowledge.send(());
                        return;
                    }
                    Command::Drop => {
                        // Drop the TCP transport without sending a WebSocket close frame.
                        drop(socket);
                        let _ = acknowledge.send(());
                        return;
                    }
                }
            }
            frame = socket.recv() => {
                let Some(Ok(frame)) = frame else { return };
                match frame {
                    Message::Text(text) => {
                        let message: Value = serde_json::from_str(&text).unwrap();
                        let authenticate = message["type"] == "authenticate";
                        if messages.send(message).is_err() {
                            return;
                        }
                        if authenticate {
                            let response = json!({
                                "type": "authenticated",
                                "payload": {"userId": "user-1"},
                                "timestamp": 1
                            });
                            if socket.send(Message::Text(response.to_string().into())).await.is_err() {
                                return;
                            }
                        }
                    }
                    Message::Close(_) => return,
                    _ => {}
                }
            }
        }
    }
}

fn snapshot(hand_id: &str) -> Value {
    json!({
        "type": "snapshot",
        "snapshot": {
            "tableId": "default",
            "handId": hand_id,
            "phase": "betting",
            "street": "preflop",
            "pot": 3,
            "board": [],
            "toActSeat": 1,
            "turnEndsAtMs": 123_456,
            "youSeat": 1,
            "seats": [{
                "seat": 1, "name": "tester", "stack": 200, "bet": 0,
                "status": "active", "isTurn": true, "cards": ["7d", "Ad"]
            }],
            "allowed": {
                "fold": true, "check": false, "call": 2,
                "betRaise": {"minTo": 4, "maxTo": 200}, "allIn": 200
            },
            "mode": "bot",
            "isObserver": false
        },
        "timestamp": 1
    })
}

async fn expect_closed(events: &mut broadcast::Receiver<GameEvent>) -> Vec<String> {
    timeout(DEADLINE, async {
        let mut errors = Vec::new();
        loop {
            match events.recv().await.unwrap() {
                GameEvent::ConnectionError(error) => errors.push(error),
                GameEvent::Closed => return errors,
                event => panic!("unexpected event while awaiting closure: {event:?}"),
            }
        }
    })
    .await
    .expect("receiver termination did not emit Closed")
}

async fn assert_disconnected(game: &GameSession) {
    assert!(
        game.is_closed().await,
        "receiver termination left session open"
    );
    assert!(
        game.snapshot().is_none(),
        "disconnected snapshot remained valid"
    );
    assert!(matches!(game.call().await, Err(Error::Closed)));
    assert!(matches!(
        game.wait_for_snapshot(0, Some(QUIET)).await,
        Err(Error::Closed)
    ));
    assert!(matches!(
        game.wait_for_turn(Some(QUIET)).await,
        Err(Error::Closed)
    ));
}

async fn termination_regression(command: Command, diagnostic: Option<&str>) {
    let mut server = TestServer::spawn().await;
    let (_client, game, socket) = server.joined().await;
    let mut events = game.subscribe_events();
    socket.command(command).await;
    let errors = expect_closed(&mut events).await;
    assert_disconnected(&game).await;
    match diagnostic {
        Some(expected) => {
            assert_eq!(errors.len(), 1, "expected one connection error: {errors:?}");
            assert!(errors[0].contains(expected), "wrong diagnostic: {errors:?}");
        }
        None => assert!(
            errors.is_empty(),
            "graceful close reported an error: {errors:?}"
        ),
    }
    game.leave().await.unwrap();
}

#[tokio::test]
async fn remote_graceful_close_invalidates_snapshot_and_rejects_actions() {
    termination_regression(Command::Close, None).await;
}

#[tokio::test]
async fn abrupt_eof_closes_session_and_reports_receive_error() {
    termination_regression(Command::Drop, Some("WebSocket receive failed")).await;
}

#[tokio::test]
async fn invalid_json_closes_session_and_invalidates_snapshot() {
    termination_regression(
        Command::Text("{invalid JSON".to_owned()),
        Some("invalid JSON"),
    )
    .await;
}

#[tokio::test]
async fn invalid_snapshot_closes_session_and_invalidates_previous_snapshot() {
    let mut malformed = snapshot("hand-2");
    malformed["snapshot"]["seats"][0]["stack"] = json!({"invalid": true});
    termination_regression(
        Command::Text(malformed.to_string()),
        Some("invalid snapshot"),
    )
    .await;
}

#[tokio::test]
async fn pending_snapshot_turn_and_event_waiters_get_closed() {
    let mut server = TestServer::spawn().await;
    let (_client, game, mut socket) = server.joined().await;
    game.call().await.unwrap();
    socket.expect_type("player_action").await;
    let snapshot_waiter = game.wait_for_snapshot(game.snapshot_version(), None);
    let turn_waiter = game.wait_for_turn(None);
    let event_waiter = game.live_rejoin(Some("default"));
    tokio::pin!(snapshot_waiter, turn_waiter, event_waiter);

    // Poll each waiter before closing; none may finish with the existing snapshot.
    assert!(timeout(QUIET, &mut snapshot_waiter).await.is_err());
    assert!(timeout(QUIET, &mut turn_waiter).await.is_err());
    assert!(timeout(QUIET, &mut event_waiter).await.is_err());
    socket.expect_type("live_rejoin").await;
    socket.command(Command::Close).await;

    assert!(matches!(
        timeout(DEADLINE, snapshot_waiter).await.unwrap(),
        Err(Error::Closed)
    ));
    assert!(matches!(
        timeout(DEADLINE, turn_waiter).await.unwrap(),
        Err(Error::Closed)
    ));
    assert!(matches!(
        timeout(DEADLINE, event_waiter).await.unwrap(),
        Err(Error::Closed)
    ));
    // Closed takes precedence even when an action was already sent for the turn.
    assert_disconnected(&game).await;
}

#[tokio::test]
async fn reconnect_holds_actions_until_a_fresh_snapshot_arrives() {
    let mut server = TestServer::spawn().await;
    let (_client, game, socket) = server.joined().await;
    let mut events = game.subscribe_events();
    socket.command(Command::Close).await;
    expect_closed(&mut events).await;
    let mut reconnect = tokio::spawn({
        let game = game.clone();
        async move { game.reconnect().await }
    });
    let mut new_socket = server.accept_join().await;
    assert!(timeout(QUIET, &mut reconnect).await.is_err());
    assert!(
        game.snapshot().is_none(),
        "reconnect exposed an old snapshot"
    );
    assert!(
        matches!(game.call().await, Err(Error::IllegalAction(_))),
        "action accepted before fresh snapshot"
    );
    new_socket.assert_no_messages().await;

    new_socket.snapshot("hand-2").await;
    let fresh = timeout(DEADLINE, reconnect)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(fresh.hand_id.as_deref(), Some("hand-2"));
    assert!(!game.is_closed().await);
    assert_eq!(game.snapshot().unwrap().hand_id.as_deref(), Some("hand-2"));
    game.call().await.unwrap();
    let action = new_socket.expect_type("player_action").await;
    assert_eq!(action["payload"]["handId"], "hand-2");
    assert!(matches!(game.call().await, Err(Error::StaleTurn)));
    new_socket.assert_no_messages().await;
    game.leave().await.unwrap();
}

#[tokio::test]
async fn reconnect_preserves_same_turn_duplicate_guard_without_replay() {
    let mut server = TestServer::spawn().await;
    let (_client, game, mut socket) = server.joined().await;
    game.call().await.unwrap();
    socket.expect_type("player_action").await;
    let mut reconnect = tokio::spawn({
        let game = game.clone();
        async move { game.reconnect().await }
    });
    let mut new_socket = server.accept_join().await;
    assert!(timeout(QUIET, &mut reconnect).await.is_err());
    assert!(game.snapshot().is_none());
    assert!(matches!(game.call().await, Err(Error::IllegalAction(_))));
    new_socket.assert_no_messages().await;

    // A new connection is not a new authoritative turn.
    new_socket.snapshot("hand-1").await;
    let fresh = timeout(DEADLINE, reconnect)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(fresh.hand_id.as_deref(), Some("hand-1"));
    assert!(!game.is_closed().await);
    new_socket.assert_no_messages().await;
    assert!(matches!(game.call().await, Err(Error::StaleTurn)));
    new_socket.assert_no_messages().await;

    let baseline = game.snapshot_version();
    new_socket.snapshot("hand-2").await;
    game.wait_for_snapshot(baseline, None).await.unwrap();
    game.call().await.unwrap();
    let action = new_socket.expect_type("player_action").await;
    assert_eq!(action["payload"]["handId"], "hand-2");
    new_socket.assert_no_messages().await;
    game.leave().await.unwrap();
}
