use std::{sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    response::Response,
    routing::{any, post},
};
use futures_util::{SinkExt, StreamExt};
use gambit_rs::{ClientConfig, Credentials, Error, GambitClient, GameSession};
use serde_json::{Value, json};
use tokio::{
    net::TcpListener,
    sync::{Barrier, Mutex, broadcast, mpsc},
    task::JoinHandle,
    time::timeout,
};
use url::Url;

const WAIT: Duration = Duration::from_secs(2);
const QUIET: Duration = Duration::from_millis(60);

#[derive(Clone)]
struct SocketState {
    actions: mpsc::UnboundedSender<Value>,
    outbound: broadcast::Sender<Value>,
    latest: Arc<Mutex<Value>>,
}

struct TestServer {
    config: ClientConfig,
    actions: mpsc::UnboundedReceiver<Value>,
    state: SocketState,
    task: JoinHandle<()>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl TestServer {
    async fn spawn(initial: Value) -> Self {
        let (actions, received) = mpsc::unbounded_channel();
        let (outbound, _) = broadcast::channel(64);
        let state = SocketState {
            actions,
            outbound,
            latest: Arc::new(Mutex::new(initial)),
        };
        let app = Router::new()
            .route("/api/twirp/gambit.v1.AuthService/Login", post(login))
            .route("/ws", any(websocket))
            .with_state(state.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            config: ClientConfig {
                http_origin: Url::parse(&format!("http://{address}")).unwrap(),
                websocket_url: Url::parse(&format!("ws://{address}/ws")).unwrap(),
                request_timeout: WAIT,
                websocket_timeout: WAIT,
                heartbeat_interval: Duration::from_secs(60),
                ..ClientConfig::default()
            },
            actions: received,
            state,
            task,
        }
    }

    async fn game(&self) -> (GambitClient, GameSession) {
        let client = GambitClient::new(
            Credentials::new("person@example.com", "password"),
            self.config.clone(),
        )
        .unwrap();
        client.authenticate().await.unwrap();
        let game = client.create_bot_game(200, false).await.unwrap();
        (client, game)
    }

    async fn observe(&self, game: &GameSession, message: Value) {
        let version = game.snapshot_version();
        *self.state.latest.lock().await = message.clone();
        self.state.outbound.send(message).unwrap();
        game.wait_for_snapshot(version, Some(WAIT)).await.unwrap();
    }

    async fn next_action(&mut self) -> Value {
        timeout(WAIT, self.actions.recv())
            .await
            .expect("player_action was not sent")
            .expect("mock socket stopped")
    }

    async fn assert_no_action(&mut self) {
        assert!(
            timeout(QUIET, self.actions.recv()).await.is_err(),
            "unexpected duplicate player_action frame"
        );
    }
}

async fn login() -> Json<Value> {
    Json(json!({
        "token": "test-token",
        "expiresIn": "86400",
        "isNewUser": false,
        "user": {"id": "user-1", "username": "tester", "email": "person@example.com"}
    }))
}

async fn websocket(ws: WebSocketUpgrade, State(state): State<SocketState>) -> Response {
    ws.on_upgrade(move |socket| serve_socket(socket, state))
}

async fn serve_socket(socket: WebSocket, state: SocketState) {
    let (mut writer, mut reader) = socket.split();
    let mut outbound = state.outbound.subscribe();
    loop {
        let response = tokio::select! {
            frame = reader.next() => {
                let Some(Ok(Message::Text(text))) = frame else { break };
                let message: Value = serde_json::from_str(&text).unwrap();
                match message["type"].as_str() {
                    Some("authenticate") => Some(json!({
                        "type": "authenticated", "payload": {"userId": "user-1"}, "timestamp": 1
                    })),
                    Some("join_table") => Some(state.latest.lock().await.clone()),
                    Some("player_action") => {
                        if state.actions.send(message).is_err() { break; }
                        None
                    }
                    _ => None,
                }
            }
            message = outbound.recv() => {
                let Ok(message) = message else { break };
                Some(message)
            }
        };
        if let Some(response) = response
            && writer
                .send(Message::Text(response.to_string().into()))
                .await
                .is_err()
        {
            break;
        }
    }
}

fn hero_snapshot(deadline: Option<u64>) -> Value {
    let mut message = json!({
        "type": "snapshot",
        "snapshot": {
            "tableId": "default", "handId": "hand-1",
            "phase": "betting", "street": "preflop", "pot": 3, "board": [],
            "toActSeat": 1, "youSeat": 1, "mode": "bot", "isObserver": false,
            "seats": [
                {"seat": 1, "name": "tester", "stack": 200, "bet": 0,
                 "status": "active", "isTurn": true, "cards": ["7d", "Ad"]},
                {"seat": 2, "name": "opponent", "stack": 198, "bet": 2,
                 "status": "active", "isTurn": false}
            ],
            "allowed": {
                "fold": true, "check": false, "call": 2,
                "betRaise": {"minTo": 4, "maxTo": 200}, "allIn": 200
            }
        },
        "timestamp": 1
    });
    if let Some(deadline) = deadline {
        message["snapshot"]["turnEndsAtMs"] = json!(deadline);
    }
    message
}

fn opponent_snapshot(hero: &Value) -> Value {
    let mut message = hero.clone();
    message["snapshot"]["toActSeat"] = json!(2);
    message["snapshot"]["allowed"] = Value::Null;
    message["snapshot"]["seats"][0]["isTurn"] = json!(false);
    message["snapshot"]["seats"][1]["isTurn"] = json!(true);
    message
}

async fn assert_consumed(game: &GameSession) {
    assert!(matches!(game.call().await, Err(Error::StaleTurn)));
    assert!(matches!(
        game.wait_for_turn(Some(QUIET)).await,
        Err(Error::Timeout(_))
    ));
}

async fn same_street_reraise(first_deadline: Option<u64>, next_deadline: Option<u64>) {
    let initial = hero_snapshot(first_deadline);
    let mut server = TestServer::spawn(initial.clone()).await;
    let (_client, game) = server.game().await;
    game.raise_to(4).await.unwrap();
    assert_eq!(server.next_action().await["payload"]["amount"], 4);
    server.observe(&game, opponent_snapshot(&initial)).await;
    assert!(!game.snapshot().unwrap().is_our_turn());
    assert!(matches!(game.call().await, Err(Error::IllegalAction(_))));

    let mut next = hero_snapshot(next_deadline);
    next["snapshot"]["pot"] = json!(16);
    next["snapshot"]["seats"][0]["bet"] = json!(4);
    next["snapshot"]["seats"][1]["bet"] = json!(12);
    next["snapshot"]["allowed"]["call"] = json!(8);
    next["snapshot"]["allowed"]["betRaise"]["minTo"] = json!(20);
    server.observe(&game, next).await;
    game.raise_to(20)
        .await
        .expect("same-street re-raise rejected");
    let action = server.next_action().await;
    assert_eq!(action["payload"]["action"], "raise");
    assert_eq!(action["payload"]["amount"], 20);
    assert_eq!(action["payload"]["handId"], "hand-1");
    assert_consumed(&game).await;
    server.assert_no_action().await;
    game.leave().await.unwrap();
}

#[tokio::test]
async fn same_street_reraise_without_deadlines() {
    same_street_reraise(None, None).await;
}

#[tokio::test]
async fn same_street_reraise_with_unchanged_deadline() {
    same_street_reraise(Some(123_456), Some(123_456)).await;
}

#[tokio::test]
async fn same_street_reraise_with_changed_deadline() {
    same_street_reraise(Some(123_456), Some(234_567)).await;
}

#[tokio::test]
async fn observed_opponent_turn_reopens_identical_hero_snapshot() {
    let initial = hero_snapshot(None);
    let mut server = TestServer::spawn(initial.clone()).await;
    let (_client, game) = server.game().await;
    game.wait_for_turn(Some(WAIT)).await.unwrap();
    game.call().await.unwrap();
    server.next_action().await;
    assert_consumed(&game).await;
    server.observe(&game, opponent_snapshot(&initial)).await;
    assert!(matches!(
        game.wait_for_turn(Some(QUIET)).await,
        Err(Error::Timeout(_))
    ));

    let waiter = tokio::spawn({
        let game = game.clone();
        async move { game.wait_for_turn(Some(WAIT)).await }
    });
    server.observe(&game, initial).await;
    let ready = timeout(WAIT, waiter).await.unwrap().unwrap().unwrap();
    assert!(ready.is_our_turn());
    game.call()
        .await
        .expect("wait_for_turn returned a stale turn");
    server.next_action().await;
    assert_consumed(&game).await;
    server.assert_no_action().await;
    game.leave().await.unwrap();
}

#[tokio::test]
async fn hero_refreshes_never_reopen_consumed_turn() {
    let initial = hero_snapshot(None);
    let mut server = TestServer::spawn(initial.clone()).await;
    let (_client, game) = server.game().await;
    game.call().await.unwrap();
    server.next_action().await;

    let mut metadata = initial.clone();
    metadata["timestamp"] = json!(2);
    metadata["snapshot"]["seats"][0]["name"] = json!("renamed");
    metadata["snapshot"]["futureMetadata"] = json!({"revision": 2});
    let mut null_deadline = initial.clone();
    null_deadline["snapshot"]["turnEndsAtMs"] = Value::Null;
    for refresh in [
        initial.clone(),
        metadata,
        hero_snapshot(Some(123_456)),
        hero_snapshot(Some(234_567)),
        null_deadline,
        initial,
    ] {
        server.observe(&game, refresh).await;
        assert_consumed(&game).await;
    }
    server.assert_no_action().await;
    game.leave().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_actions_send_once_per_observed_turn() {
    let initial = hero_snapshot(None);
    let mut server = TestServer::spawn(initial.clone()).await;
    let (_client, game) = server.game().await;
    for turn in 0..4 {
        if turn > 0 {
            server.observe(&game, opponent_snapshot(&initial)).await;
            server.observe(&game, initial.clone()).await;
        }
        let barrier = Arc::new(Barrier::new(16));
        let mut attempts = Vec::new();
        for _ in 0..16 {
            let barrier = Arc::clone(&barrier);
            let game = game.clone();
            attempts.push(tokio::spawn(async move {
                barrier.wait().await;
                game.call().await
            }));
        }
        let mut sent = 0;
        for attempt in attempts {
            match timeout(WAIT, attempt).await.unwrap().unwrap() {
                Ok(()) => sent += 1,
                Err(Error::StaleTurn) => {}
                other => panic!("unexpected action result: {other:?}"),
            }
        }
        assert_eq!(sent, 1, "expected one reservation for turn {turn}");
        server.next_action().await;
        server.assert_no_action().await;
    }
    game.leave().await.unwrap();
}

#[tokio::test]
async fn table_hand_and_street_transitions_each_allow_another_action() {
    let mut current = hero_snapshot(None);
    let mut server = TestServer::spawn(current.clone()).await;
    let (_client, game) = server.game().await;
    game.call().await.unwrap();
    server.next_action().await;
    for (field, value) in [
        ("handId", "hand-2"),
        ("street", "flop"),
        ("tableId", "another-table"),
    ] {
        current["snapshot"][field] = json!(value);
        server.observe(&game, current.clone()).await;
        game.wait_for_turn(Some(WAIT)).await.unwrap();
        game.call().await.unwrap();
        let action = server.next_action().await;
        assert_eq!(action["payload"]["tableId"], current["snapshot"]["tableId"]);
        assert_eq!(action["payload"]["handId"], current["snapshot"]["handId"]);
        assert_consumed(&game).await;
    }
    server.assert_no_action().await;
    game.leave().await.unwrap();
}

#[tokio::test]
async fn reconnect_preserves_consumed_turn_despite_deadline_refresh() {
    let initial = hero_snapshot(Some(123_456));
    let mut server = TestServer::spawn(initial).await;
    let (_client, game) = server.game().await;
    game.call().await.unwrap();
    server.next_action().await;
    let refreshed = hero_snapshot(Some(234_567));
    *server.state.latest.lock().await = refreshed.clone();
    game.reconnect().await.unwrap();
    assert_consumed(&game).await;
    server.assert_no_action().await;

    server.observe(&game, opponent_snapshot(&refreshed)).await;
    server.observe(&game, refreshed).await;
    game.wait_for_turn(Some(WAIT)).await.unwrap();
    game.call().await.unwrap();
    server.next_action().await;
    game.reconnect().await.unwrap();
    assert_consumed(&game).await;
    server.assert_no_action().await;
    game.leave().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_progression_racing_actions_does_not_replay_old_hand() {
    let mut current = hero_snapshot(None);
    let mut server = TestServer::spawn(current.clone()).await;
    let (_client, game) = server.game().await;
    game.call().await.unwrap();
    server.next_action().await;

    // Public APIs cannot pause a reservation internally. Race publication against
    // submissions instead; any accepted action must use the new, unconsumed hand.
    for hand in 2..18 {
        let mut next = current.clone();
        next["snapshot"]["handId"] = json!(format!("hand-{hand}"));
        let barrier = Arc::new(Barrier::new(17));
        let mut attempts = Vec::new();
        for _ in 0..16 {
            let game = game.clone();
            let barrier = Arc::clone(&barrier);
            attempts.push(tokio::spawn(async move {
                barrier.wait().await;
                game.call().await
            }));
        }
        barrier.wait().await;
        server.observe(&game, next.clone()).await;
        let mut sent = 0;
        for attempt in attempts {
            match timeout(WAIT, attempt).await.unwrap().unwrap() {
                Ok(()) => sent += 1,
                Err(Error::StaleTurn) => {}
                other => panic!("unexpected racing action result: {other:?}"),
            }
        }
        assert!(sent <= 1, "multiple reservations during hand transition");
        if sent == 0 {
            game.wait_for_turn(Some(WAIT)).await.unwrap();
            game.call().await.unwrap();
        }
        assert!(matches!(game.call().await, Err(Error::StaleTurn)));
        let action = server.next_action().await;
        assert_eq!(action["payload"]["handId"], next["snapshot"]["handId"]);
        server.assert_no_action().await;
        current = next;
    }
    game.leave().await.unwrap();
}
