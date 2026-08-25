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
use gambit_rs::{ClientConfig, Credentials, Error, GambitClient};
use serde_json::{Value, json};
use tokio::{
    net::TcpListener,
    sync::{Mutex, mpsc},
};
use url::Url;

#[derive(Clone)]
struct TestState {
    login_bodies: Arc<Mutex<Vec<Value>>>,
    messages: mpsc::UnboundedSender<Value>,
}

struct TestServer {
    config: ClientConfig,
    login_bodies: Arc<Mutex<Vec<Value>>>,
    messages: mpsc::UnboundedReceiver<Value>,
}

impl TestServer {
    async fn spawn() -> Self {
        let (messages_tx, messages_rx) = mpsc::unbounded_channel();
        let login_bodies = Arc::new(Mutex::new(Vec::new()));
        let state = TestState {
            login_bodies: Arc::clone(&login_bodies),
            messages: messages_tx,
        };
        let app = Router::new()
            .route("/api/twirp/gambit.v1.AuthService/Login", post(login))
            .route("/ws", any(websocket))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        Self {
            config: ClientConfig {
                http_origin: Url::parse(&format!("http://{address}")).unwrap(),
                websocket_url: Url::parse(&format!("ws://{address}/ws")).unwrap(),
                origin_header: "https://www.gambit.com".to_owned(),
                request_timeout: Duration::from_secs(2),
                websocket_timeout: Duration::from_secs(2),
                heartbeat_interval: Duration::from_secs(60),
                timezone_offset: Some(480),
            },
            login_bodies,
            messages: messages_rx,
        }
    }

    async fn next_type(&mut self, wanted: &str) -> Value {
        loop {
            let message = tokio::time::timeout(Duration::from_secs(2), self.messages.recv())
                .await
                .unwrap()
                .unwrap();
            if message["type"] == wanted {
                return message;
            }
        }
    }
}

async fn login(State(state): State<TestState>, Json(body): Json<Value>) -> Json<Value> {
    state.login_bodies.lock().await.push(body);
    Json(json!({
        "token": "secret-token",
        "expiresIn": "86400",
        "isNewUser": false,
        "user": {
            "id": "user-1",
            "username": "tester",
            "email": "person@example.com"
        }
    }))
}

async fn websocket(ws: WebSocketUpgrade, State(state): State<TestState>) -> Response {
    ws.on_upgrade(move |socket| serve_socket(socket, state))
}

async fn serve_socket(socket: WebSocket, state: TestState) {
    let (mut writer, mut reader) = socket.split();
    while let Some(Ok(frame)) = reader.next().await {
        let Message::Text(text) = frame else {
            continue;
        };
        let message: Value = serde_json::from_str(&text).unwrap();
        state.messages.send(message.clone()).unwrap();
        let response = match message["type"].as_str() {
            Some("authenticate") => Some(json!({
                "type": "authenticated",
                "payload": {"userId": "user-1"},
                "timestamp": 1
            })),
            Some("join_table") => {
                let table_id = message["payload"]["tableId"].as_str().unwrap();
                Some(snapshot(table_id, false, None))
            }
            Some("request_seat") => {
                let wire_position = message["payload"]["position"].as_u64().unwrap();
                if wire_position == 0 {
                    Some(json!({
                        "type": "seat_rejected",
                        "payload": {"position": 0, "reason": "seat_not_available"},
                        "timestamp": 1
                    }))
                } else {
                    Some(snapshot("live-table", true, Some(wire_position + 1)))
                }
            }
            Some("stand_up") => Some(snapshot(
                message["payload"]
                    .get("tableId")
                    .and_then(Value::as_str)
                    .unwrap_or("live-table"),
                false,
                None,
            )),
            Some("live_rejoin") => Some(json!({
                "type": "live_rejoin_ok",
                "payload": {"slug": "live-table", "sameTable": true},
                "timestamp": 1
            })),
            Some("create_friend_table") => Some(json!({
                "type": "friend_table_created",
                "payload": {"slug": "friend-table"},
                "timestamp": 1
            })),
            Some("find_live_table") => Some(json!({
                "type": "live_table_matched",
                "payload": {"slug": "live-table", "created": false, "openSeat": 5},
                "timestamp": 1
            })),
            _ => None,
        };
        if let Some(response) = response {
            writer
                .send(Message::Text(
                    serde_json::to_string(&response).unwrap().into(),
                ))
                .await
                .unwrap();
        }
    }
}

fn snapshot(table_id: &str, seated: bool, you_seat: Option<u64>) -> Value {
    let mode = if table_id == "live-table" {
        "live"
    } else {
        "bot"
    };
    let seats = if seated {
        json!([
            {
                "seat": you_seat,
                "name": "tester",
                "stack": 200,
                "bet": 0,
                "status": "active",
                "isTurn": true,
                "cards": ["7d", "Ad"]
            }
        ])
    } else if table_id == "live-table" {
        json!([
            {"seat": 1, "name": null, "status": "empty", "stack": 0, "bet": 0},
            {"seat": 6, "name": null, "status": "empty", "stack": 0, "bet": 0}
        ])
    } else {
        json!([
            {
                "seat": 1,
                "name": "tester",
                "stack": 200,
                "bet": 0,
                "status": "active",
                "isTurn": true,
                "cards": ["7d", "Ad"]
            }
        ])
    };
    json!({
        "type": "snapshot",
        "snapshot": {
            "tableId": table_id,
            "handId": "hand-1",
            "phase": "betting",
            "street": "preflop",
            "pot": 3,
            "board": [],
            "toActSeat": if seated { you_seat } else if table_id == "default" { Some(1) } else { None },
            "turnEndsAtMs": 123_456,
            "youSeat": if table_id == "default" { Some(1) } else { you_seat },
            "seats": seats,
            "allowed": if seated || table_id == "default" {
                json!({
                    "fold": true,
                    "check": false,
                    "call": 2,
                    "betRaise": {"minTo": 4, "maxTo": 200},
                    "allIn": 200
                })
            } else {
                Value::Null
            },
            "mode": mode,
            "isObserver": !seated && table_id == "live-table"
        },
        "timestamp": 1
    })
}

async fn authenticated_client(server: &TestServer) -> GambitClient {
    let client = GambitClient::new(
        Credentials::new("person@example.com", "password"),
        server.config.clone(),
    )
    .unwrap();
    let auth = client.authenticate().await.unwrap();
    assert_eq!(auth.user.username, "tester");
    assert!(!format!("{auth:?}").contains("secret-token"));
    client
}

#[tokio::test]
async fn authenticates_and_runs_guarded_bot_lifecycle() {
    let mut server = TestServer::spawn().await;
    let client = authenticated_client(&server).await;
    assert_eq!(
        server.login_bodies.lock().await.as_slice(),
        &[json!({"email": "person@example.com", "password": "password"})]
    );

    let game = client.create_bot_game(200, false).await.unwrap();
    assert_eq!(
        server.next_type("client_capabilities").await["payload"],
        json!({"invisibleDeployHandoff": true})
    );
    let authenticate = server.next_type("authenticate").await;
    assert_eq!(authenticate["payload"]["token"], "secret-token");
    assert!(
        authenticate["payload"]["sessionId"]
            .as_str()
            .unwrap()
            .starts_with("rs-")
    );
    assert!(
        server
            .next_type("set_buyin")
            .await
            .get("actionId")
            .is_some()
    );
    assert_eq!(
        server.next_type("set_bot_auto_all_in").await["payload"]["enabled"],
        false
    );
    assert_eq!(
        server.next_type("join_table").await["payload"]["timezoneOffset"],
        480
    );

    let state = game.snapshot().unwrap();
    assert!(state.is_our_turn());
    game.call().await.unwrap();
    let action = server.next_type("player_action").await;
    assert_eq!(
        action["payload"],
        json!({
            "action": "call",
            "amount": null,
            "handId": "hand-1",
            "tableId": "default"
        })
    );
    assert!(matches!(game.call().await, Err(Error::StaleTurn)));

    game.leave().await.unwrap();
    assert_eq!(server.next_type("stand_up").await["type"], "stand_up");
    assert_eq!(server.next_type("sit_out").await["type"], "sit_out");
    assert_eq!(server.next_type("leave_table").await["type"], "leave_table");
}

#[tokio::test]
async fn live_match_converts_suggested_seat_at_wire_boundary() {
    let mut server = TestServer::spawn().await;
    let client = authenticated_client(&server).await;

    let game_task = tokio::spawn({
        let client = client.clone();
        async move { client.create_live_game(true, None).await.unwrap() }
    });
    assert_eq!(
        server.next_type("find_live_table").await["type"],
        "find_live_table"
    );
    let join = server.next_type("join_table").await;
    assert_eq!(join["payload"]["tableId"], "live-table");
    let seat = server.next_type("request_seat").await;
    assert_eq!(seat["payload"]["position"], 5);
    assert!(seat.get("actionId").is_some());

    let game = game_task.await.unwrap();
    assert_eq!(game.snapshot().unwrap().you_seat, Some(6));
    assert_eq!(game.table_match().await.unwrap().suggested_seat(), Some(6));
    game.leave().await.unwrap();
}

#[tokio::test]
async fn seat_rejection_is_typed_and_not_retried() {
    let mut server = TestServer::spawn().await;
    let client = authenticated_client(&server).await;
    let game = client
        .join_game_by_code("live-table", false, None)
        .await
        .unwrap();
    let _ = server.next_type("join_table").await;

    let result = game.request_seat(1, None).await;
    assert!(matches!(
        result,
        Err(Error::SeatRejected {
            seat: Some(1),
            ref reason,
        }) if reason == "seat_not_available"
    ));
    let request = server.next_type("request_seat").await;
    assert_eq!(request["payload"]["position"], 0);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), server.messages.recv())
            .await
            .is_err()
    );
    game.leave().await.unwrap();
}

#[tokio::test]
async fn reconnect_reauthenticates_and_rejoins_without_replaying_action() {
    let mut server = TestServer::spawn().await;
    let client = authenticated_client(&server).await;
    let game = client.create_bot_game(200, false).await.unwrap();
    let _ = server.next_type("join_table").await;
    game.call().await.unwrap();
    let _ = server.next_type("player_action").await;

    let reconnect = tokio::spawn({
        let game = game.clone();
        async move { game.reconnect().await.unwrap() }
    });
    let _ = server.next_type("client_capabilities").await;
    let _ = server.next_type("authenticate").await;
    let join = server.next_type("join_table").await;
    assert_eq!(join["payload"]["tableId"], "default");
    let state = reconnect.await.unwrap();
    assert_eq!(state.table_id, "default");
    assert!(
        tokio::time::timeout(Duration::from_millis(50), server.messages.recv())
            .await
            .is_err()
    );

    let rejoin_task = tokio::spawn({
        let game = game.clone();
        async move { game.live_rejoin(Some("live-table")).await.unwrap() }
    });
    let request = server.next_type("live_rejoin").await;
    assert_eq!(request["payload"]["slug"], "live-table");
    let rejoin = rejoin_task.await.unwrap();
    assert!(rejoin.same_table);
    game.leave().await.unwrap();
}

#[tokio::test]
async fn heartbeat_uses_application_ping_messages() {
    let mut server = TestServer::spawn().await;
    server.config.heartbeat_interval = Duration::from_millis(20);
    let client = authenticated_client(&server).await;
    let game = client.open_game().await.unwrap();

    let ping = server.next_type("ping").await;
    assert_eq!(ping["payload"], json!({}));
    game.leave().await.unwrap();
}
