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
use gambit_rs::{ClientConfig, Credentials, Error, GambitClient, GameEvent, HandResult};
use serde_json::{Value, json};
use tokio::{
    net::TcpListener,
    sync::{Mutex, broadcast, mpsc},
};
use url::Url;

#[derive(Clone)]
struct TestState {
    login_bodies: Arc<Mutex<Vec<Value>>>,
    messages: mpsc::UnboundedSender<Value>,
    outbound: broadcast::Sender<Value>,
}

struct TestServer {
    config: ClientConfig,
    login_bodies: Arc<Mutex<Vec<Value>>>,
    messages: mpsc::UnboundedReceiver<Value>,
    outbound: broadcast::Sender<Value>,
}

impl TestServer {
    async fn spawn() -> Self {
        let (messages_tx, messages_rx) = mpsc::unbounded_channel();
        let (outbound, _) = broadcast::channel(32);
        let login_bodies = Arc::new(Mutex::new(Vec::new()));
        let state = TestState {
            login_bodies: Arc::clone(&login_bodies),
            messages: messages_tx,
            outbound: outbound.clone(),
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
            outbound,
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
    let mut outbound = state.outbound.subscribe();
    loop {
        let frame = tokio::select! {
            frame = reader.next() => match frame {
                Some(Ok(frame)) => frame,
                _ => break,
            },
            message = outbound.recv() => {
                let Ok(message) = message else { break };
                if writer.send(Message::Text(message.to_string().into())).await.is_err() {
                    break;
                }
                continue;
            }
        };
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
                "cards": ["7d", "Ad"],
                "isDealer": true,
                "isSB": true,
                "isBB": false,
                "lastAction": {"type": "raise", "amount": 20, "to": 40}
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
                "cards": ["7d", "Ad"],
                "isDealer": true,
                "isSB": true,
                "isBB": false,
                "lastAction": {"type": "raise", "amount": 20, "to": 40}
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

async fn next_event(events: &mut broadcast::Receiver<GameEvent>) -> GameEvent {
    tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn typed_results_and_raw_envelopes_arrive_once_without_replay() {
    let server = TestServer::spawn().await;
    let client = authenticated_client(&server).await;
    let game = client.open_game().await.unwrap();
    let mut events = game.subscribe_events();
    let mut raw = game.subscribe_raw_messages();
    game.join_table("default", None).await.unwrap();
    let GameEvent::Snapshot(state) = next_event(&mut events).await else {
        panic!("snapshot expected")
    };
    let hero = state.hero().unwrap();
    assert!(hero.is_dealer && hero.is_small_blind && !hero.is_big_blind);
    assert_eq!(
        hero.last_action.as_ref().unwrap().display_amount(),
        Some(40)
    );
    assert!(!hero.raw.contains_key("isDealer"));
    assert_eq!(
        raw.recv().await.unwrap().as_ref(),
        &snapshot("default", false, None)
    );
    assert!(events.try_recv().is_err());
    assert!(raw.try_recv().is_err());

    let mut complete = snapshot("default", false, None);
    complete["snapshot"]["phase"] = json!("handComplete");
    complete["snapshot"]["toActSeat"] = Value::Null;
    complete["snapshot"]["handResult"] = json!({
        "type": "showdown",
        "pot": 700,
        "pots": [
            {"amount": 300, "eligibleSeats": [1, 2, 3]},
            {"amount": 400, "eligibleSeats": [2, 3]}
        ],
        "payouts": [
            {"seat": 1, "amount": 300, "potIndex": 0},
            {"seat": 2, "amount": 400, "potIndex": 1}
        ],
        "winnerSeats": [1, 2],
        "chipsWonBySeat": [200, 100, -300],
        "allInRunout": true
    });
    complete["futureEnvelope"] = json!({"keep": true});
    server.outbound.send(complete.clone()).unwrap();
    let GameEvent::Snapshot(state) = next_event(&mut events).await else {
        panic!("snapshot expected")
    };
    assert!(state.is_hand_complete());
    assert!(state.is_all_in_runout());
    assert!(state.is_winner(1));
    assert!(state.is_winner(2));
    assert!(!state.is_winner(3));
    let result = state.hand_result.as_ref().unwrap();
    assert_eq!(result.gross_payout_for(1), Some(300));
    assert_eq!(result.net_chips_for(1), Some(200));
    let HandResult::Showdown(showdown) = result else {
        panic!("showdown expected")
    };
    assert_eq!(showdown.payouts_for_pot(1).next().unwrap().seat, 2);
    assert_eq!(raw.recv().await.unwrap().as_ref(), &complete);
    assert!(events.try_recv().is_err());
    assert!(raw.try_recv().is_err());

    complete["snapshot"]["handResult"] = json!({"type": "future", "opaque": [1, 2]});
    server.outbound.send(complete.clone()).unwrap();
    let GameEvent::Snapshot(state) = next_event(&mut events).await else {
        panic!("snapshot expected")
    };
    assert!(!state.is_winner(1));
    let Some(HandResult::Unknown(value)) = state.hand_result.as_ref() else {
        panic!("unknown result expected")
    };
    assert_eq!(value, &complete["snapshot"]["handResult"]);
    assert_eq!(raw.recv().await.unwrap().as_ref(), &complete);

    let notification = json!({"type": "future_event", "payload": {"opaque": true}});
    server.outbound.send(notification.clone()).unwrap();
    let GameEvent::Raw(value) = next_event(&mut events).await else {
        panic!("raw event expected")
    };
    assert_eq!(value.as_ref(), &notification);
    assert_eq!(raw.recv().await.unwrap().as_ref(), &notification);
    assert!(events.try_recv().is_err());
    assert!(raw.try_recv().is_err());
    game.leave().await.unwrap();
}

#[tokio::test]
async fn nullable_raise_minimum_rejects_unverified_raise_without_sending() {
    let mut server = TestServer::spawn().await;
    let client = authenticated_client(&server).await;
    let game = client.create_bot_game(200, false).await.unwrap();
    let _ = server.next_type("join_table").await;
    let mut events = game.subscribe_events();
    let mut message = snapshot("default", false, None);
    message["snapshot"]["allowed"]["betRaise"]["minTo"] = Value::Null;
    server.outbound.send(message).unwrap();
    assert!(matches!(
        next_event(&mut events).await,
        GameEvent::Snapshot(_)
    ));
    assert!(matches!(
        game.raise_to(200).await,
        Err(Error::IllegalAction(_))
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), server.messages.recv())
            .await
            .is_err()
    );
    game.all_in().await.unwrap();
    assert_eq!(
        server.next_type("player_action").await["payload"]["action"],
        "all-in"
    );
    game.leave().await.unwrap();
}

#[tokio::test]
async fn rebuy_is_guarded_sends_empty_payload_and_is_not_replayed() {
    let mut server = TestServer::spawn().await;
    let client = authenticated_client(&server).await;
    let game = client.open_game().await.unwrap();
    assert!(matches!(game.rebuy().await, Err(Error::IllegalAction(_))));
    game.join_table("live-table", None).await.unwrap();
    let _ = server.next_type("join_table").await;
    assert!(matches!(game.rebuy().await, Err(Error::IllegalAction(_))));
    let mut events = game.subscribe_events();
    let mut message = snapshot("live-table", true, Some(6));
    server.outbound.send(message.clone()).unwrap();
    assert!(matches!(
        next_event(&mut events).await,
        GameEvent::Snapshot(_)
    ));
    assert!(matches!(game.rebuy().await, Err(Error::IllegalAction(_))));
    message["snapshot"]["seats"][0]["stack"] = json!(0);
    message["snapshot"]["seats"][0]["status"] = json!("allin");
    server.outbound.send(message.clone()).unwrap();
    assert!(matches!(
        next_event(&mut events).await,
        GameEvent::Snapshot(_)
    ));
    assert!(matches!(game.rebuy().await, Err(Error::IllegalAction(_))));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), server.messages.recv())
            .await
            .is_err()
    );

    message["snapshot"]["seats"][0]["status"] = json!("folded");
    server.outbound.send(message).unwrap();
    assert!(matches!(
        next_event(&mut events).await,
        GameEvent::Snapshot(_)
    ));
    // The mock sends no acknowledgement: a successful send is not a grant.
    game.rebuy().await.unwrap();
    let request = server.next_type("rebuy").await;
    assert_eq!(request["payload"], json!({}));
    assert!(request["timestamp"].is_u64());
    assert!(request["actionId"].as_str().unwrap().starts_with("rs-"));
    assert_eq!(game.snapshot().unwrap().hero().unwrap().stack, 0);
    game.rebuy().await.unwrap();
    let second = server.next_type("rebuy").await;
    assert_eq!(second["payload"], json!({}));
    assert_ne!(request["actionId"], second["actionId"]);
    assert_eq!(second.as_object().unwrap().len(), 4);
    game.reconnect().await.unwrap();
    let _ = server.next_type("join_table").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), server.messages.recv())
            .await
            .is_err()
    );
    game.leave().await.unwrap();
    assert!(matches!(game.rebuy().await, Err(Error::Closed)));
}

#[tokio::test]
async fn rebuy_rejects_missing_seats_stale_tables_and_hidden_all_in_hands() {
    let mut server = TestServer::spawn().await;
    let client = authenticated_client(&server).await;
    let game = client.join_table("live-table", None).await.unwrap();
    let _ = server.next_type("join_table").await;
    let mut events = game.subscribe_events();
    let mut base = snapshot("live-table", true, Some(6));
    base["snapshot"]["seats"][0]["stack"] = json!(0);
    base["snapshot"]["seats"][0]["cards"] = Value::Null;
    for change in ["missing", "empty", "observer", "other-table", "allin"] {
        let mut message = base.clone();
        match change {
            "missing" => message["snapshot"]["seats"] = json!([]),
            "empty" => message["snapshot"]["seats"][0]["status"] = json!("empty"),
            "observer" => message["snapshot"]["isObserver"] = json!(true),
            "other-table" => message["snapshot"]["tableId"] = json!("stale-table"),
            "allin" => message["snapshot"]["seats"][0]["status"] = json!("allin"),
            _ => unreachable!(),
        }
        server.outbound.send(message).unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            GameEvent::Snapshot(_)
        ));
        assert!(
            matches!(game.rebuy().await, Err(Error::IllegalAction(_))),
            "{change}"
        );
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(50), server.messages.recv())
            .await
            .is_err()
    );
    base["snapshot"]["phase"] = json!("waiting");
    base["snapshot"]["toActSeat"] = Value::Null;
    server.outbound.send(base).unwrap();
    assert!(matches!(
        next_event(&mut events).await,
        GameEvent::Snapshot(_)
    ));
    game.rebuy().await.unwrap();
    assert_eq!(server.next_type("rebuy").await["payload"], json!({}));
    game.leave().await.unwrap();
}

#[tokio::test]
async fn raw_observation_includes_typed_notifications_and_fold_completion() {
    let server = TestServer::spawn().await;
    let client = authenticated_client(&server).await;
    let game = client.join_table("default", None).await.unwrap();
    let mut events = game.subscribe_events();
    let mut raw = game.subscribe_raw_messages();
    let warning = json!({"type": "afk_warning", "payload": {"handsRemaining": 2}, "future": true});
    server.outbound.send(warning.clone()).unwrap();
    let GameEvent::AfkWarning(value) = next_event(&mut events).await else {
        panic!("warning expected")
    };
    assert_eq!(value.hands_remaining, 2);
    assert_eq!(raw.recv().await.unwrap().as_ref(), &warning);
    let mut complete = snapshot("default", false, None);
    complete["snapshot"]["phase"] = json!("handComplete");
    complete["snapshot"]["handResult"] =
        json!({"type": "foldWin", "winnerSeat": 1, "pot": 60, "chipsWonBySeat": [20, -20]});
    server.outbound.send(complete.clone()).unwrap();
    let GameEvent::Snapshot(state) = next_event(&mut events).await else {
        panic!("snapshot expected")
    };
    assert!(matches!(
        state.hand_result.as_ref(),
        Some(HandResult::FoldWin(_))
    ));
    assert_eq!(
        state.hand_result.as_ref().unwrap().net_chips_for(1),
        Some(20)
    );
    assert_eq!(raw.recv().await.unwrap().as_ref(), &complete);
    assert!(events.try_recv().is_err());
    assert!(raw.try_recv().is_err());
    game.leave().await.unwrap();
}
