use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::{Map, Value, json};
use uuid::Uuid;

use crate::{Action, Error, Result, TableSnapshot};

#[derive(Debug)]
pub(crate) struct ActionIdGenerator {
    session_id: String,
    counter: u64,
}

impl ActionIdGenerator {
    pub(crate) fn create() -> Self {
        Self {
            session_id: format!("rs-{}", Uuid::new_v4().simple()),
            counter: 0,
        }
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn next(&mut self, timestamp_ms: u64) -> String {
        self.counter += 1;
        format!("{}:{timestamp_ms}:{}", self.session_id, self.counter)
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Envelope<'a> {
    #[serde(rename = "type")]
    message_type: &'a str,
    payload: Value,
    timestamp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    action_id: Option<&'a str>,
}

pub(crate) fn envelope<'a>(
    message_type: &'a str,
    payload: Value,
    timestamp: u64,
    action_id: Option<&'a str>,
) -> Envelope<'a> {
    Envelope {
        message_type,
        payload,
        timestamp,
        action_id,
    }
}

pub(crate) fn player_action_payload(
    action: Action,
    table_id: &str,
    hand_id: Option<&str>,
    amount: Option<u64>,
) -> Value {
    json!({
        "action": action.as_str(),
        "amount": amount,
        "handId": hand_id,
        "tableId": table_id,
    })
}

pub(crate) fn now_ms() -> u64 {
    let milliseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(milliseconds).unwrap_or(u64::MAX)
}

#[derive(Clone, Debug)]
pub(crate) struct ServerMessage {
    pub message_type: String,
    pub payload: Value,
    pub snapshot: Option<TableSnapshot>,
    pub raw: Value,
}

pub(crate) fn decode_server_message(text: &str) -> Result<ServerMessage> {
    let raw: Value = serde_json::from_str(text)?;
    let object = raw
        .as_object()
        .ok_or_else(|| Error::Protocol("server message is not an object".to_owned()))?;
    let message_type = string_field(object, "type")?.to_owned();
    let payload = object.get("payload").cloned().unwrap_or_else(|| json!({}));
    let snapshot = if message_type == "snapshot" {
        let value = object
            .get("snapshot")
            .ok_or_else(|| Error::Protocol("snapshot message has no snapshot field".to_owned()))?;
        Some(
            serde_json::from_value(value.clone())
                .map_err(|_| Error::Protocol("server sent an invalid snapshot".to_owned()))?,
        )
    } else {
        None
    };
    Ok(ServerMessage {
        message_type,
        payload,
        snapshot,
        raw,
    })
}

fn string_field<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a str> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Protocol(format!("server message has no {field}")))
}

#[cfg(test)]
mod tests {
    use serde_json::to_value;

    use super::*;

    #[test]
    fn envelope_matches_observed_shape() {
        let value = to_value(envelope(
            "set_buyin",
            json!({"amount": 200}),
            123,
            Some("session:123:1"),
        ))
        .unwrap();
        assert_eq!(
            value,
            json!({
                "type": "set_buyin",
                "payload": {"amount": 200},
                "timestamp": 123,
                "actionId": "session:123:1"
            })
        );
    }

    #[test]
    fn action_ids_are_monotonic() {
        let mut ids = ActionIdGenerator {
            session_id: "rs-session".to_owned(),
            counter: 0,
        };
        assert_eq!(ids.next(100), "rs-session:100:1");
        assert_eq!(ids.next(100), "rs-session:100:2");
    }

    #[test]
    fn player_action_includes_null_fields() {
        assert_eq!(
            player_action_payload(Action::Check, "table", Some("hand"), None),
            json!({
                "action": "check",
                "amount": null,
                "handId": "hand",
                "tableId": "table"
            })
        );
    }
}
