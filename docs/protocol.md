# Gambit web protocol

This crate implements protocol behavior observed from Gambit's first-party web client and documented by the Python reference in `../gambit-py`. It is not a supported public API and can change without notice.

## Authentication

Login uses:

```text
POST https://www.gambit.com/api/twirp/gambit.v1.AuthService/Login
Content-Type: application/json
```

```json
{"email":"<GAMBIT_USERNAME>","password":"<GAMBIT_PASSWORD>"}
```

A successful response contains `token` and `user`, with optional `expiresIn` and `isNewUser`. The crate does not include response bodies in authentication errors.

## WebSocket startup

The default endpoint is `wss://www.gambit.com/ws`, with `Origin: https://www.gambit.com`. Text frames use this envelope:

```json
{
  "type": "message_type",
  "payload": {},
  "timestamp": 1787307014227,
  "actionId": "optional-session:timestamp:sequence"
}
```

The client sends `client_capabilities`, then `authenticate` with the bearer token and a client session ID. It waits for `authenticated` before returning the game session. It sends an application-level `ping` every 10 seconds by default.

Mutating messages such as `set_buyin`, `request_seat`, `stand_up`, and `player_action` carry a unique action ID. Server idempotency is unknown. The crate never automatically retries seat or gameplay actions after an ambiguous send.

## Table lifecycle

`join_table` includes `tableId`, user identity, optional avatar, timezone offset, and an optional `buyInAmount`. Bot games first send `set_buyin` and `set_bot_auto_all_in`, then join the `default` table.

Private tables use `create_friend_table` with blind and starting-chip settings. The response includes a slug, which is then joined.

Live matching sends `find_live_table`. `live_table_matched.payload.openSeat` and `request_seat.payload.position` are zero-based. The public Rust API uses one-based seat numbers.

Leaving a joined table sends `stand_up` when seated, then `sit_out` and `leave_table`, before closing the socket. Cleanup messages are best effort so local shutdown does not depend on a remote response.

## Authoritative snapshots

Server snapshot messages place state in a top-level `snapshot` field:

```json
{
  "type": "snapshot",
  "snapshot": {
    "tableId": "default",
    "handId": "hand-id",
    "phase": "betting",
    "street": "preflop",
    "pot": 3,
    "board": [],
    "toActSeat": 1,
    "youSeat": 1,
    "seats": [],
    "allowed": {
      "fold": true,
      "check": false,
      "call": 2,
      "betRaise": {"minTo": 4, "maxTo": 200},
      "allIn": 200
    }
  }
}
```

Each new snapshot replaces local state. Unknown fields are preserved in flattened raw maps. `allowed` is the only source of action legality.

## Player actions

All player actions use one wire shape, including explicit null fields:

```json
{
  "type": "player_action",
  "payload": {
    "action": "call",
    "amount": null,
    "handId": "hand-id",
    "tableId": "default"
  },
  "timestamp": 1787307014227,
  "actionId": "rs-session:1787307014227:2"
}
```

Actions are `fold`, `check`, `call`, `bet`, `raise`, and `all-in`. Bet and raise amounts are target totals. The library validates that the hero is seated, it is the hero's turn, the action is present in the latest `allowed` state, and no action was already reserved for the same turn key.

## Reconnect

Reconnect closes the previous socket, repeats capabilities and authentication, resends the stored `join_table` payload, and waits for a newer full snapshot. It never resends the previous player action.

## Known limits

Login, startup authentication, bot configuration, live matching, seating, private-table creation, snapshots, common actions, heartbeat, stand-up, leave, and reconnect have known wire representations. Parallel-game stress, invitations, chat, purchases, forced disconnect behavior, server-side idempotency, token refresh, and backoff remain unverified.
