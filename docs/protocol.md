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

## Typed seat state and actions

The observed seat schema includes `seat`, `name`, `stack`, `bet`, `lastAction`,
`status`, `isBot`, `userId`, `rating`, `tournamentPoints`, `avatarUrl`, `isDealer`,
`isSB`, `isBB`, `isTurn`, `cards`, `isDealtOut`, and `checkRaisedThisStreet`.
Position flags are independent: a heads-up dealer can also be the small blind.
Missing flags default to false. Empty-seat `name: null` becomes an empty string.
Seat user IDs accept numbers or strings and normalize to optional strings.
Ratings and tournament points retain fractional values.

`LastAction` retains its `type` as a string, including unknown action names:

```json
{"type":"raise","amount":20,"to":40}
```

`target_total()` returns `to`, falling back to `amount`. `display_amount()` uses
`amount` for a call and the target total for bet, raise, betraise, allin, and
all-in. Other kinds have no display amount. A folded status without an explicit
action does not synthesize an action. Card codes remain strings, including hidden
placeholders such as `X`. Mid-hand hand-rank labels are calculated client-side,
not supplied by a new snapshot field or evaluated by this library.

`betRaise.minTo` can be null or absent. Its public type is `Option<u64>`.
Without a known minimum, normal bets and raises fail local validation, even at
`maxTo`. The separate all-in action remains usable only when `allowed.allIn`
is present. No meaning for a null minimum is inferred.

## Hand completion

A street transition changes street, pot, and actor state but is not a completed
hand. `is_hand_complete()` requires `phase: "handComplete"` and a result.
`is_all_in_runout()` detects `phase: "runout"` or the result's `allInRunout` flag.
Consumers should announce winners from a known authoritative result, not from a
street transition or a changed pot alone.

`TableSnapshot::is_winner(seat)` identifies whether a one-based seat is a winner
of the completed hand. It uses the fold winner, showdown winner list, or payout
recipients. All split and side-pot winners count, even if their net chip change
is negative. Refunds alone do not count. This helper reads the result directly;
it does not add a synthetic wire field to seats or cache a flag across hands.

It returns false before hand completion, for invalid seat numbers, and when no
known result identifies the seat as a winner. False is not proof of a loss when
results are absent, incomplete, or unknown. The result can identify a seat even
when that seat is absent from the snapshot's seat list.

```rust
for player in &snapshot.seats {
    let is_winner = snapshot.is_winner(player.seat);
    // Use is_winner to mark this player in the completed-hand view.
}
```

The observed discriminators are exactly `foldWin` and `showdown`:

```json
{
  "type": "foldWin",
  "winnerSeat": 1,
  "pot": 60,
  "chipsWonBySeat": [20, -20]
}
```

The following synthetic example illustrates separate main-pot and side-pot
awards. It is a test fixture, not a captured authenticated server message:

```json
{
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
  "refunds": [],
  "winnerSeats": [1, 2],
  "chipsWonBySeat": [200, 100, -300],
  "allInRunout": true,
  "hands": [{"seat": 1, "name": "player", "label": "PAIR", "cards": []}]
}
```

- `pots[0]` is the main pot; later entries are side pots. Eligibility is per pot.
- Optional `Payout::pot_index` identifies a pot. Multiple recipients for the same
  index indicate a split. One seat can receive awards from several pots.
- Embedded-engine paths omit `potIndex` and include `pot` as an amount instead.
  Neither equal pot amounts nor payout ordering establish an index. The library
  does not guess one. `payouts_for_pot()` matches only explicit indices.
- Refunds return uncalled chips. They are not winnings and are excluded from
  `gross_payout_for()`.
- Payout amounts and a fold winner's pot are gross awards. `chipsWonBySeat` is
  observed as net stack changes indexed by `seat - 1`. A contribution of 100 and
  award of 300 means a gross payout of 300 and net change of +200.
- Missing payout rows produce `None`, not an invented zero or an equal share of
  the total pot. Summing awards uses checked arithmetic and returns `None` on
  overflow. A result with no known shape remains inspectable as `Unknown(Value)`.

Known results retain nested unknown fields in `raw`; unknown tags and malformed
known results retain their original JSON without disconnecting the session.
`lastFolder` and `participants` remain raw because production shapes are not
verified. The embedded engine uses participant identifiers, not seat numbers.
Missing optional collections default to empty. This is an observed, provisional
model, not a stable server contract.

Fixture accounting must distinguish pot construction stages. When `pots` includes
uncalled contributions, payouts plus refunds equal total pot amounts. When pots
are already net of refunds, payouts alone equal total pot amounts. Do not enforce
either equation on live snapshots without establishing that representation.
Eligibility, index range, recipient validity, exact split awards, and net/gross
separation are covered by deterministic tests rather than receive-loop rejection.

## Optional analysis state and seat layout

The typed snapshot also exposes `heroEquity`, `heroDecision`, `seatEquities`,
`skipHandPaused`, `unratedHand`, `unratedPracticePending`, `heroIsDealtOut`,
`blindLevel`, `autoAllIn`, `autoCheckRaise`, and `isDebug`. Optional analysis
objects retain unknown nested fields. Unknown phase, street, status, action,
and decision strings remain strings rather than closed enums.

`MAX_SEATS` defines the observed six-slot table. `seat()` finds a numbered slot.
`next_seat_number()` wraps at the last slot. `clockwise_seat_numbers_after()`
yields every other slot once, including missing or empty slots. Starting after
seat 4 yields 5, 6, 1, 2, 3. Invalid start numbers return `None`. This is visual
layout order, not poker action order.

## Raw inbound observation

`GameSession::subscribe_raw_messages()` provides a separate bounded broadcast
subscription for every successfully decoded inbound envelope, including snapshots
and messages already handled by typed events. A snapshot still produces only one
`GameEvent::Snapshot`; raw observation does not duplicate gameplay events.

Subscribe before the operation whose messages you need. The stream does not
replay messages received before subscription, including initial authentication.
Slow consumers can receive a broadcast lag error. Values preserve complete JSON
fields, including envelope fields, but not original whitespace or byte encoding.
Malformed JSON or snapshots that fail typed decoding are not sent on this stream.
Snapshot errors identify the failing field path without dumping its value.

Raw data is untrusted and may contain personal or future sensitive fields. The
channel contains inbound data only and does not expose outbound login or bearer
payloads. It is not a sanitizer: an inbound echo could still contain sensitive
values. Do not log authentication payloads or bearer tokens. Sanitize captures
before storage or sharing.

## Rebuy reconnaissance

Evidence checked on 2026-09-11 from the [public homepage](https://www.gambit.com)
and its [first-party entry bundle](https://www.gambit.com/_expo/static/js/web/entry-365b1cacbdb00addcf0210f63f86955b.js).
The entry bundle SHA-256 is
`08700dea3ec5fa3e6be90d7e42990c08faaa1ef682c3b684777bdac1a48aaf99`.
No authenticated game or mutating server request was used for this research.

The shared client's `rebuy()` sends:

```json
{
  "type": "rebuy",
  "payload": {},
  "timestamp": 1787307014227,
  "actionId": "rs-session:1787307014227:3"
}
```

Its shared sender adds a unique action ID. There is no amount, table ID, hand ID,
or user ID in the payload. `set_buyin` is a different command. The sender does
not queue rebuy while disconnected. No rebuy-specific success or rejection
message was found; generic snapshot and error handlers do exist.

`GameSession::rebuy()` sends this command without waiting for an invented
acknowledgement. `Ok(())` means a successful WebSocket send, not confirmed chip
restoration. It does not change the local stack. Observe later authoritative
snapshots for the outcome. It never retries or replays on reconnect. Repeated
explicit calls remain separate requests; server idempotency is unverified.

As a local safety policy, the method requires an open session, a seated hero,
a matching joined table, a zero stack, and no active hand. An explicit all-in
status is rejected even if cards are hidden. The observed UI considers nonempty cards with
a non-folded status to be an active hand. It also gates its button by active
screen/connection ownership, which is a UI concern rather than an extra wire
field. These guards do not establish all remote server rules.

The shared friends/live screen uses the generic command. Live-bust reservation
recovery separately uses `live_rejoin`; that API remains distinct. The online
bot screen's Rebuy button reconnects and rejoins, while offline bot mode resets
its local engine. Generic rebuy is not documented as a replacement for those
flows or as tournament support. The offline engine replenishes to its configured
buy-in, but the remote amount, eligibility, rejection schema, timing, and
idempotency have not been verified.

### Confidence and remaining captures

- **Public schema observed:** seat metadata, optional snapshot analysis fields,
  and nullable raise minimum.
- **Public code observed:** rebuy envelope, UI guards, separate recovery flows,
  `foldWin`/`showdown` tags, and embedded-engine result structures.
- **Mock tested:** typed decoding, exact outbound rebuy shape, no automatic replay,
  raw stream delivery, safe nullable-minimum handling, and result accounting.
- **Not authenticated-fixture verified:** production payout pot identity, refund
  representation, odd chips, multiple side pots, participant identifiers, card
  visibility at completion, result timing, and differences between table modes.

Real sanitized fold, showdown, split, side-pot, refund, and rebuy captures are
still required before the hand-result API or rebuy outcome semantics are called
stable. Do not treat synthetic fixtures as production evidence.

## HTTP transport decision

Keep reqwest for login. Its asynchronous implementation already runs on Tokio
and uses Hyper and Rustls. Gameplay already uses Tokio TCP streams through
`tokio-tungstenite`; replacing reqwest cannot speed up that path.

Tokio TCP alone does not implement HTTP or TLS. A lower-level replacement needs
Hyper, connection adaptation, TLS roots and server-name validation, response-body
collection, JSON handling, and equivalent timeout/error behavior. The current
client uses a reusable reqwest client for one login endpoint. No measured
latency, memory, or binary-size problem justifies that maintenance cost. This is
an architecture assessment, not a performance benchmark.
