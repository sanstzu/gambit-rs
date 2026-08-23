# Gambit Rust client

An asynchronous Rust library for the browser-observed Gambit poker protocol. It provides REST authentication and one WebSocket connection per game session.

This is an unofficial client. Gambit does not publish this protocol as a supported API, so it can change without notice. Use it only with an account and gameplay that you are authorized to control.

## Credentials

Set credentials in the process environment or an uncommitted `.env` file:

```text
GAMBIT_USERNAME=you@example.com
GAMBIT_PASSWORD=replace-me
```

Process environment values take precedence over `.env`. Passwords and bearer tokens stay in memory and are redacted from debug output.

## Example

```no_run
use gambit_client::{ClientConfig, GambitClient, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let client = GambitClient::from_env(".env", ClientConfig::default())?;
    client.authenticate().await?;

    let game = client.create_bot_game(200, false).await?;
    let state = game.wait_for_turn(None).await?;
    if state.allowed.as_ref().is_some_and(|allowed| allowed.check) {
        game.check().await?;
    } else {
        game.fold().await?;
    }

    game.leave().await?;
    client.close().await;
    Ok(())
}
```

## Main API

- `GambitClient::authenticate` logs in and retains an in-memory token.
- `GambitClient::create_bot_game` configures and joins a bot game.
- `GambitClient::create_live_game` matches and joins a live table as an observer, with optional seating.
- `GambitClient::create_friend_game` creates and joins a private table.
- `GambitClient::join_game_by_code` accepts a table slug or first-party play URL.
- `GameSession::wait_for_snapshot` and `GameSession::subscribe_events` expose authoritative updates.
- `GameSession::fold`, `check`, `call`, `bet`, `raise_to`, and `all_in` validate the latest server state before sending.
- `GameSession::reconnect` reauthenticates and rejoins without retrying an action.
- `GameSession::leave` performs bounded best-effort table cleanup and closes its socket.

Public seat numbers are one-based. The library converts them to zero-based wire positions. Mutating seat and gameplay requests are never automatically retried after an ambiguous send.

## Development

The test suite uses local mock servers and does not contact Gambit:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
```

See `docs/protocol.md` for the observed message shapes and known limits.
