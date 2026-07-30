# rust-MQTT — Claude session guide

Repo: `github.com/SoundMatt/rust-MQTT`
Local path: `/Users/matt/Documents/Coding/SoundMatt/rust-MQTT`

## Project overview

A pure-Rust MQTT client library with swappable transport backends.
Safety-oriented, broker-agnostic, COVESA/VISSR ready.
Targets **RELAY spec v2.0** with **ASIL-B / SIL 2** safety annotations.

| Module | What it is |
|---|---|
| `src/lib.rs` | Library root, re-exports |
| `src/client.rs` | `Client` trait, `Subscription`, `BackPressurePolicy` |
| `src/message.rs` | `Message`, `QoS`, `UserProperty` |
| `src/topic.rs` | `match_topic` — MQTT §4.7 wildcard semantics |
| `src/adapt.rs` | `adapt(client) → relay::Node` |
| `src/relay.rs` | Bundled RELAY types (mirrors RELAY spec §18.3) |
| `src/error.rs` | `Error` with RELAY sentinel mapping |
| `src/mock/` | In-process broker — use for unit tests |
| `src/virtual/` | §13.7.2 alias for `mock` |
| `src/v3/` | MQTT v3.1.1 TCP client |
| `src/broker/` | Minimal embedded TCP broker for integration tests |
| `src/bin/main.rs` | RELAY CLI (§11): version, capabilities, status, send, subscribe, convert |

## Per-PR checklist

1. `git checkout main && git pull origin main`
2. `git checkout -b fix/<area>-<short>` or `feat/<area>-<short>`
3. Implement + tests.
4. `cargo build --all-targets`
5. `cargo fmt --check`
6. `cargo clippy --all-targets -- -D warnings`
7. `cargo test --locked`
8. Commit with DCO sign-off (see style below).
9. `git push origin <branch>`, open PR targeting `main`.
10. Wait for all CI checks green (build-test, relay-conform, safety), then merge (squash).
11. Tag patch/minor releases after merge.

## Commit message style

```
type(scope): short summary

Body explaining *why*, not what. Reference relevant ROADMAP.md items.

Signed-off-by: Matt Jones <matt@jellybaby.com>
```

Use git heredoc to avoid shell escaping issues:
```bash
git commit -m "$(cat <<'COMMIT'
feat(mock): add inject() for direct message delivery

Allows tests to simulate broker-side retained delivery
without going through the publish path.

Signed-off-by: Matt Jones <matt@jellybaby.com>
COMMIT
)"
```

## Rust conventions

- Sentinel errors in `error.rs` — use `Error::Closed`, `Error::TopicEmpty`, etc.
- `match_topic` is the canonical §4.7 implementation — do not duplicate it.
- `mock::MockClient` is the default test backend.
- All public API must have tests; `cargo test` must pass.
- No `unsafe` blocks in library code.
- `cargo fmt --check` and `cargo clippy -D warnings` must pass before pushing.

## Requirements traceability

Every safety-relevant function MUST carry `//fusa:req REQ-xxx-NNN` annotations.
Requirements are defined in `requirements.json`. Never reuse or renumber IDs.

## RELAY spec

- `SPEC_VERSION = "2.0"` (always matches `spec/version.json` in SoundMatt/RELAY)
- Verify with: `relay conform --strict ./target/release/rust-mqtt`
- Interop: `relay interop --protocol MQTT --impl ./target/release/rust-mqtt`

## Version history

| Tag | Highlights |
|---|---|
| v1.0.0 | Foundation: Client trait, mock broker, v3.1.1 TCP client, embedded broker, RELAY v1.10 conformance, 80 requirements, ASIL-B safety artefacts |
| v1.1.0 | Full safety pack: 131 reqs, HARA, TARA×10, FMEA×15, GSN safety case, boundary diagram, safety manual, ISO/IEC/DO compliance matrices |
| v1.2.0 | RELAY v2.0 conformance (§17.7 CLI waiver removed — no impl change needed) |
| v1.3.0 | Full QoS 2 exactly-once handshake in `v3` client + receiver-side QoS 2 in embedded broker |
| v1.4.0 | Real-broker CLI (`send`/`subscribe` over TCP), feature-gated live-Mosquitto interop tests |
| v1.5.0 | Audit fix pass: `read_varint`/`read_varint_sync` off-by-one (5th continuation byte) reject fixed, `encode_remaining_length`/`build_publish` bounds guards, `match_topic` `+`+`/#` fix, `HealthState`/`HealthStatus` naming corrected to spec §9 (`Health`/`HealthStatus`), HARA ASIL recomputation, doc/version reconciliation to spec v2.0 |
