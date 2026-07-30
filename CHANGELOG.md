# Changelog

All notable changes to rust-MQTT are documented here.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [1.5.0] — 2026-07-30 — Audit fix pass

### Fixed

- **Critical (rust-MQTT-01)**: `read_varint` (`v3/mod.rs`) and
  `read_varint_sync` (`broker/mod.rs`) off-by-one in the Remaining Length
  decoder — a Remaining Length field with the continuation bit still set
  after 4 bytes was not rejected until a disallowed 5th byte had already
  been consumed. Both now reject before ever reading a 5th byte, per MQTT
  3.1.1/5.0 §2.2.3 (max 4-byte, ≤268,435,455 Remaining Length)
- `encode_remaining_length` (rust-MQTT-02) now refuses to encode a length
  above 268,435,455 instead of silently emitting a malformed 5+ byte
  wire packet
- `build_publish` (rust-MQTT-N2-03) now refuses to emit a QoS>0 PUBLISH
  without a Packet Identifier instead of silently dropping it and
  corrupting the wire stream (MQTT §3.3.2.2)
- `match_topic` (rust-MQTT-N2-02) now correctly expands an embedded `+`
  combined with a trailing `/#` (e.g. `a/+/#`), per MQTT §4.7.1
- HARA (rust-MQTT-N2-01): recomputed ASIL for HE-002/005/006 (ASIL-B →
  ASIL-A) and HE-004 (ASIL-A → QM) per ISO 26262-3 Table 4; corrected
  rationale strings and reconciled SG ASILs
- `MockClient::publish`/`subscribe` (rust-MQTT-06) now records the
  requested QoS on delivered messages instead of discarding it
- Declared RELAY spec version (rust-MQTT-03) reconciled to the governing
  v2.0 spec everywhere (was a 3-way disagreement between 1.10/1.11/2.0
  across code, README, ROADMAP, SAFETY_PLAN, SAFETY_MANUAL, CLAUDE.md,
  Cargo.toml, and CI)

### Changed

- **Breaking**: `client::HealthState` renamed to `client::HealthStatus`
  (enum) and `client::HealthStatus` renamed to `client::Health` (struct),
  per RELAY spec §9 canonical naming (rust-MQTT-04)
- README/CLI no longer claim MQTT v5 wire support (`v5-properties`
  removed from `capabilities`); v5 fields exist on `Message` but are not
  wire-negotiated until a v5 client ships (rust-MQTT-07)
- SAFETY_MANUAL.md §12.2 updated: QoS 2 exactly-once is implemented in
  `v3`/`broker`, no longer downgraded (rust-MQTT-05)
- Requirement count reconciled to 131 across CHANGELOG/CLAUDE.md/
  SAFETY_MANUAL.md (rust-MQTT-08); SAFETY_MANUAL.md §2 ASIL-B claim
  qualified with the §3 ASIL-D exclusion (rust-MQTT-09); CLAUDE.md
  version-history table extended through v1.4.0 (rust-MQTT-10)

### Note

- rust-MQTT-11 (capabilities `commands` list omits `connect`, adds
  non-canonical `convert`) remains open — advisory, not addressed in this
  pass.

---

## [1.4.0] — 2026-07-27 — Interop testing infrastructure

### Added

- `tests/mqtt_interop.rs` (gated behind the new `mqtt-interop` Cargo
  feature, `#[ignore]`d): real-broker round-trip and third-party-peer
  interop tests against a live Eclipse Mosquitto broker
  - `real_broker_publish_subscribe_round_trip` — rust-MQTT's own
    `v3::Client`, in both publisher and subscriber roles, exchanging
    genuine MQTT wire traffic with a real broker, field-exact receipt
  - `rust_publisher_verified_by_mosquitto_sub` /
    `mosquitto_pub_verified_by_rust_subscriber` — independent
    cross-verification against Mosquitto's own bundled `mosquitto_pub`/
    `mosquitto_sub` CLI tools, in both directions
  - Probe-then-skip-cleanly posture at both the CI-job and individual-test
    level: a broker that never becomes reachable leaves the job green
    (`::notice::`, exit 0), not red
- New `mqtt-interop` CI job: a GitHub Actions `services:` container running
  `eclipse-mosquitto`, `mosquitto-clients` (apt) for the CLI tools, mirrors
  the live-third-party-peer pattern rust-DDS's `cyclone-interop` job
  established for CycloneDDS
- `ROADMAP.md`: new "Interop testing" section documenting the capability

---

## [1.3.0] — 2026-07-27

### Fixed

- CLI `send`/`subscribe` now connect to `--broker` over a real MQTT v3.1.1 TCP
  connection by default instead of always using the in-process mock broker;
  pass `--mock` to keep the old mock-only behavior
- `v3::Client::publish()` now implements the full QoS 2 (`ExactlyOnce`)
  PUBLISH→PUBREC→PUBREL→PUBCOMP handshake instead of silently behaving like
  QoS 0; the embedded test `broker` gained matching receiver-side QoS 2
  support
- `BackPressurePolicy::DropOldest` now genuinely drains the oldest buffered
  message before enqueuing the new one (spec §10.5.3) in every
  implementation (`mock`, `v3`, the bundled `relay` module, and the `Adapt()`
  layer), instead of behaving identically to `DropNewest`
- `Adapt()`'s `NodeAdapter` no longer collapses every `send`/`close` failure
  to a fixed sentinel; it now preserves the underlying error's real
  `relay::Error` kind via `Error::kind()` (spec §5.2)
- `Error::TopicEmpty` and `Error::QoSUnsupported` now wrap
  `relay::Error::NotConnected` per spec §5.4
- `Cargo.toml` package version now matches the tagged release (was frozen at
  `1.0.0` since the very first release)

### Added

- `relay::Context` now carries an optional deadline (`with_timeout`/`done`)
  per spec §18.3; `Adapt()`'s `NodeAdapter::send` honors it, returning
  `RelayError::Timeout` promptly after the deadline expires
- `HealthStatus`/`MetricsSnapshot` field names and per-field semantics now
  mirror the RELAY spec §9/§9.1 canonical `Health`/`Metrics` shape
  (`status`, `write_count`, `deliver_count`, `drop_count`, `bytes_written`,
  `bytes_delivered`, `error_count`) so implementations of different
  protocols report comparable numbers
- `backpressure` module: a bounded ring channel that can genuinely implement
  all three RELAY back-pressure policies, used internally wherever
  `DropOldest` eviction is required

---

## [1.2.0] — 2026-06-19 — RELAY v1.11 conformance

### Changed

- `SPEC_VERSION` and `RELAY_SPEC_VERSION` bumped to `"1.11"`
- CI relay CLI pin updated to `relay@v1.11.0`
- §17.7 CLI waiver removed in v1.11: rust-MQTT already ships a full CLI — no change required

---

## [1.1.0] — 2026-06-19

### Added

- Full safety pack: 131 requirements, HARA, TARA×10, FMEA×15, GSN, DO-178C, safety manual

---

## [1.0.0] — 2026-06-19

### Added

- Core library: `Client` trait, `Subscription`, `Message`, `QoS`, `BackPressurePolicy`
- `mock` module: in-process broker with full §4.7 wildcard semantics, retained messages, peer clients
- `virtual` module: §13.7.2 alias for `mock`
- `v3` module: MQTT v3.1.1 TCP client (TCP, QoS 0/1, keepalive, LWT, CONNACK validation)
- `broker` module: minimal embedded TCP broker for integration tests
- `adapt` module: `adapt(client) → relay::Node` per RELAY §13.7
- `topic` module: `match_topic` per MQTT §4.7 (including system-topic protection)
- RELAY spec v1.10 conformance: `SPEC_VERSION = "1.10"`, `PROTOCOL_INT = 4`
- MQTT v5 message properties: response_topic, correlation_data, user_properties, content_type, expiry_interval
- CLI (`rust-mqtt`): `version`, `capabilities`, `status`, `send`, `subscribe`, `convert`
- `convert --protocol MQTT` driver for `relay interop` (exit 0/1/2 per spec §11.2)
- `send --format json` NDJSON crossbar sink (RELAY v1.8 §11.2)
- `subscribe --format json` NDJSON crossbar source
- `HealthProvider` and `MetricsProvider` traits; implemented by `MockClient`
- `Drainer` trait for close-with-drain (RELAY §9)
- 70+ unit and integration tests; 80 requirements in `requirements.json`
- FMEA (`fmea.json`), TARA (`tara.json`), safety case (`safety-case.json`)
- SBOM (`sbom.json`), build provenance (`provenance.json`)
- CI: `build-test` (Ubuntu + macOS), `relay-conform` (relay conform --strict + relay interop), `safety` (x-FuSa full lifecycle)
- Documentation: README, SAFETY_PLAN, CONTRIBUTING, SECURITY, ROADMAP, INCIDENT-RESPONSE
