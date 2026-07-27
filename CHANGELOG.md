# Changelog

All notable changes to rust-MQTT are documented here.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

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

- Full safety pack: 124 requirements, HARA, TARA×10, FMEA×15, GSN, DO-178C, safety manual

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
