# Changelog

All notable changes to rust-MQTT are documented here.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [Unreleased] — RELAY v1.11 conformance

### Changed

- `SPEC_VERSION` and `RELAY_SPEC_VERSION` bumped to `"1.11"`
- CI relay CLI pin updated to `relay@v1.11.0`
- §17.7 CLI waiver removed in v1.11: rust-MQTT already ships a full CLI — no change required

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
