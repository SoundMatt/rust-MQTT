# rust-MQTT Roadmap

## Vision

rust-MQTT is a modern, Rust-native MQTT client library built for safety-critical
vehicle-signal and IoT applications.

The project focuses on:

- Clean, swappable transport interface (mock → v3 → v5)
- COVESA VISSR / W3C VISSv2 compatibility
- Safety-oriented development with rust-FuSa (x-FuSa spec) annotations
- Broker-agnostic design (Mosquitto, HiveMQ, EMQX, AWS IoT, …)
- Zero unsafe, minimal external dependencies
- RELAY spec v1.11 full conformance (§20 continuous conformance)

---

## Release Plan

| Version | Theme | Status |
|---|---|---|
| **v1.0** | Foundation: Client trait, mock broker, v3.1.1 TCP client, RELAY v1.11 conformance, CI, safety artefacts | ✅ |
| v1.1 | MQTT v5.0 client (`v5/`) — user properties, response topic, correlation data | planned |
| v1.2 | TLS/mTLS (`v3::ConnectOptions::tls`, `tls` feature) | planned |
| v1.3 | WebSocket transport (`v3::ConnectOptions::websocket`, `websocket` feature) | planned |
| v1.4 | COVESA VISSR bridge (`bridge/vissr/`) — VSS path ↔ MQTT topic mapping | planned |
| v1.5 | REST bridge (`bridge/rest/`) — HTTP pub/sub gateway | planned |
| v1.6 | MQTT federation bridge (`bridge/mqtt/`) — broker-to-broker forwarding | planned |
| v2.0 | Stable API, full safety certification artefacts | planned |

> **Cross-protocol bridges (DDS, SOME-IP, gRPC) are not on the roadmap.**
> The RELAY `adapt()` entry point handles cross-protocol routing generically
> at the RELAY layer. This keeps rust-MQTT free of cross-protocol dependencies.

---

## Guiding Principles

1. Safe Rust only — no `unsafe` blocks in library code
2. Interface-driven — swap transport without changing application code
3. MQTT §4.7 wildcard semantics enforced in all implementations
4. Safety as a first-class concern (rust-FuSa, ASIL-B / SIL 2)
5. COVESA VISSR topic conventions by default
6. Testability by default — mock broker, no network required for unit tests
7. RELAY §20 continuous conformance — every merge is conformant
