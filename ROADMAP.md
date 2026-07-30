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
- RELAY spec v2.0 full conformance (§20 continuous conformance)

---

## Release Plan

| Version | Theme | Status |
|---|---|---|
| **v1.0** | Foundation: Client trait, mock broker, v3.1.1 TCP client, RELAY v2.0 conformance, CI, safety artefacts | ✅ |
| v1.1 | MQTT v5.0 client (`v5/`) — user properties, response topic, correlation data | planned |
| v1.2 | TLS/mTLS (`v3::ConnectOptions::tls`, `tls` feature) | planned |
| v1.3 | WebSocket transport (`v3::ConnectOptions::websocket`, `websocket` feature) | planned |
| **v1.4** | Interop testing infrastructure — live Mosquitto broker round-trip + third-party `mosquitto_pub`/`mosquitto_sub` CLI interop (`mqtt-interop` CI job) | ✅ |
| v1.5 | COVESA VISSR bridge (`bridge/vissr/`) — VSS path ↔ MQTT topic mapping | planned |
| v1.6 | REST bridge (`bridge/rest/`) — HTTP pub/sub gateway | planned |
| v1.7 | MQTT federation bridge (`bridge/mqtt/`) — broker-to-broker forwarding | planned |
| v2.0 | Stable API, full safety certification artefacts | planned |

> **Cross-protocol bridges (DDS, SOME-IP, gRPC) are not on the roadmap.**
> The RELAY `adapt()` entry point handles cross-protocol routing generically
> at the RELAY layer. This keeps rust-MQTT free of cross-protocol dependencies.

---

## Interop testing — a real gap beyond RELAY's `interop` command

RELAY's `relay interop` (`relay-conform` CI job, §20.2) checks that the
`convert` driver's JSON output is *equivalent* to the RELAY canonical
message shape — it never puts a packet on a wire, and it never runs against
an implementation rust-MQTT didn't write itself. Passing it alone doesn't
prove `v3::Client` actually speaks MQTT v3.1.1 correctly to a real broker,
or that a broker written by someone else can make sense of what
`v3::Client` sends. Until this landed, every test in this repo (`mock`,
the bundled `broker` module) only ever talked to rust-MQTT's own code.

**Done (v1.4), mirroring rust-DDS's own `tests/cyclone_interop.rs` /
`cyclone-interop` CI job as closely as the two protocols allow — see that
file's module doc comment for the detailed mapping:**

- **Real-broker round-trip** — `tests/mqtt_interop.rs`'s
  `real_broker_publish_subscribe_round_trip`: rust-MQTT's own `v3::Client`,
  in both publisher and subscriber roles, exchanging genuine MQTT wire
  traffic (real TCP, real CONNECT/PUBLISH/SUBSCRIBE packets) with a real
  Eclipse Mosquitto broker, asserting field-exact receipt (topic, payload,
  QoS, retained flag).
- **A third-party, independent oracle** — the same file's
  `rust_publisher_verified_by_mosquitto_sub` and
  `mosquitto_pub_verified_by_rust_subscriber`: publish via rust-MQTT's own
  API and independently verify with Mosquitto's own bundled `mosquitto_sub`
  CLI, and the reverse (publish via `mosquitto_pub`, verify via
  `v3::Client::subscribe`). Two implementations agreeing with each other,
  not just with themselves, is the actual interop bar — the same rationale
  as rust-DDS's live-CycloneDDS-peer deliverable.
- Gated behind the `mqtt-interop` Cargo feature (`#![cfg(feature =
  "mqtt-interop")]` — without it `tests/mqtt_interop.rs` does not even
  compile) and `#[ignore]`d on every test, so it stays out of the normal
  `cargo test`/default-CI sweep. Runs in a new `mqtt-interop` CI job: a
  GitHub Actions `services:` container running `eclipse-mosquitto` (no
  special OS privileges needed, unlike the DDS/CAN sibling repos' UDP
  multicast / SocketCAN interop harnesses) plus `mosquitto-clients` (apt)
  for the CLI tools. Probes broker reachability first and skips cleanly
  (`::notice::`, exit 0) rather than hard-failing if the broker never comes
  up — the same posture as rust-DDS's `cyclone-interop` job's Docker-image
  probe, and as each individual test's own connect-or-skip check.

---

## Guiding Principles

1. Safe Rust only — no `unsafe` blocks in library code
2. Interface-driven — swap transport without changing application code
3. MQTT §4.7 wildcard semantics enforced in all implementations
4. Safety as a first-class concern (rust-FuSa, ASIL-B / SIL 2)
5. COVESA VISSR topic conventions by default
6. Testability by default — mock broker, no network required for unit tests
7. RELAY §20 continuous conformance — every merge is conformant
