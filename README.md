# rust-MQTT

Pure-Rust MQTT client library — safety-oriented, broker-agnostic, COVESA/VISSR ready.

**RELAY spec v2.0 conformant** · ASIL-B / SIL 2 · ISO 26262 · IEC 61508 · ISO 21434

[![CI](https://github.com/SoundMatt/rust-MQTT/actions/workflows/ci.yml/badge.svg)](https://github.com/SoundMatt/rust-MQTT/actions/workflows/ci.yml)

---

## Architecture

| Module | Description |
|---|---|
| `mock` | In-process broker — zero network, ideal for unit tests |
| `virtual` | §13.7.2 alias for `mock` |
| `v3` | MQTT v3.1.1 TCP client (TCP, TLS, WebSocket) |
| `broker` | Minimal embedded TCP broker for integration tests |
| `adapt` | RELAY adapter — `adapt(client) → Node` |
| `topic` | `match_topic` — MQTT §4.7 wildcard semantics |

All backends implement the `Client` trait.

---

## Quick start

```rust
use rust_mqtt::mock::MockClient;
use rust_mqtt::{Client, QoS, SubscriberConfig};

#[tokio::main]
async fn main() {
    let client = MockClient::new();
    let mut sub = client
        .subscribe("sensors/#", QoS::AtMostOnce, SubscriberConfig::default())
        .await
        .unwrap();
    client
        .publish("sensors/temp", QoS::AtMostOnce, b"21.5".to_vec())
        .await
        .unwrap();
    let msg = sub.recv().await.unwrap();
    println!("{} = {:?}", msg.topic, msg.payload);
}
```

## Connect to a real broker

```rust
use rust_mqtt::v3::{Client, ConnectOptions};
use rust_mqtt::{Client as MqttClient, QoS, SubscriberConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = ConnectOptions::new("localhost:1883").client_id("my-sensor");
    let client = Client::connect(opts).await?;
    client.publish("sensors/temp", QoS::AtMostOnce, b"21.5".to_vec()).await?;
    client.close().await?;
    Ok(())
}
```

---

## CLI (RELAY §11)

```
rust-mqtt version --format json
rust-mqtt capabilities
rust-mqtt status --format json
rust-mqtt subscribe --broker localhost:1883 --topic sensors/# --format json
rust-mqtt send --broker localhost:1883 --topic sensors/temp --payload 21.5
rust-mqtt send --format json --mock   # NDJSON crossbar sink (stdin), no live broker
rust-mqtt convert --protocol MQTT     # relay interop driver (stdin → stdout)
```

`send`/`subscribe` connect to `--broker` over TCP (MQTT v3.1.1) by default.
Pass `--mock` to use the in-process mock broker instead — useful for local
testing without a live broker.

---

## RELAY compliance (v2.0)

| Gate | Status |
|---|---|
| `relay conform --strict` | ✅ CI gate (`relay-conform` job) |
| `relay interop` | ✅ `convert` driver implemented |
| `send --format json` NDJSON sink | ✅ Crossbar destination mode |
| x-FuSa full lifecycle | ✅ `safety` CI job |
| Requirements registry | ✅ `requirements.json` (131 requirements) |
| HARA / dFMEA / TARA | ✅ `fmea.json`, `tara.json` |
| SBOM + provenance | ✅ `sbom.json`, `provenance.json` |

---

## Safety

- Targeted standard: **ISO 26262 ASIL-B** / **IEC 61508 SIL 2**
- All safety-relevant functions carry `//fusa:req REQ-xxx-NNN` annotations
- Requirements defined in `requirements.json` (131 requirements)
- FMEA in `fmea.json`; TARA in `tara.json`
- Safety case in `safety-case.json`
- CI runs the full x-FuSa lifecycle (lint, analyze, check, trace, FMEA, TARA, qualify)

See `SAFETY_PLAN.md` for the full Software Safety Plan.

---

## Feature equivalence with go-mqtt

| Feature | go-mqtt | rust-MQTT |
|---|---|---|
| Mock in-process broker | ✅ | ✅ |
| MQTT v3.1.1 TCP client | ✅ | ✅ |
| QoS 0/1/2 | ✅ | ✅ |
| Retained messages | ✅ | ✅ |
| Last-will-and-testament | ✅ | ✅ |
| Topic wildcards §4.7 | ✅ | ✅ |
| MQTT v5 message properties | ✅ | ⚠️ struct fields only (not wire-negotiated) |
| RELAY adapt() | ✅ | ✅ |
| relay conform gate | ✅ | ✅ |
| relay interop convert | ✅ | ✅ |
| send --format json (crossbar) | ✅ | ✅ |
| subscribe --format json | ✅ | ✅ |
| BackPressurePolicy | ✅ | ✅ |
| Embedded broker | ✅ | ✅ |
| HealthProvider / MetricsProvider | ✅ | ✅ |
| COVESA VISSR bridge | planned | planned |
| REST bridge | planned | planned |
| MQTT federation bridge | planned | planned |

---

## License

Mozilla Public License 2.0 — see [LICENSE](LICENSE).
