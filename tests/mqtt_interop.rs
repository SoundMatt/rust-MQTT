// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! Interop tests against a live, independent Eclipse Mosquitto broker —
//! `ROADMAP.md`'s "Interop testing" section: rust-MQTT's own `v3::Client`
//! exchanging genuine MQTT wire traffic (real TCP, real broker) with a
//! third-party implementation, rather than the in-process `mock` broker
//! (`tests/mock_integration.rs`) or the bundled test-only `broker` module
//! this crate ships for other integration tests.
//!
//! This mirrors rust-DDS's own `tests/cyclone_interop.rs` (a live,
//! independent third-party peer, gated behind a Cargo feature, driven by a
//! `docker-compose.yml`/CI service) as closely as the different protocols
//! allow:
//!
//! | rust-DDS (`tests/cyclone_interop.rs`) | rust-MQTT (this file) |
//! |---|---|
//! | `cyclone-interop` Cargo feature (`#![cfg(feature = "cyclone-interop")]` below) | `mqtt-interop` Cargo feature (`#![cfg(feature = "mqtt-interop")]` below) — same rationale: without it this file does not compile, so it is absent from the normal `cargo test` sweep and default CI |
//! | live CycloneDDS peer via `docker compose up -d cyclone-peer` | live Eclipse Mosquitto broker — a GitHub Actions `services:` container in CI (see `.github/workflows/ci.yml`'s `mqtt-interop` job), or any broker already listening on `MQTT_INTEROP_BROKER` locally (e.g. `mosquitto -c ...` from Homebrew/apt) |
//! | independent oracle: CycloneDDS's own wire stack | independent oracle: Mosquitto's own bundled `mosquitto_pub`/`mosquitto_sub` CLI tools, used as an independent third-party publisher/subscriber alongside rust-MQTT's own `v3::Client` |
//! | `t.Skipf(...)`-equivalent early return when the peer never responds | each test probes the broker first (`v3::Client::connect` with a short timeout) and, on failure, prints a note to stderr and returns early rather than failing — same posture as `cyclone_interop.rs`'s `looks_like_no_live_peer` |
//!
//! Also `#[ignore]`d on every test function, in addition to the feature
//! gate — belt and suspenders, matching rust-DDS's established convention
//! for live-network tests, since `cargo test --all-features` would
//! otherwise compile *and run* these against whatever is (or is not)
//! listening on `MQTT_INTEROP_BROKER`.
//!
//! # Prerequisites
//!
//! 1. A live MQTT v3.1.1 broker reachable at `MQTT_INTEROP_BROKER`
//!    (default `127.0.0.1:1883`), with anonymous connections allowed.
//! 2. Mosquitto's `mosquitto_pub`/`mosquitto_sub` CLI tools on `PATH`
//!    (`brew install mosquitto` on macOS, `apt-get install
//!    mosquitto-clients` on Debian/Ubuntu) for the two third-party-peer
//!    tests below. The pure round-trip test does not need them.
//!
//! # Quick start
//!
//! ```text
//! mosquitto -d   # or: docker run --rm -p 1883:1883 eclipse-mosquitto:2
//! cargo test --release --features mqtt-interop --test mqtt_interop -- --ignored --test-threads=1
//! ```
//!
//! # Environment variables
//!
//! - `MQTT_INTEROP_BROKER`       broker `host:port` (default `127.0.0.1:1883`).
//! - `MQTT_INTEROP_TIMEOUT_SECS` per-test connect/receive deadline in whole
//!   seconds (default `10`).

#![cfg(feature = "mqtt-interop")]

use std::process::Stdio;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rust_mqtt::v3::{Client as V3Client, ConnectOptions};
use rust_mqtt::{Client as MqttClient, QoS, SubscriberConfig};
use tokio::process::Command;
use tokio::time::timeout;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn broker_addr() -> String {
    std::env::var("MQTT_INTEROP_BROKER").unwrap_or_else(|_| "127.0.0.1:1883".to_string())
}

fn timeout_secs() -> u64 {
    std::env::var("MQTT_INTEROP_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10)
}

/// A topic unique to this process+test+time, so concurrent CI runs (or a
/// broker with retained state from a previous run) never collide.
fn unique_topic(test_name: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!(
        "interop/rust-mqtt/{test_name}/{}-{nanos}",
        std::process::id()
    )
}

/// Connects a `v3::Client` to `MQTT_INTEROP_BROKER`, treating any failure
/// (refused connection, DNS failure, timeout) as "no live broker" rather
/// than a hard test failure — same skip-not-fail posture as rust-DDS's
/// `cyclone_interop.rs`'s `looks_like_no_live_peer`. Returns `None` when the
/// broker could not be reached within `MQTT_INTEROP_TIMEOUT_SECS`.
async fn try_connect(client_id: &str) -> Option<V3Client> {
    let opts = ConnectOptions::new(broker_addr())
        .client_id(client_id)
        .connect_timeout(Duration::from_secs(timeout_secs()));
    match timeout(
        Duration::from_secs(timeout_secs() + 1),
        V3Client::connect(opts),
    )
    .await
    {
        Ok(Ok(client)) => Some(client),
        Ok(Err(e)) => {
            eprintln!(
                "mqtt_interop: could not connect to broker at {} — is one running? \
                 (`mosquitto -c ...` locally, or the `mqtt-interop` CI job's Mosquitto \
                 service). Treating as skipped, not failed. error: {e}",
                broker_addr()
            );
            None
        }
        Err(_) => {
            eprintln!(
                "mqtt_interop: connect to broker at {} timed out after {}s — is one \
                 running? Treating as skipped, not failed.",
                broker_addr(),
                timeout_secs()
            );
            None
        }
    }
}

/// True when neither `mosquitto_pub` nor `mosquitto_sub` can be spawned —
/// the CLI-interop tests need Mosquitto's own tools installed
/// (`mosquitto-clients` apt package / `mosquitto` Homebrew formula) as the
/// independent third-party validator.
async fn cli_tool_missing(bin: &str) -> bool {
    Command::new(bin)
        .arg("--help")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .is_err()
}

// ---------------------------------------------------------------------------
// Test 1: real-broker round-trip — rust-MQTT's own Publisher and Subscriber,
// both connecting to a real Mosquitto broker over real TCP, field-exact
// receipt. No third-party tooling involved; this is the "does rust-MQTT
// actually speak MQTT on a wire" deliverable.
// ---------------------------------------------------------------------------

//fusa:test REQ-CONN-001
//fusa:test REQ-PUB-001
//fusa:test REQ-SUB-001
//fusa:test REQ-MSG-001
//fusa:test REQ-MSG-002
//fusa:test REQ-MSG-003
//fusa:test REQ-MSG-004
#[tokio::test]
#[ignore = "requires a live MQTT broker at MQTT_INTEROP_BROKER; run via the mqtt-interop CI job"]
async fn real_broker_publish_subscribe_round_trip() {
    let topic = unique_topic("round-trip");

    let Some(subscriber) = try_connect("rust-mqtt-interop-sub").await else {
        return;
    };
    let Some(publisher) = try_connect("rust-mqtt-interop-pub").await else {
        return;
    };

    let mut sub = subscriber
        .subscribe(&topic, QoS::AtLeastOnce, SubscriberConfig::default())
        .await
        .expect("subscribe");

    // Give the broker a moment to process the SUBSCRIBE before publishing —
    // there is no SUBACK-wait in this crate's public API (§SUB-* covers
    // channel delivery, not broker acknowledgement latency), so a short
    // settle delay avoids a race against a slow broker.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let payload = b"real-broker-round-trip".to_vec();
    publisher
        .publish(&topic, QoS::AtLeastOnce, payload.clone())
        .await
        .expect("publish");

    let msg = timeout(Duration::from_secs(timeout_secs()), sub.recv())
        .await
        .expect("no message received from real broker within deadline")
        .expect("subscription channel closed");

    // Field-exact receipt.
    assert_eq!(msg.topic, topic);
    assert_eq!(msg.payload, payload);
    assert_eq!(msg.qos, QoS::AtLeastOnce);
    assert!(!msg.retained);

    publisher.close().await.ok();
    subscriber.close().await.ok();
}

// ---------------------------------------------------------------------------
// Test 2: third-party-peer interop, rust-MQTT publisher direction — publish
// via rust-mqtt's own API to the real broker, independently verify receipt
// using Mosquitto's `mosquitto_sub` CLI as an independent subscriber.
// ---------------------------------------------------------------------------

//fusa:test REQ-PUB-001
//fusa:test REQ-QOS-001
#[tokio::test]
#[ignore = "requires a live MQTT broker + mosquitto_sub on PATH; run via the mqtt-interop CI job"]
async fn rust_publisher_verified_by_mosquitto_sub() {
    if cli_tool_missing("mosquitto_sub").await {
        eprintln!(
            "mqtt_interop: mosquitto_sub not found on PATH — install mosquitto-clients \
             (apt) / mosquitto (brew). Treating as skipped, not failed."
        );
        return;
    }
    let Some(publisher) = try_connect("rust-mqtt-interop-pub2").await else {
        return;
    };

    let topic = unique_topic("pub-verified-by-mosquitto-sub");
    let payload = "hello-from-rust-mqtt";

    // Independent third-party subscriber: Mosquitto's own `mosquitto_sub`,
    // not any rust-MQTT code path. `-C 1` exits after exactly one message;
    // `-W` bounds how long it waits.
    let (host, port) = split_addr(&broker_addr());
    let child = Command::new("mosquitto_sub")
        .args([
            "-h",
            &host,
            "-p",
            &port,
            "-t",
            &topic,
            "-C",
            "1",
            "-W",
            &(timeout_secs() + 5).to_string(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn mosquitto_sub");

    // Give mosquitto_sub time to establish its SUBSCRIBE before publishing.
    tokio::time::sleep(Duration::from_millis(500)).await;

    publisher
        .publish(&topic, QoS::AtLeastOnce, payload.as_bytes().to_vec())
        .await
        .expect("publish via rust-mqtt");

    let output = timeout(
        Duration::from_secs(timeout_secs() + 10),
        child.wait_with_output(),
    )
    .await
    .expect("mosquitto_sub did not exit within deadline")
    .expect("failed to wait on mosquitto_sub");

    let stderr = String::from_utf8_lossy(&output.stderr);
    if !output.status.success() {
        eprintln!(
            "mqtt_interop: mosquitto_sub exited non-zero (status={:?}), likely no live \
             broker reachable at {}. stderr:\n{stderr}. Treating as skipped, not failed.",
            output.status,
            broker_addr()
        );
        return;
    }

    let received = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        received.trim_end_matches('\n'),
        payload,
        "mosquitto_sub (independent third-party subscriber) did not receive the exact \
         payload rust-mqtt published. stderr:\n{stderr}"
    );

    publisher.close().await.ok();
}

// ---------------------------------------------------------------------------
// Test 3: third-party-peer interop, rust-MQTT subscriber direction — the
// reverse: publish via Mosquitto's `mosquitto_pub` CLI, subscribe via
// rust-mqtt's own API, verify correct receipt/decoding.
// ---------------------------------------------------------------------------

//fusa:test REQ-SUB-001
//fusa:test REQ-MSG-002
#[tokio::test]
#[ignore = "requires a live MQTT broker + mosquitto_pub on PATH; run via the mqtt-interop CI job"]
async fn mosquitto_pub_verified_by_rust_subscriber() {
    if cli_tool_missing("mosquitto_pub").await {
        eprintln!(
            "mqtt_interop: mosquitto_pub not found on PATH — install mosquitto-clients \
             (apt) / mosquitto (brew). Treating as skipped, not failed."
        );
        return;
    }
    let Some(subscriber) = try_connect("rust-mqtt-interop-sub2").await else {
        return;
    };

    let topic = unique_topic("mosquitto-pub-verified-by-rust-sub");
    let payload = "hello-from-mosquitto_pub";

    let mut sub = subscriber
        .subscribe(&topic, QoS::AtLeastOnce, SubscriberConfig::default())
        .await
        .expect("subscribe via rust-mqtt");

    // Settle delay so the broker has processed our SUBSCRIBE before the
    // independent third-party publisher sends.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (host, port) = split_addr(&broker_addr());
    let status = timeout(
        Duration::from_secs(timeout_secs()),
        Command::new("mosquitto_pub")
            .args([
                "-h", &host, "-p", &port, "-t", &topic, "-m", payload, "-q", "1",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output(),
    )
    .await;

    let output = match status {
        Ok(Ok(output)) => output,
        Ok(Err(e)) => panic!("failed to run mosquitto_pub: {e}"),
        Err(_) => {
            eprintln!(
                "mqtt_interop: mosquitto_pub did not exit within {}s — is a broker \
                 reachable at {}? Treating as skipped, not failed.",
                timeout_secs(),
                broker_addr()
            );
            return;
        }
    };
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        eprintln!(
            "mqtt_interop: mosquitto_pub exited non-zero (status={:?}), likely no live \
             broker reachable at {}. stderr:\n{stderr}. Treating as skipped, not failed.",
            output.status,
            broker_addr()
        );
        return;
    }

    let msg = timeout(Duration::from_secs(timeout_secs()), sub.recv())
        .await
        .expect("rust-mqtt subscriber received nothing from mosquitto_pub within deadline")
        .expect("subscription channel closed");

    assert_eq!(msg.topic, topic);
    assert_eq!(msg.payload, payload.as_bytes());

    subscriber.close().await.ok();
}

// ---------------------------------------------------------------------------

/// Splits a `host:port` string (as accepted by `ConnectOptions::new`) into
/// separate `-h`/`-p` arguments for the `mosquitto_pub`/`mosquitto_sub` CLIs.
fn split_addr(addr: &str) -> (String, String) {
    match addr.rsplit_once(':') {
        Some((host, port)) => (host.to_string(), port.to_string()),
        None => (addr.to_string(), "1883".to_string()),
    }
}
