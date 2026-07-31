// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! End-to-end regression test for rust-MQTT-04.
//!
//! OASIS MQTT v3.1.1 §3.3.1.2 / MQTT-3.3.1-4: "A PUBLISH Packet MUST NOT
//! have both QoS bits set to 1. If a Server or Client receives a PUBLISH
//! Packet which has both QoS bits set to 1 it MUST close the Network
//! Connection." This drives a real v3 `Client` against a raw TCP peer
//! acting as a malicious/buggy broker that sends such a malformed PUBLISH,
//! and asserts the client tears the connection down (reports `Down`)
//! rather than merely dropping the frame and staying connected.

use rust_mqtt::v3::{Client as V3Client, ConnectOptions};
use rust_mqtt::{HealthProvider, HealthStatus};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::{sleep, timeout, Duration};

#[tokio::test]
async fn qos3_publish_from_broker_closes_client_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();

        // Drain the CONNECT packet (fixed header + remaining length +
        // variable header/payload) before replying — we don't need to
        // parse it, just consume enough bytes to stay in sync.
        let mut first = [0u8; 1];
        sock.read_exact(&mut first).await.unwrap();
        let mut rem_len: usize = 0;
        let mut shift = 0;
        loop {
            let mut b = [0u8; 1];
            sock.read_exact(&mut b).await.unwrap();
            rem_len |= ((b[0] & 0x7F) as usize) << shift;
            shift += 7;
            if b[0] & 0x80 == 0 {
                break;
            }
        }
        let mut discard = vec![0u8; rem_len];
        sock.read_exact(&mut discard).await.unwrap();

        // CONNACK: 0x20, len=2, session-present=0, return-code=0 (accepted).
        sock.write_all(&[0x20, 0x02, 0x00, 0x00]).await.unwrap();

        // Malformed PUBLISH: fixed-header byte = (PUBLISH=3 << 4) | flags,
        // flags = qos_bits(0b11) << 1 | retain(0) = 0b0110 = 0x06.
        // Body: 2-byte topic length + topic bytes (no packet id encoded —
        // matches how a real broker would still frame QoS bits in the
        // fixed header regardless of payload shape).
        let topic = b"malformed/qos3";
        let mut body = Vec::new();
        body.push((topic.len() >> 8) as u8);
        body.push(topic.len() as u8);
        body.extend_from_slice(topic);
        body.extend_from_slice(b"x");

        let mut pkt = vec![(3u8 << 4) | 0x06];
        pkt.push(body.len() as u8); // remaining length fits in one byte here
        pkt.extend_from_slice(&body);
        sock.write_all(&pkt).await.unwrap();

        // Keep the socket alive for a bit so the client has time to react;
        // a real MUST-close client tears down its own side regardless of
        // what the (malicious) peer does.
        sleep(Duration::from_millis(500)).await;
    });

    let client = V3Client::connect(ConnectOptions::new(addr.to_string()).client_id("victim"))
        .await
        .expect("CONNECT/CONNACK handshake must succeed before the malformed PUBLISH arrives");

    // Poll health() until the client observes the malformed PUBLISH and
    // closes, or time out and fail the test.
    let became_down = timeout(Duration::from_secs(5), async {
        loop {
            if client.health().await.status == HealthStatus::Down {
                return;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await;

    assert!(
        became_down.is_ok(),
        "MQTT-3.3.1-4: client MUST close the Network Connection on receipt of a \
         QoS-3 (both-bits-set) PUBLISH, but it stayed Up"
    );

    server.abort();
}
