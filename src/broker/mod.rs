// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! Minimal embedded TCP MQTT v3.1.1 broker.
//!
//! For integration testing. Not intended for production use.
//! Binds on an OS-assigned port; use `addr()` to retrieve it.
//!
//! # Example
//! ```rust,no_run
//! use rust_mqtt::broker::Broker;
//! use rust_mqtt::v3::{Client, ConnectOptions};
//! use rust_mqtt::Client as MqttClient;
//!
//! #[tokio::main]
//! async fn main() {
//!     let broker = Broker::start("127.0.0.1:0").await.unwrap();
//!     let addr = broker.addr();
//!     let opts = ConnectOptions::new(addr.to_string());
//!     let client = Client::connect(opts).await.unwrap();
//!     client.close().await.unwrap();
//!     broker.close().await;
//! }
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};

use crate::message::{Message, QoS};
use crate::topic::match_topic;

// ---------------------------------------------------------------------------
// BrokerState
// ---------------------------------------------------------------------------

struct Subscription {
    filter: String,
    tx: mpsc::Sender<Vec<u8>>,
}

struct BrokerState {
    subs: Mutex<HashMap<u64, Subscription>>,
    retained: Mutex<HashMap<String, Message>>,
    next_id: AtomicU64,
    closed: AtomicBool,
}

impl BrokerState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            subs: Mutex::new(HashMap::new()),
            retained: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            closed: AtomicBool::new(false),
        })
    }
}

// ---------------------------------------------------------------------------
// Broker
// ---------------------------------------------------------------------------

/// Minimal embedded MQTT v3.1.1 broker.
//fusa:req REQ-BROKER-001
//fusa:req REQ-BROKER-002
//fusa:req REQ-BROKER-003
pub struct Broker {
    addr: SocketAddr,
    state: Arc<BrokerState>,
    shutdown_tx: mpsc::Sender<()>,
}

impl Broker {
    /// Start the broker listening on `addr`. Use `"127.0.0.1:0"` for an
    /// OS-assigned port.
    pub async fn start(addr: &str) -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind(addr).await?;
        let bound_addr = listener.local_addr()?;
        let state = BrokerState::new();
        let state_clone = state.clone();
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept = listener.accept() => {
                        match accept {
                            Ok((stream, _peer)) => {
                                let s = state_clone.clone();
                                tokio::spawn(handle_client(stream, s));
                            }
                            Err(_) => break,
                        }
                    }
                    _ = shutdown_rx.recv() => break,
                }
            }
        });

        Ok(Self {
            addr: bound_addr,
            state,
            shutdown_tx,
        })
    }

    /// The address the broker is listening on.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Shut down the broker.
    pub async fn close(self) {
        let _ = self.shutdown_tx.send(()).await;
        self.state.closed.store(true, Ordering::SeqCst);
    }
}

// ---------------------------------------------------------------------------
// Per-client handler
// ---------------------------------------------------------------------------

async fn handle_client(stream: TcpStream, state: Arc<BrokerState>) {
    let (mut reader, mut writer) = stream.into_split();

    // Expect CONNECT packet
    let first = match reader.read_u8().await {
        Ok(b) => b,
        Err(_) => return,
    };
    if first >> 4 != 1 {
        return;
    } // not CONNECT

    let rem_len = match read_varint_sync(&mut reader).await {
        Ok(v) => v,
        Err(_) => return,
    };
    let mut buf = vec![0u8; rem_len];
    if rem_len > 0 && reader.read_exact(&mut buf).await.is_err() {
        return;
    }

    // Send CONNACK (return code 0)
    if writer.write_all(&[0x20, 0x02, 0x00, 0x00]).await.is_err() {
        return;
    }

    // Allocate a client-specific write channel
    let (write_tx, mut write_rx) = mpsc::channel::<Vec<u8>>(256);

    // Per-session subscription IDs
    let session_sub_ids: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    // QoS 2 messages held between PUBREC and PUBREL, keyed by packet id.
    // Per-connection: MQTT packet ids are scoped to a single client session.
    let mut pending_qos2: HashMap<u16, (String, Vec<u8>)> = HashMap::new();
    tokio::spawn(async move {
        while let Some(pkt) = write_rx.recv().await {
            if writer.write_all(&pkt).await.is_err() {
                break;
            }
        }
    });

    // Reader loop
    while let Ok(byte) = reader.read_u8().await {
        let ptype = byte >> 4;
        let flags = byte & 0x0F;

        let rem_len = match read_varint_sync(&mut reader).await {
            Ok(v) => v,
            Err(_) => break,
        };
        let mut body = vec![0u8; rem_len];
        if rem_len > 0 && reader.read_exact(&mut body).await.is_err() {
            break;
        }

        match ptype {
            3 => {
                // PUBLISH
                let qos_bits = (flags >> 1) & 0x03;
                let retained_flag = flags & 0x01 != 0;
                if body.len() < 2 {
                    continue;
                }
                let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
                if body.len() < 2 + topic_len {
                    continue;
                }
                let topic = match std::str::from_utf8(&body[2..2 + topic_len]) {
                    Ok(s) => s.to_owned(),
                    Err(_) => continue,
                };
                let mut payload_off = 2 + topic_len;
                let mut pid = 0u16;
                if qos_bits > 0 && body.len() >= payload_off + 2 {
                    pid = u16::from_be_bytes([body[payload_off], body[payload_off + 1]]);
                    payload_off += 2;
                }
                let payload = body[payload_off..].to_vec();
                let qos = QoS::try_from(qos_bits).unwrap_or(QoS::AtMostOnce);

                if qos_bits == 2 {
                    // MQTT v3.1.1 §3.3/§4.3.3: hold the message and reply
                    // PUBREC; only broadcast once PUBREL releases it.
                    if retained_flag {
                        let msg = Message {
                            topic: topic.clone(),
                            payload: payload.clone(),
                            qos,
                            retained: true,
                            ..Default::default()
                        };
                        state.retained.lock().await.insert(topic.clone(), msg);
                    }
                    pending_qos2.insert(pid, (topic, payload));
                    let _ = write_tx.try_send(vec![0x50, 0x02, (pid >> 8) as u8, pid as u8]);
                    continue;
                }

                if retained_flag {
                    let msg = Message {
                        topic: topic.clone(),
                        payload: payload.clone(),
                        qos,
                        retained: true,
                        ..Default::default()
                    };
                    state.retained.lock().await.insert(topic.clone(), msg);
                }

                // Broadcast to matching subscribers. This broker does not
                // track per-subscriber packet ids or acks, so fan-out is
                // always framed at QoS 0 regardless of the publisher's QoS —
                // framing it at a higher QoS without a packet id would
                // produce a malformed PUBLISH (the receiver would misread
                // the first two payload bytes as a packet id).
                let pub_pkt = build_publish_pkt(&topic, &payload, QoS::AtMostOnce, false, None);
                let subs = state.subs.lock().await;
                for sub in subs.values() {
                    if match_topic(&sub.filter, &topic) {
                        let _ = sub.tx.try_send(pub_pkt.clone());
                    }
                }

                // PUBACK for QoS 1
                if qos_bits == 1 {
                    let _ = write_tx.try_send(vec![0x40, 0x02, (pid >> 8) as u8, pid as u8]);
                }
            }
            6 => {
                // PUBREL: release a held QoS 2 message, broadcast it, PUBCOMP.
                if body.len() < 2 {
                    continue;
                }
                let pid = u16::from_be_bytes([body[0], body[1]]);
                if let Some((topic, payload)) = pending_qos2.remove(&pid) {
                    let pub_pkt = build_publish_pkt(&topic, &payload, QoS::AtMostOnce, false, None);
                    let subs = state.subs.lock().await;
                    for sub in subs.values() {
                        if match_topic(&sub.filter, &topic) {
                            let _ = sub.tx.try_send(pub_pkt.clone());
                        }
                    }
                }
                let _ = write_tx.try_send(vec![0x70, 0x02, (pid >> 8) as u8, pid as u8]);
            }
            8 => {
                // SUBSCRIBE
                if body.len() < 4 {
                    continue;
                }
                let pid = u16::from_be_bytes([body[0], body[1]]);
                let filter_len = u16::from_be_bytes([body[2], body[3]]) as usize;
                if body.len() < 4 + filter_len {
                    continue;
                }
                let filter = match std::str::from_utf8(&body[4..4 + filter_len]) {
                    Ok(s) => s.to_owned(),
                    Err(_) => continue,
                };
                let requested_qos = if body.len() > 4 + filter_len {
                    body[4 + filter_len]
                } else {
                    0
                };

                let id = state.next_id.fetch_add(1, Ordering::SeqCst);
                let write_tx_clone = write_tx.clone();
                state.subs.lock().await.insert(
                    id,
                    Subscription {
                        filter: filter.clone(),
                        tx: write_tx_clone,
                    },
                );
                session_sub_ids.lock().await.push(id);

                // Deliver retained messages (always framed at QoS 0 — see
                // the fan-out comment above).
                let retained = state.retained.lock().await.clone();
                for (topic, msg) in &retained {
                    if match_topic(&filter, topic) {
                        let pkt = build_publish_pkt(
                            &msg.topic,
                            &msg.payload,
                            QoS::AtMostOnce,
                            true,
                            None,
                        );
                        let _ = write_tx.try_send(pkt);
                    }
                }

                // SUBACK
                let _ =
                    write_tx.try_send(vec![0x90, 0x03, (pid >> 8) as u8, pid as u8, requested_qos]);
            }
            10 => {
                // UNSUBSCRIBE
                if body.len() < 4 {
                    continue;
                }
                let pid = u16::from_be_bytes([body[0], body[1]]);
                let filter_len = u16::from_be_bytes([body[2], body[3]]) as usize;
                if body.len() < 4 + filter_len {
                    continue;
                }
                let filter = match std::str::from_utf8(&body[4..4 + filter_len]) {
                    Ok(s) => s.to_owned(),
                    Err(_) => continue,
                };
                let mut subs = state.subs.lock().await;
                subs.retain(|_, sub| sub.filter != filter);
                let _ = write_tx.try_send(vec![0xB0, 0x02, (pid >> 8) as u8, pid as u8]);
            }
            12 => {
                // PINGREQ
                let _ = write_tx.try_send(vec![0xD0, 0x00]);
            }
            14 => break, // DISCONNECT
            _ => {}
        }
    }

    // Clean up subscriptions for this session
    let ids = session_sub_ids.lock().await.clone();
    let mut subs = state.subs.lock().await;
    for id in ids {
        subs.remove(&id);
    }
}

async fn read_varint_sync(r: &mut tokio::net::tcp::OwnedReadHalf) -> Result<usize, ()> {
    let mut result = 0usize;
    let mut shift = 0;
    loop {
        let byte = r.read_u8().await.map_err(|_| ())?;
        result |= ((byte & 0x7F) as usize) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            break;
        }
        // MQTT §2.2.3: Remaining Length is at most 4 bytes. Reject before
        // consuming a disallowed 5th continuation byte.
        if shift >= 28 {
            return Err(());
        }
    }
    Ok(result)
}

fn build_publish_pkt(
    topic: &str,
    payload: &[u8],
    qos: QoS,
    retained: bool,
    pid: Option<u16>,
) -> Vec<u8> {
    let mut body = Vec::new();
    let t = topic.as_bytes();
    // OASIS MQTT v3.1.1 §1.5.3: a UTF-8 Encoded String is prefixed by a
    // two-byte length and MUST NOT exceed 65535 bytes, matching the guard in
    // the client-side `encode_string` (src/v3/packet.rs).
    assert!(
        t.len() <= 0xFFFF,
        "MQTT topic length {} exceeds §1.5.3 maximum of 65535 bytes",
        t.len()
    );
    body.push((t.len() >> 8) as u8);
    body.push(t.len() as u8);
    body.extend_from_slice(t);
    if let Some(p) = pid {
        body.push((p >> 8) as u8);
        body.push(p as u8);
    }
    body.extend_from_slice(payload);

    let flags: u8 = ((qos as u8) << 1) | (retained as u8);
    let mut out = vec![(3 << 4) | flags];
    // MQTT §2.2.3: Remaining Length is bounded to 268,435,455 (4 bytes). The
    // client-side encoder (`encode_remaining_length`) already enforces this;
    // apply the same guard here so the embedded broker never emits an
    // oversized (5+ byte) malformed length.
    assert!(
        body.len() <= 268_435_455,
        "remaining length {} exceeds MQTT §2.2.3 maximum of 268435455",
        body.len()
    );
    let mut rem = body.len();
    loop {
        let mut d = (rem % 128) as u8;
        rem /= 128;
        if rem > 0 {
            d |= 0x80;
        }
        out.push(d);
        if rem == 0 {
            break;
        }
    }
    out.extend_from_slice(&body);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{Client, SubscriberConfig};
    use crate::message::QoS;
    use crate::v3::{Client as V3Client, ConnectOptions};
    use tokio::time::{timeout, Duration};

    #[test]
    #[should_panic(expected = "exceeds MQTT §2.2.3 maximum of 268435455")]
    fn build_publish_pkt_above_remaining_length_max_panics() {
        // Regression test for rust-MQTT-02: the embedded broker's hand-rolled
        // PUBLISH encoder previously had no upper-bound guard on Remaining
        // Length (unlike the client-side `encode_remaining_length`), so it
        // could silently emit a 5+ byte malformed length. Build a body one
        // byte over the §2.2.3 268,435,455 ceiling and confirm it now fails
        // loudly instead of emitting a malformed packet.
        let oversized_payload = vec![0u8; 268_435_455]; // + 2-byte topic prefix + topic bytes pushes body over the limit
        let _ = build_publish_pkt("t", &oversized_payload, QoS::AtMostOnce, false, None);
    }

    #[test]
    #[should_panic(expected = "exceeds §1.5.3 maximum of 65535 bytes")]
    fn build_publish_pkt_above_topic_length_max_panics() {
        // Regression test for rust-MQTT-02's shared MQTT-01 topic-length
        // truncation: build_publish_pkt hand-rolls its own 16-bit topic
        // length prefix rather than calling the client-side `encode_string`,
        // so it needs its own guard.
        let topic = "a".repeat(0x10000); // 65536 bytes, one over the limit
        let _ = build_publish_pkt(&topic, b"x", QoS::AtMostOnce, false, None);
    }

    #[tokio::test]
    async fn broker_start_and_connect() {
        let broker = Broker::start("127.0.0.1:0").await.unwrap();
        let addr = broker.addr();
        let opts = ConnectOptions::new(addr.to_string());
        let client = V3Client::connect(opts).await.unwrap();
        client.close().await.unwrap();
        broker.close().await;
    }

    #[tokio::test]
    async fn broker_pubsub_roundtrip() {
        let broker = Broker::start("127.0.0.1:0").await.unwrap();
        let addr = broker.addr();

        let c1 = V3Client::connect(ConnectOptions::new(addr.to_string()).client_id("sub"))
            .await
            .unwrap();
        let c2 = V3Client::connect(ConnectOptions::new(addr.to_string()).client_id("pub"))
            .await
            .unwrap();

        let mut sub = c1
            .subscribe("test/#", QoS::AtMostOnce, SubscriberConfig::default())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        c2.publish("test/topic", QoS::AtMostOnce, b"hello".to_vec())
            .await
            .unwrap();

        let msg = timeout(Duration::from_secs(2), sub.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg.topic, "test/topic");
        assert_eq!(msg.payload, b"hello");

        c1.close().await.unwrap();
        c2.close().await.unwrap();
        broker.close().await;
    }

    #[tokio::test]
    //fusa:req REQ-QOS-003
    async fn broker_qos2_exactly_once_roundtrip() {
        // §5 issue: v3 Client::publish() at QoS::ExactlyOnce must complete
        // the full PUBLISH→PUBREC→PUBREL→PUBCOMP handshake rather than
        // silently behaving like QoS 0.
        let broker = Broker::start("127.0.0.1:0").await.unwrap();
        let addr = broker.addr();

        let c1 = V3Client::connect(ConnectOptions::new(addr.to_string()).client_id("sub2"))
            .await
            .unwrap();
        let c2 = V3Client::connect(ConnectOptions::new(addr.to_string()).client_id("pub2"))
            .await
            .unwrap();

        // Subscribed at QoS 0: this test's focus is the *publisher*-side
        // PUBLISH→PUBREC→PUBREL→PUBCOMP handshake (the bug in issue #5),
        // not the broker's (out of scope) subscriber-side QoS 2 fan-out.
        let mut sub = c1
            .subscribe("qos2/#", QoS::AtMostOnce, SubscriberConfig::default())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // publish() must not return until the full handshake completes.
        timeout(
            Duration::from_secs(5),
            c2.publish("qos2/topic", QoS::ExactlyOnce, b"exactly-once".to_vec()),
        )
        .await
        .expect("publish must not hang")
        .expect("publish must succeed");

        let msg = timeout(Duration::from_secs(2), sub.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg.topic, "qos2/topic");
        assert_eq!(msg.payload, b"exactly-once");

        c1.close().await.unwrap();
        c2.close().await.unwrap();
        broker.close().await;
    }

    // ------------------------------------------------------------------
    // Independent reviewer checks for rust-MQTT-01 (off-by-one in the
    // Remaining Length varint decoder). These call `read_varint_sync`
    // directly (the actual shipped private fn, not a reimplementation)
    // over a real TCP loopback pair, so they exercise exactly the code
    // path a malicious/malformed peer would hit. Not part of the audited
    // diff — added independently to verify the fix rather than trust it.
    // ------------------------------------------------------------------

    async fn loopback_pair() -> (tokio::net::tcp::OwnedReadHalf, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (server_read, _server_write) = server.into_split();
        (server_read, client)
    }

    #[tokio::test]
    async fn read_varint_sync_rejects_5th_continuation_byte() {
        let (mut server_read, mut client) = loopback_pair().await;
        // 4 bytes all with the continuation bit set, then a 5th byte.
        // MQTT 3.1.1 §2.2.3: Remaining Length is at most 4 bytes; a 4th
        // byte with the continuation bit still set is malformed and MUST
        // be rejected WITHOUT consuming a 5th byte.
        client.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).await.unwrap();
        let result = timeout(
            Duration::from_millis(500),
            read_varint_sync(&mut server_read),
        )
        .await
        .expect("must not hang waiting for a 5th byte");
        assert!(
            result.is_err(),
            "a 4-byte sequence with the continuation bit still set on byte 4 must be rejected"
        );

        // Prove the parser never read a 5th byte: bytes still unread on
        // the wire are observable by writing one more and reading it back
        // raw on a fresh connection would be a stronger check, but the
        // simplest proof is that the above resolved without blocking on
        // a 5th byte — if the old `shift > 28` bug were present, the
        // function would still be awaiting `read_u8()` for a 5th byte
        // right now and the `timeout` above would have fired instead.
    }

    #[tokio::test]
    async fn read_varint_sync_accepts_exact_max_268435455() {
        let (mut server_read, mut client) = loopback_pair().await;
        // Canonical 4-byte encoding of 268,435,455 (0x0FFF_FFFF), the MQTT
        // 3.1.1 §2.2.3 maximum Remaining Length.
        client.write_all(&[0xFF, 0xFF, 0xFF, 0x7F]).await.unwrap();
        let result = timeout(
            Duration::from_millis(500),
            read_varint_sync(&mut server_read),
        )
        .await
        .expect("must not hang on a legal 4-byte varint");
        assert_eq!(result, Ok(268_435_455));
    }

    #[tokio::test]
    async fn read_varint_sync_rejects_over_max_via_5byte_encoding() {
        let (mut server_read, mut client) = loopback_pair().await;
        // A 5-byte encoding of a value one past the maximum. Even though
        // the payload of this specific packet is nonsense, the point is
        // structural: any encoding needing a 5th byte is illegal, and this
        // one in particular represents a value > 268,435,455.
        client
            .write_all(&[0xFF, 0xFF, 0xFF, 0xFF, 0x01])
            .await
            .unwrap();
        let result = timeout(
            Duration::from_millis(500),
            read_varint_sync(&mut server_read),
        )
        .await
        .expect("must not hang");
        assert!(result.is_err());
    }
}
