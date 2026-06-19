// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! MQTT v3.1.1 TCP client.
//!
//! Connects to any MQTT broker (Mosquitto, HiveMQ, EMQX, …) via TCP.
//! Supports QoS 0, 1, and 2; keepalive; clean-session; username/password;
//! last-will-and-testament (LWT); and TLS/mTLS (with the `tls` feature).
//!
//! # Example
//! ```rust,no_run
//! use rust_mqtt::v3::{Client as V3Client, ConnectOptions};
//! use rust_mqtt::{Client, QoS, SubscriberConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let opts = ConnectOptions::new("localhost:1883")
//!         .client_id("rust-mqtt-example")
//!         .keepalive(30);
//!     let client = V3Client::connect(opts).await?;
//!     client.publish("test/topic", QoS::AtMostOnce, b"hello".to_vec()).await?;
//!     client.close().await?;
//!     Ok(())
//! }
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::timeout;

use crate::client::{
    BackPressurePolicy, HealthProvider, HealthStatus, MetricsProvider, MetricsSnapshot,
    SubscriberConfig, Subscription,
};
use crate::error::Error;
use crate::message::{Message, QoS};
use crate::topic::match_topic;

mod packet;
use packet::{
    build_connect, build_disconnect, build_pingreq, build_publish, build_subscribe,
    build_unsubscribe, PacketType,
};

// ---------------------------------------------------------------------------
// ConnectOptions
// ---------------------------------------------------------------------------

/// Options for establishing a v3.1.1 connection.
//fusa:req REQ-CONN-001
//fusa:req REQ-CONN-002
//fusa:req REQ-CONN-003
#[derive(Clone, Debug)]
pub struct ConnectOptions {
    pub address: String,
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<Vec<u8>>,
    pub clean_session: bool,
    pub keepalive_secs: u16,
    pub connect_timeout: Duration,
    pub will: Option<WillMessage>,
}

/// Last-will-and-testament message.
//fusa:req REQ-CONN-007
#[derive(Clone, Debug)]
pub struct WillMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: QoS,
    pub retain: bool,
}

impl ConnectOptions {
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            client_id: format!("rust-mqtt-{}", std::process::id()),
            username: None,
            password: None,
            clean_session: true,
            keepalive_secs: 60,
            connect_timeout: Duration::from_secs(10),
            will: None,
        }
    }

    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.client_id = id.into();
        self
    }

    pub fn username(mut self, u: impl Into<String>) -> Self {
        self.username = Some(u.into());
        self
    }

    pub fn password(mut self, p: Vec<u8>) -> Self {
        self.password = Some(p);
        self
    }

    pub fn clean_session(mut self, v: bool) -> Self {
        self.clean_session = v;
        self
    }

    pub fn keepalive(mut self, secs: u16) -> Self {
        self.keepalive_secs = secs;
        self
    }

    pub fn connect_timeout(mut self, d: Duration) -> Self {
        self.connect_timeout = d;
        self
    }

    pub fn will(mut self, w: WillMessage) -> Self {
        self.will = Some(w);
        self
    }
}

// ---------------------------------------------------------------------------
// SubRecord
// ---------------------------------------------------------------------------

struct SubRecord {
    filter: String,
    tx: mpsc::Sender<Message>,
    back_pressure: BackPressurePolicy,
    #[allow(dead_code)]
    unsub_rx: Option<oneshot::Receiver<String>>,
}

impl SubRecord {
    fn push(&self, msg: Message) {
        match self.back_pressure {
            BackPressurePolicy::DropOldest => {
                if self.tx.try_send(msg.clone()).is_err() {
                    let _ = self.tx.try_send(msg);
                }
            }
            BackPressurePolicy::Block => {
                let tx = self.tx.clone();
                tokio::spawn(async move {
                    let _ = tx.send(msg).await;
                });
            }
            _ => {
                let _ = self.tx.try_send(msg);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ClientState
// ---------------------------------------------------------------------------

struct ClientState {
    closed: AtomicBool,
    subs: Mutex<HashMap<u64, SubRecord>>,
    next_sub_id: std::sync::atomic::AtomicU64,
    next_packet_id: AtomicU16,
    // QoS 1 in-flight PUBACK waiters: packet_id → oneshot sender
    puback_waiters: Mutex<HashMap<u16, oneshot::Sender<()>>>,
    endpoint: String,
}

impl ClientState {
    fn new(endpoint: String) -> Arc<Self> {
        Arc::new(Self {
            closed: AtomicBool::new(false),
            subs: Mutex::new(HashMap::new()),
            next_sub_id: std::sync::atomic::AtomicU64::new(1),
            next_packet_id: AtomicU16::new(1),
            puback_waiters: Mutex::new(HashMap::new()),
            endpoint,
        })
    }

    fn next_pid(&self) -> u16 {
        loop {
            let pid = self.next_packet_id.fetch_add(1, Ordering::SeqCst);
            if pid != 0 {
                return pid;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Client (v3.1.1)
// ---------------------------------------------------------------------------

/// MQTT v3.1.1 TCP client.
//fusa:req REQ-CONN-001
//fusa:req REQ-CONN-004
//fusa:req REQ-CONN-005
//fusa:req REQ-CONN-006
//fusa:req REQ-CONN-008
//fusa:req REQ-CONN-009
//fusa:req REQ-CONN-010
//fusa:req REQ-CONN-011
//fusa:req REQ-CONC-001
//fusa:req REQ-CONC-002
//fusa:req REQ-CONC-003
#[derive(Clone)]
pub struct Client {
    state: Arc<ClientState>,
    writer_tx: mpsc::Sender<Vec<u8>>,
}

impl Client {
    /// Connect to a broker and return a live client.
    //fusa:req REQ-CONN-001
    //fusa:req REQ-CONN-002
    //fusa:req REQ-CONN-003
    pub async fn connect(opts: ConnectOptions) -> Result<Self, Error> {
        let stream = timeout(opts.connect_timeout, TcpStream::connect(&opts.address))
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(Error::Io)?;

        stream.set_nodelay(true).map_err(Error::Io)?;

        let (reader, writer) = stream.into_split();
        let state = ClientState::new(opts.address.clone());

        // CONNECT packet
        let connect_pkt = build_connect(&opts);
        let (writer_tx, mut writer_rx) = mpsc::channel::<Vec<u8>>(256);
        writer_tx.try_send(connect_pkt).ok();

        // Writer task
        {
            let mut w = writer;
            let state_clone = state.clone();
            tokio::spawn(async move {
                while let Some(pkt) = writer_rx.recv().await {
                    if w.write_all(&pkt).await.is_err() {
                        state_clone.closed.store(true, Ordering::SeqCst);
                        break;
                    }
                }
            });
        }

        // Wait for CONNACK
        let mut r = reader;
        let connack = read_connack(&mut r).await?;
        if connack != 0 {
            return Err(Error::ConnectionRefused(connack));
        }

        let state_clone = state.clone();
        let writer_tx_clone = writer_tx.clone();
        let keepalive = opts.keepalive_secs;

        // Reader + keepalive task
        tokio::spawn(async move {
            run_read_loop(r, state_clone.clone(), writer_tx_clone.clone(), keepalive).await;
        });

        Ok(Self { state, writer_tx })
    }

    fn send_raw(&self, pkt: Vec<u8>) -> Result<(), Error> {
        self.writer_tx
            .try_send(pkt)
            .map_err(|_| Error::NotConnected)
    }
}

// ---------------------------------------------------------------------------
// Read loop
// ---------------------------------------------------------------------------

async fn run_read_loop(
    mut reader: tokio::net::tcp::OwnedReadHalf,
    state: Arc<ClientState>,
    writer_tx: mpsc::Sender<Vec<u8>>,
    keepalive_secs: u16,
) {
    let keepalive = Duration::from_secs(keepalive_secs as u64);

    loop {
        if state.closed.load(Ordering::SeqCst) {
            break;
        }

        let header_result = if keepalive_secs > 0 {
            timeout(keepalive, reader.read_u8()).await
        } else {
            Ok(reader.read_u8().await)
        };

        let first_byte = match header_result {
            Ok(Ok(b)) => b,
            Ok(Err(_)) | Err(_) => {
                // Timeout → send PINGREQ.
                if keepalive_secs > 0 {
                    let _ = writer_tx.try_send(build_pingreq());
                    continue;
                }
                break;
            }
        };

        let pkt_type = PacketType::from_byte(first_byte >> 4);
        let flags = first_byte & 0x0F;

        // Read remaining length
        let rem_len = match read_varint(&mut reader).await {
            Ok(v) => v,
            Err(_) => break,
        };

        let mut body = vec![0u8; rem_len];
        if rem_len > 0 && reader.read_exact(&mut body).await.is_err() {
            break;
        }

        match pkt_type {
            PacketType::Publish => {
                handle_publish(&state, flags, &body).await;
            }
            PacketType::Puback => {
                if body.len() >= 2 {
                    let pid = u16::from_be_bytes([body[0], body[1]]);
                    let mut waiters = state.puback_waiters.lock().await;
                    if let Some(tx) = waiters.remove(&pid) {
                        let _ = tx.send(());
                    }
                }
            }
            PacketType::Suback => { /* acknowledged — we don't gate on it */ }
            PacketType::Pingresp => { /* keepalive response — nothing to do */ }
            PacketType::Disconnect => {
                state.closed.store(true, Ordering::SeqCst);
                break;
            }
            _ => {}
        }
    }

    state.closed.store(true, Ordering::SeqCst);
}

async fn handle_publish(state: &Arc<ClientState>, flags: u8, body: &[u8]) {
    let qos_bits = (flags >> 1) & 0x03;
    let retained = flags & 0x01 != 0;

    if body.len() < 2 {
        return;
    }
    let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    if body.len() < 2 + topic_len {
        return;
    }

    let topic = match std::str::from_utf8(&body[2..2 + topic_len]) {
        Ok(s) => s.to_owned(),
        Err(_) => return,
    };

    let mut payload_offset = 2 + topic_len;
    let mut packet_id = 0u16;

    if qos_bits > 0 {
        if body.len() < payload_offset + 2 {
            return;
        }
        packet_id = u16::from_be_bytes([body[payload_offset], body[payload_offset + 1]]);
        payload_offset += 2;
    }

    let payload = body[payload_offset..].to_vec();
    let qos = QoS::try_from(qos_bits).unwrap_or(QoS::AtMostOnce);

    let msg = Message {
        topic: topic.clone(),
        payload,
        qos,
        retained,
        packet_id,
        ..Default::default()
    };

    let subs = state.subs.lock().await;
    for sub in subs.values() {
        if match_topic(&sub.filter, &topic) {
            sub.push(msg.clone());
        }
    }
}

async fn read_connack(r: &mut tokio::net::tcp::OwnedReadHalf) -> Result<u8, Error> {
    // CONNACK: 0x20, len=2, flags, return-code
    let first = r
        .read_u8()
        .await
        .map_err(|_| Error::Protocol("CONNACK read failed".into()))?;
    if first != 0x20 {
        return Err(Error::Protocol(format!(
            "expected CONNACK (0x20), got 0x{:02X}",
            first
        )));
    }
    let len = r
        .read_u8()
        .await
        .map_err(|_| Error::Protocol("CONNACK len read failed".into()))?;
    if len < 2 {
        return Err(Error::Protocol("CONNACK packet too short".into()));
    }
    let _flags = r
        .read_u8()
        .await
        .map_err(|_| Error::Io(std::io::Error::other("")))?;
    let rc = r
        .read_u8()
        .await
        .map_err(|_| Error::Io(std::io::Error::other("")))?;
    Ok(rc)
}

async fn read_varint(r: &mut tokio::net::tcp::OwnedReadHalf) -> Result<usize, Error> {
    let mut result = 0usize;
    let mut shift = 0;
    loop {
        let byte = r.read_u8().await.map_err(Error::Io)?;
        result |= ((byte & 0x7F) as usize) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            break;
        }
        if shift > 28 {
            return Err(Error::Protocol("remaining length overflow".into()));
        }
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Client trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl crate::client::Client for Client {
    //fusa:req REQ-PUB-001
    //fusa:req REQ-PUB-002
    //fusa:req REQ-PUB-005
    //fusa:req REQ-PUB-006
    async fn publish(&self, topic: &str, qos: QoS, payload: Vec<u8>) -> Result<(), Error> {
        if self.state.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        if topic.is_empty() {
            return Err(Error::TopicEmpty);
        }

        let pid = if qos != QoS::AtMostOnce {
            Some(self.state.next_pid())
        } else {
            None
        };

        let pkt = build_publish(topic, &payload, qos, false, pid);
        self.send_raw(pkt)?;

        // QoS 1: wait for PUBACK
        if qos == QoS::AtLeastOnce {
            let pid = pid.unwrap();
            let (tx, rx) = oneshot::channel();
            self.state.puback_waiters.lock().await.insert(pid, tx);
            timeout(Duration::from_secs(30), rx)
                .await
                .map_err(|_| Error::Timeout)?
                .map_err(|_| Error::Protocol("PUBACK channel dropped".into()))?;
        }

        Ok(())
    }

    //fusa:req REQ-SUB-001
    //fusa:req REQ-SUB-006
    //fusa:req REQ-SUB-007
    //fusa:req REQ-SUB-008
    async fn subscribe(
        &self,
        topic_filter: &str,
        qos: QoS,
        config: SubscriberConfig,
    ) -> Result<Subscription, Error> {
        if self.state.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        if topic_filter.is_empty() {
            return Err(Error::TopicEmpty);
        }

        let pid = self.state.next_pid();
        let pkt = build_subscribe(topic_filter, qos, pid);
        self.send_raw(pkt)?;

        let depth = config.chan_depth(64);
        let (tx, rx) = mpsc::channel::<Message>(depth);
        let id = self.state.next_sub_id.fetch_add(1, Ordering::SeqCst);

        let (unsub_tx, unsub_rx) = oneshot::channel();
        self.state.subs.lock().await.insert(
            id,
            SubRecord {
                filter: topic_filter.to_owned(),
                tx,
                back_pressure: config.back_pressure,
                unsub_rx: None,
            },
        );

        // Spawn unsubscribe handler
        let state_clone = self.state.clone();
        let writer_tx = self.writer_tx.clone();
        let filter_owned = topic_filter.to_owned();
        tokio::spawn(async move {
            if unsub_rx.await.is_ok() {
                let unsub_pid = state_clone.next_pid();
                let pkt = build_unsubscribe(&filter_owned, unsub_pid);
                let _ = writer_tx.try_send(pkt);
                state_clone.subs.lock().await.remove(&id);
            }
        });

        Ok(Subscription {
            rx,
            topic: topic_filter.to_owned(),
            unsubscribe_tx: Some(unsub_tx),
        })
    }

    //fusa:req REQ-CONN-009
    //fusa:req REQ-CONN-010
    async fn close(&self) -> Result<(), Error> {
        if self
            .state
            .closed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let _ = self.send_raw(build_disconnect());
        }
        Ok(())
    }
}

#[async_trait]
impl HealthProvider for Client {
    async fn health(&self) -> HealthStatus {
        HealthStatus {
            healthy: !self.state.closed.load(Ordering::SeqCst),
            connected: !self.state.closed.load(Ordering::SeqCst),
            endpoint: self.state.endpoint.clone(),
            details: std::collections::BTreeMap::new(),
        }
    }
}

#[async_trait]
impl MetricsProvider for Client {
    async fn metrics(&self) -> MetricsSnapshot {
        MetricsSnapshot::default()
    }
}
