// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! In-process mock MQTT broker for unit testing.
//!
//! Implements full publish/subscribe semantics including wildcard topic
//! matching per §4.7, retained messages, QoS 0/1/2 (all treated as
//! best-effort in the mock), and back-pressure policies.
//!
//! No network — zero external dependencies. Use this as the test backend.
//!
//! # Example
//! ```rust,no_run
//! use rust_mqtt::mock::MockClient;
//! use rust_mqtt::{Client, QoS, SubscriberConfig};
//!
//! #[tokio::main]
//! async fn main() {
//!     let client = MockClient::new();
//!     let mut sub = client.subscribe("sensors/#", QoS::AtMostOnce, SubscriberConfig::default()).await.unwrap();
//!     client.publish("sensors/temp", QoS::AtMostOnce, b"21.5".to_vec()).await.unwrap();
//!     let msg = sub.recv().await.unwrap();
//!     assert_eq!(msg.topic, "sensors/temp");
//!     assert_eq!(msg.payload, b"21.5");
//! }
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::client::{
    BackPressurePolicy, Client, Drainer, HealthProvider, HealthStatus, MetricsProvider,
    MetricsSnapshot, SubscriberConfig, Subscription,
};
use crate::error::Error;
use crate::message::{Message, QoS};
use crate::topic::match_topic;

// ---------------------------------------------------------------------------
// Internal subscription record
// ---------------------------------------------------------------------------

struct SubRecord {
    filter: String,
    tx: mpsc::Sender<Message>,
    back_pressure: BackPressurePolicy,
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
// BrokerState — shared internal state
// ---------------------------------------------------------------------------

struct BrokerState {
    closed: AtomicBool,
    subs: Mutex<HashMap<u64, SubRecord>>,
    retained: Mutex<HashMap<String, Message>>,
    next_id: AtomicU64,
    msgs_sent: AtomicU64,
    msgs_received: AtomicU64,
    bytes_sent: AtomicU64,
    errors: AtomicU64,
}

impl BrokerState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            closed: AtomicBool::new(false),
            subs: Mutex::new(HashMap::new()),
            retained: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            msgs_sent: AtomicU64::new(0),
            msgs_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        })
    }
}

// ---------------------------------------------------------------------------
// MockClient
// ---------------------------------------------------------------------------

/// An in-process MQTT client backed by the mock broker.
///
/// All operations are local — no network is involved.
//fusa:req REQ-MOCK-001
//fusa:req REQ-MOCK-002
//fusa:req REQ-MOCK-003
//fusa:req REQ-MOCK-004
//fusa:req REQ-MOCK-005
#[derive(Clone)]
pub struct MockClient {
    state: Arc<BrokerState>,
}

impl MockClient {
    /// Create a new independent mock client (its own in-process broker).
    pub fn new() -> Self {
        Self {
            state: BrokerState::new(),
        }
    }

    /// Return a second client that shares the same in-process broker.
    ///
    /// Use this to simulate two clients on the same broker in tests.
    pub fn peer(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }

    /// Inject a message directly into all matching subscriptions.
    ///
    /// Useful for simulating a broker delivering a message without going
    /// through the publish path.
    pub async fn inject(&self, msg: Message) {
        let subs = self.state.subs.lock().await;
        for sub in subs.values() {
            if match_topic(&sub.filter, &msg.topic) {
                sub.push(msg.clone());
            }
        }
    }

    /// Return all messages that have been retained by the broker.
    pub async fn retained_messages(&self) -> HashMap<String, Message> {
        self.state.retained.lock().await.clone()
    }

    /// Clear all retained messages.
    pub async fn clear_retained(&self) {
        self.state.retained.lock().await.clear();
    }

    /// Number of active subscriptions.
    pub async fn subscription_count(&self) -> usize {
        self.state.subs.lock().await.len()
    }
}

impl Default for MockClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Client for MockClient {
    //fusa:req REQ-PUB-001
    //fusa:req REQ-PUB-002
    async fn publish(&self, topic: &str, _qos: QoS, payload: Vec<u8>) -> Result<(), Error> {
        if self.state.closed.load(Ordering::SeqCst) {
            self.state.errors.fetch_add(1, Ordering::Relaxed);
            return Err(Error::Closed);
        }
        if topic.is_empty() {
            self.state.errors.fetch_add(1, Ordering::Relaxed);
            return Err(Error::TopicEmpty);
        }

        let len = payload.len() as u64;
        let msg = Message {
            topic: topic.to_owned(),
            payload,
            qos: _qos,
            ..Default::default()
        };

        self.state.msgs_sent.fetch_add(1, Ordering::Relaxed);
        self.state.bytes_sent.fetch_add(len, Ordering::Relaxed);

        let subs = self.state.subs.lock().await;
        for sub in subs.values() {
            if match_topic(&sub.filter, topic) {
                sub.push(msg.clone());
                self.state.msgs_received.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    //fusa:req REQ-PUB-003
    //fusa:req REQ-PUB-004
    async fn publish_retained(&self, topic: &str, qos: QoS, payload: Vec<u8>) -> Result<(), Error> {
        if self.state.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        if topic.is_empty() {
            return Err(Error::TopicEmpty);
        }

        let msg = Message {
            topic: topic.to_owned(),
            payload: payload.clone(),
            qos,
            retained: true,
            ..Default::default()
        };

        // Store retained message.
        {
            let mut retained = self.state.retained.lock().await;
            if payload.is_empty() {
                retained.remove(topic);
            } else {
                retained.insert(topic.to_owned(), msg.clone());
            }
        }

        self.publish(topic, qos, payload).await
    }

    //fusa:req REQ-SUB-001
    //fusa:req REQ-SUB-002
    //fusa:req REQ-SUB-003
    //fusa:req REQ-SUB-004
    //fusa:req REQ-SUB-005
    async fn subscribe(
        &self,
        topic_filter: &str,
        _qos: QoS,
        config: SubscriberConfig,
    ) -> Result<Subscription, Error> {
        if self.state.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        if topic_filter.is_empty() {
            return Err(Error::TopicEmpty);
        }

        let depth = config.chan_depth(64);
        let (tx, rx) = mpsc::channel::<Message>(depth);
        let id = self.state.next_id.fetch_add(1, Ordering::SeqCst);

        let record = SubRecord {
            filter: topic_filter.to_owned(),
            tx,
            back_pressure: config.back_pressure,
        };

        self.state.subs.lock().await.insert(id, record);

        // Deliver matching retained messages.
        {
            let retained = self.state.retained.lock().await;
            let subs = self.state.subs.lock().await;
            if let Some(sub) = subs.get(&id) {
                for (topic, msg) in retained.iter() {
                    if match_topic(topic_filter, topic) {
                        sub.push(msg.clone());
                    }
                }
            }
        }

        // Unsubscribe channel — broker listens for the filter string and removes the record.
        let (unsub_tx, unsub_rx) = oneshot::channel::<String>();
        let state_clone = self.state.clone();
        tokio::spawn(async move {
            if let Ok(_filter) = unsub_rx.await {
                state_clone.subs.lock().await.remove(&id);
            }
        });

        Ok(Subscription {
            rx,
            topic: topic_filter.to_owned(),
            unsubscribe_tx: Some(unsub_tx),
        })
    }

    //fusa:req REQ-CONN-008
    async fn close(&self) -> Result<(), Error> {
        self.state.closed.store(true, Ordering::SeqCst);
        self.state.subs.lock().await.clear();
        Ok(())
    }
}

//fusa:req REQ-RELAY-014
#[async_trait]
impl Drainer for MockClient {
    async fn close_with_drain(&self, _deadline: std::time::Duration) -> Result<(), Error> {
        self.close().await
    }
}

#[async_trait]
impl HealthProvider for MockClient {
    async fn health(&self) -> HealthStatus {
        HealthStatus {
            healthy: !self.state.closed.load(Ordering::SeqCst),
            connected: !self.state.closed.load(Ordering::SeqCst),
            endpoint: "mock://in-process".into(),
            details: std::collections::BTreeMap::new(),
        }
    }
}

#[async_trait]
impl MetricsProvider for MockClient {
    async fn metrics(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            messages_sent: self.state.msgs_sent.load(Ordering::Relaxed),
            messages_received: self.state.msgs_received.load(Ordering::Relaxed),
            bytes_sent: self.state.bytes_sent.load(Ordering::Relaxed),
            bytes_received: 0,
            active_subscriptions: self.state.subs.try_lock().map(|s| s.len()).unwrap_or(0),
            errors: self.state.errors.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn basic_pubsub() {
        let client = MockClient::new();
        let mut sub = client
            .subscribe("test/topic", QoS::AtMostOnce, SubscriberConfig::default())
            .await
            .unwrap();

        client
            .publish("test/topic", QoS::AtMostOnce, b"hello".to_vec())
            .await
            .unwrap();

        let msg = timeout(Duration::from_secs(1), sub.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg.topic, "test/topic");
        assert_eq!(msg.payload, b"hello");
    }

    #[tokio::test]
    async fn wildcard_hash() {
        let client = MockClient::new();
        let mut sub = client
            .subscribe("sensors/#", QoS::AtMostOnce, SubscriberConfig::default())
            .await
            .unwrap();

        client
            .publish("sensors/temp", QoS::AtMostOnce, b"21.5".to_vec())
            .await
            .unwrap();
        client
            .publish("sensors/pressure", QoS::AtMostOnce, b"101".to_vec())
            .await
            .unwrap();

        let m1 = timeout(Duration::from_secs(1), sub.recv())
            .await
            .unwrap()
            .unwrap();
        let m2 = timeout(Duration::from_secs(1), sub.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m1.topic, "sensors/temp");
        assert_eq!(m2.topic, "sensors/pressure");
    }

    #[tokio::test]
    async fn wildcard_plus() {
        let client = MockClient::new();
        let mut sub = client
            .subscribe("a/+/c", QoS::AtMostOnce, SubscriberConfig::default())
            .await
            .unwrap();

        client
            .publish("a/b/c", QoS::AtMostOnce, b"1".to_vec())
            .await
            .unwrap();
        client
            .publish("a/z/c", QoS::AtMostOnce, b"2".to_vec())
            .await
            .unwrap();
        client
            .publish("a/b/d", QoS::AtMostOnce, b"3".to_vec())
            .await
            .unwrap();

        let m1 = timeout(Duration::from_secs(1), sub.recv())
            .await
            .unwrap()
            .unwrap();
        let m2 = timeout(Duration::from_secs(1), sub.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m1.payload, b"1");
        assert_eq!(m2.payload, b"2");
    }

    #[tokio::test]
    async fn topic_empty_error() {
        let client = MockClient::new();
        let err = client
            .publish("", QoS::AtMostOnce, b"x".to_vec())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::TopicEmpty));
    }

    #[tokio::test]
    async fn closed_error() {
        let client = MockClient::new();
        client.close().await.unwrap();
        let err = client
            .publish("t", QoS::AtMostOnce, b"x".to_vec())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Closed));
    }

    #[tokio::test]
    async fn subscribe_empty_topic_error() {
        let client = MockClient::new();
        let result = client
            .subscribe("", QoS::AtMostOnce, SubscriberConfig::default())
            .await;
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(matches!(e, Error::TopicEmpty));
        }
    }

    #[tokio::test]
    async fn retained_delivered_on_subscribe() {
        let client = MockClient::new();
        client
            .publish_retained("sensors/temp", QoS::AtMostOnce, b"42.0".to_vec())
            .await
            .unwrap();

        let mut sub = client
            .subscribe("sensors/#", QoS::AtMostOnce, SubscriberConfig::default())
            .await
            .unwrap();

        let msg = timeout(Duration::from_secs(1), sub.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg.topic, "sensors/temp");
        assert_eq!(msg.payload, b"42.0");
    }

    #[tokio::test]
    async fn peer_shared_broker() {
        let c1 = MockClient::new();
        let c2 = c1.peer();

        let mut sub = c1
            .subscribe("#", QoS::AtMostOnce, SubscriberConfig::default())
            .await
            .unwrap();

        c2.publish("any/topic", QoS::AtMostOnce, b"from c2".to_vec())
            .await
            .unwrap();

        let msg = timeout(Duration::from_secs(1), sub.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg.payload, b"from c2");
    }

    #[tokio::test]
    async fn multiple_subscribers() {
        let client = MockClient::new();
        let mut sub1 = client
            .subscribe("#", QoS::AtMostOnce, SubscriberConfig::default())
            .await
            .unwrap();
        let mut sub2 = client
            .subscribe("data/+", QoS::AtMostOnce, SubscriberConfig::default())
            .await
            .unwrap();

        client
            .publish("data/speed", QoS::AtMostOnce, b"100".to_vec())
            .await
            .unwrap();

        let m1 = timeout(Duration::from_secs(1), sub1.recv())
            .await
            .unwrap()
            .unwrap();
        let m2 = timeout(Duration::from_secs(1), sub2.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m1.topic, "data/speed");
        assert_eq!(m2.topic, "data/speed");
    }

    #[tokio::test]
    async fn health_and_metrics() {
        let client = MockClient::new();
        let h = client.health().await;
        assert!(h.healthy);
        assert!(h.connected);

        client
            .publish("t", QoS::AtMostOnce, b"x".to_vec())
            .await
            .unwrap();
        let m = client.metrics().await;
        assert_eq!(m.messages_sent, 1);
    }

    #[tokio::test]
    async fn concurrent_publish() {
        let client = Arc::new(MockClient::new());
        let mut sub = client
            .subscribe(
                "#",
                QoS::AtMostOnce,
                SubscriberConfig {
                    channel_depth: 256,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let mut handles = vec![];
        for i in 0u64..10 {
            let c = client.clone();
            handles.push(tokio::spawn(async move {
                c.publish("t", QoS::AtMostOnce, i.to_be_bytes().to_vec())
                    .await
                    .unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        let mut count = 0usize;
        while let Ok(Some(_)) = timeout(Duration::from_millis(50), sub.recv()).await {
            count += 1;
        }
        assert_eq!(count, 10);
    }

    #[tokio::test]
    async fn close_idempotent() {
        let client = MockClient::new();
        client.close().await.unwrap();
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn system_topic_no_wildcard_match() {
        let client = MockClient::new();
        let mut sub = client
            .subscribe("#", QoS::AtMostOnce, SubscriberConfig::default())
            .await
            .unwrap();

        client
            .publish("$SYS/broker/version", QoS::AtMostOnce, b"1".to_vec())
            .await
            .unwrap();

        let result = timeout(Duration::from_millis(50), sub.recv()).await;
        assert!(result.is_err() || result.unwrap().is_none());
    }
}
