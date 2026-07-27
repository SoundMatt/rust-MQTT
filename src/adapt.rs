// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! RELAY adapter — wraps a Client as a relay::Node.
//!
//! Implements §10.3, §10.4, §10.5, and §15.7.4 of the RELAY spec.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::backpressure;
use crate::client::{BackPressurePolicy, Client, SubscriberConfig};
use crate::message::{Message, QoS};
use crate::relay::{self, Context, Error as RelayError, Node, Protocol, SubscriberOptions};

/// How often `send()` polls `ctx.done()` while waiting on the underlying
/// publish. `relay::Context` only exposes a point-in-time `done()` check
/// (spec §18.3), not a duration, so cancellation is observed by polling at
/// this interval rather than sleeping until an exact instant.
const CTX_POLL_INTERVAL: Duration = Duration::from_millis(10);

// ---------------------------------------------------------------------------
// NodeAdapter
// ---------------------------------------------------------------------------

struct NodeAdapter<C: Client> {
    client: Arc<C>,
}

#[async_trait]
impl<C: Client> Node for NodeAdapter<C> {
    fn protocol(&self) -> Protocol {
        Protocol::Mqtt
    }

    //fusa:req REQ-RELAY-007
    async fn send(&self, ctx: Context, msg: relay::Message) -> Result<(), RelayError> {
        let qos = match msg.meta.get("mqtt.qos").map(|s| s.as_str()) {
            Some("1") => QoS::AtLeastOnce,
            Some("2") => QoS::ExactlyOnce,
            _ => QoS::AtMostOnce,
        };

        let publish = self.client.publish(&msg.id, qos, msg.payload);
        tokio::pin!(publish);
        loop {
            tokio::select! {
                res = &mut publish => {
                    // §5.2: preserve the underlying error's real sentinel
                    // instead of blindly collapsing every failure to
                    // NotConnected.
                    return res.map_err(|e| e.kind().unwrap_or(RelayError::NotConnected));
                }
                _ = tokio::time::sleep(CTX_POLL_INTERVAL) => {
                    if ctx.done() {
                        return Err(RelayError::Timeout);
                    }
                }
            }
        }
    }

    async fn subscribe(
        &self,
        opts: SubscriberOptions,
    ) -> Result<mpsc::Receiver<relay::Message>, RelayError> {
        let depth = opts.chan_depth(64);
        let bp = match opts.back_pressure {
            relay::BackPressurePolicy::DropOldest => BackPressurePolicy::DropOldest,
            relay::BackPressurePolicy::Block => BackPressurePolicy::Block,
            _ => BackPressurePolicy::DropNewest,
        };

        let cfg = SubscriberConfig {
            channel_depth: depth,
            back_pressure: bp,
        };
        let mut sub = self
            .client
            .subscribe("#", QoS::AtMostOnce, cfg)
            .await
            .map_err(|e| e.kind().unwrap_or(RelayError::NotConnected))?;

        // relay::Node::subscribe (spec §18.3) must return a literal
        // tokio::sync::mpsc::Receiver, whose sending half cannot evict a
        // queued item once handed to the caller. So `DropOldest` is applied
        // correctly on an intermediate ring buffer (§10.5.3) that this task
        // owns end-to-end; the final stage below just forwards in order.
        let (ring_tx, mut ring_rx) = backpressure::channel::<relay::Message>(depth);
        let back_pressure = opts.back_pressure;
        tokio::spawn(async move {
            while let Some(m) = sub.recv().await {
                let rm = m.to_relay_message();
                match back_pressure {
                    relay::BackPressurePolicy::DropOldest => {
                        ring_tx.push_drop_oldest(rm).await;
                    }
                    relay::BackPressurePolicy::Block => {
                        ring_tx.push_block(rm).await;
                    }
                    relay::BackPressurePolicy::DropNewest => {
                        ring_tx.push_drop_newest(rm).await;
                    }
                }
            }
            ring_tx.close();
        });

        let (tx, rx) = mpsc::channel::<relay::Message>(depth);
        tokio::spawn(async move {
            while let Some(rm) = ring_rx.recv().await {
                if tx.send(rm).await.is_err() {
                    break;
                }
            }
        });

        Ok(rx)
    }

    async fn close(&self) -> Result<(), RelayError> {
        self.client
            .close()
            .await
            .map_err(|e| e.kind().unwrap_or(RelayError::Closed))
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Wrap a `Client` implementation as a `relay::Node`.
///
/// This is the §13.7 `adapt` entry point: `adapt(client) → Node`.
//fusa:req REQ-RELAY-007
pub fn adapt<C: Client>(client: C) -> Arc<dyn Node> {
    Arc::new(NodeAdapter {
        client: Arc::new(client),
    })
}

/// Convert an MQTT `Message` to a `relay::Message` per §15.7.4.
//fusa:req REQ-RELAY-008
pub fn to_message(m: &Message) -> relay::Message {
    m.to_relay_message()
}

/// Convert a `relay::Message` to an MQTT `Message` per §15.7.4.
//fusa:req REQ-RELAY-009
pub fn from_message(msg: &relay::Message) -> Result<Message, crate::error::Error> {
    Message::from_relay_message(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::QoS;

    #[test]
    fn to_message_fields() {
        let m = Message {
            topic: "test/topic".into(),
            payload: b"hello".to_vec(),
            qos: QoS::AtLeastOnce,
            retained: false,
            ..Default::default()
        };
        let rm = to_message(&m);
        assert_eq!(rm.protocol, Protocol::Mqtt);
        assert_eq!(rm.id, "test/topic");
        assert_eq!(rm.meta["mqtt.qos"], "1");
    }

    #[test]
    fn from_message_roundtrip() {
        let m = Message {
            topic: "sensors/speed".into(),
            payload: b"42.0".to_vec(),
            qos: QoS::ExactlyOnce,
            retained: true,
            ..Default::default()
        };
        let rm = to_message(&m);
        let m2 = from_message(&rm).unwrap();
        assert_eq!(m2.topic, m.topic);
        assert_eq!(m2.payload, m.payload);
        assert_eq!(m2.qos, m.qos);
        assert_eq!(m2.retained, m.retained);
    }

    /// A `Client` whose `publish` never completes, so `send()`'s only way to
    /// return is by honoring `ctx.done()` (spec §18.3, §6 requirement 5).
    struct NeverRespondClient;

    #[async_trait]
    impl Client for NeverRespondClient {
        async fn publish(
            &self,
            _topic: &str,
            _qos: QoS,
            _payload: Vec<u8>,
        ) -> Result<(), crate::error::Error> {
            std::future::pending::<()>().await;
            unreachable!("pending future never resolves")
        }

        async fn subscribe(
            &self,
            _topic_filter: &str,
            _qos: QoS,
            _config: SubscriberConfig,
        ) -> Result<crate::client::Subscription, crate::error::Error> {
            Err(crate::error::Error::NotConnected)
        }

        async fn close(&self) -> Result<(), crate::error::Error> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn send_honors_ctx_timeout() {
        let node = adapt(NeverRespondClient);
        let msg = relay::Message::new(Protocol::Mqtt, "t", b"x".to_vec());
        let start = std::time::Instant::now();
        let res = node
            .send(Context::with_timeout(Duration::from_millis(50)), msg)
            .await;
        assert!(
            matches!(res, Err(RelayError::Timeout)),
            "expected Timeout, got {:?}",
            res
        );
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "send() must return promptly after ctx deadline expires"
        );
    }

    #[tokio::test]
    async fn close_preserves_real_error_kind() {
        // §5.2: Adapt() must not blindly collapse every close() failure to
        // RelayError::Closed when the underlying error maps to a different
        // sentinel.
        struct TimeoutOnCloseClient;

        #[async_trait]
        impl Client for TimeoutOnCloseClient {
            async fn publish(
                &self,
                _topic: &str,
                _qos: QoS,
                _payload: Vec<u8>,
            ) -> Result<(), crate::error::Error> {
                Ok(())
            }
            async fn subscribe(
                &self,
                _topic_filter: &str,
                _qos: QoS,
                _config: SubscriberConfig,
            ) -> Result<crate::client::Subscription, crate::error::Error> {
                Err(crate::error::Error::NotConnected)
            }
            async fn close(&self) -> Result<(), crate::error::Error> {
                Err(crate::error::Error::Timeout)
            }
        }

        let node = adapt(TimeoutOnCloseClient);
        let res = node.close().await;
        assert_eq!(res, Err(RelayError::Timeout));
    }
}
