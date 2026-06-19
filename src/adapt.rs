// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! RELAY adapter — wraps a Client as a relay::Node.
//!
//! Implements §10.3, §10.4, §10.5, and §15.7.4 of the RELAY spec.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::client::{BackPressurePolicy, Client, SubscriberConfig};
use crate::message::{Message, QoS};
use crate::relay::{self, Context, Error as RelayError, Node, Protocol, SubscriberOptions};

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

    async fn send(&self, _ctx: Context, msg: relay::Message) -> Result<(), RelayError> {
        let qos = match msg.meta.get("mqtt.qos").map(|s| s.as_str()) {
            Some("1") => QoS::AtLeastOnce,
            Some("2") => QoS::ExactlyOnce,
            _ => QoS::AtMostOnce,
        };
        self.client
            .publish(&msg.id, qos, msg.payload)
            .await
            .map_err(|_| RelayError::NotConnected)
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
            .map_err(|_| RelayError::NotConnected)?;

        let (tx, rx) = mpsc::channel::<relay::Message>(depth);
        let back_pressure = opts.back_pressure;

        tokio::spawn(async move {
            while let Some(m) = sub.recv().await {
                let rm = m.to_relay_message();
                match back_pressure {
                    relay::BackPressurePolicy::DropOldest => {
                        if tx.try_send(rm.clone()).is_err() {
                            let _ = tx.try_send(rm);
                        }
                    }
                    relay::BackPressurePolicy::Block => {
                        let _ = tx.send(rm).await;
                    }
                    _ => {
                        let _ = tx.try_send(rm);
                    }
                }
            }
        });

        Ok(rx)
    }

    async fn close(&self) -> Result<(), RelayError> {
        self.client.close().await.map_err(|_| RelayError::Closed)
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
}
