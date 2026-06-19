// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! MQTT message types and RELAY conversion.

use std::collections::BTreeMap;

use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::relay::{Message as RelayMessage, Protocol, Version};

// ---------------------------------------------------------------------------
// QoS
// ---------------------------------------------------------------------------

/// MQTT Quality of Service delivery guarantee.
//fusa:req REQ-QOS-001
//fusa:req REQ-QOS-002
//fusa:req REQ-QOS-003
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(into = "u8", try_from = "u8")]
pub enum QoS {
    /// Fire-and-forget. No acknowledgement. Messages may be lost.
    #[default]
    AtMostOnce = 0,
    /// Acknowledged delivery. At least once; duplicates possible.
    AtLeastOnce = 1,
    /// Exactly-once delivery. Highest overhead.
    ExactlyOnce = 2,
}

impl From<QoS> for u8 {
    fn from(q: QoS) -> u8 {
        q as u8
    }
}

impl TryFrom<u8> for QoS {
    type Error = String;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(QoS::AtMostOnce),
            1 => Ok(QoS::AtLeastOnce),
            2 => Ok(QoS::ExactlyOnce),
            _ => Err(format!("invalid QoS: {}", v)),
        }
    }
}

impl std::fmt::Display for QoS {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", *self as u8)
    }
}

// ---------------------------------------------------------------------------
// UserProperty
// ---------------------------------------------------------------------------

/// MQTT v5 user-defined key/value property pair.
//fusa:req REQ-V5-MSG-003
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserProperty {
    pub key: String,
    pub value: String,
}

// ---------------------------------------------------------------------------
// Message
// ---------------------------------------------------------------------------

/// A single MQTT publish message.
//fusa:req REQ-MSG-001
//fusa:req REQ-MSG-002
//fusa:req REQ-MSG-003
//fusa:req REQ-MSG-004
//fusa:req REQ-MSG-005
//fusa:req REQ-V5-MSG-001
//fusa:req REQ-V5-MSG-002
//fusa:req REQ-V5-MSG-003
//fusa:req REQ-V5-MSG-004
//fusa:req REQ-V5-MSG-005
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    /// MQTT topic the message was published on.
    pub topic: String,
    /// Raw message bytes.
    #[serde(with = "crate::base64_serde")]
    pub payload: Vec<u8>,
    /// Quality of service level.
    pub qos: QoS,
    /// Broker sent this as a retained message.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub retained: bool,
    /// Non-zero for QoS 1 and QoS 2 messages.
    #[serde(default, skip_serializing_if = "is_zero_u16")]
    pub packet_id: u16,

    // MQTT v5 properties — zero/empty means "not set"
    /// Response topic (v5 §3.3.2.3.5).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub response_topic: String,
    /// Correlation data (v5 §3.3.2.3.6).
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        with = "crate::base64_serde_opt"
    )]
    pub correlation_data: Vec<u8>,
    /// User properties (v5 §3.3.2.3.7).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub user_properties: Vec<UserProperty>,
    /// Content type (v5 §3.3.2.3.9).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub content_type: String,
    /// Message expiry interval in seconds; 0 = no expiry (v5 §3.3.2.3.3).
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub expiry_interval: u32,
}

fn is_zero_u16(v: &u16) -> bool {
    *v == 0
}
fn is_zero_u32(v: &u32) -> bool {
    *v == 0
}

impl Message {
    /// Convert this MQTT message to a relay::Message envelope per §15.7.4.
    //fusa:req REQ-RELAY-008
    pub fn to_relay_message(&self) -> RelayMessage {
        let mut meta = BTreeMap::new();
        meta.insert("mqtt.qos".into(), (self.qos as u8).to_string());
        meta.insert("mqtt.retained".into(), self.retained.to_string());
        if !self.response_topic.is_empty() {
            meta.insert("mqtt.response_topic".into(), self.response_topic.clone());
        }
        if !self.content_type.is_empty() {
            meta.insert("mqtt.content_type".into(), self.content_type.clone());
        }
        if self.expiry_interval > 0 {
            meta.insert(
                "mqtt.expiry_interval".into(),
                self.expiry_interval.to_string(),
            );
        }
        RelayMessage {
            protocol: Protocol::Mqtt,
            version: Version::default(),
            id: self.topic.clone(),
            payload: self.payload.clone(),
            timestamp: Utc::now(),
            seq: 0,
            meta,
        }
    }

    /// Convert a relay::Message envelope to an MQTT Message per §15.7.4.
    //fusa:req REQ-RELAY-009
    pub fn from_relay_message(msg: &RelayMessage) -> Result<Self, crate::error::Error> {
        let mut m = Message {
            topic: msg.id.clone(),
            payload: msg.payload.clone(),
            ..Default::default()
        };
        if let Some(q) = msg.meta.get("mqtt.qos") {
            m.qos = match q.as_str() {
                "1" => QoS::AtLeastOnce,
                "2" => QoS::ExactlyOnce,
                _ => QoS::AtMostOnce,
            };
        }
        if msg
            .meta
            .get("mqtt.retained")
            .map(|v| v == "true")
            .unwrap_or(false)
        {
            m.retained = true;
        }
        if let Some(rt) = msg.meta.get("mqtt.response_topic") {
            m.response_topic = rt.clone();
        }
        if let Some(ct) = msg.meta.get("mqtt.content_type") {
            m.content_type = ct.clone();
        }
        if let Some(ei) = msg.meta.get("mqtt.expiry_interval") {
            m.expiry_interval = ei.parse().unwrap_or(0);
        }
        Ok(m)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qos_roundtrip() {
        for (v, expected) in [
            (0u8, QoS::AtMostOnce),
            (1, QoS::AtLeastOnce),
            (2, QoS::ExactlyOnce),
        ] {
            let q = QoS::try_from(v).unwrap();
            assert_eq!(q, expected);
            assert_eq!(u8::from(q), v);
        }
    }

    #[test]
    fn qos_invalid() {
        assert!(QoS::try_from(3u8).is_err());
    }

    #[test]
    fn to_relay_message_fields() {
        let m = Message {
            topic: "sensors/temp".into(),
            payload: b"21.5".to_vec(),
            qos: QoS::AtLeastOnce,
            retained: true,
            ..Default::default()
        };
        let rm = m.to_relay_message();
        assert_eq!(rm.protocol, Protocol::Mqtt);
        assert_eq!(rm.id, "sensors/temp");
        assert_eq!(rm.payload, b"21.5".to_vec());
        assert_eq!(rm.meta.get("mqtt.qos").map(|s| s.as_str()), Some("1"));
        assert_eq!(
            rm.meta.get("mqtt.retained").map(|s| s.as_str()),
            Some("true")
        );
    }

    #[test]
    fn from_relay_message_roundtrip() {
        let original = Message {
            topic: "sensors/temp".into(),
            payload: b"21.5".to_vec(),
            qos: QoS::AtLeastOnce,
            retained: true,
            ..Default::default()
        };
        let rm = original.to_relay_message();
        let restored = Message::from_relay_message(&rm).unwrap();
        assert_eq!(restored.topic, original.topic);
        assert_eq!(restored.payload, original.payload);
        assert_eq!(restored.qos, original.qos);
        assert_eq!(restored.retained, original.retained);
    }

    #[test]
    fn message_json_roundtrip() {
        let m = Message {
            topic: "test/topic".into(),
            payload: b"hello".to_vec(),
            qos: QoS::AtMostOnce,
            ..Default::default()
        };
        let json = serde_json::to_string(&m).unwrap();
        let m2: Message = serde_json::from_str(&json).unwrap();
        assert_eq!(m, m2);
    }

    #[test]
    fn user_property_serde() {
        let up = UserProperty {
            key: "k".into(),
            value: "v".into(),
        };
        let json = serde_json::to_string(&up).unwrap();
        let up2: UserProperty = serde_json::from_str(&json).unwrap();
        assert_eq!(up, up2);
    }

    #[test]
    fn v5_fields_roundtrip() {
        let m = Message {
            topic: "req/test".into(),
            payload: b"data".to_vec(),
            qos: QoS::AtLeastOnce,
            response_topic: "resp/test".into(),
            correlation_data: vec![1, 2, 3],
            content_type: "application/json".into(),
            expiry_interval: 60,
            user_properties: vec![UserProperty {
                key: "x".into(),
                value: "y".into(),
            }],
            ..Default::default()
        };
        let json = serde_json::to_string(&m).unwrap();
        let m2: Message = serde_json::from_str(&json).unwrap();
        assert_eq!(m, m2);
    }
}
