// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! RELAY golden-vector conformance tests (RELAY spec §15.7, §20.2).

use std::collections::BTreeMap;

use rust_mqtt::message::{Message, QoS};
use rust_mqtt::relay::Error as RelayError;
use rust_mqtt::relay::Protocol;
use rust_mqtt::{from_message, to_message, Error};

// ---------------------------------------------------------------------------
// Vector fixture
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct MqttMessageVector {
    #[allow(dead_code)]
    name: String,
    value: Message,
    message: VectorRelayMessage,
}

#[derive(serde::Deserialize)]
struct VectorRelayMessage {
    #[allow(dead_code)]
    protocol: i32,
    id: String,
    #[serde(deserialize_with = "deser_base64")]
    #[allow(dead_code)]
    payload: Vec<u8>,
    #[allow(dead_code)]
    meta: BTreeMap<String, String>,
}

fn deser_base64<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
    use base64::{engine::general_purpose::STANDARD, Engine};
    let s = String::deserialize(d)?;
    STANDARD
        .decode(s.as_bytes())
        .map_err(serde::de::Error::custom)
}

use serde::Deserialize;

fn load_vector() -> MqttMessageVector {
    let data = std::fs::read_to_string("testdata/mqtt-message.json")
        .expect("testdata/mqtt-message.json must exist");
    serde_json::from_str(&data).expect("parse mqtt-message.json")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn spec_version_is_1_11() {
    //fusa:req REQ-RELAY-001
    assert_eq!(rust_mqtt::SPEC_VERSION, "1.11");
    assert_eq!(rust_mqtt::RELAY_SPEC_VERSION, "1.11");
}

#[test]
fn protocol_int_is_4() {
    assert_eq!(rust_mqtt::PROTOCOL_INT, 4);
}

#[test]
fn vector_message_decodes() {
    //fusa:req REQ-MSG-001
    let v = load_vector();
    assert_eq!(v.value.topic, "sensors/temp");
    assert_eq!(v.value.payload, b"21.5".to_vec());
    assert_eq!(v.value.qos, QoS::AtLeastOnce);
    assert!(v.value.retained);
}

#[test]
fn vector_to_relay_message() {
    //fusa:req REQ-RELAY-008
    let v = load_vector();
    let rm = to_message(&v.value);
    assert_eq!(rm.protocol, Protocol::Mqtt);
    assert_eq!(rm.id, v.message.id);
    assert_eq!(rm.payload, b"21.5".to_vec());
    assert_eq!(rm.meta.get("mqtt.qos").map(|s| s.as_str()), Some("1"));
    assert_eq!(
        rm.meta.get("mqtt.retained").map(|s| s.as_str()),
        Some("true")
    );
}

#[test]
fn vector_from_relay_message_roundtrip() {
    //fusa:req REQ-RELAY-009
    let v = load_vector();
    let rm = to_message(&v.value);
    let restored = from_message(&rm).unwrap();
    assert_eq!(restored.topic, v.value.topic);
    assert_eq!(restored.payload, v.value.payload);
    assert_eq!(restored.qos, v.value.qos);
    assert_eq!(restored.retained, v.value.retained);
}

#[test]
fn relay_message_meta_canonical() {
    //fusa:req REQ-RELAY-008
    let m = Message {
        topic: "v/s".into(),
        payload: b"42".to_vec(),
        qos: QoS::ExactlyOnce,
        retained: false,
        ..Default::default()
    };
    let rm = to_message(&m);
    assert_eq!(rm.meta["mqtt.qos"], "2");
    assert_eq!(rm.meta["mqtt.retained"], "false");
}

#[test]
fn relay_message_protocol_int() {
    let m = Message {
        topic: "t".into(),
        payload: vec![],
        ..Default::default()
    };
    let rm = to_message(&m);
    let as_int: i32 = rm.protocol.into();
    assert_eq!(as_int, 4);
}

#[test]
fn error_sentinel_kind_mapping() {
    //fusa:req REQ-RELAY-003
    let errs: Vec<(Error, RelayError)> = vec![
        (Error::Closed, RelayError::Closed),
        (Error::NotConnected, RelayError::NotConnected),
        (Error::Timeout, RelayError::Timeout),
        (Error::PayloadTooLarge, RelayError::PayloadTooLarge),
    ];
    for (e, expected) in errs {
        assert_eq!(e.kind(), Some(expected));
    }
}

#[test]
fn back_pressure_policy_values() {
    //fusa:req REQ-RELAY-004
    use rust_mqtt::BackPressurePolicy;
    assert_eq!(BackPressurePolicy::DropNewest as u8, 0);
    assert_eq!(BackPressurePolicy::DropOldest as u8, 1);
    assert_eq!(BackPressurePolicy::Block as u8, 2);
}

#[test]
fn error_relay_conversion() {
    //fusa:req REQ-RELAY-003
    let e: Error = RelayError::Closed.into();
    assert!(matches!(e, Error::Closed));
}

#[test]
fn qos_try_from_valid() {
    //fusa:req REQ-QOS-001
    assert_eq!(QoS::try_from(0u8).unwrap(), QoS::AtMostOnce);
    assert_eq!(QoS::try_from(1u8).unwrap(), QoS::AtLeastOnce);
    assert_eq!(QoS::try_from(2u8).unwrap(), QoS::ExactlyOnce);
    assert!(QoS::try_from(3u8).is_err());
}

#[test]
fn match_topic_conformance() {
    //fusa:req REQ-WILD-001
    use rust_mqtt::match_topic;
    assert!(match_topic("a/b/c", "a/b/c"));
    assert!(match_topic("#", "a/b/c"));
    assert!(!match_topic("#", "$SYS/broker"));
    assert!(match_topic("a/#", "a/b/c"));
    assert!(match_topic("a/+/c", "a/b/c"));
    assert!(!match_topic("+", "$SYS/broker"));
    assert!(match_topic("$SYS/#", "$SYS/broker/version"));
}
