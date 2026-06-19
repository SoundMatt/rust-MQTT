// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! Integration tests against the mock in-process broker.

use rust_mqtt::mock::MockClient;
use rust_mqtt::{Client, QoS, SubscriberConfig};
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn mock_publish_subscribe() {
    //fusa:req REQ-MOCK-001
    let client = MockClient::new();
    let mut sub = client
        .subscribe("t/+", QoS::AtMostOnce, SubscriberConfig::default())
        .await
        .unwrap();
    client
        .publish("t/a", QoS::AtMostOnce, b"data".to_vec())
        .await
        .unwrap();
    let msg = timeout(Duration::from_secs(1), sub.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.topic, "t/a");
    assert_eq!(msg.payload, b"data");
}

#[tokio::test]
async fn mock_hash_wildcard() {
    //fusa:req REQ-MOCK-002
    let client = MockClient::new();
    let mut sub = client
        .subscribe("sensors/#", QoS::AtMostOnce, SubscriberConfig::default())
        .await
        .unwrap();
    for i in 0..3u8 {
        client
            .publish(&format!("sensors/s{}", i), QoS::AtMostOnce, vec![i])
            .await
            .unwrap();
    }
    for i in 0..3u8 {
        let m = timeout(Duration::from_secs(1), sub.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.payload, vec![i]);
    }
}

#[tokio::test]
async fn mock_no_cross_topic_delivery() {
    let client = MockClient::new();
    let mut sub = client
        .subscribe("a/b", QoS::AtMostOnce, SubscriberConfig::default())
        .await
        .unwrap();
    client
        .publish("x/y", QoS::AtMostOnce, b"nope".to_vec())
        .await
        .unwrap();
    client
        .publish("a/b", QoS::AtMostOnce, b"yes".to_vec())
        .await
        .unwrap();
    let m = timeout(Duration::from_secs(1), sub.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.payload, b"yes");
}

#[tokio::test]
async fn mock_closed_errors() {
    //fusa:req REQ-MOCK-003
    let client = MockClient::new();
    client.close().await.unwrap();
    assert!(client.publish("t", QoS::AtMostOnce, vec![]).await.is_err());
    assert!(client
        .subscribe("t", QoS::AtMostOnce, SubscriberConfig::default())
        .await
        .is_err());
}

#[tokio::test]
async fn mock_retained_delivery() {
    //fusa:req REQ-MOCK-004
    let client = MockClient::new();
    client
        .publish_retained("sensor/t", QoS::AtMostOnce, b"42".to_vec())
        .await
        .unwrap();
    let mut sub = client
        .subscribe("sensor/+", QoS::AtMostOnce, SubscriberConfig::default())
        .await
        .unwrap();
    let m = timeout(Duration::from_millis(200), sub.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.payload, b"42");
}

#[tokio::test]
async fn mock_peer_sees_messages() {
    //fusa:req REQ-MOCK-005
    let c1 = MockClient::new();
    let c2 = c1.peer();
    let mut sub = c1
        .subscribe("#", QoS::AtMostOnce, SubscriberConfig::default())
        .await
        .unwrap();
    c2.publish("any/t", QoS::AtMostOnce, b"hello".to_vec())
        .await
        .unwrap();
    let m = timeout(Duration::from_secs(1), sub.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.payload, b"hello");
}

#[tokio::test]
async fn mock_channel_depth_config() {
    let client = MockClient::new();
    let mut sub = client
        .subscribe(
            "#",
            QoS::AtMostOnce,
            SubscriberConfig {
                channel_depth: 10,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    for i in 0u8..5 {
        client.publish("t", QoS::AtMostOnce, vec![i]).await.unwrap();
    }
    for i in 0u8..5 {
        let m = timeout(Duration::from_secs(1), sub.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.payload, vec![i]);
    }
}

#[tokio::test]
async fn mock_system_topic_no_hash() {
    //fusa:req REQ-WILD-002
    let client = MockClient::new();
    let mut sub = client
        .subscribe("#", QoS::AtMostOnce, SubscriberConfig::default())
        .await
        .unwrap();
    client
        .publish("$SYS/broker/version", QoS::AtMostOnce, b"1.0".to_vec())
        .await
        .unwrap();
    client
        .publish("normal/topic", QoS::AtMostOnce, b"ok".to_vec())
        .await
        .unwrap();
    // Only normal/topic should arrive
    let m = timeout(Duration::from_millis(300), sub.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.topic, "normal/topic");
}

#[tokio::test]
async fn adapt_node_send_and_subscribe() {
    //fusa:req REQ-RELAY-007
    use rust_mqtt::adapt::adapt;
    use rust_mqtt::relay::{Context, Message as RelayMsg, Protocol, SubscriberOptions};

    let client = MockClient::new();
    let node = adapt(client);
    assert_eq!(node.protocol(), Protocol::Mqtt);

    let mut rx = node.subscribe(SubscriberOptions::default()).await.unwrap();

    let mut relay_msg = RelayMsg::new(Protocol::Mqtt, "sensors/v", b"99".to_vec());
    relay_msg.meta.insert("mqtt.qos".into(), "0".into());
    relay_msg
        .meta
        .insert("mqtt.retained".into(), "false".into());

    node.send(Context::background(), relay_msg).await.unwrap();

    let received = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.id, "sensors/v");
    assert_eq!(received.payload, b"99");
}
