// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! MQTT v3.1.1 packet serialisation helpers.

use crate::message::QoS;
use crate::v3::ConnectOptions;

// ---------------------------------------------------------------------------
// Fixed header packet types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketType {
    Connect = 1,
    Connack = 2,
    Publish = 3,
    Puback = 4,
    Pubrec = 5,
    Pubrel = 6,
    Pubcomp = 7,
    Subscribe = 8,
    Suback = 9,
    Unsubscribe = 10,
    Unsuback = 11,
    Pingreq = 12,
    Pingresp = 13,
    Disconnect = 14,
    Unknown = 0,
}

impl PacketType {
    pub fn from_byte(b: u8) -> Self {
        match b {
            1 => PacketType::Connect,
            2 => PacketType::Connack,
            3 => PacketType::Publish,
            4 => PacketType::Puback,
            5 => PacketType::Pubrec,
            6 => PacketType::Pubrel,
            7 => PacketType::Pubcomp,
            8 => PacketType::Subscribe,
            9 => PacketType::Suback,
            10 => PacketType::Unsubscribe,
            11 => PacketType::Unsuback,
            12 => PacketType::Pingreq,
            13 => PacketType::Pingresp,
            14 => PacketType::Disconnect,
            _ => PacketType::Unknown,
        }
    }
}

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

fn encode_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    buf.push((bytes.len() >> 8) as u8);
    buf.push(bytes.len() as u8);
    buf.extend_from_slice(bytes);
}

fn encode_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    buf.push((b.len() >> 8) as u8);
    buf.push(b.len() as u8);
    buf.extend_from_slice(b);
}

fn encode_remaining_length(buf: &mut Vec<u8>, mut len: usize) {
    loop {
        let mut digit = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            digit |= 0x80;
        }
        buf.push(digit);
        if len == 0 {
            break;
        }
    }
}

fn packet_with_header(ptype: u8, flags: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push((ptype << 4) | flags);
    encode_remaining_length(&mut out, payload.len());
    out.extend_from_slice(payload);
    out
}

// ---------------------------------------------------------------------------
// CONNECT
// ---------------------------------------------------------------------------

/// Build a CONNECT packet for MQTT v3.1.1.
pub fn build_connect(opts: &ConnectOptions) -> Vec<u8> {
    let mut payload = Vec::new();

    // Protocol name
    encode_string(&mut payload, "MQTT");
    // Protocol level (4 = v3.1.1)
    payload.push(4);

    // Connect flags
    let mut flags: u8 = 0;
    if opts.clean_session {
        flags |= 0x02;
    }
    if let Some(will) = &opts.will {
        flags |= 0x04;
        flags |= (will.qos as u8) << 3;
        if will.retain {
            flags |= 0x20;
        }
    }
    if opts.username.is_some() {
        flags |= 0x80;
    }
    if opts.password.is_some() {
        flags |= 0x40;
    }
    payload.push(flags);

    // Keepalive
    payload.push((opts.keepalive_secs >> 8) as u8);
    payload.push(opts.keepalive_secs as u8);

    // Payload
    encode_string(&mut payload, &opts.client_id);

    if let Some(will) = &opts.will {
        encode_string(&mut payload, &will.topic);
        encode_bytes(&mut payload, &will.payload);
    }
    if let Some(u) = &opts.username {
        encode_string(&mut payload, u);
    }
    if let Some(p) = &opts.password {
        encode_bytes(&mut payload, p);
    }

    packet_with_header(1, 0, &payload)
}

// ---------------------------------------------------------------------------
// DISCONNECT
// ---------------------------------------------------------------------------

pub fn build_disconnect() -> Vec<u8> {
    vec![0xE0, 0x00]
}

// ---------------------------------------------------------------------------
// PINGREQ
// ---------------------------------------------------------------------------

pub fn build_pingreq() -> Vec<u8> {
    vec![0xC0, 0x00]
}

// ---------------------------------------------------------------------------
// PUBLISH
// ---------------------------------------------------------------------------

pub fn build_publish(
    topic: &str,
    payload: &[u8],
    qos: QoS,
    retained: bool,
    packet_id: Option<u16>,
) -> Vec<u8> {
    let mut body = Vec::new();
    encode_string(&mut body, topic);
    if qos != QoS::AtMostOnce {
        if let Some(pid) = packet_id {
            body.push((pid >> 8) as u8);
            body.push(pid as u8);
        }
    }
    body.extend_from_slice(payload);

    let flags: u8 = ((qos as u8) << 1) | (retained as u8);
    packet_with_header(3, flags, &body)
}

// ---------------------------------------------------------------------------
// SUBSCRIBE / UNSUBSCRIBE
// ---------------------------------------------------------------------------

pub fn build_subscribe(filter: &str, qos: QoS, packet_id: u16) -> Vec<u8> {
    let mut body = Vec::new();
    body.push((packet_id >> 8) as u8);
    body.push(packet_id as u8);
    encode_string(&mut body, filter);
    body.push(qos as u8);
    packet_with_header(8, 0x02, &body)
}

pub fn build_unsubscribe(filter: &str, packet_id: u16) -> Vec<u8> {
    let mut body = Vec::new();
    body.push((packet_id >> 8) as u8);
    body.push(packet_id as u8);
    encode_string(&mut body, filter);
    packet_with_header(10, 0x02, &body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v3::{ConnectOptions, WillMessage};

    #[test]
    fn connect_packet_structure() {
        let opts = ConnectOptions::new("localhost:1883")
            .client_id("test")
            .clean_session(true)
            .keepalive(60);
        let pkt = build_connect(&opts);
        // First byte: CONNECT type (1) | flags (0) = 0x10
        assert_eq!(pkt[0], 0x10);
        // Protocol name starts after fixed header
        let body = &pkt[2..];
        assert_eq!(&body[0..6], &[0x00, 0x04, b'M', b'Q', b'T', b'T']);
        assert_eq!(body[6], 4); // protocol level
    }

    #[test]
    fn publish_packet_qos0() {
        let pkt = build_publish("test/t", b"hello", QoS::AtMostOnce, false, None);
        assert_eq!(pkt[0], 0x30); // PUBLISH, QoS=0, no retain
    }

    #[test]
    fn publish_packet_qos1() {
        let pkt = build_publish("test/t", b"hello", QoS::AtLeastOnce, false, Some(1));
        assert_eq!(pkt[0], 0x32); // PUBLISH, QoS=1
    }

    #[test]
    fn subscribe_packet() {
        let pkt = build_subscribe("a/b/#", QoS::AtMostOnce, 42);
        assert_eq!(pkt[0], 0x82); // SUBSCRIBE + flags=2
                                  // packet id
        assert_eq!(pkt[2], 0x00);
        assert_eq!(pkt[3], 42);
    }

    #[test]
    fn disconnect_packet() {
        let pkt = build_disconnect();
        assert_eq!(pkt, vec![0xE0, 0x00]);
    }

    #[test]
    fn pingreq_packet() {
        let pkt = build_pingreq();
        assert_eq!(pkt, vec![0xC0, 0x00]);
    }

    #[test]
    fn connect_with_will() {
        let opts = ConnectOptions::new("localhost:1883")
            .client_id("c")
            .will(WillMessage {
                topic: "lwt/topic".into(),
                payload: b"offline".to_vec(),
                qos: QoS::AtLeastOnce,
                retain: false,
            });
        let pkt = build_connect(&opts);
        assert_eq!(pkt[0], 0x10);
        // Will flag should be set: byte 9 in body (after protocol name + level)
        let body = &pkt[2..];
        let flags = body[7];
        assert!(flags & 0x04 != 0); // will flag
        assert!(flags & (0x01 << 3) != 0); // will qos=1
    }
}
