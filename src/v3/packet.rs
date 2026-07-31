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
    // OASIS MQTT v3.1.1 §1.5.3 "UTF-8 encoded strings": prefixed by a two-byte
    // length and MUST NOT exceed 65535 bytes. Truncating the length to 16 bits
    // would emit a malformed packet whose payload bytes are re-parsed as later
    // fields.
    assert!(
        bytes.len() <= 0xFFFF,
        "MQTT string length {} exceeds §1.5.3 maximum of 65535 bytes",
        bytes.len()
    );
    buf.push((bytes.len() >> 8) as u8);
    buf.push(bytes.len() as u8);
    buf.extend_from_slice(bytes);
}

fn encode_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    // OASIS MQTT v3.1.1 has no standalone "Binary Data" section (that is a
    // v5-only construct); binary fields (e.g. CONNECT Password, §3.1.3.5) use
    // the same two-byte length-prefix encoding as UTF-8 strings (§1.5.3) and
    // are bound by the same 65535-byte limit.
    assert!(
        b.len() <= 0xFFFF,
        "MQTT binary length {} exceeds 65535-byte maximum (cf. §1.5.3)",
        b.len()
    );
    buf.push((b.len() >> 8) as u8);
    buf.push(b.len() as u8);
    buf.extend_from_slice(b);
}

fn encode_remaining_length(buf: &mut Vec<u8>, mut len: usize) {
    // MQTT §2.2.3: Remaining Length is bounded to 268,435,455 (4 bytes). Refuse
    // to emit a malformed wire packet with an oversized (5+ byte) length.
    assert!(
        len <= 268_435_455,
        "remaining length {len} exceeds MQTT §2.2.3 maximum of 268435455"
    );
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
        // MQTT §3.3.2.2: a PUBLISH with QoS > 0 MUST carry a Packet Identifier.
        // Never silently emit a QoS>0 PUBLISH without one — that corrupts the
        // wire stream (payload bytes get parsed as the packet id).
        let pid =
            packet_id.expect("build_publish: QoS > 0 requires a Packet Identifier (MQTT §3.3.2.2)");
        body.push((pid >> 8) as u8);
        body.push(pid as u8);
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

// ---------------------------------------------------------------------------
// QoS 2 handshake: PUBREC / PUBREL / PUBCOMP
// ---------------------------------------------------------------------------

/// Build a PUBREC packet (receiver → sender, acknowledges receipt of a QoS 2
/// PUBLISH; MQTT v3.1.1 §3.5).
pub fn build_pubrec(packet_id: u16) -> Vec<u8> {
    let body = [(packet_id >> 8) as u8, packet_id as u8];
    packet_with_header(5, 0, &body)
}

/// Build a PUBREL packet (sender → receiver, releases a QoS 2 PUBLISH after
/// PUBREC; MQTT v3.1.1 §3.6). Fixed header flags MUST be `0b0010` (reserved).
pub fn build_pubrel(packet_id: u16) -> Vec<u8> {
    let body = [(packet_id >> 8) as u8, packet_id as u8];
    packet_with_header(6, 0x02, &body)
}

/// Build a PUBCOMP packet (receiver → sender, completes a QoS 2 exchange
/// after PUBREL; MQTT v3.1.1 §3.7).
pub fn build_pubcomp(packet_id: u16) -> Vec<u8> {
    let body = [(packet_id >> 8) as u8, packet_id as u8];
    packet_with_header(7, 0, &body)
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
    #[should_panic(expected = "Packet Identifier")]
    fn publish_qos1_without_pid_panics() {
        // MQTT §3.3.2.2: QoS > 0 PUBLISH without a Packet Identifier is illegal.
        let _ = build_publish("test/t", b"hello", QoS::AtLeastOnce, false, None);
    }

    #[test]
    fn remaining_length_at_max_boundary() {
        let mut buf = Vec::new();
        encode_remaining_length(&mut buf, 268_435_455);
        assert_eq!(buf, vec![0xFF, 0xFF, 0xFF, 0x7F]);
    }

    #[test]
    #[should_panic(expected = "exceeds MQTT")]
    fn remaining_length_above_max_panics() {
        let mut buf = Vec::new();
        encode_remaining_length(&mut buf, 268_435_456);
    }

    #[test]
    fn encode_string_at_max_boundary_ok() {
        // OASIS MQTT v3.1.1 §1.5.3: exactly 65535 bytes is the largest legal
        // UTF-8 Encoded String length; it must round-trip through the length
        // prefix without truncation or panic.
        let s = "a".repeat(0xFFFF);
        let mut buf = Vec::new();
        encode_string(&mut buf, &s);
        assert_eq!(buf[0], 0xFF);
        assert_eq!(buf[1], 0xFF);
        assert_eq!(buf.len(), 2 + 0xFFFF);
    }

    #[test]
    #[should_panic(expected = "exceeds §1.5.3 maximum of 65535 bytes")]
    fn encode_string_above_max_panics() {
        // Regression test for rust-MQTT-01: previously the 16-bit length
        // prefix silently wrapped (`(len >> 8) as u8` / `len as u8`),
        // corrupting the wire packet instead of rejecting the oversized
        // input. This must now fail loudly rather than emit a malformed
        // packet whose payload bytes get re-parsed as later fields.
        let s = "a".repeat(0x10000); // 65536 bytes, one over the limit
        let mut buf = Vec::new();
        encode_string(&mut buf, &s);
    }

    #[test]
    #[should_panic(expected = "65535-byte maximum")]
    fn encode_bytes_above_max_panics() {
        // Regression test for rust-MQTT-01 (binary-data sibling of
        // encode_string): same silent-truncation bug, same fix.
        let b = vec![0u8; 0x10000];
        let mut buf = Vec::new();
        encode_bytes(&mut buf, &b);
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
    fn pubrec_packet() {
        let pkt = build_pubrec(0x1234);
        assert_eq!(pkt, vec![0x50, 0x02, 0x12, 0x34]);
    }

    #[test]
    fn pubrel_packet_has_reserved_flags() {
        let pkt = build_pubrel(0x0001);
        // PUBREL fixed header flags MUST be 0b0010.
        assert_eq!(pkt, vec![0x62, 0x02, 0x00, 0x01]);
    }

    #[test]
    fn pubcomp_packet() {
        let pkt = build_pubcomp(0x0042);
        assert_eq!(pkt, vec![0x70, 0x02, 0x00, 0x42]);
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
