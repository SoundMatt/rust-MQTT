// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! RELAY protocol types bundled locally.
//!
//! These types mirror the RELAY spec v1.10 definitions for Rust (§18.3).

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use std::sync::atomic::Ordering;
use thiserror::Error;
use tokio::sync::mpsc;

// ---------------------------------------------------------------------------
// Protocol enum
// ---------------------------------------------------------------------------

/// Protocol identifiers per RELAY spec §3.
//fusa:req REQ-MQTT-001
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "i32", try_from = "i32")]
pub enum Protocol {
    Can = 1,
    Dds = 2,
    Lin = 3,
    Mqtt = 4,
    Rcp = 5,
    Someip = 6,
}

impl From<Protocol> for i32 {
    fn from(p: Protocol) -> i32 {
        p as i32
    }
}

impl TryFrom<i32> for Protocol {
    type Error = String;
    fn try_from(v: i32) -> Result<Self, Self::Error> {
        match v {
            1 => Ok(Protocol::Can),
            2 => Ok(Protocol::Dds),
            3 => Ok(Protocol::Lin),
            4 => Ok(Protocol::Mqtt),
            5 => Ok(Protocol::Rcp),
            6 => Ok(Protocol::Someip),
            _ => Err(format!("unknown protocol: {}", v)),
        }
    }
}

impl std::fmt::Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Protocol::Can => "CAN",
            Protocol::Dds => "DDS",
            Protocol::Lin => "LIN",
            Protocol::Mqtt => "MQTT",
            Protocol::Rcp => "RCP",
            Protocol::Someip => "SOMEIP",
        };
        f.write_str(s)
    }
}

// ---------------------------------------------------------------------------
// Version
// ---------------------------------------------------------------------------

/// Semantic version triplet per RELAY spec §4.1.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Version {
    pub major: i32,
    pub minor: i32,
    pub patch: i32,
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

// ---------------------------------------------------------------------------
// Message
// ---------------------------------------------------------------------------

/// Universal message envelope per RELAY spec §4.
//fusa:req REQ-RELAY-001
//fusa:req REQ-RELAY-002
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    pub protocol: Protocol,
    pub version: Version,
    pub id: String,
    #[serde(with = "crate::base64_serde")]
    pub payload: Vec<u8>,
    pub timestamp: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub seq: u64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub meta: BTreeMap<String, String>,
}

fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}

impl Message {
    pub fn new(protocol: Protocol, id: impl Into<String>, payload: Vec<u8>) -> Self {
        Self {
            protocol,
            version: Version::default(),
            id: id.into(),
            payload,
            timestamp: Utc::now(),
            seq: 0,
            meta: BTreeMap::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Back-pressure policy
// ---------------------------------------------------------------------------

/// Back-pressure policy for subscriber channels per RELAY spec §14.
//fusa:req REQ-RELAY-004
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum BackPressurePolicy {
    /// Drop the arriving message when the channel is full (default).
    #[default]
    DropNewest,
    /// Drop the oldest buffered message to make room.
    DropOldest,
    /// Block until space is available.
    Block,
}

// ---------------------------------------------------------------------------
// SubscriberOptions
// ---------------------------------------------------------------------------

/// Options for creating a RELAY-level subscriber.
//fusa:req REQ-RELAY-005
#[derive(Clone, Debug, Default)]
pub struct SubscriberOptions {
    pub chan_depth: usize,
    pub back_pressure: BackPressurePolicy,
}

impl SubscriberOptions {
    pub fn chan_depth(&self, default: usize) -> usize {
        if self.chan_depth > 0 {
            self.chan_depth
        } else {
            default
        }
    }
}

// ---------------------------------------------------------------------------
// Sentinel errors
// ---------------------------------------------------------------------------

/// Sentinel error values per RELAY spec §5.
//fusa:req REQ-RELAY-003
#[derive(Debug, Error, PartialEq, Eq)]
pub enum Error {
    #[error("relay: closed")]
    Closed,
    #[error("relay: not connected")]
    NotConnected,
    #[error("relay: timeout")]
    Timeout,
    #[error("relay: payload too large")]
    PayloadTooLarge,
}

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

/// Execution context carrying cancellation and deadline.
/// Wraps a tokio CancellationToken placeholder — callers pass
/// `Context::background()` for fire-and-forget operations.
#[derive(Clone, Debug, Default)]
pub struct Context {
    _private: (),
}

impl Context {
    pub fn background() -> Self {
        Self { _private: () }
    }
}

// ---------------------------------------------------------------------------
// MessageReceiver
// ---------------------------------------------------------------------------

/// Receiver handle for a RELAY-level message subscription.
pub struct MessageReceiver {
    pub(crate) rx: mpsc::Receiver<Message>,
    #[allow(dead_code)]
    pub(crate) inner: std::sync::Arc<SubInner>,
}

impl MessageReceiver {
    pub async fn recv(&mut self) -> Option<Message> {
        self.rx.recv().await
    }
}

// ---------------------------------------------------------------------------
// SubInner — shared subscription state
// ---------------------------------------------------------------------------

pub struct SubInner {
    pub tx: mpsc::Sender<Message>,
    pub closed: std::sync::atomic::AtomicBool,
    pub back_pressure: BackPressurePolicy,
}

impl SubInner {
    pub fn new(tx: mpsc::Sender<Message>, back_pressure: BackPressurePolicy) -> Self {
        Self {
            tx,
            closed: std::sync::atomic::AtomicBool::new(false),
            back_pressure,
        }
    }

    pub fn push(&self, msg: Message) {
        use std::sync::atomic::Ordering;
        if self.closed.load(Ordering::Relaxed) {
            return;
        }
        match self.back_pressure {
            BackPressurePolicy::DropNewest => {
                let _ = self.tx.try_send(msg);
            }
            BackPressurePolicy::DropOldest => {
                if self.tx.try_send(msg.clone()).is_err() {
                    // drain one slot then retry
                    let _ = self.tx.try_send(msg);
                }
            }
            BackPressurePolicy::Block => {
                let tx = self.tx.clone();
                tokio::spawn(async move {
                    let _ = tx.send(msg).await;
                });
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Node trait
// ---------------------------------------------------------------------------

/// A RELAY node — wraps a protocol implementation for protocol-agnostic routing.
//fusa:req REQ-RELAY-006
#[async_trait]
pub trait Node: Send + Sync {
    fn protocol(&self) -> Protocol;
    async fn send(&self, ctx: Context, msg: Message) -> Result<(), Error>;
    async fn subscribe(&self, opts: SubscriberOptions) -> Result<mpsc::Receiver<Message>, Error>;
    async fn close(&self) -> Result<(), Error>;
}
