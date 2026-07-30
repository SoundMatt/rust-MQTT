// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! Client and Subscription traits.

use async_trait::async_trait;

use crate::backpressure::RingReceiver;
use crate::error::Error;
use crate::message::{Message, QoS};

// ---------------------------------------------------------------------------
// BackPressurePolicy
// ---------------------------------------------------------------------------

/// Controls what happens when a subscription channel is full.
//fusa:req REQ-RELAY-004
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum BackPressurePolicy {
    /// Drop the arriving message (default).
    #[default]
    DropNewest = 0,
    /// Drop the oldest buffered message to make room.
    DropOldest = 1,
    /// Block until space is available.
    Block = 2,
}

// ---------------------------------------------------------------------------
// SubscriberConfig
// ---------------------------------------------------------------------------

/// Per-subscription options applied at creation time.
//fusa:req REQ-SUB-004
//fusa:req REQ-SUB-005
#[derive(Clone, Debug, Default)]
pub struct SubscriberConfig {
    /// Capacity of the internal channel. 0 means implementation default (64).
    pub channel_depth: usize,
    /// What to do when the channel is full.
    pub back_pressure: BackPressurePolicy,
}

impl SubscriberConfig {
    /// Return the resolved channel depth.
    pub fn chan_depth(&self, default: usize) -> usize {
        if self.channel_depth > 0 {
            self.channel_depth
        } else {
            default
        }
    }
}

// ---------------------------------------------------------------------------
// Subscription
// ---------------------------------------------------------------------------

/// Delivers messages from a subscribed topic filter.
///
/// The receiver channel is closed when the subscription or client is closed.
//fusa:req REQ-SUB-001
//fusa:req REQ-SUB-002
//fusa:req REQ-SUB-003
pub struct Subscription {
    pub(crate) rx: RingReceiver<Message>,
    pub(crate) topic: String,
    pub(crate) unsubscribe_tx: Option<tokio::sync::oneshot::Sender<String>>,
}

impl Subscription {
    /// Borrow the message channel.
    pub fn receiver(&mut self) -> &mut RingReceiver<Message> {
        &mut self.rx
    }

    /// Receive the next message, returning None when the channel is closed.
    pub async fn recv(&mut self) -> Option<Message> {
        self.rx.recv().await
    }

    /// Topic filter this subscription is registered for.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Remove this subscription from the broker without closing the channel.
    /// No new messages will be delivered after this returns.
    pub async fn unsubscribe(mut self) -> Result<(), Error> {
        if let Some(tx) = self.unsubscribe_tx.take() {
            let _ = tx.send(self.topic.clone());
        }
        Ok(())
    }

    /// Unsubscribe and close the message channel.
    pub async fn close(self) -> Result<(), Error> {
        self.unsubscribe().await
    }
}

// ---------------------------------------------------------------------------
// Client trait
// ---------------------------------------------------------------------------

/// Connects to an MQTT broker and provides publish/subscribe operations.
///
/// Implementations are expected to be `Clone + Send + Sync + 'static` so they
/// can be shared across async tasks.
//fusa:req REQ-PUB-001
//fusa:req REQ-PUB-002
//fusa:req REQ-PUB-003
//fusa:req REQ-PUB-004
//fusa:req REQ-SUB-001
//fusa:req REQ-SUB-002
//fusa:req REQ-SUB-003
//fusa:req REQ-CONN-008
//fusa:req REQ-CONC-001
//fusa:req REQ-CONC-002
//fusa:req REQ-CONC-003
#[async_trait]
pub trait Client: Send + Sync + 'static {
    /// Publish `payload` to `topic` at the given QoS level.
    ///
    /// Returns `Error::TopicEmpty` if `topic` is empty,
    /// `Error::Closed` if the client is closed, or
    /// `Error::QoSUnsupported` if the implementation does not support `qos`.
    async fn publish(&self, topic: &str, qos: QoS, payload: Vec<u8>) -> Result<(), Error>;

    /// Publish with retained flag.
    async fn publish_retained(&self, topic: &str, qos: QoS, payload: Vec<u8>) -> Result<(), Error> {
        // Default: delegates to publish (retained flag ignored for implementations
        // that don't support it). Override in concrete types.
        self.publish(topic, qos, payload).await
    }

    /// Create a subscription on `topic_filter` with the given QoS.
    ///
    /// `topic_filter` may contain MQTT wildcard characters `+` and `#`.
    /// Returns `Error::TopicEmpty` if empty, `Error::Closed` if closed.
    async fn subscribe(
        &self,
        topic_filter: &str,
        qos: QoS,
        config: SubscriberConfig,
    ) -> Result<Subscription, Error>;

    /// Release all resources.
    async fn close(&self) -> Result<(), Error>;
}

/// Optional drain-on-close interface (RELAY spec §9).
//fusa:req REQ-RELAY-014
#[async_trait]
pub trait Drainer: Client {
    async fn close_with_drain(&self, deadline: std::time::Duration) -> Result<(), Error>;
}

/// Optional health-reporting interface (RELAY spec §9).
//fusa:req REQ-RELAY-015
#[async_trait]
pub trait HealthProvider {
    async fn health(&self) -> Health;
}

/// Health status enum per RELAY spec §9 `HealthStatus` (`HealthOK`/`HealthDegraded`/`HealthDown`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    Ok,
    Degraded,
    #[default]
    Down,
}

/// Health report from `HealthProvider`, per RELAY spec §9 canonical `Health`.
///
/// `status` and `details` mirror the RELAY spec §9 canonical `Health{Status,
/// Details}` shape field-for-field so that cross-implementation tooling
/// (`relay report`, `relay compare`) can compare rust-mqtt's health against
/// implementations of other x-Net protocols. `connected`/`endpoint` are
/// implementation-specific extras beyond the canonical shape.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct Health {
    pub status: HealthStatus,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub details: String,
    pub connected: bool,
    pub endpoint: String,
}

/// Optional metrics interface (RELAY spec §9).
//fusa:req REQ-RELAY-016
#[async_trait]
pub trait MetricsProvider {
    async fn metrics(&self) -> MetricsSnapshot;
}

/// Point-in-time metrics snapshot.
///
/// Field names and per-field counting semantics mirror the RELAY spec §9.1
/// `Metrics` table exactly, so that two implementations of different
/// protocols report comparable numbers.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct MetricsSnapshot {
    /// One per accepted `publish()` call that returns without error.
    pub write_count: u64,
    /// One per successful enqueue onto a subscriber delivery channel,
    /// counted once per receiving subscriber.
    pub deliver_count: u64,
    /// One per sample discarded by back-pressure when a subscriber channel
    /// is full, counted once per affected subscriber.
    pub drop_count: u64,
    /// Sum of `payload.len()` (application payload only) over the sends
    /// counted by `write_count`.
    pub bytes_written: u64,
    /// Sum of `payload.len()` over the deliveries counted by `deliver_count`.
    pub bytes_delivered: u64,
    /// One per node operation that returns an error.
    pub error_count: u64,
}
