// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! rust-MQTT — Pure-Rust MQTT client library.
//!
//! Safety-oriented, broker-agnostic, COVESA/VISSR ready.
//! Conforms to RELAY spec v1.10 with ASIL-B / SIL 2 safety annotations.
//!
//! # Architecture
//!
//! Choose a backend by importing one of the sub-modules:
//!
//! - [`mock`] — in-process broker, no network, use in unit tests
//! - [`v3`] — MQTT v3.1.1 TCP client (real broker)
//! - [`broker`] — minimal embedded TCP broker for integration tests
//!
//! All backends implement the [`Client`] trait.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use rust_mqtt::mock::MockClient;
//! use rust_mqtt::{Client, QoS, SubscriberConfig};
//!
//! #[tokio::main]
//! async fn main() {
//!     let client = MockClient::new();
//!     let mut sub = client
//!         .subscribe("sensors/#", QoS::AtMostOnce, SubscriberConfig::default())
//!         .await
//!         .unwrap();
//!     client
//!         .publish("sensors/temp", QoS::AtMostOnce, b"21.5".to_vec())
//!         .await
//!         .unwrap();
//!     let msg = sub.recv().await.unwrap();
//!     println!("received: {} = {:?}", msg.topic, msg.payload);
//! }
//! ```

//fusa:req REQ-MQTT-001
//fusa:req REQ-MQTT-002
//fusa:req REQ-MQTT-003
//fusa:req REQ-MQTT-004
//fusa:req REQ-MQTT-005
//fusa:req REQ-MQTT-006
//fusa:req REQ-MQTT-007
//fusa:req REQ-MQTT-008
//fusa:req REQ-RELAY-001
//fusa:req REQ-RELAY-002
//fusa:req REQ-RELAY-003
//fusa:req REQ-RELAY-004
//fusa:req REQ-RELAY-005
//fusa:req REQ-RELAY-006
//fusa:req REQ-RELAY-007
//fusa:req REQ-RELAY-008
//fusa:req REQ-RELAY-009
//fusa:req REQ-RELAY-010
//fusa:req REQ-RELAY-011
//fusa:req REQ-RELAY-012
//fusa:req REQ-RELAY-013
//fusa:req REQ-RELAY-014
//fusa:req REQ-RELAY-015
//fusa:req REQ-RELAY-016

pub(crate) mod base64_serde;
pub(crate) mod base64_serde_opt;

pub mod adapt;
pub mod broker;
pub mod client;
pub mod error;
pub mod message;
pub mod mock;
pub mod relay;
pub mod topic;
pub mod v3;

/// §13.7.2 standard RELAY module name for the in-process virtual transport.
pub mod r#virtual {
    pub use crate::mock::*;
}

// ---------------------------------------------------------------------------
// Re-exports for ergonomic top-level use
// ---------------------------------------------------------------------------

pub use adapt::{adapt, from_message, to_message};
pub use client::{
    BackPressurePolicy, Client, Drainer, HealthProvider, HealthStatus, MetricsProvider,
    MetricsSnapshot, SubscriberConfig, Subscription,
};
pub use error::Error;
pub use message::{Message, QoS, UserProperty};
pub use topic::match_topic;

/// The RELAY specification version this library targets.
//fusa:req REQ-RELAY-001
pub const SPEC_VERSION: &str = "1.10";

/// Alias for `SPEC_VERSION` for explicitness in CLI contexts.
pub const RELAY_SPEC_VERSION: &str = "1.10";

/// Protocol integer for MQTT per RELAY spec §3.
pub const PROTOCOL_INT: i32 = 4;
