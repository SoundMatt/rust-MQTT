// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! Unified error type for rust-MQTT.
//!
//! Wraps the four mandatory RELAY sentinels (§5.1) and MQTT-specific errors.

use thiserror::Error;

/// The unified error type for all rust-MQTT operations.
//fusa:req REQ-MQTT-005
#[derive(Debug, Error)]
pub enum Error {
    /// Operation on a closed client or subscription.
    #[error("mqtt: closed")]
    Closed,

    /// Client is not connected to a broker.
    #[error("mqtt: not connected")]
    NotConnected,

    /// Operation timed out.
    #[error("mqtt: timeout")]
    Timeout,

    /// Payload exceeds the broker limit (§3.1.3.5).
    #[error("mqtt: payload too large")]
    PayloadTooLarge,

    /// Topic string is empty.
    //fusa:req REQ-MQTT-006
    #[error("mqtt: topic must not be empty")]
    TopicEmpty,

    /// QoS level is not supported by this client.
    //fusa:req REQ-QOS-004
    #[error("mqtt: QoS level not supported")]
    QoSUnsupported,

    /// MQTT protocol-level error (CONNECT refused, SUBACK error, etc.).
    //fusa:req REQ-CONN-008
    #[error("mqtt: protocol error: {0}")]
    Protocol(String),

    /// Broker returned a non-zero CONNACK return code.
    #[error("mqtt: connection refused: code={0}")]
    ConnectionRefused(u8),

    /// Underlying I/O error.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl From<crate::relay::Error> for Error {
    fn from(e: crate::relay::Error) -> Self {
        match e {
            crate::relay::Error::Closed => Error::Closed,
            crate::relay::Error::NotConnected => Error::NotConnected,
            crate::relay::Error::Timeout => Error::Timeout,
            crate::relay::Error::PayloadTooLarge => Error::PayloadTooLarge,
        }
    }
}

impl Error {
    /// Return the RELAY sentinel this error maps to, if any.
    pub fn kind(&self) -> Option<crate::relay::Error> {
        match self {
            Error::Closed => Some(crate::relay::Error::Closed),
            Error::NotConnected => Some(crate::relay::Error::NotConnected),
            Error::Timeout => Some(crate::relay::Error::Timeout),
            Error::PayloadTooLarge => Some(crate::relay::Error::PayloadTooLarge),
            _ => None,
        }
    }

    /// Convenience: is this a Closed error?
    pub fn is_closed(&self) -> bool {
        matches!(self, Error::Closed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relay_error_conversion() {
        let e: Error = crate::relay::Error::Closed.into();
        assert!(matches!(e, Error::Closed));
        assert_eq!(e.kind(), Some(crate::relay::Error::Closed));
    }

    #[test]
    fn topic_empty_kind_is_none() {
        let e = Error::TopicEmpty;
        assert!(e.kind().is_none());
    }

    #[test]
    fn error_display() {
        assert_eq!(Error::Closed.to_string(), "mqtt: closed");
        assert_eq!(Error::NotConnected.to_string(), "mqtt: not connected");
        assert_eq!(
            Error::TopicEmpty.to_string(),
            "mqtt: topic must not be empty"
        );
        assert_eq!(
            Error::PayloadTooLarge.to_string(),
            "mqtt: payload too large"
        );
    }

    #[test]
    fn is_closed() {
        assert!(Error::Closed.is_closed());
        assert!(!Error::TopicEmpty.is_closed());
    }
}
