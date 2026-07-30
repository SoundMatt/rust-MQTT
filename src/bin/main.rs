// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! rust-mqtt CLI — RELAY spec §11 conformant command surface.

use std::io::BufRead;

use clap::{Parser, Subcommand, ValueEnum};
use serde_json::json;

use rust_mqtt::adapt::{from_message, to_message};
use rust_mqtt::client::{Client, SubscriberConfig};
use rust_mqtt::message::{Message, QoS};
use rust_mqtt::mock::MockClient;
use rust_mqtt::relay::Message as RelayMessage;
use rust_mqtt::v3::{Client as V3Client, ConnectOptions};

// ---------------------------------------------------------------------------
// CLI argument definitions
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(
    name = "rust-mqtt",
    version = env!("CARGO_PKG_VERSION"),
    about = "rust-MQTT: RELAY-conformant MQTT tool"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Report tool and protocol version.
    Version {
        #[arg(long, value_enum, default_value = "text")]
        format: OutputFormat,
    },

    /// Report supported capabilities as JSON.
    Capabilities,

    /// Report self-assessed health status.
    Status {
        #[arg(long, value_enum, default_value = "text")]
        format: OutputFormat,
    },

    /// Publish a message to a broker (or read NDJSON relay.Message from stdin).
    ///
    /// Use `--format json` to read a stream of relay.Message values as NDJSON
    /// on stdin and publish each until EOF (crossbar destination mode, RELAY
    /// v1.8 §send). In that mode --topic and --payload are ignored.
    Send {
        /// Broker address (host:port). Connected to over TCP unless --mock
        /// is set or --format json is used (crossbar sink mode).
        #[arg(long, default_value = "localhost:1883")]
        broker: String,
        /// MQTT topic to publish to. Ignored when --format json.
        #[arg(long, default_value = "")]
        topic: String,
        /// Message payload as UTF-8 string. Ignored when --format json.
        #[arg(long, default_value = "")]
        payload: String,
        /// QoS level (0, 1, or 2).
        #[arg(long, default_value = "0")]
        qos: u8,
        /// Send as retained message.
        #[arg(long)]
        retain: bool,
        /// Input format: 'text' (uses --topic/--payload) or 'json' (NDJSON
        /// relay.Message from stdin, crossbar sink).
        #[arg(long, value_enum, default_value = "text")]
        format: OutputFormat,
        /// Use the in-process mock broker instead of connecting to
        /// --broker. Useful for local testing without a live broker.
        #[arg(long)]
        mock: bool,
    },

    /// Subscribe to a topic and stream messages.
    Subscribe {
        /// Broker address (host:port). Connected to over TCP unless --mock
        /// is set.
        #[arg(long, default_value = "localhost:1883")]
        broker: String,
        /// Topic filter (MQTT §4.7 wildcards supported).
        #[arg(long, default_value = "#")]
        topic: String,
        /// Stop after receiving N messages (0 = unlimited).
        #[arg(long, default_value = "0")]
        count: usize,
        /// Output format.
        #[arg(long, value_enum, default_value = "text")]
        format: OutputFormat,
        /// QoS level for the subscription.
        #[arg(long, default_value = "0")]
        qos: u8,
        /// Use the in-process mock broker instead of connecting to
        /// --broker. Useful for local testing without a live broker.
        #[arg(long)]
        mock: bool,
    },

    /// Convert a canonical mqtt.Message JSON from stdin to relay.Message JSON on stdout.
    ///
    /// Reads one mqtt.Message as JSON on stdin, converts through this
    /// implementation's to_message() path, writes relay.Message JSON on stdout.
    /// Used by `relay interop` (RELAY spec §11.2).
    ///
    /// Exit codes: 0 = converted, 1 = invalid input, 2 = invalid args.
    Convert {
        /// Protocol; must be MQTT for this tool.
        #[arg(long, default_value = "MQTT")]
        protocol: String,
        /// Output format.
        #[arg(long, value_enum, default_value = "json")]
        format: OutputFormat,
    },
}

#[derive(Clone, ValueEnum)]
enum OutputFormat {
    Text,
    Json,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let exit_code = match run(cli).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("rust-mqtt: error: {}", e);
            1
        }
    };
    std::process::exit(exit_code);
}

async fn run(cli: Cli) -> Result<i32, Box<dyn std::error::Error>> {
    match cli.command {
        Commands::Version { format } => cmd_version(format),
        Commands::Capabilities => cmd_capabilities(),
        Commands::Status { format } => cmd_status(format),
        Commands::Send {
            broker,
            topic,
            payload,
            qos,
            retain,
            format,
            mock,
        } => cmd_send(broker, topic, payload, qos, retain, format, mock).await,
        Commands::Subscribe {
            broker,
            topic,
            count,
            format,
            qos,
            mock,
        } => cmd_subscribe(broker, topic, count, format, qos, mock).await,
        Commands::Convert { protocol, format } => cmd_convert(protocol, format),
    }
}

// ---------------------------------------------------------------------------
// version
// ---------------------------------------------------------------------------

fn cmd_version(format: OutputFormat) -> Result<i32, Box<dyn std::error::Error>> {
    let doc = json!({
        "tool":         "rust-mqtt",
        "protocol":     "MQTT",
        "protocol_int": rust_mqtt::PROTOCOL_INT,
        "version":      env!("CARGO_PKG_VERSION"),
        "spec_version": rust_mqtt::SPEC_VERSION,
        "language":     "rust",
        "runtime":      "rustc 1.75+",
    });
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&doc)?),
        OutputFormat::Text => {
            println!("tool:         {}", doc["tool"].as_str().unwrap_or(""));
            println!("protocol:     {}", doc["protocol"].as_str().unwrap_or(""));
            println!("version:      {}", doc["version"].as_str().unwrap_or(""));
            println!(
                "spec_version: {}",
                doc["spec_version"].as_str().unwrap_or("")
            );
            println!("language:     {}", doc["language"].as_str().unwrap_or(""));
        }
    }
    Ok(0)
}

// ---------------------------------------------------------------------------
// capabilities
// ---------------------------------------------------------------------------

fn cmd_capabilities() -> Result<i32, Box<dyn std::error::Error>> {
    let doc = json!({
        "kind":                "capabilities",
        "tool":                "rust-mqtt",
        "protocol":            "MQTT",
        "protocol_int":        rust_mqtt::PROTOCOL_INT,
        "version":             env!("CARGO_PKG_VERSION"),
        "spec_version":        rust_mqtt::SPEC_VERSION,
        "commands":            ["version", "capabilities", "status", "send", "subscribe", "convert"],
        "transports":          ["tcp"],
        "features":            ["wildcard-subscriptions", "retained-messages", "qos012", "lwt"],
        "interfaces":          ["Client", "Subscription"],
        "optional_interfaces": ["HealthProvider", "MetricsProvider", "Drainer"],
        "adapt":               true,
    });
    println!("{}", serde_json::to_string_pretty(&doc)?);
    Ok(0)
}

// ---------------------------------------------------------------------------
// status
// ---------------------------------------------------------------------------

fn cmd_status(format: OutputFormat) -> Result<i32, Box<dyn std::error::Error>> {
    let doc = json!({
        "protocol":  "MQTT",
        "tool":      "rust-mqtt",
        "version":   env!("CARGO_PKG_VERSION"),
        "healthy":   true,
        "connected": false,
        "endpoint":  "",
        "details":   {},
    });
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&doc)?),
        OutputFormat::Text => {
            println!("tool:      rust-mqtt");
            println!("protocol:  MQTT");
            println!("version:   {}", env!("CARGO_PKG_VERSION"));
            println!("healthy:   true");
            println!("connected: false");
        }
    }
    Ok(0)
}

// ---------------------------------------------------------------------------
// broker selection
// ---------------------------------------------------------------------------

/// Build the `Client` backend `send`/`subscribe` operate over: the
/// in-process mock (only when `--mock` is passed explicitly) or a real
/// MQTT v3.1.1 TCP connection to `broker`.
async fn build_client(
    broker: &str,
    mock: bool,
) -> Result<Box<dyn Client>, Box<dyn std::error::Error>> {
    if mock {
        Ok(Box::new(MockClient::new()))
    } else {
        let client = V3Client::connect(ConnectOptions::new(broker)).await?;
        Ok(Box::new(client))
    }
}

// ---------------------------------------------------------------------------
// send
// ---------------------------------------------------------------------------

//fusa:req REQ-RELAY-017
async fn cmd_send(
    broker: String,
    topic: String,
    payload: String,
    qos_val: u8,
    retain: bool,
    format: OutputFormat,
    mock: bool,
) -> Result<i32, Box<dyn std::error::Error>> {
    match format {
        OutputFormat::Json => {
            // NDJSON crossbar sink (RELAY spec §11.2): reads relay.Message
            // per line from stdin and publishes each to --broker (or the
            // mock, with --mock), same backend selection as text mode.
            let stdin = std::io::stdin();
            let client = build_client(&broker, mock).await?;
            for line in stdin.lock().lines() {
                let line = line?;
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let relay_msg: RelayMessage = serde_json::from_str(trimmed)?;
                let mqtt_msg = from_message(&relay_msg)?;
                let qos = mqtt_msg.qos;
                let t = mqtt_msg.topic.clone();
                client.publish(&t, qos, mqtt_msg.payload).await?;
            }
            Ok(0)
        }
        OutputFormat::Text => {
            // Text mode: use --topic and --payload against a real broker
            // (or the mock, with --mock).
            if topic.is_empty() {
                eprintln!("rust-mqtt: --topic is required in text mode");
                return Ok(2);
            }
            let qos = QoS::try_from(qos_val)?;
            let client = build_client(&broker, mock).await?;
            if retain {
                client
                    .publish_retained(&topic, qos, payload.into_bytes())
                    .await?;
            } else {
                client.publish(&topic, qos, payload.into_bytes()).await?;
            }
            println!("sent: {}", topic);
            Ok(0)
        }
    }
}

// ---------------------------------------------------------------------------
// subscribe
// ---------------------------------------------------------------------------

//fusa:req REQ-RELAY-018
async fn cmd_subscribe(
    broker: String,
    topic: String,
    count: usize,
    format: OutputFormat,
    qos_val: u8,
    mock: bool,
) -> Result<i32, Box<dyn std::error::Error>> {
    let qos = QoS::try_from(qos_val)?;
    let client = build_client(&broker, mock).await?;
    let mut sub = client
        .subscribe(&topic, qos, SubscriberConfig::default())
        .await?;

    let mut received = 0usize;
    while let Some(msg) = sub.recv().await {
        let relay_msg = to_message(&msg);
        match format {
            OutputFormat::Json => {
                println!("{}", serde_json::to_string(&relay_msg)?);
            }
            OutputFormat::Text => {
                println!(
                    "topic={} payload={}",
                    msg.topic,
                    String::from_utf8_lossy(&msg.payload)
                );
            }
        }
        received += 1;
        if count > 0 && received >= count {
            break;
        }
    }
    Ok(0)
}

// ---------------------------------------------------------------------------
// convert
// ---------------------------------------------------------------------------

//fusa:req REQ-RELAY-013
//fusa:req REQ-RELAY-020
fn cmd_convert(protocol: String, _format: OutputFormat) -> Result<i32, Box<dyn std::error::Error>> {
    if protocol.to_uppercase() != "MQTT" {
        eprintln!(
            "rust-mqtt: convert --protocol must be MQTT, got: {}",
            protocol
        );
        return Ok(2);
    }

    let stdin = std::io::stdin();
    let mut input = String::new();
    for line in stdin.lock().lines() {
        let l = line?;
        input.push_str(&l);
        input.push('\n');
    }
    let input = input.trim();
    if input.is_empty() {
        eprintln!("rust-mqtt: convert: no input on stdin");
        return Ok(1);
    }

    let msg: Message = match serde_json::from_str(input) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("rust-mqtt: invalid input: {}", e);
            return Ok(1);
        }
    };

    if msg.topic.is_empty() {
        eprintln!("rust-mqtt: invalid input: topic must not be empty");
        return Ok(1);
    }

    let mut relay_msg = to_message(&msg);
    // Zero timestamp for reproducibility (relay interop §11.2)
    relay_msg.timestamp = chrono::DateTime::from_timestamp(0, 0)
        .unwrap_or_default()
        .with_timezone(&chrono::Utc);

    println!("{}", serde_json::to_string(&relay_msg)?);
    Ok(0)
}
