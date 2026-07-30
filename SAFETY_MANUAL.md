# rust-MQTT Safety Manual

**Document ID:** SM-001  
**Version:** 1.0  
**Date:** 2026-06-19  
**Status:** Active  
**Author:** Matt Jones (matt@jellybaby.com)  
**Standards:** ISO 26262:2018 Part 8 §6 (SEOOC), IEC 61508-3:2010 §7.4  

---

## 1. Purpose

This Safety Manual defines the obligations that an integrating application must
fulfil when incorporating rust-MQTT into a safety-related system. rust-MQTT is
developed as a **Safety Element Out Of Context (SEOOC)** targeting ASIL-B /
SIL 2 for library functions. The integrating system is responsible for all
system-level HARA, hardware fault modelling, and ASIL allocation.

---

## 2. Claimed safety properties

| Property | Claim | Evidence |
|---|---|---|
| ASIL | ASIL-B for all library API functions (with the §3 ASIL-D exclusion for HE-001/HE-003) | `hara.json`, `safety-case.json` |
| SIL | SIL 2 per IEC 61508-3 | `SAFETY_PLAN.md` |
| Memory safety | No `unsafe` blocks in library code | `src/`, CI rsfusa check |
| Requirement coverage | 131 requirements, 100% traced | `requirements.json` |
| RELAY conformance | RELAY spec v2.0 §20 | CI relay-conform job |
| FMEA | 15 failure modes analysed, RPN < 60 | `fmea.json` |
| TARA | 10 threats analysed, CVSS scored | `tara.json` |
| HARA | 6 hazardous events, 6 safety goals | `hara.json` |

---

## 3. Integration obligations for ASIL-D functions (SG-001, SG-003)

**Hazardous events HE-001 and HE-003 in `hara.json` are rated ASIL-D.**
rust-MQTT is rated ASIL-B and therefore **MUST NOT** be used as the sole
communication path for functions with ASIL-D allocation.

Integrators who need ASIL-D communication **SHALL**:

1. Provide a redundant, independent communication channel (e.g. CAN, a second
   MQTT path, or a hardware interlock) for all brake-by-wire and direct
   actuation commands.
2. Perform ASIL decomposition per ISO 26262 Part 9 §5 before assigning
   rust-MQTT as one of the ASIL decomposed elements.
3. Ensure the two paths share no common-cause failures (different hardware,
   different network paths).

---

## 4. Topic namespace segregation (SG-003)

To prevent topic misrouting (HE-003, HAZ-003):

1. **Safety-critical topics** (actuator commands, driver alerts) **SHALL** use a
   dedicated namespace that does not overlap with non-safety namespaces.
   Recommended pattern: `safety/<subsystem>/<signal>`.
2. **Wildcard subscriptions** (`#`, `+`) **SHALL NOT** be used for
   safety-critical subscriptions unless the subscription filter exactly scopes
   to the safety namespace (e.g. `safety/#`).
3. **System topics** (`$SYS/*`) are correctly isolated by rust-MQTT's
   `match_topic` implementation (REQ-CYBER-004, REQ-WILD-002). No additional
   integrator action is required for system-topic isolation.

---

## 5. QoS selection (SG-002, SG-006)

| Signal class | Minimum QoS | Rationale |
|---|---|---|
| Safety-critical (ADAS alert, brake) | QoS 1 (AtLeastOnce) | Delivery guaranteed; acceptable for brief duplication |
| Safety-relevant (monitoring, logging) | QoS 0 or 1 | ASIL-A — at-most-once acceptable with watchdog |
| Non-safety (telemetry, debug) | QoS 0 | No safety impact |

For ASIL-D functions, QoS 1 at the MQTT layer is **insufficient alone** — see §3.

---

## 6. Reconnection obligation (SG-006)

rust-MQTT validates CONNACK return codes (REQ-CONN-009) and returns
`Error::ConnectionRefused(code)` on rejection. It does **not** implement
automatic reconnection.

Integrators **SHALL**:

1. Implement a reconnection loop with exponential back-off.
2. Treat `Error::NotConnected`, `Error::Closed`, and `Error::Timeout` as
   triggers for reconnection.
3. Implement a watchdog that restarts the MQTT task if no messages have been
   received within a configurable deadline.

---

## 7. Back-pressure configuration (SG-005)

Select `BackPressurePolicy` based on signal class:

| Policy | Behaviour | Use when |
|---|---|---|
| `DropNewest` (default) | Drops new messages when full | Safety monitoring: preserve oldest known state |
| `DropOldest` | Drops oldest message when full | Live signals: newest reading is most accurate |
| `Block` | Async task waits for space | Low-throughput critical: never lose a message, accept latency |

For safety-critical subscribers processing time-sensitive signals, `DropOldest`
is recommended — stale data is more dangerous than a brief gap.

Set `SubscriberConfig::channel_depth` large enough to absorb burst traffic
without triggering back-pressure during normal operation.

---

## 8. TLS / mTLS

The `tls` Cargo feature enables TLS via `rustls`. Integrators **SHALL** enable
TLS for any MQTT connection traversing an untrusted network (THREAT-003,
THREAT-001 in `tara.json`).

```toml
[dependencies]
rust-mqtt = { version = "1", features = ["tls"] }
```

Broker certificate validation is mandatory. Self-signed certificates are
acceptable in closed vehicle networks if the CA root is pinned.

---

## 9. Retained messages and expiry (SG-004)

Use `expiry_interval` (REQ-V5-MSG-005) on all retained messages carrying
safety-relevant state. A subscriber receiving a retained message at startup
**SHALL** validate the message timestamp against the expiry interval before
acting on the value.

Recommended maximum `expiry_interval` for safety signals: ≤ 5 seconds.

---

## 10. Embedded broker (test only)

`rust_mqtt::broker` is a minimal TCP broker provided **for integration testing
only**. It is:

- QM-rated (not safety-qualified)
- Not hardened against adversarial inputs
- Not suitable for vehicle deployment

**Do not use `rust_mqtt::broker` in production or safety contexts.**

---

## 11. Assumptions of use (SEOOC)

Per ISO 26262 Part 8 §6.4.3, the following assumptions must be validated by
the integrating system:

| Ref | Assumption |
|---|---|
| AoU-001 | The integrating application provides a correctly configured Tokio async runtime |
| AoU-002 | The operating system provides reliable TCP socket semantics |
| AoU-003 | The MQTT broker is trusted within the vehicle network segment |
| AoU-004 | Topic names are validated by the application before passing to rust-MQTT |
| AoU-005 | Payload sizes are bounded by the application to avoid MQTT maximum violations |
| AoU-006 | The integrating application implements reconnection logic (§6) |
| AoU-007 | Safety-critical and non-safety topics use non-overlapping namespaces (§4) |

---

## 12. Known limitations

1. **MQTT v5.0 not yet implemented.** v5 user properties are carried in
   `Message` but the TCP client (`v3/`) sends MQTT v3.1.1. v5 `expiry_interval`
   enforcement requires the broker.
2. **QoS 2 exactly-once is implemented** in the `v3` TCP client (full
   PUBLISH→PUBREC→PUBREL→PUBCOMP handshake) and the embedded broker gained
   receiver-side QoS 2 support. The in-process mock is best-effort and records
   but does not enforce QoS; use the `v3` client for exactly-once semantics.
3. **No automatic reconnection.** See §6.
4. **Embedded broker is test-only.** See §10.

---

*For safety defects or vulnerabilities, contact: matt@jellybaby.com*  
*See also: INCIDENT-RESPONSE.md, SECURITY.md, hara.json, fmea.json, tara.json*
