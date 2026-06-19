# Incident Response Plan
## rust-MQTT

**Document ID:** IRP-001
**Version:** 1.0
**Date:** 2026-06-19

---

## 1. Scope

This plan covers security incidents and functional safety defects affecting
rust-MQTT (`github.com/SoundMatt/rust-MQTT`).

---

## 2. Severity classification

| Severity | Criteria | Response time |
|---|---|---|
| Critical | Memory safety, RELAY conformance regression, authentication bypass | 24 hours |
| High | Data corruption, incorrect message routing, QoS violation | 48 hours |
| Medium | Incorrect wildcard matching, retained message failure | 7 days |
| Low | Documentation error, minor API defect | 30 days |

---

## 3. Response process

1. **Triage** — Assign severity, open private issue or security advisory
2. **Reproduce** — Write a failing regression test
3. **Fix** — Implement fix in a `fix/<area>-<short>` branch
4. **Verify** — All CI gates green (build, test, clippy, relay conform, x-FuSa)
5. **Release** — Squash-merge, tag new patch version, update CHANGELOG
6. **Notify** — Close advisory, credit reporter if consented

---

## 4. Contact

Security issues: matt@jellybaby.com (private)
General defects: GitHub Issues
