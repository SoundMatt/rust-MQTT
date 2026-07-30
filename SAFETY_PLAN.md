# Software Safety Plan
## rust-MQTT — ISO 26262 ASIL-B / IEC 61508 SIL 2

**Document ID:** SSP-001
**Version:** 1.1
**Date:** 2026-06-19
**Status:** Active
**Author:** Matt Jones (matt@jellybaby.com)
**Standards:** ISO 26262:2018 Part 8 §7, IEC 61508-3:2010 §5

---

## 1. Purpose and scope

This Software Safety Plan (SSP) defines the lifecycle, activities, methods, and
responsibilities for the development of rust-MQTT
(`github.com/SoundMatt/rust-MQTT`) in accordance with:

- ISO 26262:2018 — Road vehicles — Functional Safety (Parts 3, 4, 6, 8)
- IEC 61508:2010 — Functional Safety of E/E/PE Safety-related Systems (Part 3)
- ISO 21434:2021 — Road vehicles — Cybersecurity engineering

rust-MQTT is developed as a **Safety Element Out Of Context (SEOOC)** targeting
ASIL-B (ISO 26262) / SIL 2 (IEC 61508). The integrating system is responsible
for system-level HARA, hardware fault model (FMEDA), and allocation.

---

## 2. Safety requirements

Requirements are annotated in source code with `//fusa:req REQ-XXX-NNN`
comments and managed by [rust-FuSa](https://github.com/SoundMatt/rust-FuSa).
The machine-readable requirement set is in `requirements.json`.

### 2.1 Requirement families

| Family | Count | ASIL | Scope |
|---|---|---|---|
| REQ-MQTT | 8 | ASIL-B | Core library: spec version, client trait, lifecycle |
| REQ-MSG | 5 | ASIL-B | MQTT message structure |
| REQ-V5-MSG | 5 | QM | MQTT v5 message properties |
| REQ-QOS | 4 | ASIL-B | Quality of Service levels |
| REQ-PUB | 6 | ASIL-B | Publish path |
| REQ-SUB | 8 | ASIL-B | Subscribe path |
| REQ-WILD | 8 | ASIL-B | Topic wildcard matching (§4.7) |
| REQ-CONN | 11 | ASIL-B | Connection lifecycle |
| REQ-CONC | 3 | ASIL-B | Concurrency |
| REQ-MOCK | 5 | QM | In-process mock broker |
| REQ-BROKER | 3 | QM | Embedded TCP broker |
| REQ-RELAY | 20 | ASIL-B | RELAY spec v2.0 conformance |
| REQ-SAFE | 10 | ASIL-B | Functional safety (ISO 26262 Part 6) |
| REQ-CYBER | 8 | ASIL-A | Cybersecurity (ISO 21434) |
| REQ-HARA | 5 | ASIL-B | HARA-derived requirements |
| REQ-ERR | 6 | ASIL-B | Error handling |
| REQ-DIAG | 4 | QM | Diagnostics / health |
| REQ-DO | 5 | ASIL-B | DO-178C compatibility |
| **Total** | **124** | | |

---

## 3. Development activities

| Activity | Method | Evidence |
|---|---|---|
| Requirements management | Machine-readable registry + `//fusa:req` annotations | `requirements.json` (124 reqs) |
| Hazard analysis (HARA) | ISO 26262 Part 3 S×E×C matrix | `hara.json` (6 HEs, 6 SGs) |
| Failure mode analysis (FMEA) | FMECA with RPN, ASIL per entry | `fmea.json` (15 entries) |
| Threat analysis (TARA) | STRIDE / CVSS 3.1 / ISO 21434 | `tara.json` (10 threats) |
| System boundary | Trust boundaries, interfaces | `boundary-diagram.json` |
| Safety case | GSN structured argument | `safety-case.json` (12 clauses, 20 evidence items) |
| Safety manual | SEOOC assumptions and obligations | `SAFETY_MANUAL.md` |
| Design | Module decomposition, client trait abstraction | `CLAUDE.md`, `boundary-diagram.json` |
| Implementation | Rust (safe subset), no `unsafe` blocks | Source code |
| Unit testing | `cargo test --locked` (49+ tests in 3 test files) | CI artifact |
| Static analysis | `cargo clippy -D warnings`, `rsfusa analyze` | CI artifact |
| Coding standard | `cargo fmt`, `rsfusa lint` (ISO 26262 Part 6) | CI artifact |
| Cyclomatic complexity | `rsfusa comp`, V(G) ≤ 10 | CI artifact |
| Requirement traceability | `rsfusa trace`; impl+tests in every req | `trace.json` |
| Tool qualification | `rsfusa qualify`, SAFETY_PLAN.md §4 | `qualify-report.json` |
| Supply-chain | `cargo audit`, SBOM (140 pkgs), provenance | `sbom.json`, `provenance.json` |
| RELAY conformance | `relay conform --strict`, `relay interop` | CI relay-conform job |
| Cybersecurity | `rsfusa cyber`, TARA, cargo audit | `cyber-report.json`, `tara.json` |
| Standards compliance | ISO 26262 / IEC 61508 / DO-178C matrices | `docs/ISO-26262.md`, `docs/IEC-61508.md`, `docs/DO-178C.md` |

---

## 4. Tool qualification

The following tools are subject to qualification per IEC 61508-3:2010 §7.4.4:

| Tool | Version | Role | TQL |
|---|---|---|---|
| rustc | stable | Compiler | TQL-4 (qualification by process) |
| rsfusa | 0.2.x | Safety analysis tool | TQL-3 (qualified by developer) |
| relay | 2.0.x | Conformance checker | TQL-3 (qualified by developer) |
| cargo-audit | latest | Dependency vulnerability scanner | TQL-3 |

---

## 5. RELAY §20 continuous conformance

Per RELAY spec §20, every CI run MUST:

1. Pass `relay conform --strict` (§20.1.1)
2. Pass the full x-FuSa lifecycle (§20.1.2)
3. Maintain 100% requirement traceability (traced AND tested)
4. Pass `relay interop` (§20.2) — EQUIVALENT for every golden vector
5. Include SBOM + build provenance (§20.5)
