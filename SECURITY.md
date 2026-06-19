# Security Policy

## Supported versions

| Version | Supported |
|---|---|
| 1.x | ✅ |

## Reporting a vulnerability

Please report security vulnerabilities privately to: matt@jellybaby.com

Do **not** open a public GitHub issue for security vulnerabilities.

Include:
- Description of the vulnerability
- Steps to reproduce
- Potential impact

We will acknowledge within 48 hours and aim to resolve within 14 days.

## Security design

- No `unsafe` Rust code in the library
- All packet parsers use length-checked slicing
- Topic wildcard matching enforces §4.7 system-topic protection
- TLS/mTLS available via the `tls` feature
- Dependency vulnerabilities scanned in CI via `cargo audit`
- Cybersecurity threat analysis in `tara.json` (ISO 21434)
- CWE analysis via `rsfusa cyber` in CI
