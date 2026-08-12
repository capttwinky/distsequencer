# ADR-008 Operational Benchmarks And Physical Deployment

## Status

Accepted.

## Decision

Hardware benchmarks, physical node deployment manifests, local PKI automation, and PTP-style sync
estimation are first-class package capabilities.

## Consequences

Raspberry Pi-class readiness is evaluated with recorded latency, throughput, memory, buffer margin,
and deployment-security data rather than prose-only follow-up work. Remote control paths require
mTLS-enabled manifests.
