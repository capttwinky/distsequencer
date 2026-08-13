# ADR-005 Secure Bounded Transport Input

## Status

Accepted.

## Decision

Network-facing payloads use bounded JSON envelopes with schema version, message ID, sender ID, kind,
payload, and optional per-sender HMAC. Python object deserialization is not used for untrusted input.

## Consequences

The reference implementation can run locally with object messages over `InMemoryBus`, while the application protocol has a
safe serialization boundary for a real network transport. Future mTLS can replace or complement the
interim per-node secret mechanism.
