# ADR-002 Composition, Variation, And Performance Boundaries

## Status

Accepted.

## Decision

Composition selects canonical material, node-local variation interprets it, and the scheduler only
plays already-prepared phrases from local memory. The scheduler does no network I/O, broker I/O, or
ML inference.

## Consequences

Network latency and model latency are absorbed before the realtime path. Tests use a virtual clock
and recording synth to verify deterministic note ordering.
