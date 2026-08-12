# ADR-001 Model-Independent Musical IR

## Status

Accepted.

## Decision

Canonical music is represented as `Phrase` and `MusicalEvent` values with phrase IDs,
revisions, and per-part sequence numbers. MIDI, OSC, notebooks, and future model tokens are
adapters or consumers, not the source of truth.

## Consequences

Composition can be procedural or ML-backed without changing node scheduling. Nodes receive
canonical phrases and apply local variation before buffering performance events.
