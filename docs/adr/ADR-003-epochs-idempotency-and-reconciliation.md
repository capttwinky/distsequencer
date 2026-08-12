# ADR-003 Epochs, Idempotency, And Reconciliation

## Status

Accepted.

## Decision

The coordinator owns desired global state for one transport epoch. Nodes reject old-epoch messages,
deduplicate state-changing messages by `message_id`, reject stale assignment generations, and begin
reconnect by applying an `AuthoritativeSnapshot`.

## Consequences

The in-memory bus and simulated network are transports, not sources of truth. The system assumes
at-least-once delivery and does not need a global total message order.
