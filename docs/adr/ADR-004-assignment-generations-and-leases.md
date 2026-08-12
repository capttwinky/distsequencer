# ADR-004 Assignment Generations And Bounded Leases

## Status

Accepted.

## Decision

Part ownership is fenced by assignment generation and bounded by `PartLease`. A node is authorized
to perform only when epoch, owner, generation, and lease validity agree through the centralized
`is_part_authorized` check.

## Consequences

Disconnected nodes may continue locally only inside the current lease. The coordinator refuses to
reassign exclusive ownership to another node until the previous lease has expired.
