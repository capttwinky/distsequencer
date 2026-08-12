# ADR-007 Consensus-Backed Coordinator

## Status

Accepted.

## Decision

Coordinator high availability uses a Raft-style replicated command log. The implementation includes
persisted terms, votes, log entries, request-vote handling, append-entries handling, quorum commit,
leader-only command append, and committed-entry replay into coordinator state.

## Consequences

Coordinator mutations can be fenced behind quorum commit instead of relying on one process's memory.
The first transport implementation remains local/in-process, but the consensus semantics are not a
mock and are covered by tests.
