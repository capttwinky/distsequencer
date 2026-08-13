# Distributed Sequencer System Design

This document is the coherence map for the reference implementation. It describes how the lab,
runtime daemons, score DSL, model adapters, coordinator, node processes, dashboards, OSC output,
PKI, and physical deployment artifacts fit together.

## Goals

- Keep musical material model-independent until the final output adapter.
- Make coordinator authority explicit through epochs, assignment generations, and bounded leases.
- Let nodes prepare and perform locally without owning global truth.
- Support procedural and learned composition behind the same score contract.
- Use the same dashboard tables for in-process labs and live daemon `/snapshot` responses.
- Treat physical node identity, routes, and PKI paths as deployment material, not notebook-only
  notes.

## Architecture

```text
music DSL
  -> PerformanceScore
  -> CompositionContext + VariationPolicy
  -> CompositionModel candidates + critics
  -> Phrase IR
  -> Coordinator assignment + lease
  -> node-local VariationEngine
  -> phrase buffer + Scheduler
  -> RecordingSynth, Strudel code, or OSC /dirt/play -> SuperDirt
```

The in-process `ReferencePerformanceLab` exercises the same boundaries as the process runtime. The
lab is optimized for inspection and notebooks; the runtime daemon is optimized for process isolation,
bounded network input, readiness probes, and physical deployment.

## Score DSL

The DSL parses into `PerformanceScore` and is the highest-level input shared by the lab and tests.
It contains:

- `performance`: title and tempo.
- `section`: named form blocks with bars and repeats.
- `motif`: interval/rhythm material that can transform generated candidate phrases.
- `part`: musical role, register, density, variation bounds, and optional motif reference.
- `lane`: probability and mutation controls for a part.
- `route`: device/output routing, including SuperDirt sound, orbit, gain, and channel metadata.
- `node`: mesh node capabilities.
- `profile`: physical node model, location, latency estimate, and PKI paths.
- `composition`: model adapter metadata for procedural or learned composition.

The parser is deliberately line-oriented so notebook examples, tests, and deployment snippets stay
diffable. The parsed score remains structured data; downstream code should use the dataclasses rather
than reparsing strings.

## Composition And Critics

`CompositionModel.generate_candidates(context, count)` is the model boundary. The default
`ProceduralCompositionModel` is deterministic and dependency-free. Learned adapters such as
`MidiGPTCompositionAdapter` validate optional runtime and model-path availability, then delegate to a
project-specific backend. Both paths return canonical `Phrase` objects.

The lab wraps the selected model with score-aware behavior:

- probability lanes override the part density used in `CompositionContext`;
- mutation lanes widen node-local `VariationPolicy` freedom;
- motifs transform accepted candidate events while preserving the phrase contract.

Critics score candidate phrases independently from model generation. This keeps learned critics and
procedural critics interchangeable.

## Coordinator Authority

The coordinator owns desired assignment state. It starts transport, composes one canonical phrase per
part, allocates compatible nodes, assigns bounded leases, and records readiness reported by nodes.
The core fences are:

- transport epoch;
- assignment generation;
- part lease validity window;
- idempotent message IDs;
- authoritative snapshot reconciliation.

The HA layer adds a Raft-style replicated command log around coordinator commands. Production
multi-process HA still needs operational packaging, but the implementation contains quorum commit,
leader election, durable terms/votes/log entries, and replayable coordinator commands.

## Node Runtime

Nodes own observed state, not global truth. A node accepts only assignments for its capability set and
current epoch, applies local variation, buffers prepared phrases, reports readiness, and schedules
from local time. If disconnected, a node may replay cached canonical material only inside its active
lease.

The process runtime uses bounded JSON envelopes over length-prefixed TCP with optional HMAC and
TLS/mTLS contexts. The in-process lab uses the same coordinator/node application services over an
`InMemoryBus`.

## Dashboard Model

`DashboardSnapshot.as_dict()` is the local lab shape. The coordinator daemon exposes related JSON at
`GET /snapshot`. The lab normalizes both into the same dashboard tables:

- assignments;
- readiness;
- nodes.

`render_dashboard_html()` produces a richer notebook UI from either source. This makes the dashboard
cells useful before and after switching from in-process rehearsal to daemon rehearsal.

## Output Adapters

The canonical phrase IR can flow to several targets:

- `RecordingSynth` for deterministic test and scheduler observation;
- local WAV rendering for browser-playable notebook fallback;
- generated Strudel DSL for browser editing;
- OSC `/dirt/play` events for SuperDirt.

The preferred live-audio path is:

```text
score DSL -> Phrase IR -> OSC /dirt/play events -> SuperDirt
```

Device routes from the score select SuperDirt sounds, orbits, gain, and channel metadata. If no route
is defined, role-based defaults keep the existing lab behavior.

## Physical Deployment And PKI

Physical profiles attach deployment material to score-level nodes: hardware model, location,
latency estimate, certificate path, key path, and CA path. Runtime TOML still owns process startup
configuration; the DSL gives the lab enough structured material to reason about real node identity
and output routing.

The PKI helper creates a local CA and node certificates for development mTLS clusters. Production
deployments should replace those local artifacts with the operator's certificate lifecycle.

## Source Documentation

The public API reference can be regenerated from source docstrings:

```bash
make docs-api
```

This writes `docs/api-reference.md`. The generator is intentionally small and dependency-free; it
uses Python introspection to list modules, classes, functions, signatures, and docstrings under
`distributed_sequencer`.

## Related Decisions

The ADRs in `docs/adr/` provide focused rationale for the main boundaries:

- model-independent musical IR;
- composition/variation/performance separation;
- epochs, idempotency, and reconciliation;
- assignment generations and leases;
- secure bounded transport input;
- Jupyter and optional ML dependencies;
- consensus-backed coordinator;
- operational benchmarks and physical deployment.
