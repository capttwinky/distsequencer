# Distributed Sequencer

A runnable distributed-systems MVP for a generative music sequencer. The implementation proves the
runtime boundaries first: composition produces canonical material, nodes prepare local variation
ahead of playback, and realtime scheduling consumes only validated buffered phrases.

```text
composition intent/state
  -> candidate generation/scoring
  -> canonical phrase revision/sequence
  -> fenced part assignment + bounded lease
  -> node-local variation
  -> phrase-ahead buffer
  -> deterministic local scheduler
  -> recording or OSC synth output
```

## Requirements

- `uv`
- `make`
- Git for promotion/release targets
- optional `gh` CLI for `make pr` / `make promote`

## Start Here

```bash
make bootstrap
make check
make demo
```

Launch JupyterLab locally with token authentication left on:

```bash
make lab
```

For an explicitly remote Jupyter server:

```bash
make lab-remote
```

Optional workstation ML dependencies are separate from the base runtime:

```bash
make ml
```

The default MVP, CI, node runtime, simulator, and demo do not require the `ml` group.

## CLI

```bash
uv run distsequencer sim --nodes 2 --tempo 720 --bars 24 --seed 2026
uv run distsequencer coordinator --tempo 120
uv run distsequencer node --node-id pi-bass
uv run distsequencer benchmark --output artifacts/benchmarks.json
uv run distsequencer manifest --output artifacts/deployment.json
uv run distsequencer pki --dir .local/pki --node-id pi-bass
```

The legacy `uv run sequencer-sim` entry point remains available.

## Implemented Correctness Boundaries

- Transport epochs are first-class; nodes reject delayed messages from old epochs.
- State-changing messages are idempotent by `message_id`.
- Assignment generations fence part ownership.
- `PartLease` bounds autonomous node authority and prevents indefinite split brain.
- Reconnect starts from `AuthoritativeSnapshot`, not historical event replay.
- The coordinator owns desired assignments/readiness; nodes own observed state.
- Internal bus queues are bounded with explicit overflow behavior.
- Network input uses bounded JSON envelopes with optional per-sender HMAC.
- Clock sync is represented as offset, drift, and uncertainty estimates.
- The scheduler uses only local clocks, prepared phrases, and synth backends.
- Coordinator HA uses a Raft-style replicated log for leader election, quorum command commit,
  persisted terms/votes/logs, and replayable coordinator commands.
- Composition history is persisted to SQLite and can reload accepted phrase revisions.
- Optional ML adapters exist for MidiGPT-style composition, MusicBERT-style critics,
  MIDI-RWKV-style variation, and ONNX variation without importing model runtimes in core code.
- PKI automation creates a local CA and node certificates for mTLS-enabled development clusters.
- Physical deployment manifests and benchmark records are first-class package artifacts.

## Package Map

```text
src/distributed_sequencer/
  domain/          musical IR, desired/observed state, leases, snapshots
  application/     composition, variation, coordinator, node, scheduler
  adapters/        recording and OSC synth boundaries
  infrastructure/  bounded messaging, JSON/HMAC codec, clocks/sync
  simulation/      in-process reference deployment and fault demonstration
```

## Developer Workflow

```bash
make format
make check
make build
```

Useful narrower gates:

```bash
make test-unit
make test-bdd
make lint
make typecheck
make benchmark
make manifest
make pki
```

## GitHub Promotion

```bash
make push
make pr
make promote
```

Release from a clean branch:

```bash
make release VERSION=0.2.0
```

The release workflow builds distributions and attaches them to a GitHub Release. It does not publish
to PyPI.

## ADRs

See `docs/adr/` for decisions covering:

- model-independent musical IR
- composition/variation/performance separation
- epochs, idempotency, and reconciliation
- assignment generations and bounded leases
- secure bounded transport input
- Jupyter and optional ML dependencies
- consensus-backed coordinator
- operational benchmarks and physical deployment

## Implemented Advanced Scope

- Real ML adapter boundaries are implemented for local model checkpoints and injected backends.
- Quantization/hardware benchmark records capture model size, memory, latency, throughput, buffer
  safety margin, and musical quality metrics.
- Local PKI automation issues development CA and node certificates.
- PTP-style synchronization estimates offset, drift, and uncertainty from timestamp exchanges.
- SQLite composition history stores and reloads versioned canonical phrases.
- Coordinator HA is backed by a Raft-style replicated command log.
- Physical swarm deployment manifests validate node identity, control-plane security, and OSC
  locality defaults.
