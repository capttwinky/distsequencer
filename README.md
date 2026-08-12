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

## Deferred Work

- Real composition ML adapters and learned critics
- MIDI-RWKV/ONNX learned variation adapters
- Quantization and hardware benchmarks
- Stronger mTLS/PKI automation
- Advanced synchronization/PTP
- Persistent composition history
- Coordinator HA/consensus
- Physical swarm deployment and benchmarking
