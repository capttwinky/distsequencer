# API Reference

Generated from source docstrings with `make docs-api`.

## `distributed_sequencer.adapters.ml`

### `class MidiGPTCompositionAdapter(model_path: 'Path', runtime_module: 'str' = 'torch', backend: 'Any | None' = None) -> None`

Adapter boundary for a local symbolic composition model.

The adapter is real in the sense that it validates runtime/model availability and calls an
injected or loaded backend. It does not ship a model checkpoint or synthesize fake ML output.

### `class MidiRWKVVariationAdapter(model_path: 'Path', runtime_module: 'str' = 'torch', backend: 'Any | None' = None) -> None`

Adapter for learned symbolic variation on node-local prepared material.

### `class MusicBertCriticAdapter(model_path: 'Path', runtime_module: 'str' = 'torch', backend: 'Any | None' = None) -> None`

Adapter for a local learned phrase critic backend.

### `class OnnxVariationAdapter(model_path: 'Path', encoder: 'Any | None' = None, decoder: 'Any | None' = None, runtime_module: 'str' = 'onnxruntime') -> None`

ONNX Runtime learned-variation adapter.

This class keeps ONNX out of imports until the adapter is used. A project-specific encoder and
decoder are required because the core package deliberately does not own model token semantics.

### `class OptionalDependencyUnavailable`

_No docstring._

### `class OptionalMlRuntimeProbe(module_name: 'str' = 'torch', group: 'str' = 'ml') -> None`

Small helper for notebooks/benchmarks without importing ML in core runtime.

### `def require_optional_dependency(module_name: 'str', *, group: 'str' = 'ml') -> 'None'`

_No docstring._


## `distributed_sequencer.adapters.superdirt`

### `class DirtEvent(cps: 'float', cycle: 'float', delta: 'float', orbit: 'int', sound: 'str', note: 'float', velocity: 'float', gain: 'float', pan: 'float', sustain: 'float') -> None`

A single SuperDirt `/dirt/play` event derived from canonical phrase IR.

### `class SuperDirtOscBackend(host: 'str' = '127.0.0.1', port: 'int' = 57120, latency_seconds: 'float' = 0.35) -> None`

Direct OSC target for a running SuperDirt server.

SuperDirt conventionally listens on UDP `127.0.0.1:57120` and receives timestamped
OSC bundles containing `/dirt/play` messages with name/value parameters.

### `def default_superdirt_gain(phrase: 'Phrase') -> 'float'`

_No docstring._

### `def default_superdirt_sound(phrase: 'Phrase') -> 'str'`

_No docstring._

### `def encode_dirt_bundle(event: 'DirtEvent', *, timestamp: 'float') -> 'bytes'`

_No docstring._

### `def encode_dirt_message(event: 'DirtEvent') -> 'bytes'`

_No docstring._

### `def phrase_to_dirt_events(phrase: 'Phrase', *, tempo_bpm: 'float', sound: 'str | None' = None, orbit: 'int' = 0, gain: 'float' = 0.8, pan: 'float' = 0.5, cycle_offset: 'float' = 0.0) -> 'tuple[DirtEvent, ...]'`

_No docstring._


## `distributed_sequencer.adapters.synth`

### `class OscSynthBackend(host: 'str' = '127.0.0.1', port: 'int' = 9000) -> None`

Minimal local OSC-compatible UDP backend.

The reference runtime keeps OSC on loopback by default. Messages are simple text OSC-like
packets for easy inspection by local synth shims: ``/note_on p v c`` and
``/note_off p c``.

### `class RecordingSynth(events: 'list[SynthEvent]' = <factory>, echo: 'bool' = False, label: 'str' = 'synth') -> None`

_No docstring._

### `class SynthBackend(*args, **kwargs)`

_No docstring._

### `class SynthEvent(kind: 'str', pitch: 'int', velocity: 'int', channel: 'int') -> None`

_No docstring._


## `distributed_sequencer.application.audio`

### `def render_phrases_wav(phrases: 'Iterable[Phrase]', *, tempo_bpm: 'float', sample_rate: 'int' = 44100) -> 'bytes'`

Render model-independent phrases as a browser-playable stereo WAV file.


## `distributed_sequencer.application.benchmark`

### `class BenchmarkRecord(name: 'str', platform: 'str', python_version: 'str', model_size_mb: 'float | None', resident_memory_mb: 'float | None', variation_latency_ms: 'float | None', throughput_events_per_second: 'float | None', buffer_safety_margin_bars: 'float | None', musical_quality_score: 'float | None') -> None`

_No docstring._

### `class BenchmarkSuite() -> 'None'`

_No docstring._

### `class HardwareProfile(node_id: 'str', cpu: 'str', memory_mb: 'int', os: 'str', audio_backend: 'str', network: 'str') -> None`

_No docstring._


## `distributed_sequencer.application.composition`

### `class CandidateScore(phrase: 'Phrase', dimensions: 'tuple[tuple[str, float], ...]') -> None`

_No docstring._

### `class CompositionEngine(model: 'CompositionModel', critics: 'tuple[Critic, ...]', accepted_sequences: 'dict[str, int] | None' = None) -> None`

_No docstring._

### `class CompositionModel(*args, **kwargs)`

_No docstring._

### `class Critic(*args, **kwargs)`

_No docstring._

### `class DensityCritic() -> None`

Rewards phrases near the requested note density.

### `class ProceduralCompositionModel(seed: 'int' = 1) -> None`

_No docstring._

### `class RegisterCritic(low: 'int' = 24, high: 'int' = 96) -> None`

_No docstring._


## `distributed_sequencer.application.coordinator`

### `class Coordinator(composition: 'CompositionEngine', bus: 'InMemoryBus', nodes: 'dict[str, NodeCapabilities]' = <factory>, transport_epoch: 'int' = 1, transport_state: 'TransportState' = <TransportState.STOPPED: 'stopped'>, tempo_bpm: 'float' = 120.0, current_bar: 'int' = 0, lease_bars: 'int' = 16, generation_by_part: 'dict[str, int]' = <factory>, desired_assignments: 'dict[str, Assignment]' = <factory>, readiness: 'dict[tuple[str, str], PhraseReady]' = <factory>) -> None`

_No docstring._


## `distributed_sequencer.application.ha`

### `class ConsensusBackedCoordinator(coordinator: 'Coordinator', cluster: 'RaftCluster', command_counter: 'int' = 0, applied_command_ids: 'set[str]' = <factory>) -> None`

Coordinator facade that commits mutations through Raft before applying them.

### `class ConsensusCoordinatorService(local_member_id: 'str', ha: 'ConsensusBackedCoordinator') -> None`

Operational HA facade for one local coordinator member.

### `class ConsensusCoordinatorSettings(local_member_id: 'str', members: 'tuple[ConsensusMemberSettings, ...]', bootstrap_leader_id: 'str | None' = None) -> None`

_No docstring._

### `class ConsensusMemberSettings(member_id: 'str', storage_path: 'Path | None' = None) -> None`

_No docstring._

### `class CoordinatorCommand(command_id: 'str', kind: 'str', payload: 'dict[str, object]') -> None`

_No docstring._

### `class NotLeaderError`

Raised when a follower receives a leader-only coordinator mutation.


## `distributed_sequencer.application.history`

### `class CompositionHistoryRecord(composition_id: 'str', part_id: 'str', phrase_sequence: 'int', phrase_revision: 'int', phrase_id: 'str', state_revision: 'int') -> None`

_No docstring._

### `class CompositionHistoryStore(path: 'Path') -> 'None'`

SQLite-backed persistent composition history.


## `distributed_sequencer.application.lab`

### `class CompositionBackendSpec(backend: 'str' = 'procedural', adapter: 'str | None' = None, model_path: 'str | None' = None, runtime_module: 'str | None' = None) -> None`

_No docstring._

### `class DashboardSnapshot(title: 'str', tempo_bpm: 'float', transport_epoch: 'int', transport_state: 'str', current_bar: 'int', nodes: 'tuple[dict[str, object], ...]', assignments: 'tuple[dict[str, object], ...]', readiness: 'tuple[dict[str, object], ...]') -> None`

_No docstring._

### `class DeviceRoute(part_id: 'str', target: 'str' = 'superdirt', sound: 'str | None' = None, orbit: 'int | None' = None, channel: 'int' = 0, gain: 'float | None' = None) -> None`

_No docstring._

### `class FormSection(section_id: 'str', bars: 'int', repeats: 'int' = 1) -> None`

_No docstring._

### `class MotifSpec(motif_id: 'str', intervals: 'tuple[int, ...]', rhythm_ticks: 'tuple[int, ...]' = ()) -> None`

_No docstring._

### `class NodeSpec(node_id: 'str', roles: 'tuple[str, ...]', max_polyphony: 'int' = 8, learned_variation: 'bool' = False) -> None`

_No docstring._

### `class PerformanceScore(title: 'str', tempo_bpm: 'float', parts: 'tuple[ScorePart, ...]', nodes: 'tuple[NodeSpec, ...]' = (), form: 'tuple[FormSection, ...]' = (), motifs: 'tuple[MotifSpec, ...]' = (), probability_lanes: 'tuple[ProbabilityLane, ...]' = (), routes: 'tuple[DeviceRoute, ...]' = (), physical_profiles: 'tuple[PhysicalNodeProfile, ...]' = (), composition_backend: 'CompositionBackendSpec' = <factory>) -> None`

_No docstring._

### `class PhysicalNodeProfile(node_id: 'str', device_model: 'str' = '', location: 'str' = '', latency_ms: 'float' = 0.0, pki_cert_path: 'str | None' = None, pki_key_path: 'str | None' = None, pki_ca_path: 'str | None' = None) -> None`

_No docstring._

### `class PreparedPart(part_id: 'str', node_id: 'str', assignment_generation: 'int', phrase_id: 'str', phrase_sequence: 'int', event_count: 'int', valid_from_bar: 'int', valid_through_bar: 'int') -> None`

_No docstring._

### `class PreparedPerformance(title: 'str', tempo_bpm: 'float', parts: 'tuple[PreparedPart, ...]') -> None`

_No docstring._

### `class ProbabilityLane(lane_id: 'str', part_id: 'str', density: 'float', mutate: 'float' = 0.0) -> None`

_No docstring._

### `class ReferencePerformanceLab(score: 'PerformanceScore', seed: 'int' = 1, composition_model: 'CompositionModel | None' = None, critics: 'tuple[Critic, ...]' = (DensityCritic(), RegisterCritic(low=24, high=96)), bus: 'InMemoryBus' = <factory>, nodes: 'dict[str, SequencerNode]' = <factory>, synths: 'dict[str, RecordingSynth]' = <factory>, prepared_phrases: 'dict[str, Phrase]' = <factory>) -> None`

_No docstring._

### `class ScorePart(part_id: 'str', root_pitch: 'int', density: 'float', motif_id: 'str | None' = None, bars: 'int' = 1, beats_per_bar: 'int' = 4, ticks_per_beat: 'int' = 24, velocity_jitter: 'int' = 0, timing_jitter_ticks: 'int' = 0, pitch_shift_semitones: 'int' = 0) -> None`

_No docstring._

### `def dashboard_tables(snapshot: 'DashboardSnapshot | Mapping[str, object]') -> 'dict[str, tuple[dict[str, object], ...]]'`

_No docstring._

### `def fetch_daemon_snapshot(url: 'str' = 'http://127.0.0.1:8081/snapshot', *, timeout_seconds: 'float' = 2.0) -> 'Mapping[str, object]'`

_No docstring._

### `def parse_music_dsl(text: 'str') -> 'PerformanceScore'`

_No docstring._

### `def render_dashboard_html(snapshot: 'DashboardSnapshot | Mapping[str, object]') -> 'str'`

_No docstring._

### `def try_fetch_daemon_snapshot(url: 'str' = 'http://127.0.0.1:8081/snapshot', *, timeout_seconds: 'float' = 2.0) -> 'Mapping[str, object]'`

_No docstring._


## `distributed_sequencer.application.node`

### `class SequencerNode(capabilities: 'NodeCapabilities', bus: 'InMemoryBus', variation: 'VariationEngine', scheduler: 'Scheduler', phrase_buffer: 'asyncio.Queue[Phrase]' = <factory>, last_assignment: 'Assignment | None' = None, processed_message_ids: 'set[str]' = <factory>, ready_reports: 'list[PhraseReady]' = <factory>, local_replay_count: 'int' = 0) -> None`

_No docstring._


## `distributed_sequencer.application.scheduler`

### `class Scheduler(synth: 'SynthBackend', clock: 'Clock', bpm: 'float' = 120.0) -> None`

_No docstring._


## `distributed_sequencer.application.variation`

### `class LearnedVariationModel(*args, **kwargs)`

_No docstring._

### `class VariationEngine(seed: 'int' = 1, learned_model: 'LearnedVariationModel | None' = None) -> None`

_No docstring._


## `distributed_sequencer.cli`

### `def add_simulation_args(parser: 'argparse.ArgumentParser') -> 'None'`

_No docstring._

### `def build_parser() -> 'argparse.ArgumentParser'`

_No docstring._

### `def main() -> 'None'`

_No docstring._


## `distributed_sequencer.domain.music`

### `class MusicalEvent(onset_tick: 'int', pitch: 'int', duration_ticks: 'int', velocity: 'int' = 96, channel: 'int' = 0) -> None`

A note event positioned in canonical musical ticks.

### `class Phrase(phrase_id: 'str', role: 'str', events: 'tuple[MusicalEvent, ...]', phrase_revision: 'int' = 1, phrase_sequence: 'int' = 0, bars: 'int' = 1, beats_per_bar: 'int' = 4, ticks_per_beat: 'int' = 24) -> None`

Model-independent canonical phrase.


## `distributed_sequencer.domain.state`

### `class Assignment(node_id: 'str', phrase: 'Phrase', policy: 'VariationPolicy', assignment_generation: 'int' = 1, transport_epoch: 'int' = 1, part_id: 'str | None' = None, assignment_id: 'str | None' = None, lease: 'PartLease | None' = None, message_id: 'str | None' = None, schema_version: 'int' = 1) -> None`

Versioned desired-state assignment for one part.

The first four constructor arguments intentionally match the starter API:
``Assignment(node_id, phrase, policy, generation)``.

### `class AuthoritativeSnapshot(transport_epoch: 'int', transport_state: 'TransportState', tempo_bpm: 'float', current_bar: 'int', assignments: 'tuple[Assignment, ...]' = (), schema_version: 'int' = 1, message_id: 'str' = 'snapshot') -> None`

_No docstring._

### `class CompositionContext(role: 'str', root_pitch: 'int', desired_density: 'float', bars: 'int' = 1, beats_per_bar: 'int' = 4, ticks_per_beat: 'int' = 24) -> None`

_No docstring._

### `class CompositionState(composition_id: 'str', dimensions: 'tuple[tuple[str, float], ...]' = (), revision: 'int' = 1) -> None`

_No docstring._

### `class NodeCapabilities(node_id: 'str', roles: 'frozenset[str]', max_polyphony: 'int' = 8, learned_variation: 'bool' = False) -> None`

_No docstring._

### `class NodeDesiredState(transport_epoch: 'int', transport_state: 'TransportState', tempo_bpm: 'float', assignments: 'tuple[Assignment, ...]' = ()) -> None`

_No docstring._

### `class NodeObservedState(node_id: 'str', transport_epoch: 'int | None' = None, active_assignment_generation: 'dict[str, int]' = <factory>, current_bar: 'int' = 0, policy_version: 'int | None' = None, buffered_through_bar: 'int' = 0, scheduler_lateness_ms: 'float' = 0.0, sync_uncertainty_ms: 'float' = 0.0, synth_healthy: 'bool' = True, stale_message_drops: 'int' = 0, duplicate_message_drops: 'int' = 0, buffer_underruns: 'int' = 0, lease_expirations: 'int' = 0) -> None`

_No docstring._

### `class PartLease(transport_epoch: 'int', part_id: 'str', node_id: 'str', assignment_generation: 'int', valid_from_bar: 'int', valid_through_bar: 'int', exclusive: 'bool' = True) -> None`

_No docstring._

### `class PhraseReady(node_id: 'str', part_id: 'str', phrase_sequence: 'int', assignment_generation: 'int', ready_through_bar: 'int', transport_epoch: 'int') -> None`

_No docstring._

### `class TransportState(*values)`

_No docstring._

### `class VariationPolicy(policy_version: 'int' = 1, timing_jitter_ticks: 'int' = 0, velocity_jitter: 'int' = 0, omission_probability: 'float' = 0.0, pitch_shift_semitones: 'int' = 0, rhythmic_freedom: 'float' = 0.0, pitch_freedom: 'float' = 0.0, density_variance: 'float' = 0.0, fill_probability: 'float' = 0.0) -> None`

Bounded interpretive freedom for one node/part.

### `def is_part_authorized(*, assignment: 'Assignment', node_id: 'str', transport_epoch: 'int | None', current_bar: 'int') -> 'bool'`

Centralized fencing/lease check used before local performance authority.


## `distributed_sequencer.infrastructure.clock`

### `class AdvancingClock(elapsed: 'float' = 0.0) -> None`

Test clock: advances virtual elapsed time without wall-clock delay.

### `class AsyncioClock() -> None`

_No docstring._

### `class Clock(*args, **kwargs)`

_No docstring._

### `class ClockSynchronizer(max_healthy_uncertainty_seconds: 'float' = 0.05, estimate: 'SyncEstimate' = SyncEstimate(offset_seconds=0.0, drift_ppm=0.0, uncertainty_seconds=1.0)) -> None`

Tracks coordinator-to-local clock estimates without comparing raw clocks.

### `class SyncEstimate(offset_seconds: 'float', drift_ppm: 'float', uncertainty_seconds: 'float') -> None`

_No docstring._


## `distributed_sequencer.infrastructure.consensus`

### `class AppendEntriesRequest(term: 'int', leader_id: 'str', prev_log_index: 'int', prev_log_term: 'int', entries: 'tuple[RaftLogEntry, ...]', leader_commit: 'int') -> None`

_No docstring._

### `class AppendEntriesResponse(term: 'int', success: 'bool', match_index: 'int') -> None`

_No docstring._

### `class JsonRaftStorage(path: 'Path') -> None`

_No docstring._

### `class MemoryRaftStorage(state: 'RaftPersistentState' = RaftPersistentState(current_term=0, voted_for=None, log=())) -> None`

_No docstring._

### `class RaftCluster(nodes: 'dict[str, RaftNode]', leader_id: 'str | None' = None) -> None`

_No docstring._

### `class RaftLogEntry(term: 'int', index: 'int', command_id: 'str', command: 'str') -> None`

_No docstring._

### `class RaftNode(node_id: 'str', peer_ids: 'tuple[str, ...]', storage: 'RaftStorage', role: 'RaftRole' = <RaftRole.FOLLOWER: 'follower'>, leader_id: 'str | None' = None, commit_index: 'int' = 0, last_applied: 'int' = 0) -> None`

_No docstring._

### `class RaftPersistentState(current_term: 'int' = 0, voted_for: 'str | None' = None, log: 'tuple[RaftLogEntry, ...]' = ()) -> None`

_No docstring._

### `class RaftRole(*values)`

_No docstring._

### `class RaftStorage(*args, **kwargs)`

_No docstring._

### `class RequestVoteRequest(term: 'int', candidate_id: 'str', last_log_index: 'int', last_log_term: 'int') -> None`

_No docstring._

### `class RequestVoteResponse(term: 'int', vote_granted: 'bool') -> None`

_No docstring._


## `distributed_sequencer.infrastructure.messaging`

### `class BackpressureError`

_No docstring._

### `class InMemoryBus(default_maxsize: 'int' = 64, _subscribers: 'dict[str, list[Subscriber]]' = <factory>) -> None`

Bounded fan-out bus used by tests and the local simulator.

The bus is transport only. Durable desired state lives in the coordinator.

### `class JsonMessageCodec(secrets_by_sender: 'dict[str, bytes]' = <factory>, max_bytes: 'int' = 64000) -> None`

Bounded JSON codec with optional HMAC authentication.

This intentionally avoids pickle or object deserialization for network input.

### `class MessageEnvelope(schema_version: 'int', message_id: 'str', sender_id: 'str', kind: 'str', payload: 'dict[str, Any]', signature: 'str' = '') -> None`

_No docstring._

### `class QueueOverflow(*values)`

_No docstring._

### `class SimulatedNetwork(bus: 'InMemoryBus', latency_seconds: 'float' = 0.0, jitter_seconds: 'float' = 0.0, packet_loss: 'float' = 0.0, duplicate_probability: 'float' = 0.0, reorder_probability: 'float' = 0.0, seed: 'int' = 1, partitions: 'set[str]' = <factory>, _pending_reordered: 'list[tuple[str, object]]' = <factory>) -> None`

Failure-injection transport shim for one-machine distributed tests.

### `class Subscriber(queue: 'asyncio.Queue[object]', overflow: 'QueueOverflow') -> None`

_No docstring._


## `distributed_sequencer.infrastructure.network`

### `class AsyncioMessageServer(server: 'asyncio.Server') -> None`

Asyncio stream server that accepts bounded MessageEnvelope connections.

### `class MessageDecodeError`

Raised when a network frame cannot be decoded into a message envelope.

### `class NetworkMessageConnection(reader: 'asyncio.StreamReader', writer: 'asyncio.StreamWriter', codec: 'JsonMessageCodec', max_message_bytes: 'int | None' = None) -> None`

Length-prefixed MessageEnvelope stream over asyncio readers/writers.

### `class NetworkTransportError`

Base error for bounded network message transport failures.

### `class OversizedMessageError`

Raised when a frame declares or encodes more bytes than allowed.

### `def build_client_ssl_context(*, ca_cert: 'Path', cert: 'Path | None' = None, key: 'Path | None' = None, check_hostname: 'bool' = True) -> 'ssl.SSLContext'`

_No docstring._

### `def build_server_ssl_context(*, ca_cert: 'Path', cert: 'Path', key: 'Path', require_client_cert: 'bool' = True) -> 'ssl.SSLContext'`

_No docstring._

### `def open_message_connection(host: 'str', port: 'int', *, codec: 'JsonMessageCodec', ssl_context: 'ssl.SSLContext | None' = None, server_hostname: 'str | None' = None, max_message_bytes: 'int | None' = None) -> 'NetworkMessageConnection'`

_No docstring._

### `def serve_messages(host: 'str', port: 'int', *, codec: 'JsonMessageCodec', handler: 'ConnectionHandler', ssl_context: 'ssl.SSLContext | None' = None, max_message_bytes: 'int | None' = None) -> 'AsyncioMessageServer'`

_No docstring._


## `distributed_sequencer.infrastructure.physical`

### `class DeploymentManifest(nodes: 'tuple[PhysicalNodeDeployment, ...]') -> None`

_No docstring._

### `class PhysicalNodeDeployment(profile: 'HardwareProfile', coordinator_url: 'str', osc_host: 'str' = '127.0.0.1', osc_port: 'int' = 9000, mtls_enabled: 'bool' = True) -> None`

_No docstring._


## `distributed_sequencer.infrastructure.pki`

### `class CertificatePaths(ca_key: 'Path', ca_cert: 'Path', key: 'Path', csr: 'Path', cert: 'Path') -> None`

_No docstring._

### `class LocalCertificateAuthority(directory: 'Path', days: 'int' = 365) -> None`

Project-local PKI automation for mTLS-enabled development clusters.


## `distributed_sequencer.infrastructure.sync`

### `class ClockSample(coordinator_send_seconds: 'float', node_receive_seconds: 'float', node_send_seconds: 'float', coordinator_receive_seconds: 'float') -> None`

_No docstring._

### `class PtpStyleSynchronizer(max_samples: 'int' = 16, samples: 'list[ClockSample] | None' = None) -> None`

PTP-style sync estimator using offset samples and drift regression.


## `distributed_sequencer.runtime.config`

### `class CoordinatorRuntimeConfig(listen: 'Endpoint', tempo_bpm: 'float' = 120.0, lease_bars: 'int' = 16, parts: 'tuple[str, ...]' = ('bass', 'lead'), transport: 'TransportRuntimeConfig' = TransportRuntimeConfig(mode='tcp', insecure_dev_mode=True, ca_cert=None, cert=None, key=None, max_message_bytes=64000), ha: 'HaRuntimeConfig' = HaRuntimeConfig(enabled=False, local_member_id=None, member_ids=(), storage_dir=None, bootstrap_leader_id=None)) -> None`

_No docstring._

### `class Endpoint(scheme: 'str', host: 'str', port: 'int') -> None`

_No docstring._

### `class HaRuntimeConfig(enabled: 'bool' = False, local_member_id: 'str | None' = None, member_ids: 'tuple[str, ...]' = (), storage_dir: 'Path | None' = None, bootstrap_leader_id: 'str | None' = None) -> None`

_No docstring._

### `class NodeRuntimeConfig(node_id: 'str', parts: 'tuple[str, ...]', coordinator_url: 'Endpoint', buffer_depth_bars: 'int' = 8, max_polyphony: 'int' = 8, learned_variation: 'bool' = False, transport: 'TransportRuntimeConfig' = TransportRuntimeConfig(mode='tcp', insecure_dev_mode=True, ca_cert=None, cert=None, key=None, max_message_bytes=64000), synth: 'SynthRuntimeConfig' = SynthRuntimeConfig(backend='recording', osc_host='127.0.0.1', osc_port=9000)) -> None`

_No docstring._

### `class SynthRuntimeConfig(backend: 'str' = 'recording', osc_host: 'str' = '127.0.0.1', osc_port: 'int' = 9000) -> None`

_No docstring._

### `class TransportRuntimeConfig(mode: 'str' = 'tcp', insecure_dev_mode: 'bool' = False, ca_cert: 'Path | None' = None, cert: 'Path | None' = None, key: 'Path | None' = None, max_message_bytes: 'int' = 64000) -> None`

_No docstring._

### `def load_coordinator_config(path: 'Path') -> 'CoordinatorRuntimeConfig'`

_No docstring._

### `def load_node_config(path: 'Path') -> 'NodeRuntimeConfig'`

_No docstring._


## `distributed_sequencer.runtime.daemon`

### `class CoordinatorRuntime(config: 'CoordinatorRuntimeConfig', ready_endpoint: 'Endpoint', connections: 'dict[str, NetworkMessageConnection]' = <factory>, last_seen_by_node: 'dict[str, float]' = <factory>, node_tasks: 'dict[str, asyncio.Task[None]]' = <factory>, _message_counter: 'int' = 0, _lock: 'asyncio.Lock' = <factory>) -> None`

_No docstring._

### `class HttpStatusServer(endpoint: 'Endpoint', routes: 'Mapping[str, StateProvider]', _server: 'asyncio.Server | None' = None) -> None`

_No docstring._

### `class NodeRuntime(config: 'NodeRuntimeConfig', ready_endpoint: 'Endpoint', connected: 'bool' = False, assignments_received: 'int' = 0, _message_counter: 'int' = 0) -> None`

_No docstring._

### `def run_coordinator_daemon(config: 'CoordinatorRuntimeConfig', *, ready_endpoint: 'Endpoint') -> 'None'`

_No docstring._

### `def run_node_daemon(config: 'NodeRuntimeConfig', *, ready_endpoint: 'Endpoint') -> 'None'`

_No docstring._


## `distributed_sequencer.runtime.serde`

### `def assignment_from_payload(payload: 'dict[str, object]') -> 'Assignment'`

_No docstring._

### `def assignment_to_payload(assignment: 'Assignment') -> 'dict[str, object]'`

_No docstring._

### `def part_lease_from_payload(payload: 'dict[str, object]') -> 'PartLease'`

_No docstring._

### `def part_lease_to_payload(lease: 'PartLease') -> 'dict[str, object]'`

_No docstring._

### `def phrase_from_payload(payload: 'dict[str, object]') -> 'Phrase'`

_No docstring._

### `def phrase_ready_from_payload(payload: 'dict[str, object]') -> 'PhraseReady'`

_No docstring._

### `def phrase_ready_to_payload(ready: 'PhraseReady') -> 'dict[str, object]'`

_No docstring._

### `def phrase_to_payload(phrase: 'Phrase') -> 'dict[str, object]'`

_No docstring._

### `def variation_policy_to_payload(policy: 'VariationPolicy') -> 'dict[str, object]'`

_No docstring._


## `distributed_sequencer.simulation.runner`

### `def run_simulation(*, bpm: 'float' = 720.0, nodes: 'int' = 2, bars: 'int' = 24, seed: 'int' = 2026, packet_loss: 'float' = 0.0, latency: 'float' = 0.0, clock_skew: 'float' = 0.0) -> 'None'`

_No docstring._
