from __future__ import annotations

import asyncio
import base64
import json
import os
import shlex
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field, replace
from html import escape
from pathlib import Path
from typing import Self
from urllib.error import URLError
from urllib.parse import quote
from urllib.request import urlopen

from distributed_sequencer.adapters.superdirt import (
    DirtEvent,
    SuperDirtOscBackend,
    default_superdirt_gain,
    default_superdirt_sound,
    phrase_to_dirt_events,
)
from distributed_sequencer.adapters.synth import RecordingSynth
from distributed_sequencer.application.audio import render_phrases_wav
from distributed_sequencer.application.composition import (
    CompositionEngine,
    CompositionModel,
    Critic,
    DensityCritic,
    ProceduralCompositionModel,
    RegisterCritic,
)
from distributed_sequencer.application.coordinator import Coordinator
from distributed_sequencer.application.node import SequencerNode
from distributed_sequencer.application.scheduler import Scheduler
from distributed_sequencer.application.variation import VariationEngine
from distributed_sequencer.domain.music import MusicalEvent, Phrase
from distributed_sequencer.domain.state import (
    Assignment,
    CompositionContext,
    NodeCapabilities,
    PhraseReady,
    VariationPolicy,
)
from distributed_sequencer.infrastructure.clock import AdvancingClock
from distributed_sequencer.infrastructure.messaging import InMemoryBus

_STRUDEL_ORIGIN = "https://strudel.cc"
_DEFAULT_CRITICS = (DensityCritic(), RegisterCritic())


@dataclass(frozen=True, slots=True)
class ScorePart:
    part_id: str
    root_pitch: int
    density: float
    motif_id: str | None = None
    bars: int = 1
    beats_per_bar: int = 4
    ticks_per_beat: int = 24
    velocity_jitter: int = 0
    timing_jitter_ticks: int = 0
    pitch_shift_semitones: int = 0

    def context(self) -> CompositionContext:
        return CompositionContext(
            role=self.part_id,
            root_pitch=self.root_pitch,
            desired_density=self.density,
            bars=self.bars,
            beats_per_bar=self.beats_per_bar,
            ticks_per_beat=self.ticks_per_beat,
        )

    def policy(self) -> VariationPolicy:
        return VariationPolicy(
            timing_jitter_ticks=self.timing_jitter_ticks,
            velocity_jitter=self.velocity_jitter,
            pitch_shift_semitones=self.pitch_shift_semitones,
        )


@dataclass(frozen=True, slots=True)
class FormSection:
    section_id: str
    bars: int
    repeats: int = 1


@dataclass(frozen=True, slots=True)
class MotifSpec:
    motif_id: str
    intervals: tuple[int, ...]
    rhythm_ticks: tuple[int, ...] = ()


@dataclass(frozen=True, slots=True)
class ProbabilityLane:
    lane_id: str
    part_id: str
    density: float
    mutate: float = 0.0


_EMPTY_LANE = ProbabilityLane("none", "", 0.0)


@dataclass(frozen=True, slots=True)
class DeviceRoute:
    part_id: str
    target: str = "superdirt"
    sound: str | None = None
    orbit: int | None = None
    channel: int = 0
    gain: float | None = None


@dataclass(frozen=True, slots=True)
class PhysicalNodeProfile:
    node_id: str
    device_model: str = ""
    location: str = ""
    latency_ms: float = 0.0
    pki_cert_path: str | None = None
    pki_key_path: str | None = None
    pki_ca_path: str | None = None


@dataclass(frozen=True, slots=True)
class CompositionBackendSpec:
    backend: str = "procedural"
    adapter: str | None = None
    model_path: str | None = None
    runtime_module: str | None = None


@dataclass(frozen=True, slots=True)
class NodeSpec:
    node_id: str
    roles: tuple[str, ...]
    max_polyphony: int = 8
    learned_variation: bool = False

    def capabilities(self) -> NodeCapabilities:
        return NodeCapabilities(
            self.node_id,
            frozenset(self.roles),
            max_polyphony=self.max_polyphony,
            learned_variation=self.learned_variation,
        )


@dataclass(frozen=True, slots=True)
class PerformanceScore:
    title: str
    tempo_bpm: float
    parts: tuple[ScorePart, ...]
    nodes: tuple[NodeSpec, ...] = ()
    form: tuple[FormSection, ...] = ()
    motifs: tuple[MotifSpec, ...] = ()
    probability_lanes: tuple[ProbabilityLane, ...] = ()
    routes: tuple[DeviceRoute, ...] = ()
    physical_profiles: tuple[PhysicalNodeProfile, ...] = ()
    composition_backend: CompositionBackendSpec = field(default_factory=CompositionBackendSpec)

    def part_ids(self) -> tuple[str, ...]:
        return tuple(part.part_id for part in self.parts)

    def as_dict(self) -> dict[str, object]:
        return asdict(self)

    def motif_for_part(self, part: ScorePart) -> MotifSpec | None:
        if part.motif_id is None:
            return None
        return next((motif for motif in self.motifs if motif.motif_id == part.motif_id), None)

    def probability_lane_for_part(self, part_id: str) -> ProbabilityLane | None:
        return next(
            (lane for lane in self.probability_lanes if lane.part_id == part_id),
            None,
        )

    def route_for_part(self, part_id: str) -> DeviceRoute | None:
        return next((route for route in self.routes if route.part_id == part_id), None)


@dataclass(frozen=True, slots=True)
class PreparedPart:
    part_id: str
    node_id: str
    assignment_generation: int
    phrase_id: str
    phrase_sequence: int
    event_count: int
    valid_from_bar: int
    valid_through_bar: int


@dataclass(frozen=True, slots=True)
class PreparedPerformance:
    title: str
    tempo_bpm: float
    parts: tuple[PreparedPart, ...]

    def table(self) -> tuple[dict[str, object], ...]:
        return tuple(asdict(part) for part in self.parts)


@dataclass(frozen=True, slots=True)
class DashboardSnapshot:
    title: str
    tempo_bpm: float
    transport_epoch: int
    transport_state: str
    current_bar: int
    nodes: tuple[dict[str, object], ...]
    assignments: tuple[dict[str, object], ...]
    readiness: tuple[dict[str, object], ...]

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class _ScoreAwareCompositionModel:
    model: CompositionModel
    score: PerformanceScore

    async def generate_candidates(
        self,
        context: CompositionContext,
        *,
        count: int,
    ) -> tuple[Phrase, ...]:
        candidates = await self.model.generate_candidates(context, count=count)
        part = next(
            (score_part for score_part in self.score.parts if score_part.part_id == context.role),
            None,
        )
        if part is None:
            return candidates
        motif = self.score.motif_for_part(part)
        if motif is None:
            return candidates
        return tuple(_apply_motif(candidate, context, motif) for candidate in candidates)


@dataclass(slots=True)
class ReferencePerformanceLab:
    score: PerformanceScore
    seed: int = 1
    composition_model: CompositionModel | None = None
    critics: tuple[Critic, ...] = _DEFAULT_CRITICS
    bus: InMemoryBus = field(default_factory=InMemoryBus)
    composition: CompositionEngine = field(init=False)
    coordinator: Coordinator = field(init=False)
    nodes: dict[str, SequencerNode] = field(default_factory=dict)
    synths: dict[str, RecordingSynth] = field(default_factory=dict)
    prepared_phrases: dict[str, Phrase] = field(default_factory=dict)

    def __post_init__(self) -> None:
        base_model = self.composition_model or ProceduralCompositionModel(seed=self.seed)
        self.composition = CompositionEngine(
            _ScoreAwareCompositionModel(base_model, self.score),
            critics=self.critics,
        )
        self.coordinator = Coordinator(
            composition=self.composition,
            bus=self.bus,
            tempo_bpm=self.score.tempo_bpm,
            lease_bars=4,
        )
        for node in self.score.nodes:
            self.add_node(node)

    @classmethod
    def from_dsl(
        cls,
        text: str,
        *,
        seed: int = 1,
        composition_model: CompositionModel | None = None,
        critics: tuple[Critic, ...] = _DEFAULT_CRITICS,
    ) -> Self:
        return cls(
            score=parse_music_dsl(text),
            seed=seed,
            composition_model=composition_model,
            critics=critics,
        )

    def add_node(self, node: NodeSpec | str, roles: tuple[str, ...] = ()) -> SequencerNode:
        spec = node if isinstance(node, NodeSpec) else NodeSpec(node, roles)
        synth = RecordingSynth(label=spec.node_id)
        sequencer = SequencerNode(
            capabilities=spec.capabilities(),
            bus=self.bus,
            variation=VariationEngine(seed=self.seed + len(self.nodes) + 1),
            scheduler=Scheduler(synth, AdvancingClock(), bpm=self.score.tempo_bpm),
        )
        self.nodes[spec.node_id] = sequencer
        self.synths[spec.node_id] = synth
        self.coordinator.register(spec.capabilities())
        return sequencer

    def remove_node(self, node_id: str) -> None:
        self.nodes.pop(node_id, None)

    async def prepare_performance(self) -> PreparedPerformance:
        self.coordinator.start_transport()
        assignments: list[Assignment] = []
        for part in self.score.parts:
            assignment = await self.coordinator.compose_and_assign(
                self._composition_context(part),
                self._variation_policy(part),
            )
            assignments.append(assignment)
            self.prepared_phrases[assignment.part_id or assignment.phrase.role] = await self.nodes[
                assignment.node_id
            ].receive_once()
        for node in self.nodes.values():
            for ready in node.ready_reports:
                self.coordinator.note_ready(ready)
        return PreparedPerformance(
            title=self.score.title,
            tempo_bpm=self.score.tempo_bpm,
            parts=tuple(_prepared_part(assignment) for assignment in assignments),
        )

    async def reassign_part_after_lease(self, part_id: str, node_id: str) -> PreparedPart:
        previous = self.coordinator.desired_assignments[part_id]
        self.coordinator.mark_current_bar(previous.valid_through_bar + 1)
        node = self.nodes[node_id]
        receive = asyncio.create_task(node.receive_once())
        await asyncio.sleep(0)
        part = self._score_part(part_id)
        assignment = await self.coordinator.compose_and_assign(
            self._composition_context(part),
            self._variation_policy(part),
            node_id=node_id,
        )
        await receive
        self.prepared_phrases[assignment.part_id or assignment.phrase.role] = receive.result()
        for ready in node.ready_reports:
            self.coordinator.note_ready(ready)
        return _prepared_part(assignment)

    async def play_once(self, node_id: str) -> None:
        await self.nodes[node_id].play_once()

    def dashboard(self) -> DashboardSnapshot:
        return DashboardSnapshot(
            title=self.score.title,
            tempo_bpm=self.coordinator.tempo_bpm,
            transport_epoch=self.coordinator.transport_epoch,
            transport_state=self.coordinator.transport_state.value,
            current_bar=self.coordinator.current_bar,
            nodes=tuple(self._node_rows()),
            assignments=tuple(self._assignment_rows()),
            readiness=tuple(_ready_row(ready) for ready in self.coordinator.readiness.values()),
        )

    def dashboard_tables(
        self,
        snapshot: DashboardSnapshot | Mapping[str, object] | None = None,
    ) -> dict[str, tuple[dict[str, object], ...]]:
        """Return stable dashboard tables for local or daemon `/snapshot` data."""
        source = self.dashboard() if snapshot is None else snapshot
        return dashboard_tables(source)

    def dashboard_html(
        self,
        snapshot: DashboardSnapshot | Mapping[str, object] | None = None,
    ) -> str:
        """Render local or daemon snapshot state as notebook-friendly HTML."""
        source = self.dashboard() if snapshot is None else snapshot
        return render_dashboard_html(source)

    def fetch_daemon_snapshot(
        self,
        url: str = "http://127.0.0.1:8081/snapshot",
        *,
        timeout_seconds: float = 2.0,
    ) -> Mapping[str, object]:
        """Fetch a live coordinator daemon `/snapshot` response."""
        return fetch_daemon_snapshot(url, timeout_seconds=timeout_seconds)

    def render_audio(self, *, sample_rate: int = 44_100) -> bytes:
        """Render the current prepared performance as a stereo WAV mixdown."""
        return render_phrases_wav(
            self._prepared_phrase_list(),
            tempo_bpm=self.score.tempo_bpm,
            sample_rate=sample_rate,
        )

    def write_audio(self, path: str | Path = ".tmp/reference-lab.wav") -> Path:
        """Write the current prepared performance to a browser-playable WAV file."""
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(self.render_audio())
        return target

    def superdirt_events(self) -> tuple[DirtEvent, ...]:
        """Convert the prepared performance into `/dirt/play` OSC event payloads."""
        phrases = self._prepared_phrase_list()
        events: list[DirtEvent] = []
        for index, phrase in enumerate(phrases):
            route = self._route_for_part(phrase.role)
            sound = route.sound if route.sound is not None else default_superdirt_sound(phrase)
            orbit = route.orbit if route.orbit is not None else index
            gain = route.gain if route.gain is not None else default_superdirt_gain(phrase)
            events.extend(
                phrase_to_dirt_events(
                    phrase,
                    tempo_bpm=self.score.tempo_bpm,
                    sound=sound,
                    orbit=orbit,
                    gain=gain,
                    pan=_superdirt_pan(index, len(phrases)),
                )
            )
        return tuple(events)

    def superdirt_table(self) -> tuple[dict[str, object], ...]:
        """Return a notebook-friendly preview of the generated SuperDirt OSC events."""
        return tuple(event.as_dict() for event in self.superdirt_events())

    async def send_superdirt(
        self,
        *,
        host: str | None = None,
        port: int | None = None,
        latency_seconds: float = 0.2,
    ) -> tuple[DirtEvent, ...]:
        """Send the prepared performance to a running SuperDirt OSC target."""
        events = self.superdirt_events()
        backend = SuperDirtOscBackend(
            host=host or os.environ.get("SUPERDIRT_HOST", "127.0.0.1"),
            port=port or int(os.environ.get("SUPERDIRT_PORT", "57120")),
            latency_seconds=latency_seconds,
        )
        await backend.send_events(events)
        return events

    async def stream_superdirt(
        self,
        *,
        host: str | None = None,
        port: int | None = None,
        cycles: int = 8,
        latency_seconds: float = 0.35,
        lookahead_seconds: float = 0.25,
    ) -> tuple[DirtEvent, ...]:
        """Stream the prepared performance to SuperDirt for a fixed number of cycles."""
        events = self.superdirt_events()
        backend = SuperDirtOscBackend(
            host=host or os.environ.get("SUPERDIRT_HOST", "127.0.0.1"),
            port=port or int(os.environ.get("SUPERDIRT_PORT", "57120")),
            latency_seconds=latency_seconds,
        )
        return await backend.stream_events(
            events,
            cycles=cycles,
            lookahead_seconds=lookahead_seconds,
        )

    def strudel_code(self) -> str:
        """Convert the current prepared performance into editable Strudel pattern code."""
        phrases = self._prepared_phrase_list()
        cps = self.score.tempo_bpm / 60.0 / max(1, max(phrase.beats_per_bar for phrase in phrases))
        voices = ",\n".join(
            f'  note("{_strudel_pattern(phrase)}")'
            f'.sound("{self._route_for_part(phrase.role).sound or _strudel_sound(phrase)}")'
            f".gain({self._strudel_gain_for_phrase(phrase):.2f})"
            f".pan({_strudel_pan(index, len(phrases)):.2f})"
            for index, phrase in enumerate(phrases)
        )
        return f"setcps({cps:.6g})\nstack(\n{voices}\n)"

    def strudel_url(self) -> str:
        """Return a Strudel REPL URL containing the current prepared performance."""
        encoded = base64.b64encode(self.strudel_code().encode("utf-8")).decode("ascii")
        return f"{_STRUDEL_ORIGIN}/#{quote(encoded, safe='')}"

    def strudel_iframe(self, *, height: int = 420) -> str:
        """Return notebook HTML that embeds the current performance in the Strudel REPL."""
        return (
            f'<iframe src="{self.strudel_url()}" width="100%" height="{height}" '
            'allow="autoplay; midi; microphone" '
            'style="border:1px solid #d0d7de;border-radius:8px"></iframe>'
        )

    def _score_part(self, part_id: str) -> ScorePart:
        for part in self.score.parts:
            if part.part_id == part_id:
                return part
        raise KeyError(f"unknown score part: {part_id}")

    def _composition_context(self, part: ScorePart) -> CompositionContext:
        lane = self.score.probability_lane_for_part(part.part_id)
        context = part.context()
        if lane is None:
            return context
        return replace(context, desired_density=lane.density)

    def _variation_policy(self, part: ScorePart) -> VariationPolicy:
        lane = self.score.probability_lane_for_part(part.part_id)
        policy = part.policy()
        if lane is None:
            return policy
        return replace(
            policy,
            density_variance=max(policy.density_variance, lane.mutate),
            rhythmic_freedom=max(policy.rhythmic_freedom, lane.mutate),
            pitch_freedom=max(policy.pitch_freedom, lane.mutate),
        )

    def _route_for_part(self, part_id: str) -> DeviceRoute:
        return self.score.route_for_part(part_id) or DeviceRoute(part_id=part_id)

    def _strudel_gain_for_phrase(self, phrase: Phrase) -> float:
        route = self._route_for_part(phrase.role)
        return route.gain if route.gain is not None else _strudel_gain(phrase)

    def _prepared_phrase_list(self) -> tuple[Phrase, ...]:
        if not self.prepared_phrases:
            raise RuntimeError("prepare_performance() must run before auditioning material")
        return tuple(
            self.prepared_phrases[part.part_id]
            for part in self.score.parts
            if part.part_id in self.prepared_phrases
        )

    def _node_rows(self) -> tuple[dict[str, object], ...]:
        return tuple(
            {
                "node_id": node.capabilities.node_id,
                "roles": sorted(node.capabilities.roles),
                "buffered_phrases": node.phrase_buffer.qsize(),
                "buffered_through_bar": node.observed.buffered_through_bar,
                "stale_drops": node.observed.stale_message_drops,
                "duplicate_drops": node.observed.duplicate_message_drops,
                "lease_expirations": node.observed.lease_expirations,
            }
            for node in self.nodes.values()
        )

    def _assignment_rows(self) -> tuple[dict[str, object], ...]:
        return tuple(
            {
                "part_id": assignment.part_id,
                "node_id": assignment.node_id,
                "generation": assignment.assignment_generation,
                "phrase_sequence": assignment.phrase.phrase_sequence,
                "valid_from_bar": assignment.valid_from_bar,
                "valid_through_bar": assignment.valid_through_bar,
            }
            for assignment in self.coordinator.desired_assignments.values()
        )

    def score_blueprint_table(self) -> tuple[dict[str, object], ...]:
        return tuple(
            {
                "part_id": part.part_id,
                "motif_id": part.motif_id or "",
                "density": self._composition_context(part).desired_density,
                "mutate": (
                    self.score.probability_lane_for_part(part.part_id) or _EMPTY_LANE
                ).mutate,
                "bars": part.bars,
                "beats_per_bar": part.beats_per_bar,
            }
            for part in self.score.parts
        )

    def route_table(self) -> tuple[dict[str, object], ...]:
        return tuple(asdict(route) for route in self.score.routes)

    def physical_profile_table(self) -> tuple[dict[str, object], ...]:
        return tuple(asdict(profile) for profile in self.score.physical_profiles)

    def composition_backend_table(self) -> tuple[dict[str, object], ...]:
        spec = self.score.composition_backend
        adapter = (
            self.composition_model.__class__.__name__
            if self.composition_model is not None
            else "ProceduralCompositionModel"
        )
        return (
            {
                "backend": spec.backend,
                "adapter": spec.adapter or adapter,
                "model_path": spec.model_path or "",
                "runtime_module": spec.runtime_module or "",
                "score_contract": "CompositionModel.generate_candidates -> Phrase",
            },
        )


def parse_music_dsl(text: str) -> PerformanceScore:
    title = "Untitled Performance"
    tempo_bpm = 120.0
    parts: list[ScorePart] = []
    nodes: list[NodeSpec] = []
    form: list[FormSection] = []
    motifs: list[MotifSpec] = []
    lanes: list[ProbabilityLane] = []
    routes: list[DeviceRoute] = []
    profiles: list[PhysicalNodeProfile] = []
    composition_backend = CompositionBackendSpec()
    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.split("#", maxsplit=1)[0].strip()
        if not line:
            continue
        tokens = shlex.split(line)
        command, fields = tokens[0], _key_values(tokens[1:], line_number)
        if command == "performance":
            title = str(fields.get("title", title))
            tempo_bpm = _float(fields.get("tempo", tempo_bpm), "tempo", line_number)
        elif command == "part":
            parts.append(
                ScorePart(
                    part_id=_required(fields, "id", line_number),
                    root_pitch=_int(fields.get("root", 60), "root", line_number),
                    density=_float(fields.get("density", 1.0), "density", line_number),
                    motif_id=_optional(fields, "motif"),
                    bars=_int(fields.get("bars", 1), "bars", line_number),
                    velocity_jitter=_int(fields.get("jitter", 0), "jitter", line_number),
                    timing_jitter_ticks=_int(fields.get("timing", 0), "timing", line_number),
                    pitch_shift_semitones=_int(fields.get("shift", 0), "shift", line_number),
                )
            )
        elif command == "node":
            nodes.append(
                NodeSpec(
                    node_id=_required(fields, "id", line_number),
                    roles=tuple(_required(fields, "roles", line_number).split(",")),
                    max_polyphony=_int(fields.get("polyphony", 8), "polyphony", line_number),
                    learned_variation=_bool(fields.get("learned", False), "learned", line_number),
                )
            )
        elif command == "section":
            form.append(
                FormSection(
                    section_id=_required(fields, "id", line_number),
                    bars=_positive_int(fields.get("bars", 1), "bars", line_number),
                    repeats=_positive_int(fields.get("repeats", 1), "repeats", line_number),
                )
            )
        elif command == "motif":
            motifs.append(
                MotifSpec(
                    motif_id=_required(fields, "id", line_number),
                    intervals=_int_tuple(_required(fields, "intervals", line_number), line_number),
                    rhythm_ticks=_positive_int_tuple(
                        _optional(fields, "rhythm") or "",
                        line_number,
                    ),
                )
            )
        elif command == "lane":
            lanes.append(
                ProbabilityLane(
                    lane_id=_required(fields, "id", line_number),
                    part_id=_required(fields, "part", line_number),
                    density=_bounded_float(
                        fields.get("density", 1.0),
                        "density",
                        line_number,
                        lower=0.0,
                        upper=8.0,
                    ),
                    mutate=_bounded_float(
                        fields.get("mutate", 0.0),
                        "mutate",
                        line_number,
                        lower=0.0,
                        upper=1.0,
                    ),
                )
            )
        elif command == "route":
            routes.append(
                DeviceRoute(
                    part_id=_required(fields, "part", line_number),
                    target=str(fields.get("target", "superdirt")),
                    sound=_optional(fields, "sound"),
                    orbit=_optional_int(fields, "orbit", line_number),
                    channel=_bounded_int(
                        fields.get("channel", 0),
                        "channel",
                        line_number,
                        lower=0,
                        upper=15,
                    ),
                    gain=_optional_float(fields, "gain", line_number),
                )
            )
        elif command == "profile":
            profiles.append(
                PhysicalNodeProfile(
                    node_id=_required(fields, "node", line_number),
                    device_model=str(fields.get("device", "")),
                    location=str(fields.get("location", "")),
                    latency_ms=_float(fields.get("latency", 0.0), "latency", line_number),
                    pki_cert_path=_optional(fields, "cert"),
                    pki_key_path=_optional(fields, "key"),
                    pki_ca_path=_optional(fields, "ca"),
                )
            )
        elif command == "composition":
            composition_backend = CompositionBackendSpec(
                backend=str(fields.get("backend", "procedural")),
                adapter=_optional(fields, "adapter"),
                model_path=_optional(fields, "model"),
                runtime_module=_optional(fields, "runtime"),
            )
        else:
            raise ValueError(f"line {line_number}: unsupported DSL command {command!r}")
    if not parts:
        raise ValueError("music DSL must define at least one part")
    return PerformanceScore(
        title=title,
        tempo_bpm=tempo_bpm,
        parts=tuple(parts),
        nodes=tuple(nodes),
        form=tuple(form),
        motifs=tuple(motifs),
        probability_lanes=tuple(lanes),
        routes=tuple(routes),
        physical_profiles=tuple(profiles),
        composition_backend=composition_backend,
    )


def _key_values(tokens: list[str], line_number: int) -> dict[str, object]:
    fields: dict[str, object] = {}
    for token in tokens:
        if "=" not in token:
            raise ValueError(f"line {line_number}: expected key=value token, got {token!r}")
        key, value = token.split("=", maxsplit=1)
        fields[key] = value
    return fields


def _required(fields: dict[str, object], key: str, line_number: int) -> str:
    value = fields.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"line {line_number}: {key} is required")
    return value


def _optional(fields: dict[str, object], key: str) -> str | None:
    value = fields.get(key)
    if value is None:
        return None
    return str(value)


def _int(value: object, name: str, line_number: int) -> int:
    if isinstance(value, bool):
        raise ValueError(f"line {line_number}: {name} must be an integer")
    if not isinstance(value, int | str):
        raise ValueError(f"line {line_number}: {name} must be an integer")
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"line {line_number}: {name} must be an integer") from exc


def _positive_int(value: object, name: str, line_number: int) -> int:
    parsed = _int(value, name, line_number)
    if parsed <= 0:
        raise ValueError(f"line {line_number}: {name} must be positive")
    return parsed


def _bounded_int(
    value: object,
    name: str,
    line_number: int,
    *,
    lower: int,
    upper: int,
) -> int:
    parsed = _int(value, name, line_number)
    if not lower <= parsed <= upper:
        raise ValueError(f"line {line_number}: {name} must be in [{lower}, {upper}]")
    return parsed


def _optional_int(fields: dict[str, object], key: str, line_number: int) -> int | None:
    if key not in fields:
        return None
    return _int(fields[key], key, line_number)


def _float(value: object, name: str, line_number: int) -> float:
    if isinstance(value, bool):
        raise ValueError(f"line {line_number}: {name} must be numeric")
    if not isinstance(value, int | float | str):
        raise ValueError(f"line {line_number}: {name} must be numeric")
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"line {line_number}: {name} must be numeric") from exc


def _bounded_float(
    value: object,
    name: str,
    line_number: int,
    *,
    lower: float,
    upper: float,
) -> float:
    parsed = _float(value, name, line_number)
    if not lower <= parsed <= upper:
        raise ValueError(f"line {line_number}: {name} must be in [{lower}, {upper}]")
    return parsed


def _optional_float(fields: dict[str, object], key: str, line_number: int) -> float | None:
    if key not in fields:
        return None
    return _float(fields[key], key, line_number)


def _bool(value: object, name: str, line_number: int) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.lower()
        if lowered in {"true", "yes", "1"}:
            return True
        if lowered in {"false", "no", "0"}:
            return False
    raise ValueError(f"line {line_number}: {name} must be boolean")


def _int_tuple(value: str, line_number: int) -> tuple[int, ...]:
    if not value:
        return ()
    return tuple(_int(token, "list value", line_number) for token in value.split(","))


def _positive_int_tuple(value: str, line_number: int) -> tuple[int, ...]:
    if not value:
        return ()
    return tuple(_positive_int(token, "list value", line_number) for token in value.split(","))


def _apply_motif(phrase: Phrase, context: CompositionContext, motif: MotifSpec) -> Phrase:
    if not motif.intervals and not motif.rhythm_ticks:
        return phrase
    events: list[MusicalEvent] = []
    onset = 0
    for index, event in enumerate(phrase.events):
        duration = (
            motif.rhythm_ticks[index % len(motif.rhythm_ticks)]
            if motif.rhythm_ticks
            else event.duration_ticks
        )
        event_onset = onset if motif.rhythm_ticks else event.onset_tick
        if event_onset >= phrase.total_ticks:
            break
        if motif.intervals:
            pitch = context.root_pitch + motif.intervals[index % len(motif.intervals)]
        else:
            pitch = event.pitch
        events.append(
            replace(
                event,
                onset_tick=event_onset,
                pitch=max(0, min(127, pitch)),
                duration_ticks=max(1, min(duration, phrase.total_ticks - event_onset)),
            )
        )
        onset += duration
    return replace(phrase, events=tuple(sorted(events)))


def fetch_daemon_snapshot(
    url: str = "http://127.0.0.1:8081/snapshot",
    *,
    timeout_seconds: float = 2.0,
) -> Mapping[str, object]:
    with urlopen(url, timeout=timeout_seconds) as response:
        payload = response.read()
    value = json.loads(payload.decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("daemon snapshot response must be a JSON object")
    return value


def try_fetch_daemon_snapshot(
    url: str = "http://127.0.0.1:8081/snapshot",
    *,
    timeout_seconds: float = 2.0,
) -> Mapping[str, object]:
    try:
        return fetch_daemon_snapshot(url, timeout_seconds=timeout_seconds)
    except (OSError, TimeoutError, URLError, json.JSONDecodeError) as exc:
        return {"status": "unavailable", "url": url, "error": str(exc)}


def dashboard_tables(
    snapshot: DashboardSnapshot | Mapping[str, object],
) -> dict[str, tuple[dict[str, object], ...]]:
    mapping = snapshot.as_dict() if isinstance(snapshot, DashboardSnapshot) else dict(snapshot)
    return {
        "assignments": _table_rows(mapping.get("assignments", ())),
        "readiness": _table_rows(mapping.get("readiness", ())),
        "nodes": _table_rows(mapping.get("nodes", ())),
    }


def render_dashboard_html(snapshot: DashboardSnapshot | Mapping[str, object]) -> str:
    mapping = snapshot.as_dict() if isinstance(snapshot, DashboardSnapshot) else dict(snapshot)
    tables = dashboard_tables(mapping)
    title = escape(str(mapping.get("title", "Live Coordinator Snapshot")))
    tempo = escape(str(mapping.get("tempo_bpm", "")))
    state = escape(str(mapping.get("transport_state", mapping.get("status", "unknown"))))
    epoch = escape(str(mapping.get("transport_epoch", "")))
    bar = escape(str(mapping.get("current_bar", "")))
    return (
        "<style>"
        ".ds-dashboard{font-family:Inter,Segoe UI,Arial,sans-serif;color:#1f2328}"
        ".ds-band{border:1px solid #d0d7de;border-radius:8px;padding:14px;margin:10px 0}"
        ".ds-kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:8px}"
        ".ds-kpi{background:#f6f8fa;border:1px solid #d0d7de;border-radius:6px;padding:8px}"
        ".ds-kpi b{display:block;font-size:12px;color:#57606a;font-weight:600}"
        ".ds-kpi span{display:block;font-size:18px;margin-top:2px}"
        ".ds-dashboard table{border-collapse:collapse;width:100%;font-size:13px}"
        ".ds-dashboard th,.ds-dashboard td{border-bottom:1px solid #d8dee4;"
        "padding:6px;text-align:left}"
        ".ds-dashboard th{background:#f6f8fa;color:#57606a;font-weight:600}"
        ".ds-dashboard h3{font-size:16px;margin:0 0 8px}"
        "</style>"
        "<div class='ds-dashboard'>"
        f"<div class='ds-band'><h3>{title}</h3><div class='ds-kpis'>"
        f"<div class='ds-kpi'><b>State</b><span>{state}</span></div>"
        f"<div class='ds-kpi'><b>Tempo</b><span>{tempo}</span></div>"
        f"<div class='ds-kpi'><b>Epoch</b><span>{epoch}</span></div>"
        f"<div class='ds-kpi'><b>Bar</b><span>{bar}</span></div>"
        "</div></div>"
        f"{_html_table_band('Assignments', tables['assignments'])}"
        f"{_html_table_band('Readiness', tables['readiness'])}"
        f"{_html_table_band('Nodes', tables['nodes'])}"
        "</div>"
    )


def _table_rows(value: object) -> tuple[dict[str, object], ...]:
    if isinstance(value, Mapping):
        iterable: Sequence[object] = tuple(value.values())
    elif isinstance(value, Sequence) and not isinstance(value, str | bytes):
        iterable = value
    else:
        return ()
    rows: list[dict[str, object]] = []
    for row in iterable:
        if isinstance(row, Mapping):
            rows.append({str(key): cell for key, cell in row.items()})
    return tuple(rows)


def _html_table_band(title: str, rows: Sequence[Mapping[str, object]]) -> str:
    if not rows:
        return f"<div class='ds-band'><h3>{escape(title)}</h3><p>No rows.</p></div>"
    columns = tuple(dict.fromkeys(column for row in rows for column in row))
    header = "".join(f"<th>{escape(column)}</th>" for column in columns)
    body = "".join(
        "<tr>"
        + "".join(f"<td>{escape(_cell_text(row.get(column, '')))}</td>" for column in columns)
        + "</tr>"
        for row in rows
    )
    return (
        f"<div class='ds-band'><h3>{escape(title)}</h3>"
        f"<table><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table></div>"
    )


def _cell_text(value: object) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, Sequence) and not isinstance(value, bytes):
        return ", ".join(str(item) for item in value)
    return str(value)


def _prepared_part(assignment: Assignment) -> PreparedPart:
    return PreparedPart(
        part_id=assignment.part_id or assignment.phrase.role,
        node_id=assignment.node_id,
        assignment_generation=assignment.assignment_generation,
        phrase_id=assignment.phrase.phrase_id,
        phrase_sequence=assignment.phrase.phrase_sequence,
        event_count=len(assignment.phrase.events),
        valid_from_bar=assignment.valid_from_bar,
        valid_through_bar=assignment.valid_through_bar,
    )


def _ready_row(ready: PhraseReady) -> dict[str, object]:
    return {
        "node_id": ready.node_id,
        "part_id": ready.part_id,
        "assignment_generation": ready.assignment_generation,
        "phrase_sequence": ready.phrase_sequence,
        "ready_through_bar": ready.ready_through_bar,
        "transport_epoch": ready.transport_epoch,
    }


def _strudel_pattern(phrase: Phrase, *, steps_per_beat: int = 4) -> str:
    total_steps = phrase.bars * phrase.beats_per_bar * steps_per_beat
    step_ticks = phrase.ticks_per_beat / steps_per_beat
    steps: list[list[int]] = [[] for _ in range(total_steps)]
    for event in phrase.events:
        step = min(total_steps - 1, max(0, round(event.onset_tick / step_ticks)))
        steps[step].append(event.pitch)
    return " ".join(_strudel_step(pitches) for pitches in steps)


def _strudel_step(pitches: list[int]) -> str:
    if not pitches:
        return "~"
    if len(pitches) == 1:
        return str(pitches[0])
    return "[" + " ".join(str(pitch) for pitch in sorted(pitches)) + "]"


def _strudel_sound(phrase: Phrase) -> str:
    if phrase.role.lower() in {"bass", "sub", "low"}:
        return "pulse"
    return "saw"


def _strudel_gain(phrase: Phrase) -> float:
    if phrase.role.lower() in {"bass", "sub", "low"}:
        return 0.38
    return 0.28


def _strudel_pan(index: int, count: int) -> float:
    if count <= 1:
        return 0.0
    return -0.45 + 0.9 * index / (count - 1)


def _superdirt_pan(index: int, count: int) -> float:
    if count <= 1:
        return 0.5
    return 0.15 + 0.7 * index / (count - 1)
