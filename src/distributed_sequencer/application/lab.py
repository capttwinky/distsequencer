from __future__ import annotations

import asyncio
import shlex
from dataclasses import asdict, dataclass, field
from typing import Self

from distributed_sequencer.adapters.synth import RecordingSynth
from distributed_sequencer.application.composition import (
    CompositionEngine,
    DensityCritic,
    ProceduralCompositionModel,
    RegisterCritic,
)
from distributed_sequencer.application.coordinator import Coordinator
from distributed_sequencer.application.node import SequencerNode
from distributed_sequencer.application.scheduler import Scheduler
from distributed_sequencer.application.variation import VariationEngine
from distributed_sequencer.domain.state import (
    Assignment,
    CompositionContext,
    NodeCapabilities,
    PhraseReady,
    VariationPolicy,
)
from distributed_sequencer.infrastructure.clock import AdvancingClock
from distributed_sequencer.infrastructure.messaging import InMemoryBus


@dataclass(frozen=True, slots=True)
class ScorePart:
    part_id: str
    root_pitch: int
    density: float
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

    def part_ids(self) -> tuple[str, ...]:
        return tuple(part.part_id for part in self.parts)

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


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


@dataclass(slots=True)
class ReferencePerformanceLab:
    score: PerformanceScore
    seed: int = 1
    bus: InMemoryBus = field(default_factory=InMemoryBus)
    composition: CompositionEngine = field(init=False)
    coordinator: Coordinator = field(init=False)
    nodes: dict[str, SequencerNode] = field(default_factory=dict)
    synths: dict[str, RecordingSynth] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.composition = CompositionEngine(
            ProceduralCompositionModel(seed=self.seed),
            critics=(DensityCritic(), RegisterCritic()),
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
    def from_dsl(cls, text: str, *, seed: int = 1) -> Self:
        return cls(score=parse_music_dsl(text), seed=seed)

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
            assignment = await self.coordinator.compose_and_assign(part.context(), part.policy())
            assignments.append(assignment)
            await self.nodes[assignment.node_id].receive_once()
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
            part.context(),
            part.policy(),
            node_id=node_id,
        )
        await receive
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

    def _score_part(self, part_id: str) -> ScorePart:
        for part in self.score.parts:
            if part.part_id == part_id:
                return part
        raise KeyError(f"unknown score part: {part_id}")

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


def parse_music_dsl(text: str) -> PerformanceScore:
    title = "Untitled Performance"
    tempo_bpm = 120.0
    parts: list[ScorePart] = []
    nodes: list[NodeSpec] = []
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
        else:
            raise ValueError(f"line {line_number}: unsupported DSL command {command!r}")
    if not parts:
        raise ValueError("music DSL must define at least one part")
    return PerformanceScore(
        title=title,
        tempo_bpm=tempo_bpm,
        parts=tuple(parts),
        nodes=tuple(nodes),
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


def _int(value: object, name: str, line_number: int) -> int:
    if isinstance(value, bool):
        raise ValueError(f"line {line_number}: {name} must be an integer")
    if not isinstance(value, int | str):
        raise ValueError(f"line {line_number}: {name} must be an integer")
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"line {line_number}: {name} must be an integer") from exc


def _float(value: object, name: str, line_number: int) -> float:
    if isinstance(value, bool):
        raise ValueError(f"line {line_number}: {name} must be numeric")
    if not isinstance(value, int | float | str):
        raise ValueError(f"line {line_number}: {name} must be numeric")
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"line {line_number}: {name} must be numeric") from exc


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
