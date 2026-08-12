from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from distributed_sequencer.domain.music import Phrase


class TransportState(StrEnum):
    STOPPED = "stopped"
    PLAYING = "playing"
    PAUSED = "paused"


@dataclass(frozen=True, slots=True)
class CompositionContext:
    role: str
    root_pitch: int
    desired_density: float
    bars: int = 1
    beats_per_bar: int = 4
    ticks_per_beat: int = 24


@dataclass(frozen=True, slots=True)
class CompositionState:
    composition_id: str
    dimensions: tuple[tuple[str, float], ...] = ()
    revision: int = 1


@dataclass(frozen=True, slots=True)
class VariationPolicy:
    """Bounded interpretive freedom for one node/part."""

    policy_version: int = 1
    timing_jitter_ticks: int = 0
    velocity_jitter: int = 0
    omission_probability: float = 0.0
    pitch_shift_semitones: int = 0
    rhythmic_freedom: float = 0.0
    pitch_freedom: float = 0.0
    density_variance: float = 0.0
    fill_probability: float = 0.0

    def __post_init__(self) -> None:
        if self.policy_version <= 0:
            raise ValueError("policy_version must be positive")
        if self.timing_jitter_ticks < 0 or self.velocity_jitter < 0:
            raise ValueError("jitter magnitudes must be non-negative")
        if not 0.0 <= self.omission_probability <= 1.0:
            raise ValueError("omission_probability must be in [0, 1]")
        for name, value in (
            ("rhythmic_freedom", self.rhythmic_freedom),
            ("pitch_freedom", self.pitch_freedom),
            ("density_variance", self.density_variance),
            ("fill_probability", self.fill_probability),
        ):
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be in [0, 1]")


@dataclass(frozen=True, slots=True)
class NodeCapabilities:
    node_id: str
    roles: frozenset[str]
    max_polyphony: int = 8
    learned_variation: bool = False


@dataclass(frozen=True, slots=True)
class PartLease:
    transport_epoch: int
    part_id: str
    node_id: str
    assignment_generation: int
    valid_from_bar: int
    valid_through_bar: int
    exclusive: bool = True

    def __post_init__(self) -> None:
        if self.transport_epoch <= 0:
            raise ValueError("transport_epoch must be positive")
        if self.assignment_generation <= 0:
            raise ValueError("assignment_generation must be positive")
        if self.valid_from_bar < 0:
            raise ValueError("valid_from_bar must be non-negative")
        if self.valid_through_bar < self.valid_from_bar:
            raise ValueError("lease end must not precede lease start")

    def contains_bar(self, bar: int) -> bool:
        return self.valid_from_bar <= bar <= self.valid_through_bar


@dataclass(frozen=True, slots=True)
class Assignment:
    """Versioned desired-state assignment for one part.

    The first four constructor arguments intentionally match the starter API:
    ``Assignment(node_id, phrase, policy, generation)``.
    """

    node_id: str
    phrase: Phrase
    policy: VariationPolicy
    assignment_generation: int = 1
    transport_epoch: int = 1
    part_id: str | None = None
    assignment_id: str | None = None
    lease: PartLease | None = None
    message_id: str | None = None
    schema_version: int = 1

    def __post_init__(self) -> None:
        if self.schema_version <= 0:
            raise ValueError("schema_version must be positive")
        if self.transport_epoch <= 0:
            raise ValueError("transport_epoch must be positive")
        if self.assignment_generation <= 0:
            raise ValueError("assignment_generation must be positive")
        part_id = self.part_id or self.phrase.role
        object.__setattr__(self, "part_id", part_id)

        lease = self.lease or PartLease(
            transport_epoch=self.transport_epoch,
            part_id=part_id,
            node_id=self.node_id,
            assignment_generation=self.assignment_generation,
            valid_from_bar=0,
            valid_through_bar=self.phrase.bars,
        )
        if lease.transport_epoch != self.transport_epoch:
            raise ValueError("lease epoch must match assignment epoch")
        if lease.part_id != part_id:
            raise ValueError("lease part_id must match assignment part_id")
        if lease.node_id != self.node_id:
            raise ValueError("lease node_id must match assignment node_id")
        if lease.assignment_generation != self.assignment_generation:
            raise ValueError("lease generation must match assignment generation")
        object.__setattr__(self, "lease", lease)

        assignment_id = self.assignment_id or (
            f"epoch-{self.transport_epoch}:{part_id}:gen-{self.assignment_generation}"
        )
        object.__setattr__(self, "assignment_id", assignment_id)
        object.__setattr__(self, "message_id", self.message_id or assignment_id)

    @property
    def generation(self) -> int:
        return self.assignment_generation

    @property
    def valid_from_bar(self) -> int:
        assert self.lease is not None
        return self.lease.valid_from_bar

    @property
    def valid_through_bar(self) -> int:
        assert self.lease is not None
        return self.lease.valid_through_bar


@dataclass(frozen=True, slots=True)
class PhraseReady:
    node_id: str
    part_id: str
    phrase_sequence: int
    assignment_generation: int
    ready_through_bar: int
    transport_epoch: int


@dataclass(frozen=True, slots=True)
class NodeDesiredState:
    transport_epoch: int
    transport_state: TransportState
    tempo_bpm: float
    assignments: tuple[Assignment, ...] = ()


@dataclass(slots=True)
class NodeObservedState:
    node_id: str
    transport_epoch: int | None = None
    active_assignment_generation: dict[str, int] = field(default_factory=dict)
    current_bar: int = 0
    policy_version: int | None = None
    buffered_through_bar: int = 0
    scheduler_lateness_ms: float = 0.0
    sync_uncertainty_ms: float = 0.0
    synth_healthy: bool = True
    stale_message_drops: int = 0
    duplicate_message_drops: int = 0
    buffer_underruns: int = 0
    lease_expirations: int = 0


@dataclass(frozen=True, slots=True)
class AuthoritativeSnapshot:
    transport_epoch: int
    transport_state: TransportState
    tempo_bpm: float
    current_bar: int
    assignments: tuple[Assignment, ...] = ()
    schema_version: int = 1
    message_id: str = "snapshot"

    def __post_init__(self) -> None:
        if self.transport_epoch <= 0:
            raise ValueError("transport_epoch must be positive")
        if self.tempo_bpm <= 0:
            raise ValueError("tempo_bpm must be positive")
        if self.current_bar < 0:
            raise ValueError("current_bar must be non-negative")
        if self.schema_version <= 0:
            raise ValueError("schema_version must be positive")


def is_part_authorized(
    *,
    assignment: Assignment,
    node_id: str,
    transport_epoch: int | None,
    current_bar: int,
) -> bool:
    """Centralized fencing/lease check used before local performance authority."""

    if transport_epoch != assignment.transport_epoch:
        return False
    if assignment.node_id != node_id:
        return False
    assert assignment.lease is not None
    return assignment.lease.contains_bar(current_bar)
