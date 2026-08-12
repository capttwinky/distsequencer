from __future__ import annotations

from dataclasses import dataclass, replace


@dataclass(frozen=True, slots=True, order=True)
class MusicalEvent:
    """A note event positioned in canonical musical ticks."""

    onset_tick: int
    pitch: int
    duration_ticks: int
    velocity: int = 96
    channel: int = 0

    def __post_init__(self) -> None:
        if self.onset_tick < 0:
            raise ValueError("onset_tick must be non-negative")
        if not 0 <= self.pitch <= 127:
            raise ValueError("pitch must be a MIDI-compatible note number")
        if self.duration_ticks <= 0:
            raise ValueError("duration_ticks must be positive")
        if not 1 <= self.velocity <= 127:
            raise ValueError("velocity must be in [1, 127]")
        if not 0 <= self.channel <= 15:
            raise ValueError("channel must be in [0, 15]")

    @property
    def end_tick(self) -> int:
        return self.onset_tick + self.duration_ticks

    def shifted(
        self,
        *,
        onset_delta: int = 0,
        pitch_delta: int = 0,
        velocity_delta: int = 0,
    ) -> MusicalEvent:
        return replace(
            self,
            onset_tick=max(0, self.onset_tick + onset_delta),
            pitch=min(127, max(0, self.pitch + pitch_delta)),
            velocity=min(127, max(1, self.velocity + velocity_delta)),
        )


@dataclass(frozen=True, slots=True)
class Phrase:
    """Model-independent canonical phrase."""

    phrase_id: str
    role: str
    events: tuple[MusicalEvent, ...]
    phrase_revision: int = 1
    phrase_sequence: int = 0
    bars: int = 1
    beats_per_bar: int = 4
    ticks_per_beat: int = 24

    def __post_init__(self) -> None:
        if self.phrase_revision <= 0 or self.phrase_sequence < 0:
            raise ValueError("phrase revision must be positive and sequence non-negative")
        if self.bars <= 0 or self.beats_per_bar <= 0 or self.ticks_per_beat <= 0:
            raise ValueError("musical dimensions must be positive")
        if tuple(sorted(self.events)) != self.events:
            raise ValueError("phrase events must be sorted")
        if any(event.end_tick > self.total_ticks for event in self.events):
            raise ValueError("event extends beyond phrase boundary")

    @property
    def total_ticks(self) -> int:
        return self.bars * self.beats_per_bar * self.ticks_per_beat

    @property
    def density(self) -> float:
        return len(self.events) / max(1, self.bars * self.beats_per_bar)
