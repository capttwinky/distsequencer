from __future__ import annotations

import random
from dataclasses import dataclass, replace
from typing import Protocol

from distributed_sequencer.domain.music import MusicalEvent, Phrase
from distributed_sequencer.domain.state import CompositionContext


class CompositionModel(Protocol):
    async def generate_candidates(
        self,
        context: CompositionContext,
        *,
        count: int,
    ) -> tuple[Phrase, ...]: ...


class Critic(Protocol):
    def score(self, phrase: Phrase, context: CompositionContext) -> float: ...


@dataclass(frozen=True, slots=True)
class CandidateScore:
    phrase: Phrase
    dimensions: tuple[tuple[str, float], ...]

    @property
    def total(self) -> float:
        return sum(score for _, score in self.dimensions)


@dataclass(frozen=True, slots=True)
class ProceduralCompositionModel:
    seed: int = 1

    async def generate_candidates(
        self,
        context: CompositionContext,
        *,
        count: int,
    ) -> tuple[Phrase, ...]:
        candidates: list[Phrase] = []
        scale = (0, 3, 5, 7, 10)
        total_beats = context.bars * context.beats_per_bar

        role_seed = sum((index + 1) * ord(char) for index, char in enumerate(context.role))
        for candidate_index in range(count):
            rng = random.Random(self.seed + candidate_index + role_seed)
            note_count = max(1, round(total_beats * max(0.25, context.desired_density)))
            total_ticks = context.bars * context.beats_per_bar * context.ticks_per_beat
            step = max(1, total_ticks // note_count)
            duration = max(1, min(context.ticks_per_beat, step))
            events = []
            for note_index in range(note_count):
                onset = min(note_index * step, total_ticks - duration)
                pitch = context.root_pitch + rng.choice(scale)
                events.append(
                    MusicalEvent(
                        onset_tick=onset,
                        pitch=pitch,
                        duration_ticks=duration,
                        velocity=rng.randint(78, 108),
                    )
                )

            candidates.append(
                Phrase(
                    phrase_id=f"{context.role}-g{candidate_index}",
                    role=context.role,
                    events=tuple(sorted(events)),
                    phrase_revision=1,
                    phrase_sequence=0,
                    bars=context.bars,
                    beats_per_bar=context.beats_per_bar,
                    ticks_per_beat=context.ticks_per_beat,
                )
            )
        return tuple(candidates)


@dataclass(frozen=True, slots=True)
class DensityCritic:
    """Rewards phrases near the requested note density."""

    def score(self, phrase: Phrase, context: CompositionContext) -> float:
        return 1.0 / (1.0 + abs(phrase.density - context.desired_density))


@dataclass(frozen=True, slots=True)
class RegisterCritic:
    low: int = 24
    high: int = 96

    def score(self, phrase: Phrase, context: CompositionContext) -> float:
        del context
        if not phrase.events:
            return 0.0
        valid = sum(self.low <= event.pitch <= self.high for event in phrase.events)
        return valid / len(phrase.events)


@dataclass(slots=True)
class CompositionEngine:
    model: CompositionModel
    critics: tuple[Critic, ...]
    accepted_sequences: dict[str, int] | None = None

    async def compose(self, context: CompositionContext, *, candidate_count: int = 3) -> Phrase:
        candidates = await self.model.generate_candidates(context, count=candidate_count)
        if not candidates:
            raise RuntimeError("composition model returned no candidates")

        scored = tuple(self.score_candidate(candidate, context) for candidate in candidates)
        selected = max(scored, key=lambda candidate_score: candidate_score.total).phrase
        if self.accepted_sequences is None:
            self.accepted_sequences = {}
        sequence = self.accepted_sequences.get(context.role, 0) + 1
        self.accepted_sequences[context.role] = sequence
        return replace(
            selected,
            phrase_id=f"{context.role}-seq-{sequence}",
            phrase_revision=1,
            phrase_sequence=sequence,
        )

    def score_candidate(self, phrase: Phrase, context: CompositionContext) -> CandidateScore:
        dimensions = tuple(
            (critic.__class__.__name__, critic.score(phrase, context)) for critic in self.critics
        )
        return CandidateScore(phrase=phrase, dimensions=dimensions)
