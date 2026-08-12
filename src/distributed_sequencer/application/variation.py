from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Protocol

from distributed_sequencer.domain.music import Phrase
from distributed_sequencer.domain.state import VariationPolicy


class LearnedVariationModel(Protocol):
    async def vary(self, phrase: Phrase, policy: VariationPolicy) -> Phrase: ...


@dataclass(slots=True)
class VariationEngine:
    seed: int = 1
    learned_model: LearnedVariationModel | None = None

    async def vary(self, phrase: Phrase, policy: VariationPolicy, *, generation: int = 0) -> Phrase:
        rng = random.Random(self.seed + generation)
        varied_events = []

        for event in phrase.events:
            if rng.random() < policy.omission_probability:
                continue
            requested_timing_delta = rng.randint(
                -policy.timing_jitter_ticks, policy.timing_jitter_ticks
            )
            target_onset = min(
                phrase.total_ticks - event.duration_ticks,
                max(0, event.onset_tick + requested_timing_delta),
            )
            velocity_delta = rng.randint(-policy.velocity_jitter, policy.velocity_jitter)
            varied_events.append(
                event.shifted(
                    onset_delta=target_onset - event.onset_tick,
                    pitch_delta=policy.pitch_shift_semitones,
                    velocity_delta=velocity_delta,
                )
            )

        result = Phrase(
            phrase_id=f"{phrase.phrase_id}-v{generation}",
            role=phrase.role,
            events=tuple(sorted(varied_events)),
            bars=phrase.bars,
            beats_per_bar=phrase.beats_per_bar,
            ticks_per_beat=phrase.ticks_per_beat,
        )
        if self.learned_model is not None:
            return await self.learned_model.vary(result, policy)
        return result
