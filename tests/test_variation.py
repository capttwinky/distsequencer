import pytest

from distributed_sequencer.application.variation import VariationEngine
from distributed_sequencer.domain.music import MusicalEvent, Phrase
from distributed_sequencer.domain.state import VariationPolicy


@pytest.mark.asyncio
async def test_variation_is_seeded_and_bounded() -> None:
    phrase = Phrase("p", "bass", (MusicalEvent(0, 36, 24, 90), MusicalEvent(24, 43, 24, 90)))
    policy = VariationPolicy(timing_jitter_ticks=2, velocity_jitter=4, pitch_shift_semitones=12)
    first = await VariationEngine(seed=1).vary(phrase, policy, generation=2)
    second = await VariationEngine(seed=1).vary(phrase, policy, generation=2)
    assert first.events == second.events
    assert all(event.pitch >= 48 for event in first.events)
    assert all(86 <= event.velocity <= 94 for event in first.events)
