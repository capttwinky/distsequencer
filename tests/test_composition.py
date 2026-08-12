import pytest

from distributed_sequencer.application.composition import (
    CompositionEngine,
    DensityCritic,
    ProceduralCompositionModel,
    RegisterCritic,
)
from distributed_sequencer.domain.state import CompositionContext


@pytest.mark.asyncio
async def test_composition_returns_canonical_phrase() -> None:
    engine = CompositionEngine(
        ProceduralCompositionModel(seed=7),
        critics=(DensityCritic(), RegisterCritic()),
    )
    phrase = await engine.compose(CompositionContext("bass", 36, 1.0))
    assert phrase.role == "bass"
    assert phrase.events
    assert all(0 <= event.pitch <= 127 for event in phrase.events)
