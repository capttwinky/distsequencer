import pytest

from distributed_sequencer.adapters.synth import RecordingSynth
from distributed_sequencer.application.scheduler import Scheduler
from distributed_sequencer.domain.music import MusicalEvent, Phrase
from distributed_sequencer.infrastructure.clock import AdvancingClock


@pytest.mark.asyncio
async def test_scheduler_orders_note_on_and_off() -> None:
    phrase = Phrase(
        "p",
        "lead",
        (MusicalEvent(0, 60, 24), MusicalEvent(24, 62, 24)),
    )
    synth = RecordingSynth()
    clock = AdvancingClock()
    await Scheduler(synth, clock, bpm=120).play(phrase)

    assert [event.kind for event in synth.events] == ["note_on", "note_off", "note_on", "note_off"]
    assert clock.elapsed == pytest.approx(2.0)
