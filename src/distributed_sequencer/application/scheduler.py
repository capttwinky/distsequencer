from __future__ import annotations

import asyncio
from dataclasses import dataclass

from distributed_sequencer.adapters.synth import SynthBackend
from distributed_sequencer.domain.music import Phrase
from distributed_sequencer.infrastructure.clock import Clock


@dataclass(slots=True)
class Scheduler:
    synth: SynthBackend
    clock: Clock
    bpm: float = 120.0

    async def play(self, phrase: Phrase) -> None:
        """Play prepared phrase events. No composition or model inference is allowed here."""
        seconds_per_tick = 60.0 / self.bpm / phrase.ticks_per_beat
        cursor = 0
        note_offs: dict[int, list[tuple[int, int]]] = {}
        note_ons: dict[int, list[tuple[int, int, int]]] = {}
        for event in phrase.events:
            note_ons.setdefault(event.onset_tick, []).append(
                (event.pitch, event.velocity, event.channel)
            )
            note_offs.setdefault(event.end_tick, []).append((event.pitch, event.channel))

        for tick in sorted(set(note_ons) | set(note_offs)):
            await self.clock.sleep((tick - cursor) * seconds_per_tick)
            cursor = tick
            # Note-offs first prevent repeated notes from overlapping at a shared boundary.
            for pitch, channel in note_offs.get(tick, []):
                await self.synth.note_off(pitch=pitch, channel=channel)
            for pitch, velocity, channel in note_ons.get(tick, []):
                await self.synth.note_on(pitch=pitch, velocity=velocity, channel=channel)
            await asyncio.sleep(0)

        await self.clock.sleep((phrase.total_ticks - cursor) * seconds_per_tick)
