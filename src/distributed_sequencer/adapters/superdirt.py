from __future__ import annotations

import asyncio
import socket
import struct
import time
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass

from distributed_sequencer.domain.music import Phrase

_NTP_UNIX_EPOCH_DELTA = 2_208_988_800


@dataclass(frozen=True, slots=True)
class DirtEvent:
    """A single SuperDirt `/dirt/play` event derived from canonical phrase IR."""

    cps: float
    cycle: float
    delta: float
    orbit: int
    sound: str
    note: float
    velocity: float
    gain: float
    pan: float
    sustain: float

    def arguments(self) -> tuple[str | int | float, ...]:
        return (
            "cps",
            self.cps,
            "cycle",
            self.cycle,
            "delta",
            self.delta,
            "orbit",
            self.orbit,
            "s",
            self.sound,
            "note",
            self.note,
            "velocity",
            self.velocity,
            "gain",
            self.gain,
            "pan",
            self.pan,
            "sustain",
            self.sustain,
        )

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SuperDirtOscBackend:
    """Direct OSC target for a running SuperDirt server.

    SuperDirt conventionally listens on UDP `127.0.0.1:57120` and receives timestamped
    OSC bundles containing `/dirt/play` messages with name/value parameters.
    """

    host: str = "127.0.0.1"
    port: int = 57120
    latency_seconds: float = 0.2

    async def send_phrase(
        self,
        phrase: Phrase,
        *,
        tempo_bpm: float,
        sound: str | None = None,
        orbit: int = 0,
        gain: float = 0.8,
        pan: float = 0.5,
        cycle_offset: float = 0.0,
    ) -> tuple[DirtEvent, ...]:
        events = phrase_to_dirt_events(
            phrase,
            tempo_bpm=tempo_bpm,
            sound=sound,
            orbit=orbit,
            gain=gain,
            pan=pan,
            cycle_offset=cycle_offset,
        )
        await self.send_events(events)
        return events

    async def send_phrases(
        self,
        phrases: Iterable[Phrase],
        *,
        tempo_bpm: float,
    ) -> tuple[DirtEvent, ...]:
        events: list[DirtEvent] = []
        phrase_list = tuple(phrases)
        for index, phrase in enumerate(phrase_list):
            events.extend(
                phrase_to_dirt_events(
                    phrase,
                    tempo_bpm=tempo_bpm,
                    sound=default_superdirt_sound(phrase),
                    orbit=index,
                    gain=default_superdirt_gain(phrase),
                    pan=_pan(index, len(phrase_list)),
                )
            )
        await self.send_events(events)
        return tuple(events)

    async def send_events(self, events: Sequence[DirtEvent]) -> None:
        start_time = time.time() + self.latency_seconds
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            for event in events:
                timestamp = start_time + event.cycle / event.cps
                sock.sendto(encode_dirt_bundle(event, timestamp=timestamp), (self.host, self.port))
                await asyncio.sleep(0)


def phrase_to_dirt_events(
    phrase: Phrase,
    *,
    tempo_bpm: float,
    sound: str | None = None,
    orbit: int = 0,
    gain: float = 0.8,
    pan: float = 0.5,
    cycle_offset: float = 0.0,
) -> tuple[DirtEvent, ...]:
    if tempo_bpm <= 0:
        raise ValueError("tempo_bpm must be positive")
    cps = tempo_bpm / 60.0 / phrase.beats_per_bar
    seconds_per_tick = 60.0 / tempo_bpm / phrase.ticks_per_beat
    ticks_per_cycle = phrase.beats_per_bar * phrase.ticks_per_beat
    target_sound = sound or default_superdirt_sound(phrase)
    return tuple(
        DirtEvent(
            cps=cps,
            cycle=cycle_offset + event.onset_tick / ticks_per_cycle,
            delta=event.duration_ticks * seconds_per_tick,
            orbit=orbit,
            sound=target_sound,
            note=float(event.pitch),
            velocity=event.velocity / 127.0,
            gain=gain,
            pan=pan,
            sustain=event.duration_ticks * seconds_per_tick,
        )
        for event in phrase.events
    )


def encode_dirt_bundle(event: DirtEvent, *, timestamp: float) -> bytes:
    message = encode_dirt_message(event)
    timetag = _ntp_timetag(timestamp)
    return _osc_string("#bundle") + timetag + struct.pack(">i", len(message)) + message


def encode_dirt_message(event: DirtEvent) -> bytes:
    return _osc_message("/dirt/play", event.arguments())


def default_superdirt_sound(phrase: Phrase) -> str:
    if phrase.role.lower() in {"bass", "sub", "low"}:
        return "super808"
    return "superpiano"


def default_superdirt_gain(phrase: Phrase) -> float:
    if phrase.role.lower() in {"bass", "sub", "low"}:
        return 0.85
    return 0.65


def _osc_message(path: str, arguments: Sequence[str | int | float]) -> bytes:
    tags = "," + "".join(_osc_tag(argument) for argument in arguments)
    payload = bytearray(_osc_string(path))
    payload.extend(_osc_string(tags))
    for argument in arguments:
        payload.extend(_osc_argument(argument))
    return bytes(payload)


def _osc_string(value: str) -> bytes:
    raw = value.encode("utf-8") + b"\0"
    padding = (-len(raw)) % 4
    return raw + (b"\0" * padding)


def _osc_tag(value: str | int | float) -> str:
    if isinstance(value, str):
        return "s"
    if isinstance(value, int):
        return "i"
    if isinstance(value, float):
        return "f"
    raise TypeError(f"unsupported OSC argument type: {type(value)!r}")


def _osc_argument(value: str | int | float) -> bytes:
    if isinstance(value, str):
        return _osc_string(value)
    if isinstance(value, int):
        return struct.pack(">i", value)
    if isinstance(value, float):
        return struct.pack(">f", value)
    raise TypeError(f"unsupported OSC argument type: {type(value)!r}")


def _ntp_timetag(timestamp: float) -> bytes:
    seconds = int(timestamp)
    fraction = int((timestamp - seconds) * 2**32)
    return struct.pack(">II", seconds + _NTP_UNIX_EPOCH_DELTA, fraction)


def _pan(index: int, count: int) -> float:
    if count <= 1:
        return 0.5
    return 0.15 + 0.7 * index / (count - 1)
