from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Protocol


class SynthBackend(Protocol):
    async def note_on(self, *, pitch: int, velocity: int, channel: int) -> None: ...

    async def note_off(self, *, pitch: int, channel: int) -> None: ...


@dataclass(frozen=True, slots=True)
class SynthEvent:
    kind: str
    pitch: int
    velocity: int
    channel: int


@dataclass(slots=True)
class RecordingSynth:
    events: list[SynthEvent] = field(default_factory=list)
    echo: bool = False
    label: str = "synth"

    async def note_on(self, *, pitch: int, velocity: int, channel: int) -> None:
        event = SynthEvent("note_on", pitch, velocity, channel)
        self.events.append(event)
        if self.echo:
            print(f"[{self.label}] ON  pitch={pitch:3d} velocity={velocity:3d} channel={channel}")

    async def note_off(self, *, pitch: int, channel: int) -> None:
        event = SynthEvent("note_off", pitch, 0, channel)
        self.events.append(event)
        if self.echo:
            print(f"[{self.label}] OFF pitch={pitch:3d} channel={channel}")


@dataclass(frozen=True, slots=True)
class OscSynthBackend:
    """Minimal local OSC-compatible UDP backend.

    The MVP keeps OSC on loopback by default. Messages are simple text OSC-like
    packets for easy inspection by local synth shims: ``/note_on p v c`` and
    ``/note_off p c``.
    """

    host: str = "127.0.0.1"
    port: int = 9000

    async def note_on(self, *, pitch: int, velocity: int, channel: int) -> None:
        await self._send(f"/note_on {pitch} {velocity} {channel}")

    async def note_off(self, *, pitch: int, channel: int) -> None:
        await self._send(f"/note_off {pitch} {channel}")

    async def _send(self, message: str) -> None:
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_datagram_endpoint(
            asyncio.DatagramProtocol,
            remote_addr=(self.host, self.port),
        )
        try:
            transport.sendto(message.encode("utf-8"))
        finally:
            transport.close()
