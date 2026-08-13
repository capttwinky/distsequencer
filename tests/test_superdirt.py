from __future__ import annotations

import pytest

from distributed_sequencer.adapters import superdirt
from distributed_sequencer.adapters.superdirt import (
    SuperDirtOscBackend,
    encode_dirt_bundle,
    encode_dirt_message,
    phrase_to_dirt_events,
)
from distributed_sequencer.domain.music import MusicalEvent, Phrase


def test_phrase_to_dirt_events_uses_superdirt_play_shape() -> None:
    phrase = Phrase(
        phrase_id="bass-seq-1",
        role="bass",
        events=(
            MusicalEvent(onset_tick=0, pitch=36, duration_ticks=12, velocity=100),
            MusicalEvent(onset_tick=24, pitch=43, duration_ticks=12, velocity=90),
        ),
        bars=1,
        beats_per_bar=4,
        ticks_per_beat=24,
    )

    events = phrase_to_dirt_events(phrase, tempo_bpm=120, orbit=2)

    assert len(events) == 2
    assert events[0].cps == 0.5
    assert events[0].cycle == 0.0
    assert events[0].delta == 0.25
    assert events[0].orbit == 2
    assert events[0].sound == "imp"
    assert events[0].note == 36.0
    assert events[1].cycle == 0.25


def test_dirt_message_and_bundle_are_osc_encoded() -> None:
    event = phrase_to_dirt_events(
        Phrase(
            phrase_id="lead-seq-1",
            role="lead",
            events=(MusicalEvent(onset_tick=0, pitch=64, duration_ticks=24),),
        ),
        tempo_bpm=120,
    )[0]

    message = encode_dirt_message(event)
    bundle = encode_dirt_bundle(event, timestamp=1_800_000_000.25)

    assert message.startswith(b"/dirt/play\x00\x00")
    assert b",sfsfsf" in message
    assert b"psin" in message
    assert b"\x00n\x00\x00" in message
    assert b"note" in message
    assert bundle.startswith(b"#bundle\x00")
    assert message in bundle


@pytest.mark.asyncio
async def test_superdirt_backend_sends_timestamped_udp_bundles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sent: list[tuple[bytes, tuple[str, int]]] = []

    class FakeSocket:
        def __init__(self, *_args: object) -> None:
            pass

        def __enter__(self) -> FakeSocket:
            return self

        def __exit__(self, *_args: object) -> None:
            pass

        def sendto(self, payload: bytes, address: tuple[str, int]) -> None:
            sent.append((payload, address))

    monkeypatch.setattr(superdirt.socket, "socket", FakeSocket)
    backend = SuperDirtOscBackend(host="192.0.2.10", port=57121, latency_seconds=0.01)
    phrase = Phrase(
        phrase_id="lead-seq-1",
        role="lead",
        events=(MusicalEvent(onset_tick=0, pitch=67, duration_ticks=24),),
    )

    events = await backend.send_phrase(phrase, tempo_bpm=120)

    assert len(events) == 1
    assert len(sent) == 1
    payload, address = sent[0]
    assert address == ("192.0.2.10", 57121)
    assert payload.startswith(b"#bundle\x00")
    assert b"/dirt/play" in payload


@pytest.mark.asyncio
async def test_superdirt_backend_streams_repeated_cycles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sent: list[tuple[bytes, tuple[str, int]]] = []
    sleeps: list[float] = []
    now = 10_000.0

    class FakeSocket:
        def __init__(self, *_args: object) -> None:
            pass

        def __enter__(self) -> FakeSocket:
            return self

        def __exit__(self, *_args: object) -> None:
            pass

        def sendto(self, payload: bytes, address: tuple[str, int]) -> None:
            sent.append((payload, address))

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(superdirt.socket, "socket", FakeSocket)
    monkeypatch.setattr(superdirt.time, "time", lambda: now)
    monkeypatch.setattr(superdirt.asyncio, "sleep", fake_sleep)
    event = phrase_to_dirt_events(
        Phrase(
            phrase_id="lead-seq-1",
            role="lead",
            events=(
                MusicalEvent(onset_tick=0, pitch=67, duration_ticks=24),
                MusicalEvent(onset_tick=24, pitch=69, duration_ticks=24),
            ),
        ),
        tempo_bpm=120,
    )[0]
    backend = SuperDirtOscBackend(host="192.0.2.11", port=57122, latency_seconds=0.2)

    streamed = await backend.stream_events((event,), cycles=3, lookahead_seconds=0.1)

    assert [item.cycle for item in streamed] == [0.0, 1.0, 2.0]
    assert len(sent) == 3
    assert {address for _, address in sent} == {("192.0.2.11", 57122)}
    assert any(delay > 0 for delay in sleeps)
