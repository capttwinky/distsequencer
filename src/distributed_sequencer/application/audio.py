from __future__ import annotations

import math
import struct
import wave
from collections.abc import Iterable, Sequence
from io import BytesIO

from distributed_sequencer.domain.music import Phrase

DEFAULT_SAMPLE_RATE = 44_100
_MAX_INT16 = 32_767


def render_phrases_wav(
    phrases: Iterable[Phrase],
    *,
    tempo_bpm: float,
    sample_rate: int = DEFAULT_SAMPLE_RATE,
) -> bytes:
    """Render model-independent phrases as a browser-playable stereo WAV file."""
    phrase_list = tuple(phrases)
    if not phrase_list:
        raise ValueError("at least one phrase is required to render audio")
    if tempo_bpm <= 0:
        raise ValueError("tempo_bpm must be positive")
    if sample_rate <= 0:
        raise ValueError("sample_rate must be positive")

    duration_seconds = max(_phrase_duration_seconds(phrase, tempo_bpm) for phrase in phrase_list)
    frame_count = max(1, math.ceil(duration_seconds * sample_rate))
    left = [0.0] * frame_count
    right = [0.0] * frame_count

    for index, phrase in enumerate(phrase_list):
        pan = _pan(index, len(phrase_list))
        for event in phrase.events:
            seconds_per_tick = 60.0 / tempo_bpm / phrase.ticks_per_beat
            start = round(event.onset_tick * seconds_per_tick * sample_rate)
            end = min(
                frame_count,
                start + round(event.duration_ticks * seconds_per_tick * sample_rate),
            )
            if end <= start:
                continue
            frequency = 440.0 * 2 ** ((event.pitch - 69) / 12)
            gain = 0.16 * (event.velocity / 127)
            _mix_note(
                left,
                right,
                start=start,
                end=end,
                frequency=frequency,
                gain=gain,
                pan=pan,
                sample_rate=sample_rate,
            )

    return _encode_wav(left, right, sample_rate=sample_rate)


def _phrase_duration_seconds(phrase: Phrase, tempo_bpm: float) -> float:
    return phrase.total_ticks * 60.0 / tempo_bpm / phrase.ticks_per_beat


def _pan(index: int, count: int) -> float:
    if count <= 1:
        return 0.0
    return -0.55 + 1.1 * index / (count - 1)


def _mix_note(
    left: list[float],
    right: list[float],
    *,
    start: int,
    end: int,
    frequency: float,
    gain: float,
    pan: float,
    sample_rate: int,
) -> None:
    note_frames = end - start
    attack = max(1, min(note_frames // 8, round(0.008 * sample_rate)))
    release = max(1, min(note_frames // 4, round(0.035 * sample_rate)))
    left_gain = math.cos((pan + 1.0) * math.pi / 4.0)
    right_gain = math.sin((pan + 1.0) * math.pi / 4.0)

    for offset in range(note_frames):
        frame = start + offset
        envelope = _envelope(offset, note_frames, attack=attack, release=release)
        phase = 2.0 * math.pi * frequency * (offset / sample_rate)
        sample = gain * envelope * (math.sin(phase) + 0.25 * math.sin(phase * 2.0))
        left[frame] += sample * left_gain
        right[frame] += sample * right_gain


def _envelope(offset: int, frames: int, *, attack: int, release: int) -> float:
    if offset < attack:
        return offset / attack
    remaining = frames - offset
    if remaining < release:
        return max(0.0, remaining / release)
    return 1.0


def _encode_wav(left: Sequence[float], right: Sequence[float], *, sample_rate: int) -> bytes:
    peak = max((abs(sample) for sample in (*left, *right)), default=0.0)
    scale = 0.95 / peak if peak > 1.0 else 1.0
    frames = bytearray()
    for left_sample, right_sample in zip(left, right, strict=True):
        frames.extend(_pack_sample(left_sample, scale=scale))
        frames.extend(_pack_sample(right_sample, scale=scale))

    buffer = BytesIO()
    with wave.open(buffer, "wb") as wav:
        wav.setnchannels(2)
        wav.setsampwidth(2)
        wav.setframerate(sample_rate)
        wav.writeframes(bytes(frames))
    return buffer.getvalue()


def _pack_sample(sample: float, *, scale: float) -> bytes:
    bounded = max(-1.0, min(1.0, sample * scale))
    return struct.pack("<h", round(bounded * _MAX_INT16))
