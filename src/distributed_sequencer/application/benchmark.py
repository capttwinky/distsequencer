from __future__ import annotations

import json
import platform
import time
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class BenchmarkRecord:
    name: str
    platform: str
    python_version: str
    model_size_mb: float | None
    resident_memory_mb: float | None
    variation_latency_ms: float | None
    throughput_events_per_second: float | None
    buffer_safety_margin_bars: float | None
    musical_quality_score: float | None


@dataclass(frozen=True, slots=True)
class HardwareProfile:
    node_id: str
    cpu: str
    memory_mb: int
    os: str
    audio_backend: str
    network: str


class BenchmarkSuite:
    def __init__(self) -> None:
        self.records: list[BenchmarkRecord] = []

    def record(self, record: BenchmarkRecord) -> None:
        self.records.append(record)

    def time_variation(self, name: str, event_count: int, operation: object) -> BenchmarkRecord:
        if not callable(operation):
            raise TypeError("operation must be callable")
        start = time.perf_counter()
        operation()
        elapsed = time.perf_counter() - start
        record = BenchmarkRecord(
            name=name,
            platform=platform.platform(),
            python_version=platform.python_version(),
            model_size_mb=None,
            resident_memory_mb=None,
            variation_latency_ms=elapsed * 1000.0,
            throughput_events_per_second=event_count / elapsed if elapsed > 0 else None,
            buffer_safety_margin_bars=None,
            musical_quality_score=None,
        )
        self.record(record)
        return record

    def write_json(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps([asdict(record) for record in self.records], indent=2, sort_keys=True),
            encoding="utf-8",
        )
