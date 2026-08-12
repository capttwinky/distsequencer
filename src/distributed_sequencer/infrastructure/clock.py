from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Protocol


class Clock(Protocol):
    async def sleep(self, seconds: float) -> None: ...


@dataclass(slots=True)
class AsyncioClock:
    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(max(0.0, seconds))


@dataclass(slots=True)
class AdvancingClock:
    """Test clock: advances virtual elapsed time without wall-clock delay."""

    elapsed: float = 0.0

    async def sleep(self, seconds: float) -> None:
        self.elapsed += max(0.0, seconds)
        await asyncio.sleep(0)


@dataclass(frozen=True, slots=True)
class SyncEstimate:
    offset_seconds: float
    drift_ppm: float
    uncertainty_seconds: float

    @property
    def healthy(self) -> bool:
        return self.uncertainty_seconds <= 0.050 and abs(self.drift_ppm) <= 500.0


@dataclass(slots=True)
class ClockSynchronizer:
    """Tracks coordinator-to-local clock estimates without comparing raw clocks."""

    max_healthy_uncertainty_seconds: float = 0.050
    estimate: SyncEstimate = SyncEstimate(0.0, 0.0, 1.0)

    def update(
        self,
        *,
        estimated_offset_seconds: float,
        estimated_drift_ppm: float,
        uncertainty_seconds: float,
    ) -> SyncEstimate:
        self.estimate = SyncEstimate(
            offset_seconds=estimated_offset_seconds,
            drift_ppm=estimated_drift_ppm,
            uncertainty_seconds=uncertainty_seconds,
        )
        return self.estimate

    def is_healthy(self) -> bool:
        return (
            self.estimate.uncertainty_seconds <= self.max_healthy_uncertainty_seconds
            and abs(self.estimate.drift_ppm) <= 500.0
        )
