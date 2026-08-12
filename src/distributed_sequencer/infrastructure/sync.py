from __future__ import annotations

from dataclasses import dataclass

from distributed_sequencer.infrastructure.clock import SyncEstimate


@dataclass(frozen=True, slots=True)
class ClockSample:
    coordinator_send_seconds: float
    node_receive_seconds: float
    node_send_seconds: float
    coordinator_receive_seconds: float

    @property
    def round_trip_seconds(self) -> float:
        return self.coordinator_receive_seconds - self.coordinator_send_seconds

    @property
    def midpoint_offset_seconds(self) -> float:
        coordinator_midpoint = (
            self.coordinator_send_seconds + self.coordinator_receive_seconds
        ) / 2.0
        node_midpoint = (self.node_receive_seconds + self.node_send_seconds) / 2.0
        return node_midpoint - coordinator_midpoint


@dataclass(slots=True)
class PtpStyleSynchronizer:
    """PTP-style sync estimator using offset samples and drift regression."""

    max_samples: int = 16
    samples: list[ClockSample] | None = None

    def __post_init__(self) -> None:
        if self.samples is None:
            self.samples = []

    def observe(self, sample: ClockSample) -> SyncEstimate:
        assert self.samples is not None
        self.samples.append(sample)
        del self.samples[: max(0, len(self.samples) - self.max_samples)]
        return self.estimate()

    def estimate(self) -> SyncEstimate:
        assert self.samples is not None
        if not self.samples:
            return SyncEstimate(0.0, 0.0, 1.0)
        offsets = [sample.midpoint_offset_seconds for sample in self.samples]
        times = [
            (sample.coordinator_send_seconds + sample.coordinator_receive_seconds) / 2.0
            for sample in self.samples
        ]
        drift_ppm = _linear_drift_ppm(times, offsets)
        uncertainty = max(sample.round_trip_seconds for sample in self.samples) / 2.0
        return SyncEstimate(
            offset_seconds=offsets[-1],
            drift_ppm=drift_ppm,
            uncertainty_seconds=max(0.0, uncertainty),
        )


def _linear_drift_ppm(times: list[float], offsets: list[float]) -> float:
    if len(times) < 2:
        return 0.0
    mean_time = sum(times) / len(times)
    mean_offset = sum(offsets) / len(offsets)
    denominator = sum((time - mean_time) ** 2 for time in times)
    if denominator == 0.0:
        return 0.0
    slope = (
        sum(
            (time - mean_time) * (offset - mean_offset)
            for time, offset in zip(times, offsets, strict=True)
        )
        / denominator
    )
    return slope * 1_000_000.0
