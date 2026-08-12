from __future__ import annotations

import importlib.util
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from distributed_sequencer.application.composition import CompositionModel, Critic
from distributed_sequencer.application.variation import LearnedVariationModel
from distributed_sequencer.domain.music import MusicalEvent, Phrase
from distributed_sequencer.domain.state import CompositionContext, VariationPolicy


class OptionalDependencyUnavailable(RuntimeError):
    pass


def require_optional_dependency(module_name: str, *, group: str = "ml") -> None:
    if importlib.util.find_spec(module_name) is None:
        raise OptionalDependencyUnavailable(
            f"optional dependency {module_name!r} is unavailable; run `uv sync --group {group}`"
        )


@dataclass(frozen=True, slots=True)
class OptionalMlRuntimeProbe:
    """Small helper for notebooks/benchmarks without importing ML in core runtime."""

    module_name: str = "torch"
    group: str = "ml"

    @property
    def installed(self) -> bool:
        return importlib.util.find_spec(self.module_name) is not None

    def require(self) -> None:
        require_optional_dependency(self.module_name, group=self.group)


@dataclass(frozen=True, slots=True)
class MidiGPTCompositionAdapter(CompositionModel):
    """Adapter boundary for a local symbolic composition model.

    The adapter is real in the sense that it validates runtime/model availability and calls an
    injected or loaded backend. It does not ship a model checkpoint or synthesize fake ML output.
    """

    model_path: Path
    runtime_module: str = "torch"
    backend: Any | None = None

    async def generate_candidates(
        self,
        context: CompositionContext,
        *,
        count: int,
    ) -> tuple[Phrase, ...]:
        self._validate()
        if self.backend is None or not hasattr(self.backend, "generate_candidates"):
            raise OptionalDependencyUnavailable(
                "MidiGPTCompositionAdapter requires a backend with "
                "generate_candidates(context, count)"
            )
        candidates = self.backend.generate_candidates(context, count=count)
        return tuple(_coerce_phrase(candidate, context.role) for candidate in candidates)

    def _validate(self) -> None:
        require_optional_dependency(self.runtime_module)
        if not self.model_path.exists():
            raise FileNotFoundError(f"composition model not found: {self.model_path}")


@dataclass(frozen=True, slots=True)
class MusicBertCriticAdapter(Critic):
    """Adapter for a local learned phrase critic backend."""

    model_path: Path
    runtime_module: str = "torch"
    backend: Any | None = None

    def score(self, phrase: Phrase, context: CompositionContext) -> float:
        self._validate()
        if self.backend is None or not hasattr(self.backend, "score"):
            raise OptionalDependencyUnavailable(
                "MusicBertCriticAdapter requires a backend with score(phrase, context)"
            )
        return float(self.backend.score(phrase, context))

    def _validate(self) -> None:
        require_optional_dependency(self.runtime_module)
        if not self.model_path.exists():
            raise FileNotFoundError(f"critic model not found: {self.model_path}")


@dataclass(frozen=True, slots=True)
class MidiRWKVVariationAdapter(LearnedVariationModel):
    """Adapter for learned symbolic variation on node-local prepared material."""

    model_path: Path
    runtime_module: str = "torch"
    backend: Any | None = None

    async def vary(self, phrase: Phrase, policy: VariationPolicy) -> Phrase:
        self._validate()
        if self.backend is None or not hasattr(self.backend, "vary"):
            raise OptionalDependencyUnavailable(
                "MidiRWKVVariationAdapter requires a backend with vary(phrase, policy)"
            )
        return _coerce_phrase(self.backend.vary(phrase, policy), phrase.role)

    def _validate(self) -> None:
        require_optional_dependency(self.runtime_module)
        if not self.model_path.exists():
            raise FileNotFoundError(f"variation model not found: {self.model_path}")


@dataclass(frozen=True, slots=True)
class OnnxVariationAdapter(LearnedVariationModel):
    """ONNX Runtime learned-variation adapter.

    This class keeps ONNX out of imports until the adapter is used. A project-specific encoder and
    decoder are required because the core package deliberately does not own model token semantics.
    """

    model_path: Path
    encoder: Any | None = None
    decoder: Any | None = None
    runtime_module: str = "onnxruntime"

    async def vary(self, phrase: Phrase, policy: VariationPolicy) -> Phrase:
        self._validate()
        if self.encoder is None or self.decoder is None:
            raise OptionalDependencyUnavailable(
                "OnnxVariationAdapter requires encoder and decoder callables"
            )
        import onnxruntime as ort  # type: ignore[import-not-found]

        session = ort.InferenceSession(str(self.model_path), providers=["CPUExecutionProvider"])
        inputs = self.encoder(phrase, policy)
        outputs = session.run(None, inputs)
        return _coerce_phrase(self.decoder(outputs, phrase, policy), phrase.role)

    def _validate(self) -> None:
        require_optional_dependency(self.runtime_module)
        if not self.model_path.exists():
            raise FileNotFoundError(f"ONNX variation model not found: {self.model_path}")


def _coerce_phrase(value: object, fallback_role: str) -> Phrase:
    if isinstance(value, Phrase):
        return value
    if not isinstance(value, dict):
        raise TypeError("model backend must return Phrase or phrase-compatible dict")
    raw_events = value.get("events", ())
    events = tuple(
        event
        if isinstance(event, MusicalEvent)
        else MusicalEvent(
            onset_tick=int(event["onset_tick"]),
            pitch=int(event["pitch"]),
            duration_ticks=int(event["duration_ticks"]),
            velocity=int(event.get("velocity", 96)),
            channel=int(event.get("channel", 0)),
        )
        for event in raw_events
    )
    return Phrase(
        phrase_id=str(value.get("phrase_id", "learned")),
        role=str(value.get("role", fallback_role)),
        events=tuple(sorted(events)),
        phrase_revision=int(value.get("phrase_revision", 1)),
        phrase_sequence=int(value.get("phrase_sequence", 0)),
        bars=int(value.get("bars", 1)),
        beats_per_bar=int(value.get("beats_per_bar", 4)),
        ticks_per_beat=int(value.get("ticks_per_beat", 24)),
    )
