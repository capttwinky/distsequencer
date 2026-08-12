from __future__ import annotations

import importlib.util
from dataclasses import dataclass


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
