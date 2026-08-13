from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


@dataclass(frozen=True, slots=True)
class Endpoint:
    scheme: str
    host: str
    port: int

    @classmethod
    def parse(cls, raw: str, *, expected_scheme: str | None = None) -> Endpoint:
        parsed = urlparse(raw)
        if not parsed.scheme or parsed.hostname is None or parsed.port is None:
            raise ValueError(f"endpoint must include scheme, host, and port: {raw!r}")
        if expected_scheme is not None and parsed.scheme != expected_scheme:
            raise ValueError(f"expected {expected_scheme!r} endpoint, got {parsed.scheme!r}")
        return cls(scheme=parsed.scheme, host=parsed.hostname, port=parsed.port)


@dataclass(frozen=True, slots=True)
class TransportRuntimeConfig:
    mode: str = "tcp"
    insecure_dev_mode: bool = False
    ca_cert: Path | None = None
    cert: Path | None = None
    key: Path | None = None
    max_message_bytes: int = 64_000

    def __post_init__(self) -> None:
        if self.mode != "tcp":
            raise ValueError("only tcp transport mode is supported")
        if self.max_message_bytes <= 0:
            raise ValueError("max_message_bytes must be positive")
        if not self.insecure_dev_mode and self.ca_cert is None:
            raise ValueError("secure transport requires ca_cert or insecure_dev_mode=true")
        if (self.cert is None) != (self.key is None):
            raise ValueError("cert and key must be provided together")


@dataclass(frozen=True, slots=True)
class HaRuntimeConfig:
    enabled: bool = False
    local_member_id: str | None = None
    member_ids: tuple[str, ...] = ()
    storage_dir: Path | None = None
    bootstrap_leader_id: str | None = None

    def __post_init__(self) -> None:
        if not self.enabled:
            return
        if self.local_member_id is None:
            raise ValueError("ha.local_member_id is required when HA is enabled")
        if not self.member_ids:
            raise ValueError("ha.member_ids is required when HA is enabled")
        if self.local_member_id not in self.member_ids:
            raise ValueError("ha.local_member_id must be listed in ha.member_ids")


@dataclass(frozen=True, slots=True)
class CoordinatorRuntimeConfig:
    listen: Endpoint
    tempo_bpm: float = 120.0
    lease_bars: int = 16
    parts: tuple[str, ...] = ("bass", "lead")
    transport: TransportRuntimeConfig = TransportRuntimeConfig(insecure_dev_mode=True)
    ha: HaRuntimeConfig = HaRuntimeConfig()

    def __post_init__(self) -> None:
        if self.listen.scheme != "tcp":
            raise ValueError("coordinator.listen must be a tcp endpoint")
        if self.tempo_bpm <= 0:
            raise ValueError("coordinator.tempo_bpm must be positive")
        if self.lease_bars <= 0:
            raise ValueError("coordinator.lease_bars must be positive")
        if not self.parts:
            raise ValueError("coordinator.parts must not be empty")


@dataclass(frozen=True, slots=True)
class SynthRuntimeConfig:
    backend: str = "recording"
    osc_host: str = "127.0.0.1"
    osc_port: int = 9000

    def __post_init__(self) -> None:
        if self.backend not in {"recording", "osc"}:
            raise ValueError("synth.backend must be recording or osc")
        if self.osc_port <= 0:
            raise ValueError("synth.osc_port must be positive")


@dataclass(frozen=True, slots=True)
class NodeRuntimeConfig:
    node_id: str
    parts: tuple[str, ...]
    coordinator_url: Endpoint
    buffer_depth_bars: int = 8
    max_polyphony: int = 8
    learned_variation: bool = False
    transport: TransportRuntimeConfig = TransportRuntimeConfig(insecure_dev_mode=True)
    synth: SynthRuntimeConfig = SynthRuntimeConfig()

    def __post_init__(self) -> None:
        if not self.node_id:
            raise ValueError("node.id is required")
        if not self.parts:
            raise ValueError("node.parts must not be empty")
        if self.coordinator_url.scheme != "tcp":
            raise ValueError("node.coordinator_url must be a tcp endpoint")
        if self.buffer_depth_bars <= 0:
            raise ValueError("node.buffer_depth_bars must be positive")
        if self.max_polyphony <= 0:
            raise ValueError("node.max_polyphony must be positive")


def load_coordinator_config(path: Path) -> CoordinatorRuntimeConfig:
    data = _load_toml(path)
    coordinator = _section(data, "coordinator")
    return CoordinatorRuntimeConfig(
        listen=Endpoint.parse(_str(coordinator, "listen"), expected_scheme="tcp"),
        tempo_bpm=_float(coordinator, "tempo_bpm", 120.0),
        lease_bars=_int(coordinator, "lease_bars", 16),
        parts=_str_tuple(coordinator, "parts", ("bass", "lead")),
        transport=_transport_config(data),
        ha=_ha_config(data),
    )


def load_node_config(path: Path) -> NodeRuntimeConfig:
    data = _load_toml(path)
    node = _section(data, "node")
    return NodeRuntimeConfig(
        node_id=_str(node, "id"),
        parts=_str_tuple(node, "parts", ()),
        coordinator_url=Endpoint.parse(_str(node, "coordinator_url"), expected_scheme="tcp"),
        buffer_depth_bars=_int(node, "buffer_depth_bars", 8),
        max_polyphony=_int(node, "max_polyphony", 8),
        learned_variation=_bool(node, "learned_variation", False),
        transport=_transport_config(data),
        synth=_synth_config(data),
    )


def _load_toml(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        loaded = tomllib.load(handle)
    return dict(loaded)


def _section(data: dict[str, Any], name: str) -> dict[str, Any]:
    section = data.get(name)
    if not isinstance(section, dict):
        raise ValueError(f"[{name}] section is required")
    return section


def _optional_section(data: dict[str, Any], name: str) -> dict[str, Any]:
    section = data.get(name, {})
    if not isinstance(section, dict):
        raise ValueError(f"[{name}] section must be an object")
    return section


def _str(section: dict[str, Any], name: str, default: str | None = None) -> str:
    value = section.get(name, default)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _int(section: dict[str, Any], name: str, default: int) -> int:
    value = section.get(name, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _float(section: dict[str, Any], name: str, default: float) -> float:
    value = section.get(name, default)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{name} must be a number")
    return float(value)


def _bool(section: dict[str, Any], name: str, default: bool) -> bool:
    value = section.get(name, default)
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value


def _path(section: dict[str, Any], name: str) -> Path | None:
    value = section.get(name)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty path string")
    return Path(value)


def _str_tuple(section: dict[str, Any], name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    value = section.get(name, list(default))
    if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
        raise ValueError(f"{name} must be a list of non-empty strings")
    return tuple(value)


def _transport_config(data: dict[str, Any]) -> TransportRuntimeConfig:
    transport = _optional_section(data, "transport")
    return TransportRuntimeConfig(
        mode=_str(transport, "mode", "tcp"),
        insecure_dev_mode=_bool(transport, "insecure_dev_mode", False),
        ca_cert=_path(transport, "ca_cert"),
        cert=_path(transport, "cert"),
        key=_path(transport, "key"),
        max_message_bytes=_int(transport, "max_message_bytes", 64_000),
    )


def _ha_config(data: dict[str, Any]) -> HaRuntimeConfig:
    ha = _optional_section(data, "ha")
    return HaRuntimeConfig(
        enabled=_bool(ha, "enabled", False),
        local_member_id=(
            None if ha.get("local_member_id") is None else _str(ha, "local_member_id")
        ),
        member_ids=_str_tuple(ha, "member_ids", ()),
        storage_dir=_path(ha, "storage_dir"),
        bootstrap_leader_id=(
            None if ha.get("bootstrap_leader_id") is None else _str(ha, "bootstrap_leader_id")
        ),
    )


def _synth_config(data: dict[str, Any]) -> SynthRuntimeConfig:
    synth = _optional_section(data, "synth")
    return SynthRuntimeConfig(
        backend=_str(synth, "backend", "recording"),
        osc_host=_str(synth, "osc_host", "127.0.0.1"),
        osc_port=_int(synth, "osc_port", 9000),
    )
