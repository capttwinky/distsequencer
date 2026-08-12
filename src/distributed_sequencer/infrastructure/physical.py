from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

from distributed_sequencer.application.benchmark import HardwareProfile


@dataclass(frozen=True, slots=True)
class PhysicalNodeDeployment:
    profile: HardwareProfile
    coordinator_url: str
    osc_host: str = "127.0.0.1"
    osc_port: int = 9000
    mtls_enabled: bool = True

    def validate(self) -> None:
        if self.profile.memory_mb < 512:
            raise ValueError("node memory must be at least 512 MB")
        if not self.coordinator_url:
            raise ValueError("coordinator_url is required")
        if self.osc_host != "127.0.0.1" and not self.mtls_enabled:
            raise ValueError("remote control requires mTLS enabled")


@dataclass(slots=True)
class DeploymentManifest:
    nodes: tuple[PhysicalNodeDeployment, ...]

    def validate(self) -> None:
        node_ids = [node.profile.node_id for node in self.nodes]
        if len(node_ids) != len(set(node_ids)):
            raise ValueError("node IDs must be unique")
        for node in self.nodes:
            node.validate()

    def write_json(self, path: Path) -> None:
        self.validate()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(asdict(self), indent=2, sort_keys=True),
            encoding="utf-8",
        )
