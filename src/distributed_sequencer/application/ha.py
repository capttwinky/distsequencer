from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field

from distributed_sequencer.application.coordinator import Coordinator
from distributed_sequencer.domain.state import (
    Assignment,
    CompositionContext,
    NodeCapabilities,
    VariationPolicy,
)
from distributed_sequencer.infrastructure.consensus import RaftCluster, RaftLogEntry


def _as_int(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError("boolean is not a valid integer command field")
    if isinstance(value, int | str):
        return int(value)
    raise ValueError(f"expected integer-compatible command field, got {type(value).__name__}")


def _as_float(value: object) -> float:
    if isinstance(value, bool):
        raise ValueError("boolean is not a valid float command field")
    if isinstance(value, int | float | str):
        return float(value)
    raise ValueError(f"expected float-compatible command field, got {type(value).__name__}")


@dataclass(frozen=True, slots=True)
class CoordinatorCommand:
    command_id: str
    kind: str
    payload: dict[str, object]

    def encode(self) -> str:
        return json.dumps(asdict(self), sort_keys=True)

    @classmethod
    def decode(cls, raw: str) -> CoordinatorCommand:
        decoded = json.loads(raw)
        payload = decoded["payload"]
        if not isinstance(payload, dict):
            raise ValueError("coordinator command payload must be an object")
        return cls(
            command_id=str(decoded["command_id"]),
            kind=str(decoded["kind"]),
            payload=payload,
        )


@dataclass(slots=True)
class ConsensusBackedCoordinator:
    """Coordinator facade that commits mutations through Raft before applying them."""

    coordinator: Coordinator
    cluster: RaftCluster
    command_counter: int = 0
    applied_command_ids: set[str] = field(default_factory=set)

    @classmethod
    def create(
        cls, coordinator: Coordinator, member_ids: tuple[str, ...]
    ) -> ConsensusBackedCoordinator:
        cluster = RaftCluster.create(member_ids)
        cluster.elect(member_ids[0])
        return cls(coordinator=coordinator, cluster=cluster)

    async def register(self, capabilities: NodeCapabilities) -> RaftLogEntry:
        command = self._command(
            "register_node",
            {
                "node_id": capabilities.node_id,
                "roles": sorted(capabilities.roles),
                "max_polyphony": capabilities.max_polyphony,
                "learned_variation": capabilities.learned_variation,
            },
        )
        entry = self._commit(command)
        await self.apply_entry(entry)
        return entry

    async def start_transport(self) -> RaftLogEntry:
        entry = self._commit(self._command("start_transport", {}))
        await self.apply_entry(entry)
        return entry

    async def restart_transport(self) -> RaftLogEntry:
        entry = self._commit(self._command("restart_transport", {}))
        await self.apply_entry(entry)
        return entry

    async def compose_and_assign(
        self,
        context: CompositionContext,
        policy: VariationPolicy,
        *,
        node_id: str | None = None,
    ) -> Assignment:
        command = self._command(
            "compose_and_assign",
            {
                "role": context.role,
                "root_pitch": context.root_pitch,
                "desired_density": context.desired_density,
                "bars": context.bars,
                "beats_per_bar": context.beats_per_bar,
                "ticks_per_beat": context.ticks_per_beat,
                "policy": asdict(policy),
                "node_id": node_id,
            },
        )
        entry = self._commit(command)
        applied = await self.apply_entry(entry)
        assert isinstance(applied, Assignment)
        return applied

    def _commit(self, command: CoordinatorCommand) -> RaftLogEntry:
        return self.cluster.append_command(command.command_id, command.encode())

    async def apply_committed_entries(self, node_id: str) -> tuple[object | None, ...]:
        return tuple(
            [await self.apply_entry(entry) for entry in self.cluster.committed_entries(node_id)]
        )

    async def apply_entry(self, entry: RaftLogEntry) -> object | None:
        command = CoordinatorCommand.decode(entry.command)
        if command.command_id in self.applied_command_ids:
            return None
        if command.kind == "register_node":
            roles = command.payload["roles"]
            if not isinstance(roles, list):
                raise ValueError("roles must be a list")
            self.coordinator.register(
                NodeCapabilities(
                    node_id=str(command.payload["node_id"]),
                    roles=frozenset(str(role) for role in roles),
                    max_polyphony=_as_int(command.payload["max_polyphony"]),
                    learned_variation=bool(command.payload["learned_variation"]),
                )
            )
        elif command.kind == "start_transport":
            self.coordinator.start_transport()
        elif command.kind == "restart_transport":
            self.coordinator.restart_transport()
        elif command.kind == "compose_and_assign":
            policy = command.payload["policy"]
            if not isinstance(policy, dict):
                raise ValueError("policy must be an object")
            assignment = await self.coordinator.compose_and_assign(
                CompositionContext(
                    role=str(command.payload["role"]),
                    root_pitch=_as_int(command.payload["root_pitch"]),
                    desired_density=_as_float(command.payload["desired_density"]),
                    bars=_as_int(command.payload["bars"]),
                    beats_per_bar=_as_int(command.payload["beats_per_bar"]),
                    ticks_per_beat=_as_int(command.payload["ticks_per_beat"]),
                ),
                VariationPolicy(
                    policy_version=int(policy["policy_version"]),
                    timing_jitter_ticks=int(policy["timing_jitter_ticks"]),
                    velocity_jitter=int(policy["velocity_jitter"]),
                    omission_probability=float(policy["omission_probability"]),
                    pitch_shift_semitones=int(policy["pitch_shift_semitones"]),
                    rhythmic_freedom=float(policy["rhythmic_freedom"]),
                    pitch_freedom=float(policy["pitch_freedom"]),
                    density_variance=float(policy["density_variance"]),
                    fill_probability=float(policy["fill_probability"]),
                ),
                node_id=(
                    None if command.payload["node_id"] is None else str(command.payload["node_id"])
                ),
            )
            self._mark_applied(entry)
            return assignment
        else:
            raise ValueError(f"unsupported coordinator command: {command.kind}")
        self._mark_applied(entry)
        return None

    def _mark_applied(self, entry: RaftLogEntry) -> None:
        command = CoordinatorCommand.decode(entry.command)
        self.applied_command_ids.add(command.command_id)

    def _command(self, kind: str, payload: dict[str, object]) -> CoordinatorCommand:
        self.command_counter += 1
        return CoordinatorCommand(
            command_id=f"{self.coordinator.transport_epoch}:{kind}:{self.command_counter}",
            kind=kind,
            payload=payload,
        )
