from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

from distributed_sequencer.application.coordinator import Coordinator
from distributed_sequencer.domain.state import (
    Assignment,
    CompositionContext,
    NodeCapabilities,
    VariationPolicy,
)
from distributed_sequencer.infrastructure.consensus import (
    JsonRaftStorage,
    RaftCluster,
    RaftLogEntry,
    RaftStorage,
)


class NotLeaderError(RuntimeError):
    """Raised when a follower receives a leader-only coordinator mutation."""


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


@dataclass(frozen=True, slots=True)
class ConsensusMemberSettings:
    member_id: str
    storage_path: Path | None = None


@dataclass(frozen=True, slots=True)
class ConsensusCoordinatorSettings:
    local_member_id: str
    members: tuple[ConsensusMemberSettings, ...]
    bootstrap_leader_id: str | None = None

    @classmethod
    def from_member_ids(
        cls,
        member_ids: tuple[str, ...],
        *,
        local_member_id: str,
        storage_dir: Path | None = None,
        bootstrap_leader_id: str | None = None,
    ) -> ConsensusCoordinatorSettings:
        members = tuple(
            ConsensusMemberSettings(
                member_id=member_id,
                storage_path=None if storage_dir is None else storage_dir / f"{member_id}.json",
            )
            for member_id in member_ids
        )
        return cls(
            local_member_id=local_member_id,
            members=members,
            bootstrap_leader_id=bootstrap_leader_id,
        )

    @property
    def member_ids(self) -> tuple[str, ...]:
        return tuple(member.member_id for member in self.members)

    def storages(self) -> dict[str, RaftStorage]:
        return {
            member.member_id: JsonRaftStorage(member.storage_path)
            for member in self.members
            if member.storage_path is not None
        }


@dataclass(slots=True)
class ConsensusBackedCoordinator:
    """Coordinator facade that commits mutations through Raft before applying them."""

    coordinator: Coordinator
    cluster: RaftCluster
    command_counter: int = 0
    applied_command_ids: set[str] = field(default_factory=set)

    @classmethod
    def create(
        cls,
        coordinator: Coordinator,
        member_ids: tuple[str, ...],
        *,
        leader_id: str | None = None,
    ) -> ConsensusBackedCoordinator:
        cluster = RaftCluster.create(member_ids)
        cluster.elect(leader_id or member_ids[0])
        return cls(coordinator=coordinator, cluster=cluster)

    async def register(
        self,
        capabilities: NodeCapabilities,
        *,
        member_id: str | None = None,
        available_members: set[str] | None = None,
    ) -> RaftLogEntry:
        command = self._command(
            "register_node",
            {
                "node_id": capabilities.node_id,
                "roles": sorted(capabilities.roles),
                "max_polyphony": capabilities.max_polyphony,
                "learned_variation": capabilities.learned_variation,
            },
        )
        entry = self._commit(command, member_id=member_id, available_members=available_members)
        await self.apply_entry(entry)
        return entry

    async def start_transport(
        self,
        *,
        member_id: str | None = None,
        available_members: set[str] | None = None,
    ) -> RaftLogEntry:
        entry = self._commit(
            self._command("start_transport", {}),
            member_id=member_id,
            available_members=available_members,
        )
        await self.apply_entry(entry)
        return entry

    async def restart_transport(
        self,
        *,
        member_id: str | None = None,
        available_members: set[str] | None = None,
    ) -> RaftLogEntry:
        entry = self._commit(
            self._command("restart_transport", {}),
            member_id=member_id,
            available_members=available_members,
        )
        await self.apply_entry(entry)
        return entry

    async def compose_and_assign(
        self,
        context: CompositionContext,
        policy: VariationPolicy,
        *,
        node_id: str | None = None,
        member_id: str | None = None,
        available_members: set[str] | None = None,
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
        entry = self._commit(command, member_id=member_id, available_members=available_members)
        applied = await self.apply_entry(entry)
        assert isinstance(applied, Assignment)
        return applied

    def _commit(
        self,
        command: CoordinatorCommand,
        *,
        member_id: str | None = None,
        available_members: set[str] | None = None,
    ) -> RaftLogEntry:
        if member_id is None:
            return self.cluster.append_command(
                command.command_id,
                command.encode(),
                available_members=available_members,
            )
        return self.cluster.append_command_from(
            member_id,
            command.command_id,
            command.encode(),
            available_members=available_members,
        )

    async def apply_committed_entries(self, node_id: str) -> tuple[object | None, ...]:
        return tuple(
            [await self.apply_entry(entry) for entry in self.cluster.committed_entries(node_id)]
        )

    async def apply_entry(self, entry: RaftLogEntry) -> object | None:
        command = CoordinatorCommand.decode(entry.command)
        self._observe_command_counter(command.command_id)
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

    def _observe_command_counter(self, command_id: str) -> None:
        try:
            counter = int(command_id.rsplit(":", maxsplit=1)[1])
        except (IndexError, ValueError):
            return
        self.command_counter = max(self.command_counter, counter)


@dataclass(slots=True)
class ConsensusCoordinatorService:
    """Operational HA facade for one local coordinator member."""

    local_member_id: str
    ha: ConsensusBackedCoordinator

    @classmethod
    def create(
        cls,
        coordinator: Coordinator,
        settings: ConsensusCoordinatorSettings,
    ) -> ConsensusCoordinatorService:
        if settings.local_member_id not in settings.member_ids:
            raise ValueError("local member must be part of the consensus cluster")
        cluster = RaftCluster.create(settings.member_ids, storages=settings.storages())
        cluster.recover_commit_index()
        if settings.bootstrap_leader_id is not None:
            cluster.elect(settings.bootstrap_leader_id)
        return cls(
            local_member_id=settings.local_member_id,
            ha=ConsensusBackedCoordinator(coordinator=coordinator, cluster=cluster),
        )

    @property
    def leader_id(self) -> str | None:
        return self.ha.cluster.leader_id

    def elect_leader(
        self,
        candidate_id: str | None = None,
        *,
        available_members: set[str] | None = None,
    ) -> None:
        self.ha.cluster.elect(
            candidate_id or self.local_member_id,
            available_members=available_members,
        )

    async def replay_committed(self) -> tuple[object | None, ...]:
        return await self.ha.apply_committed_entries(self.local_member_id)

    async def register(
        self,
        capabilities: NodeCapabilities,
        *,
        available_members: set[str] | None = None,
    ) -> RaftLogEntry:
        self._require_local_leader()
        return await self.ha.register(
            capabilities,
            member_id=self.local_member_id,
            available_members=available_members,
        )

    async def start_transport(
        self,
        *,
        available_members: set[str] | None = None,
    ) -> RaftLogEntry:
        self._require_local_leader()
        return await self.ha.start_transport(
            member_id=self.local_member_id,
            available_members=available_members,
        )

    async def restart_transport(
        self,
        *,
        available_members: set[str] | None = None,
    ) -> RaftLogEntry:
        self._require_local_leader()
        return await self.ha.restart_transport(
            member_id=self.local_member_id,
            available_members=available_members,
        )

    async def compose_and_assign(
        self,
        context: CompositionContext,
        policy: VariationPolicy,
        *,
        node_id: str | None = None,
        available_members: set[str] | None = None,
    ) -> Assignment:
        self._require_local_leader()
        return await self.ha.compose_and_assign(
            context,
            policy,
            node_id=node_id,
            member_id=self.local_member_id,
            available_members=available_members,
        )

    def _require_local_leader(self) -> None:
        if self.leader_id != self.local_member_id:
            raise NotLeaderError(
                f"member {self.local_member_id!r} is not leader; "
                f"current leader is {self.leader_id!r}"
            )
