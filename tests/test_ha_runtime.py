from __future__ import annotations

from pathlib import Path

import pytest

from distributed_sequencer.application.composition import (
    CompositionEngine,
    DensityCritic,
    ProceduralCompositionModel,
)
from distributed_sequencer.application.coordinator import Coordinator
from distributed_sequencer.application.ha import (
    ConsensusCoordinatorService,
    ConsensusCoordinatorSettings,
    NotLeaderError,
)
from distributed_sequencer.domain.state import (
    CompositionContext,
    NodeCapabilities,
    TransportState,
    VariationPolicy,
)
from distributed_sequencer.infrastructure.messaging import InMemoryBus


def make_coordinator() -> Coordinator:
    return Coordinator(
        composition=CompositionEngine(ProceduralCompositionModel(), critics=(DensityCritic(),)),
        bus=InMemoryBus(),
    )


def make_settings(
    tmp_path: Path,
    *,
    local_member_id: str,
    bootstrap_leader_id: str | None = "r1",
) -> ConsensusCoordinatorSettings:
    return ConsensusCoordinatorSettings.from_member_ids(
        ("r1", "r2", "r3"),
        local_member_id=local_member_id,
        storage_dir=tmp_path / "raft",
        bootstrap_leader_id=bootstrap_leader_id,
    )


@pytest.mark.asyncio
async def test_ha_service_elects_leader_and_commits_only_with_quorum(tmp_path: Path) -> None:
    coordinator = make_coordinator()
    service = ConsensusCoordinatorService.create(
        coordinator,
        make_settings(tmp_path, local_member_id="r1", bootstrap_leader_id=None),
    )

    service.elect_leader("r1", available_members={"r1", "r2"})
    entry = await service.register(
        NodeCapabilities("node-a", frozenset({"bass"})),
        available_members={"r1", "r2"},
    )

    assert entry.index == 1
    assert coordinator.nodes["node-a"].roles == frozenset({"bass"})
    assert service.ha.cluster.nodes["r1"].commit_index == 1
    assert service.ha.cluster.nodes["r2"].commit_index == 1

    with pytest.raises(RuntimeError, match="quorum"):
        await service.start_transport(available_members={"r1"})
    assert coordinator.transport_state is TransportState.STOPPED


@pytest.mark.asyncio
async def test_ha_service_rejects_follower_writes_before_raft_append(tmp_path: Path) -> None:
    coordinator = make_coordinator()
    follower = ConsensusCoordinatorService.create(
        coordinator,
        make_settings(tmp_path, local_member_id="r2", bootstrap_leader_id="r1"),
    )

    with pytest.raises(NotLeaderError, match="not leader"):
        await follower.register(NodeCapabilities("node-a", frozenset({"bass"})))

    assert coordinator.nodes == {}
    assert all(node.last_log_index == 0 for node in follower.ha.cluster.nodes.values())


@pytest.mark.asyncio
async def test_ha_service_replays_persisted_committed_entries_after_restart(
    tmp_path: Path,
) -> None:
    original = make_coordinator()
    leader = ConsensusCoordinatorService.create(
        original,
        make_settings(tmp_path, local_member_id="r1", bootstrap_leader_id="r1"),
    )
    await leader.register(
        NodeCapabilities("node-a", frozenset({"bass"})),
        available_members={"r1", "r2"},
    )
    assignment = await leader.compose_and_assign(
        CompositionContext("bass", 36, 1.0),
        VariationPolicy(),
        node_id="node-a",
        available_members={"r1", "r2"},
    )

    restored = make_coordinator()
    restarted = ConsensusCoordinatorService.create(
        restored,
        make_settings(tmp_path, local_member_id="r1", bootstrap_leader_id=None),
    )

    applied = await restarted.replay_committed()

    assert len([result for result in applied if result is not None]) == 1
    assert restored.nodes["node-a"].roles == frozenset({"bass"})
    assert restored.desired_assignments["bass"].assignment_id == assignment.assignment_id
    assert restarted.ha.command_counter == 2

    restarted.elect_leader("r1", available_members={"r1", "r2"})
    await restarted.start_transport(available_members={"r1", "r2"})
    assert restored.transport_state is TransportState.PLAYING


@pytest.mark.asyncio
async def test_ha_service_mutates_coordinator_state_through_committed_wrapper(
    tmp_path: Path,
) -> None:
    coordinator = make_coordinator()
    service = ConsensusCoordinatorService.create(
        coordinator,
        make_settings(tmp_path, local_member_id="r1", bootstrap_leader_id="r1"),
    )

    await service.register(NodeCapabilities("node-a", frozenset({"lead"})))
    await service.start_transport()
    assignment = await service.compose_and_assign(
        CompositionContext("lead", 60, 1.0),
        VariationPolicy(timing_jitter_ticks=1),
        node_id="node-a",
    )
    await service.restart_transport()

    assert assignment.part_id == "lead"
    assert coordinator.transport_epoch == 2
    assert coordinator.transport_state is TransportState.STOPPED
    assert coordinator.desired_assignments == {}
