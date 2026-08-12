import asyncio

import pytest

from distributed_sequencer.adapters.synth import RecordingSynth
from distributed_sequencer.application.composition import (
    CompositionEngine,
    DensityCritic,
    ProceduralCompositionModel,
)
from distributed_sequencer.application.coordinator import Coordinator
from distributed_sequencer.application.node import SequencerNode
from distributed_sequencer.application.scheduler import Scheduler
from distributed_sequencer.application.variation import VariationEngine
from distributed_sequencer.domain.state import CompositionContext, NodeCapabilities, VariationPolicy
from distributed_sequencer.infrastructure.clock import AdvancingClock
from distributed_sequencer.infrastructure.messaging import InMemoryBus


@pytest.mark.asyncio
async def test_node_receives_assignment_and_can_continue_offline() -> None:
    bus = InMemoryBus()
    coordinator = Coordinator(
        composition=CompositionEngine(ProceduralCompositionModel(), critics=(DensityCritic(),)),
        bus=bus,
    )
    node = SequencerNode(
        NodeCapabilities("node-1", frozenset({"bass"})),
        bus,
        VariationEngine(seed=4),
        Scheduler(RecordingSynth(), AdvancingClock()),
    )
    coordinator.register(node.capabilities)

    receive = asyncio.create_task(node.receive_once())
    await asyncio.sleep(0)
    assignment = await coordinator.compose_and_assign(
        CompositionContext("bass", 36, 1.0), VariationPolicy(velocity_jitter=2)
    )
    prepared = await receive

    assert assignment.node_id == "node-1"
    assert prepared.role == "bass"
    offline = await node.replay_locally()
    assert offline.phrase_id != prepared.phrase_id
    assert node.phrase_buffer.qsize() == 2
