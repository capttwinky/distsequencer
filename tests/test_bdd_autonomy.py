from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest
from pytest_bdd import given, scenario, then, when

from distributed_sequencer.adapters.synth import RecordingSynth
from distributed_sequencer.application.node import SequencerNode
from distributed_sequencer.application.scheduler import Scheduler
from distributed_sequencer.application.variation import VariationEngine
from distributed_sequencer.domain.music import MusicalEvent, Phrase
from distributed_sequencer.domain.state import Assignment, NodeCapabilities, VariationPolicy
from distributed_sequencer.infrastructure.clock import AdvancingClock
from distributed_sequencer.infrastructure.messaging import InMemoryBus


@scenario("features/autonomy.feature", "Node reuses its last canonical phrase")
def test_autonomous_replay() -> None:
    pass


@dataclass
class World:
    node: SequencerNode
    bus: InMemoryBus
    offline_phrase_id: str | None = None


@pytest.fixture
def world() -> World:
    bus = InMemoryBus()
    node = SequencerNode(
        NodeCapabilities("node-bdd", frozenset({"bass"})),
        bus,
        VariationEngine(seed=9),
        Scheduler(RecordingSynth(), AdvancingClock()),
    )
    return World(node=node, bus=bus)


@given("a bass node has received a canonical phrase")
def cached_phrase(world: World) -> None:
    phrase = Phrase("canonical", "bass", (MusicalEvent(0, 36, 24),))

    async def deliver() -> None:
        receive = asyncio.create_task(world.node.receive_once())
        await asyncio.sleep(0)
        await world.bus.publish(
            world.node.assignment_topic,
            Assignment(world.node.capabilities.node_id, phrase, VariationPolicy(), 1),
        )
        await receive

    asyncio.run(deliver())


@when("the coordinator becomes unavailable")
def coordinator_unavailable(world: World) -> None:
    async def prepare() -> None:
        phrase = await world.node.replay_locally()
        world.offline_phrase_id = phrase.phrase_id

    asyncio.run(prepare())


@then("the node can prepare another local variation")
def local_variation_exists(world: World) -> None:
    assert world.offline_phrase_id is not None
    assert world.offline_phrase_id.startswith("canonical-v")
