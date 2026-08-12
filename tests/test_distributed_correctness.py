from __future__ import annotations

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
from distributed_sequencer.domain.music import MusicalEvent, Phrase
from distributed_sequencer.domain.state import (
    Assignment,
    AuthoritativeSnapshot,
    CompositionContext,
    NodeCapabilities,
    PartLease,
    TransportState,
    VariationPolicy,
)
from distributed_sequencer.infrastructure.clock import AdvancingClock, ClockSynchronizer
from distributed_sequencer.infrastructure.messaging import (
    BackpressureError,
    InMemoryBus,
    JsonMessageCodec,
    MessageEnvelope,
    QueueOverflow,
)


def make_node(node_id: str = "node-1") -> SequencerNode:
    return SequencerNode(
        NodeCapabilities(node_id, frozenset({"bass"})),
        InMemoryBus(),
        VariationEngine(seed=4),
        Scheduler(RecordingSynth(), AdvancingClock()),
    )


def make_assignment(
    *,
    generation: int = 1,
    epoch: int = 1,
    node_id: str = "node-1",
    message_id: str | None = None,
    valid_from_bar: int = 0,
    valid_through_bar: int = 4,
) -> Assignment:
    phrase = Phrase(
        "canonical",
        "bass",
        (MusicalEvent(0, 36, 24),),
        phrase_sequence=generation,
    )
    lease = PartLease(
        transport_epoch=epoch,
        part_id="bass",
        node_id=node_id,
        assignment_generation=generation,
        valid_from_bar=valid_from_bar,
        valid_through_bar=valid_through_bar,
    )
    return Assignment(
        node_id,
        phrase,
        VariationPolicy(),
        generation,
        transport_epoch=epoch,
        part_id="bass",
        lease=lease,
        message_id=message_id or f"assignment:{epoch}:bass:{generation}",
    )


@pytest.mark.asyncio
async def test_duplicate_assignment_does_not_duplicate_buffered_phrase() -> None:
    node = make_node()
    assignment = make_assignment()

    assert await node.accept_assignment(assignment) is not None
    assert await node.accept_assignment(assignment) is None

    assert node.phrase_buffer.qsize() == 1
    assert node.observed.duplicate_message_drops == 1


@pytest.mark.asyncio
async def test_stale_epoch_assignment_is_rejected() -> None:
    node = make_node()
    await node.reconcile(
        AuthoritativeSnapshot(
            transport_epoch=2,
            transport_state=TransportState.PLAYING,
            tempo_bpm=120.0,
            current_bar=0,
        )
    )

    assert await node.accept_assignment(make_assignment(epoch=1)) is None
    assert node.last_assignment is None
    assert node.observed.stale_message_drops == 1


@pytest.mark.asyncio
async def test_stale_generation_cannot_supersede_newer_assignment() -> None:
    node = make_node()

    assert await node.accept_assignment(make_assignment(generation=2)) is not None
    assert await node.accept_assignment(make_assignment(generation=1, message_id="late")) is None

    assert node.last_assignment is not None
    assert node.last_assignment.generation == 2


@pytest.mark.asyncio
async def test_lease_expiration_stops_autonomous_replay() -> None:
    node = make_node()
    await node.accept_assignment(make_assignment(valid_from_bar=1, valid_through_bar=2))

    await node.replay_locally(current_bar=2)
    with pytest.raises(RuntimeError, match="lease expired"):
        await node.replay_locally(current_bar=3)


@pytest.mark.asyncio
async def test_coordinator_does_not_reassign_before_exclusive_lease_expires() -> None:
    coordinator = Coordinator(
        composition=CompositionEngine(ProceduralCompositionModel(), critics=(DensityCritic(),)),
        bus=InMemoryBus(),
        lease_bars=4,
    )
    coordinator.register(NodeCapabilities("node-a", frozenset({"bass"})))
    coordinator.register(NodeCapabilities("node-b", frozenset({"bass"})))

    first = await coordinator.compose_and_assign(
        CompositionContext("bass", 36, 1.0),
        VariationPolicy(),
        node_id="node-a",
    )
    with pytest.raises(RuntimeError, match="before lease expiration"):
        await coordinator.compose_and_assign(
            CompositionContext("bass", 36, 1.0),
            VariationPolicy(),
            node_id="node-b",
        )

    coordinator.mark_current_bar(first.valid_through_bar + 1)
    second = await coordinator.compose_and_assign(
        CompositionContext("bass", 36, 1.0),
        VariationPolicy(),
        node_id="node-b",
    )
    assert second.node_id == "node-b"
    assert second.generation == first.generation + 1


@pytest.mark.asyncio
async def test_snapshot_reconciliation_adopts_new_epoch_and_current_assignment() -> None:
    node = make_node()
    old = make_assignment(epoch=1)
    new = make_assignment(epoch=2, generation=1, message_id="new-epoch")
    await node.accept_assignment(old)

    await node.reconcile(
        AuthoritativeSnapshot(
            transport_epoch=2,
            transport_state=TransportState.PLAYING,
            tempo_bpm=120.0,
            current_bar=8,
            assignments=(new,),
        )
    )

    assert node.observed.transport_epoch == 2
    assert node.last_assignment == new
    assert await node.accept_assignment(old) is None


@pytest.mark.asyncio
async def test_bounded_bus_backpressure_and_replace_oldest() -> None:
    bus = InMemoryBus(default_maxsize=1)
    queue = bus.subscribe("critical")
    await bus.publish("critical", "one")
    with pytest.raises(BackpressureError):
        await bus.publish("critical", "two")

    replace_queue = bus.subscribe(
        "snapshot",
        maxsize=1,
        overflow=QueueOverflow.REPLACE_OLDEST,
    )
    await bus.publish("snapshot", "old")
    await bus.publish("snapshot", "new")
    assert await replace_queue.get() == "new"
    assert await queue.get() == "one"


def test_json_codec_authenticates_and_bounds_messages() -> None:
    codec = JsonMessageCodec(secrets_by_sender={"coordinator": b"secret"}, max_bytes=512)
    envelope = MessageEnvelope(
        schema_version=1,
        message_id="m-1",
        sender_id="coordinator",
        kind="snapshot",
        payload={"transport_epoch": 1},
    )

    raw = codec.encode(envelope)
    assert codec.decode(raw).payload == {"transport_epoch": 1}
    with pytest.raises(ValueError, match="authentication failed"):
        codec.decode(raw.replace(b"snapshot", b"tampered"))


def test_clock_sync_health_degrades_with_skew_uncertainty() -> None:
    sync = ClockSynchronizer()
    assert not sync.is_healthy()

    sync.update(
        estimated_offset_seconds=0.002,
        estimated_drift_ppm=20.0,
        uncertainty_seconds=0.010,
    )
    assert sync.is_healthy()

    sync.update(
        estimated_offset_seconds=0.500,
        estimated_drift_ppm=900.0,
        uncertainty_seconds=0.250,
    )
    assert not sync.is_healthy()
