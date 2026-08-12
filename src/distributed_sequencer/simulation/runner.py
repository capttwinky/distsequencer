from __future__ import annotations

import asyncio

from distributed_sequencer.adapters.synth import RecordingSynth
from distributed_sequencer.application.composition import (
    CompositionEngine,
    DensityCritic,
    ProceduralCompositionModel,
    RegisterCritic,
)
from distributed_sequencer.application.coordinator import Coordinator
from distributed_sequencer.application.node import SequencerNode
from distributed_sequencer.application.scheduler import Scheduler
from distributed_sequencer.application.variation import VariationEngine
from distributed_sequencer.domain.state import CompositionContext, NodeCapabilities, VariationPolicy
from distributed_sequencer.infrastructure.clock import AsyncioClock
from distributed_sequencer.infrastructure.messaging import InMemoryBus


async def run_simulation(
    *,
    bpm: float = 720.0,
    nodes: int = 2,
    bars: int = 24,
    seed: int = 2026,
    packet_loss: float = 0.0,
    latency: float = 0.0,
    clock_skew: float = 0.0,
) -> None:
    del nodes, packet_loss, latency, clock_skew
    bus = InMemoryBus()
    composition = CompositionEngine(
        ProceduralCompositionModel(seed=seed),
        critics=(DensityCritic(), RegisterCritic()),
    )
    coordinator = Coordinator(composition=composition, bus=bus, tempo_bpm=bpm)

    bass_synth = RecordingSynth(echo=True, label="bass-node")
    lead_synth = RecordingSynth(echo=True, label="lead-node")
    bass = SequencerNode(
        capabilities=NodeCapabilities("pi-bass", frozenset({"bass"})),
        bus=bus,
        variation=VariationEngine(seed=10),
        scheduler=Scheduler(bass_synth, AsyncioClock(), bpm=bpm),
    )
    lead = SequencerNode(
        capabilities=NodeCapabilities("pi-lead", frozenset({"lead", "bass"})),
        bus=bus,
        variation=VariationEngine(seed=20),
        scheduler=Scheduler(lead_synth, AsyncioClock(), bpm=bpm),
    )
    for node in (bass, lead):
        coordinator.register(node.capabilities)

    print(
        f"coordinator epoch={coordinator.transport_epoch} state={coordinator.transport_state.value}"
    )
    coordinator.start_transport()
    print(f"transport started epoch={coordinator.transport_epoch} bpm={bpm} bars={bars}")

    # Subscribers must be established before the coordinator publishes.
    receive_tasks = [asyncio.create_task(node.receive_once()) for node in (bass, lead)]
    await asyncio.sleep(0)

    bass_assignment = await coordinator.compose_and_assign(
        CompositionContext("bass", root_pitch=36, desired_density=0.75),
        VariationPolicy(velocity_jitter=5, omission_probability=0.05),
    )
    lead_assignment = await coordinator.compose_and_assign(
        CompositionContext("lead", root_pitch=60, desired_density=1.0),
        VariationPolicy(timing_jitter_ticks=1, velocity_jitter=8, pitch_shift_semitones=0),
    )
    await asyncio.gather(*receive_tasks)
    for assignment in (bass_assignment, lead_assignment):
        print(
            "assigned "
            f"part={assignment.part_id} node={assignment.node_id} "
            f"generation={assignment.generation} lease={assignment.valid_from_bar}-"
            f"{assignment.valid_through_bar} phrase_seq={assignment.phrase.phrase_sequence}"
        )
    for ready in (*bass.ready_reports, *lead.ready_reports):
        coordinator.note_ready(ready)
        print(
            "ready "
            f"node={ready.node_id} part={ready.part_id} "
            f"generation={ready.assignment_generation} through_bar={ready.ready_through_bar}"
        )

    print("\n--- prepared phrases; realtime schedulers now consume only buffered events ---")
    await asyncio.gather(bass.play_once(), lead.play_once())

    print("\n--- partition: bass node continues only within its current lease ---")
    await bass.replay_locally()
    await bass.play_once()
    try:
        await bass.replay_locally(current_bar=bass_assignment.valid_through_bar + 1)
    except RuntimeError as exc:
        print(f"lease expired: {exc}")

    coordinator.mark_current_bar(bass_assignment.valid_through_bar + 1)
    receive = asyncio.create_task(lead.receive_once())
    await asyncio.sleep(0)
    reassigned = await coordinator.compose_and_assign(
        CompositionContext("bass", root_pitch=36, desired_density=0.5),
        VariationPolicy(policy_version=2, velocity_jitter=3),
        node_id="pi-lead",
    )
    await receive
    print(
        "reassigned "
        f"part={reassigned.part_id} node={reassigned.node_id} generation={reassigned.generation}"
    )

    old_epoch = coordinator.transport_epoch
    new_epoch = coordinator.restart_transport()
    print(f"\n--- coordinator restart old_epoch={old_epoch} new_epoch={new_epoch} ---")
    snapshot = coordinator.snapshot_for("pi-bass")
    await bass.reconcile(snapshot)
    stale = await bass.accept_assignment(bass_assignment)
    print(f"stale old-epoch assignment accepted={stale is not None}")
    print(
        "snapshot reconciled "
        f"node={bass.capabilities.node_id} epoch={bass.observed.transport_epoch} "
        f"assignments={len(snapshot.assignments)}"
    )
