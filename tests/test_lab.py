from __future__ import annotations

import pytest

from distributed_sequencer.application.lab import (
    NodeSpec,
    ReferencePerformanceLab,
    parse_music_dsl,
)

DSL = """
performance title="Notebook Mesh" tempo=132
part id=bass root=36 density=0.75 bars=1 jitter=4
part id=lead root=60 density=1.0 bars=1 jitter=7
node id=node-bass roles=bass
node id=node-lead roles=lead,bass learned=true
"""


def test_music_dsl_parses_score_parts_and_nodes() -> None:
    score = parse_music_dsl(DSL)

    assert score.title == "Notebook Mesh"
    assert score.tempo_bpm == 132.0
    assert score.part_ids() == ("bass", "lead")
    assert score.nodes[1].roles == ("lead", "bass")
    assert score.nodes[1].learned_variation


@pytest.mark.asyncio
async def test_reference_lab_prepares_performance_and_dashboard() -> None:
    lab = ReferencePerformanceLab.from_dsl(DSL, seed=11)

    prepared = await lab.prepare_performance()
    dashboard = lab.dashboard()

    assert [part["part_id"] for part in prepared.table()] == ["bass", "lead"]
    assert {part["node_id"] for part in prepared.table()} == {"node-bass", "node-lead"}
    assert dashboard.transport_state == "playing"
    assert len(dashboard.assignments) == 2
    assert len(dashboard.readiness) == 2


@pytest.mark.asyncio
async def test_reference_lab_can_add_mesh_node_and_reassign_after_lease() -> None:
    lab = ReferencePerformanceLab.from_dsl(DSL, seed=12)
    lab.add_node(NodeSpec("node-standby", ("bass", "lead")))
    await lab.prepare_performance()

    reassigned = await lab.reassign_part_after_lease("bass", "node-standby")
    dashboard = lab.dashboard().as_dict()

    assert reassigned.node_id == "node-standby"
    assert reassigned.assignment_generation == 2
    assert any(node["node_id"] == "node-standby" for node in dashboard["nodes"])


def test_music_dsl_rejects_unknown_commands() -> None:
    with pytest.raises(ValueError, match="unsupported DSL command"):
        parse_music_dsl("groove id=bad")
