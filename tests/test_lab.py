from __future__ import annotations

import base64
import wave
from io import BytesIO
from urllib.parse import unquote

import pytest

from distributed_sequencer.adapters.superdirt import SuperDirtOscBackend
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


@pytest.mark.asyncio
async def test_reference_lab_exports_strudel_repl_url() -> None:
    lab = ReferencePerformanceLab.from_dsl(DSL, seed=13)
    await lab.prepare_performance()

    code = lab.strudel_code()
    url = lab.strudel_url()
    fragment = url.split("#", maxsplit=1)[1]
    decoded = base64.b64decode(unquote(fragment)).decode("utf-8")

    assert code == decoded
    assert url.startswith("https://strudel.cc/#")
    assert "setcps(" in code
    assert "stack(" in code
    assert '.sound("pulse")' in code
    assert '.sound("saw")' in code
    assert "iframe" in lab.strudel_iframe()


@pytest.mark.asyncio
async def test_reference_lab_previews_superdirt_osc_events() -> None:
    lab = ReferencePerformanceLab.from_dsl(DSL, seed=15)
    await lab.prepare_performance()

    events = lab.superdirt_events()
    rows = lab.superdirt_table()

    assert len(events) == len(rows)
    assert {row["sound"] for row in rows} == {"super808", "superpiano"}
    assert {row["orbit"] for row in rows} == {0, 1}
    assert all(row["cycle"] >= 0 for row in rows)


@pytest.mark.asyncio
async def test_reference_lab_sends_prepared_performance_to_superdirt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sent: list[object] = []
    targets: list[tuple[str, int]] = []

    async def fake_send_events(self: SuperDirtOscBackend, events: object) -> None:
        targets.append((self.host, self.port))
        sent.append(events)

    monkeypatch.setenv("SUPERDIRT_HOST", "192.0.2.20")
    monkeypatch.setenv("SUPERDIRT_PORT", "57121")
    monkeypatch.setattr(
        "distributed_sequencer.adapters.superdirt.SuperDirtOscBackend.send_events",
        fake_send_events,
    )
    lab = ReferencePerformanceLab.from_dsl(DSL, seed=16)
    await lab.prepare_performance()

    events = await lab.send_superdirt()

    assert sent == [events]
    assert targets == [("192.0.2.20", 57121)]
    assert events == lab.superdirt_events()


@pytest.mark.asyncio
async def test_reference_lab_renders_browser_playable_wav(tmp_path) -> None:
    lab = ReferencePerformanceLab.from_dsl(DSL, seed=14)
    await lab.prepare_performance()

    wav_bytes = lab.render_audio()
    path = lab.write_audio(tmp_path / "audition.wav")

    assert wav_bytes.startswith(b"RIFF")
    assert path.read_bytes().startswith(b"RIFF")
    with wave.open(BytesIO(wav_bytes), "rb") as wav:
        assert wav.getnchannels() == 2
        assert wav.getsampwidth() == 2
        assert wav.getframerate() == 44_100
        assert wav.getnframes() > 0
        assert any(wav.readframes(wav.getnframes()))


def test_music_dsl_rejects_unknown_commands() -> None:
    with pytest.raises(ValueError, match="unsupported DSL command"):
        parse_music_dsl("groove id=bad")
