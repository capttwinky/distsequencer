from __future__ import annotations

import base64
import json
import wave
from io import BytesIO
from urllib.parse import unquote

import pytest

from distributed_sequencer.adapters.superdirt import SuperDirtOscBackend
from distributed_sequencer.application.lab import (
    NodeSpec,
    ReferencePerformanceLab,
    dashboard_tables,
    parse_music_dsl,
    render_dashboard_html,
    try_fetch_daemon_snapshot,
)
from distributed_sequencer.domain.music import MusicalEvent, Phrase
from distributed_sequencer.domain.state import CompositionContext

DSL = """
performance title="Notebook Mesh" tempo=132
composition backend=ml adapter=MidiGPTCompositionAdapter model=.models/midigpt.pt runtime=torch
section id=intro bars=4 repeats=2
motif id=hook intervals=0,3,7 rhythm=12,12,24
part id=bass root=36 density=0.75 bars=1 jitter=4
part id=lead root=60 density=1.0 bars=1 jitter=7 motif=hook
lane id=lead-density part=lead density=1.25 mutate=0.2
route part=bass target=superdirt sound=bd orbit=3 channel=0 gain=0.72
route part=lead target=superdirt sound=superpiano orbit=4 channel=1 gain=0.5
node id=node-bass roles=bass
node id=node-lead roles=lead,bass learned=true
profile node=node-lead device=ThinkPad location=studio latency=12 cert=n.crt key=n.key ca=ca.crt
"""


class FakeCompositionModel:
    async def generate_candidates(
        self,
        context: CompositionContext,
        *,
        count: int,
    ) -> tuple[Phrase, ...]:
        del count
        return (
            Phrase(
                phrase_id=f"{context.role}-fake",
                role=context.role,
                events=(
                    MusicalEvent(
                        onset_tick=0,
                        pitch=context.root_pitch,
                        duration_ticks=12,
                    ),
                    MusicalEvent(
                        onset_tick=24,
                        pitch=context.root_pitch + 5,
                        duration_ticks=12,
                    ),
                    MusicalEvent(
                        onset_tick=48,
                        pitch=context.root_pitch + 10,
                        duration_ticks=12,
                    ),
                ),
                bars=context.bars,
                beats_per_bar=context.beats_per_bar,
                ticks_per_beat=context.ticks_per_beat,
            ),
        )


def test_music_dsl_parses_score_parts_and_nodes() -> None:
    score = parse_music_dsl(DSL)

    assert score.title == "Notebook Mesh"
    assert score.tempo_bpm == 132.0
    assert score.part_ids() == ("bass", "lead")
    assert score.nodes[1].roles == ("lead", "bass")
    assert score.nodes[1].learned_variation
    assert score.composition_backend.adapter == "MidiGPTCompositionAdapter"
    assert score.form[0].section_id == "intro"
    assert score.motifs[0].intervals == (0, 3, 7)
    assert score.probability_lanes[0].density == 1.25
    assert score.routes[1].sound == "superpiano"
    assert score.physical_profiles[0].pki_key_path == "n.key"


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
    assert "Assignments" in lab.dashboard_html()


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
    assert '.sound("bd")' in code
    assert '.sound("superpiano")' in code
    assert "iframe" in lab.strudel_iframe()


@pytest.mark.asyncio
async def test_reference_lab_previews_superdirt_osc_events() -> None:
    lab = ReferencePerformanceLab.from_dsl(DSL, seed=15)
    await lab.prepare_performance()

    events = lab.superdirt_events()
    rows = lab.superdirt_table()

    assert len(events) == len(rows)
    assert {row["sound"] for row in rows} == {"bd", "superpiano"}
    assert {row["orbit"] for row in rows} == {3, 4}
    assert all(row["cycle"] >= 0 for row in rows)


@pytest.mark.asyncio
async def test_reference_lab_applies_motif_probability_lane_and_model_adapter() -> None:
    lab = ReferencePerformanceLab.from_dsl(DSL, composition_model=FakeCompositionModel())

    await lab.prepare_performance()
    lead = lab.prepared_phrases["lead"]
    blueprint = lab.score_blueprint_table()
    backend = lab.composition_backend_table()

    assert [event.pitch for event in lead.events] == [60, 63, 67]
    assert [event.onset_tick for event in lead.events] == [0, 12, 24]
    assert next(row for row in blueprint if row["part_id"] == "lead")["density"] == 1.25
    assert backend[0]["adapter"] == "MidiGPTCompositionAdapter"


def test_dashboard_tables_normalize_live_daemon_snapshot() -> None:
    live = {
        "status": "ready",
        "transport_epoch": 2,
        "transport_state": "playing",
        "current_bar": 9,
        "assignments": {
            "lead": {
                "part_id": "lead",
                "node_id": "node-lead",
                "assignment_generation": 3,
            }
        },
        "readiness": {
            "node-lead:lead": {
                "part_id": "lead",
                "node_id": "node-lead",
                "ready_through_bar": 12,
            }
        },
    }

    tables = dashboard_tables(live)
    html = render_dashboard_html(live)

    assert tables["assignments"][0]["node_id"] == "node-lead"
    assert tables["readiness"][0]["ready_through_bar"] == 12
    assert "Live Coordinator Snapshot" in html
    assert "node-lead" in html


def test_try_fetch_daemon_snapshot_reads_json(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeResponse:
        def __enter__(self) -> FakeResponse:
            return self

        def __exit__(self, *args: object) -> None:
            pass

        def read(self) -> bytes:
            return json.dumps({"status": "ready", "transport_epoch": 7}).encode("utf-8")

    def fake_urlopen(url: str, *, timeout: float) -> FakeResponse:
        assert url == "http://daemon/snapshot"
        assert timeout == 0.5
        return FakeResponse()

    monkeypatch.setattr("distributed_sequencer.application.lab.urlopen", fake_urlopen)

    assert (
        try_fetch_daemon_snapshot("http://daemon/snapshot", timeout_seconds=0.5)["transport_epoch"]
        == 7
    )


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
async def test_reference_lab_streams_prepared_performance_to_superdirt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    streamed: list[tuple[object, int, float]] = []

    async def fake_stream_events(
        self: SuperDirtOscBackend,
        events: object,
        *,
        cycles: int,
        lookahead_seconds: float,
    ) -> object:
        del self
        streamed.append((events, cycles, lookahead_seconds))
        return events

    monkeypatch.setattr(
        "distributed_sequencer.adapters.superdirt.SuperDirtOscBackend.stream_events",
        fake_stream_events,
    )
    lab = ReferencePerformanceLab.from_dsl(DSL, seed=17)
    await lab.prepare_performance()

    events = await lab.stream_superdirt(cycles=3, lookahead_seconds=0.05)

    assert streamed == [(events, 3, 0.05)]


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
