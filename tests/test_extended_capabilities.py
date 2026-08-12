from __future__ import annotations

from pathlib import Path

import pytest

from distributed_sequencer.adapters.ml import (
    MidiGPTCompositionAdapter,
    MidiRWKVVariationAdapter,
    MusicBertCriticAdapter,
    OnnxVariationAdapter,
    OptionalDependencyUnavailable,
)
from distributed_sequencer.application.benchmark import (
    BenchmarkRecord,
    BenchmarkSuite,
    HardwareProfile,
)
from distributed_sequencer.application.composition import (
    CompositionEngine,
    DensityCritic,
    ProceduralCompositionModel,
)
from distributed_sequencer.application.coordinator import Coordinator
from distributed_sequencer.application.ha import ConsensusBackedCoordinator
from distributed_sequencer.application.history import CompositionHistoryStore
from distributed_sequencer.domain.music import MusicalEvent, Phrase
from distributed_sequencer.domain.state import (
    CompositionContext,
    CompositionState,
    NodeCapabilities,
    VariationPolicy,
)
from distributed_sequencer.infrastructure.consensus import (
    JsonRaftStorage,
    RaftCluster,
    RaftNode,
    RaftRole,
)
from distributed_sequencer.infrastructure.messaging import InMemoryBus
from distributed_sequencer.infrastructure.physical import DeploymentManifest, PhysicalNodeDeployment
from distributed_sequencer.infrastructure.pki import LocalCertificateAuthority
from distributed_sequencer.infrastructure.sync import ClockSample, PtpStyleSynchronizer


class FakeCompositionBackend:
    def generate_candidates(
        self,
        context: CompositionContext,
        *,
        count: int,
    ) -> tuple[dict[str, object], ...]:
        return tuple(
            {
                "phrase_id": f"learned-{index}",
                "role": context.role,
                "events": [{"onset_tick": 0, "pitch": context.root_pitch, "duration_ticks": 24}],
            }
            for index in range(count)
        )


class FakeCriticBackend:
    def score(self, phrase: Phrase, context: CompositionContext) -> float:
        del phrase, context
        return 0.75


class FakeVariationBackend:
    def vary(self, phrase: Phrase, policy: VariationPolicy) -> Phrase:
        del policy
        return Phrase(
            phrase_id=f"{phrase.phrase_id}-learned",
            role=phrase.role,
            events=phrase.events,
        )


@pytest.mark.asyncio
async def test_real_ml_adapters_call_backends_when_optional_runtime_and_model_exist(
    tmp_path: Path,
) -> None:
    model_path = tmp_path / "model.bin"
    model_path.write_text("placeholder", encoding="utf-8")
    context = CompositionContext("bass", 36, 1.0)

    composition = MidiGPTCompositionAdapter(
        model_path=model_path,
        runtime_module="json",
        backend=FakeCompositionBackend(),
    )
    critic = MusicBertCriticAdapter(
        model_path=model_path,
        runtime_module="json",
        backend=FakeCriticBackend(),
    )

    phrase = await composition.generate_candidates(context, count=2)
    assert phrase[0].role == "bass"
    assert critic.score(phrase[0], context) == 0.75


@pytest.mark.asyncio
async def test_learned_variation_adapters_are_real_boundaries(tmp_path: Path) -> None:
    model_path = tmp_path / "variation.bin"
    model_path.write_text("placeholder", encoding="utf-8")
    phrase = Phrase("p", "bass", (MusicalEvent(0, 36, 24),))

    adapter = MidiRWKVVariationAdapter(
        model_path=model_path,
        runtime_module="json",
        backend=FakeVariationBackend(),
    )
    assert (await adapter.vary(phrase, VariationPolicy())).phrase_id == "p-learned"

    missing = OnnxVariationAdapter(model_path=model_path, runtime_module="missing_runtime")
    with pytest.raises(OptionalDependencyUnavailable):
        await missing.vary(phrase, VariationPolicy())


def test_raft_cluster_elects_leader_and_commits_only_with_quorum() -> None:
    cluster = RaftCluster.create(("a", "b", "c"))
    leader = cluster.elect("a")

    assert leader.role is RaftRole.LEADER
    entry = cluster.append_command("cmd-1", "start", available_members={"a", "b"})
    assert entry.index == 1
    assert cluster.nodes["a"].commit_index == 1
    assert cluster.nodes["b"].commit_index == 1

    with pytest.raises(RuntimeError, match="quorum"):
        cluster.append_command("cmd-2", "unsafe", available_members={"a"})


def test_raft_json_storage_persists_term_vote_and_log(tmp_path: Path) -> None:
    path = tmp_path / "raft.json"
    node = RaftNode("a", ("b", "c"), JsonRaftStorage(path))
    node.start_election()
    node.become_leader()
    node.append_local_command("cmd-1", "state")

    restored = RaftNode("a", ("b", "c"), JsonRaftStorage(path))
    assert restored.current_term == 1
    assert restored.voted_for == "a"
    assert restored.last_log_index == 1


@pytest.mark.asyncio
async def test_consensus_backed_coordinator_applies_assignment_after_commit() -> None:
    coordinator = Coordinator(
        composition=CompositionEngine(ProceduralCompositionModel(), critics=(DensityCritic(),)),
        bus=InMemoryBus(),
    )
    ha = ConsensusBackedCoordinator.create(coordinator, ("r1", "r2", "r3"))

    await ha.register(NodeCapabilities("node-a", frozenset({"bass"})))
    assignment = await ha.compose_and_assign(
        CompositionContext("bass", 36, 1.0),
        VariationPolicy(),
        node_id="node-a",
    )

    assert assignment.part_id == "bass"
    assert coordinator.desired_assignments["bass"] == assignment
    assert ha.cluster.nodes["r1"].commit_index >= 2


def test_persistent_composition_history_round_trips_phrase(tmp_path: Path) -> None:
    store = CompositionHistoryStore(tmp_path / "history.sqlite")
    state = CompositionState("composition-1", (("density", 0.8),), revision=3)
    phrase = Phrase("p", "bass", (MusicalEvent(0, 36, 24),), phrase_sequence=7)

    store.record_phrase(state, phrase)

    assert store.list_records("composition-1")[0].phrase_id == "p"
    loaded = store.load_phrase(
        composition_id="composition-1",
        part_id="bass",
        phrase_sequence=7,
        phrase_revision=1,
    )
    assert loaded == phrase


def test_ptp_style_synchronizer_estimates_offset_drift_and_uncertainty() -> None:
    sync = PtpStyleSynchronizer()
    estimate = sync.observe(
        ClockSample(
            coordinator_send_seconds=10.0,
            node_receive_seconds=10.101,
            node_send_seconds=10.102,
            coordinator_receive_seconds=10.004,
        )
    )

    assert estimate.offset_seconds == pytest.approx(0.0995)
    assert estimate.uncertainty_seconds == pytest.approx(0.002)


def test_benchmark_and_physical_deployment_manifest(tmp_path: Path) -> None:
    suite = BenchmarkSuite()
    suite.record(
        BenchmarkRecord(
            name="pi-node",
            platform="raspbian",
            python_version="3.12",
            model_size_mb=None,
            resident_memory_mb=128.0,
            variation_latency_ms=3.0,
            throughput_events_per_second=1000.0,
            buffer_safety_margin_bars=8.0,
            musical_quality_score=0.8,
        )
    )
    suite.write_json(tmp_path / "benchmarks.json")
    assert (tmp_path / "benchmarks.json").exists()

    manifest = DeploymentManifest(
        nodes=(
            PhysicalNodeDeployment(
                profile=HardwareProfile(
                    node_id="pi-bass",
                    cpu="cortex-a72",
                    memory_mb=2048,
                    os="raspios",
                    audio_backend="osc",
                    network="ethernet",
                ),
                coordinator_url="https://coordinator.local",
            ),
        )
    )
    manifest.write_json(tmp_path / "manifest.json")
    assert (tmp_path / "manifest.json").exists()


def test_pki_automation_issues_local_ca_and_node_certificate(tmp_path: Path) -> None:
    ca = LocalCertificateAuthority(tmp_path / "certs")
    paths = ca.paths_for("node/one")
    assert paths.key.name == "node_one.key.pem"
    ca.bootstrap_ca()
    issued = ca.issue_node_certificate("node/one")
    assert issued.ca_key.exists()
    assert issued.ca_cert.exists()
    assert issued.key.exists()
    assert issued.csr.exists()
    assert issued.cert.exists()
