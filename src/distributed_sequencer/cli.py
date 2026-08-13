from __future__ import annotations

import argparse
import asyncio
import json
import platform
from dataclasses import asdict
from pathlib import Path

from distributed_sequencer.application.benchmark import (
    BenchmarkRecord,
    BenchmarkSuite,
    HardwareProfile,
)
from distributed_sequencer.infrastructure.physical import DeploymentManifest, PhysicalNodeDeployment
from distributed_sequencer.infrastructure.pki import LocalCertificateAuthority
from distributed_sequencer.runtime.config import Endpoint, load_coordinator_config, load_node_config
from distributed_sequencer.runtime.daemon import run_coordinator_daemon, run_node_daemon
from distributed_sequencer.simulation import run_simulation


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Distributed sequencer reference simulator")
    subparsers = parser.add_subparsers(dest="command")

    sim = subparsers.add_parser("sim", help="run the in-process distributed simulation")
    add_simulation_args(sim)

    coordinator = subparsers.add_parser("coordinator", help="run a coordinator daemon")
    coordinator.add_argument("--tempo", type=float, default=120.0)
    coordinator.add_argument("--config", type=Path, default=None)
    coordinator.add_argument("--ready-listen", default="http://127.0.0.1:8081")

    node = subparsers.add_parser("node", help="run a node daemon")
    node.add_argument("--node-id", default="node-1")
    node.add_argument("--config", type=Path, default=None)
    node.add_argument("--ready-listen", default="http://127.0.0.1:8082")

    pki = subparsers.add_parser("pki", help="create local development CA and node certificate")
    pki.add_argument("--dir", default=".local/pki")
    pki.add_argument("--node-id", default="node-1")

    benchmark = subparsers.add_parser("benchmark", help="write a hardware benchmark record")
    benchmark.add_argument("--output", default="artifacts/benchmarks.json")
    benchmark.add_argument("--name", default="local-node")

    manifest = subparsers.add_parser("manifest", help="write a physical deployment manifest")
    manifest.add_argument("--output", default="artifacts/deployment.json")
    manifest.add_argument("--node-id", default="pi-bass")
    manifest.add_argument("--coordinator-url", default="https://coordinator.local")

    add_simulation_args(parser)
    return parser


def add_simulation_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--bpm", type=float, default=720.0, help="simulation playback tempo")
    parser.add_argument("--nodes", type=int, default=2, help="number of simulated nodes")
    parser.add_argument("--tempo", type=float, default=None, help="alias for --bpm")
    parser.add_argument("--bars", type=int, default=24, help="simulated bar horizon")
    parser.add_argument("--seed", type=int, default=2026, help="deterministic simulation seed")
    parser.add_argument("--latency", type=float, default=0.0, help="network latency seconds")
    parser.add_argument("--packet-loss", type=float, default=0.0, help="packet loss ratio")
    parser.add_argument("--clock-skew", type=float, default=0.0, help="clock skew seconds")
    parser.add_argument("--log-level", default="INFO", help="structured log verbosity")


def main() -> None:
    args = build_parser().parse_args()
    command = args.command or "sim"
    if command == "coordinator":
        if args.config is not None:
            asyncio.run(
                run_coordinator_daemon(
                    load_coordinator_config(args.config),
                    ready_endpoint=Endpoint.parse(args.ready_listen, expected_scheme="http"),
                )
            )
            return
        print(json.dumps({"role": "coordinator", "tempo_bpm": args.tempo}, sort_keys=True))
        return
    if command == "node":
        if args.config is not None:
            asyncio.run(
                run_node_daemon(
                    load_node_config(args.config),
                    ready_endpoint=Endpoint.parse(args.ready_listen, expected_scheme="http"),
                )
            )
            return
        print(json.dumps({"role": "node", "node_id": args.node_id}, sort_keys=True))
        return
    if command == "pki":
        ca = LocalCertificateAuthority(Path(args.dir))
        ca.bootstrap_ca()
        paths = ca.issue_node_certificate(str(args.node_id))
        print(json.dumps({field: str(value) for field, value in asdict(paths).items()}))
        return
    if command == "benchmark":
        suite = BenchmarkSuite()
        suite.record(
            BenchmarkRecord(
                name=str(args.name),
                platform="local",
                python_version=platform.python_version(),
                model_size_mb=None,
                resident_memory_mb=None,
                variation_latency_ms=None,
                throughput_events_per_second=None,
                buffer_safety_margin_bars=None,
                musical_quality_score=None,
            )
        )
        suite.write_json(Path(args.output))
        print(json.dumps({"benchmark_output": str(args.output)}, sort_keys=True))
        return
    if command == "manifest":
        deployment = DeploymentManifest(
            nodes=(
                PhysicalNodeDeployment(
                    profile=HardwareProfile(
                        node_id=str(args.node_id),
                        cpu="unknown",
                        memory_mb=1024,
                        os="unknown",
                        audio_backend="osc",
                        network="unknown",
                    ),
                    coordinator_url=str(args.coordinator_url),
                ),
            )
        )
        deployment.write_json(Path(args.output))
        print(json.dumps({"manifest_output": str(args.output)}, sort_keys=True))
        return
    bpm = float(args.tempo if args.tempo is not None else args.bpm)
    asyncio.run(
        run_simulation(
            bpm=bpm,
            nodes=int(args.nodes),
            bars=int(args.bars),
            seed=int(args.seed),
            latency=float(args.latency),
            packet_loss=float(args.packet_loss),
            clock_skew=float(args.clock_skew),
        )
    )


if __name__ == "__main__":
    main()
