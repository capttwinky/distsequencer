from __future__ import annotations

import argparse
import asyncio
import json

from distributed_sequencer.simulation import run_simulation


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Distributed sequencer reference simulator")
    subparsers = parser.add_subparsers(dest="command")

    sim = subparsers.add_parser("sim", help="run the in-process distributed simulation")
    add_simulation_args(sim)

    coordinator = subparsers.add_parser("coordinator", help="print coordinator defaults")
    coordinator.add_argument("--tempo", type=float, default=120.0)

    node = subparsers.add_parser("node", help="print node runtime defaults")
    node.add_argument("--node-id", default="node-1")

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
        print(json.dumps({"role": "coordinator", "tempo_bpm": args.tempo}, sort_keys=True))
        return
    if command == "node":
        print(json.dumps({"role": "node", "node_id": args.node_id}, sort_keys=True))
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
