from __future__ import annotations

import json
import socket
import subprocess
import sys
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen

READY_TIMEOUT_SECONDS = 20.0


@dataclass(frozen=True)
class ProcessSpec:
    name: str
    command: tuple[str, ...]
    ready_url: str


def _free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _distsequencer_command(*args: str) -> tuple[str, ...]:
    return (sys.executable, "-m", "distributed_sequencer.cli", *args)


def _write_configs(tmp_path: Path, coordinator_port: int) -> dict[str, Path]:
    coordinator = tmp_path / "coordinator.toml"
    bass = tmp_path / "node-bass.toml"
    lead = tmp_path / "node-lead.toml"

    coordinator.write_text(
        "\n".join(
            [
                "[coordinator]",
                f'listen = "tcp://127.0.0.1:{coordinator_port}"',
                "tempo_bpm = 120.0",
                "lease_bars = 4",
                'parts = ["bass", "lead"]',
                "",
                "[transport]",
                'mode = "tcp"',
                "insecure_dev_mode = true",
            ]
        ),
        encoding="utf-8",
    )
    for path, node_id, parts in [
        (bass, "node-bass", '["bass"]'),
        (lead, "node-lead", '["lead", "bass"]'),
    ]:
        path.write_text(
            "\n".join(
                [
                    "[node]",
                    f'id = "{node_id}"',
                    f"parts = {parts}",
                    f'coordinator_url = "tcp://127.0.0.1:{coordinator_port}"',
                    "buffer_depth_bars = 8",
                    "",
                    "[transport]",
                    'mode = "tcp"',
                    "insecure_dev_mode = true",
                    "",
                    "[synth]",
                    'backend = "recording"',
                ]
            ),
            encoding="utf-8",
        )
    return {"coordinator": coordinator, "bass": bass, "lead": lead}


def _runtime_specs(tmp_path: Path) -> tuple[ProcessSpec, ProcessSpec, ProcessSpec]:
    coordinator_port = _free_tcp_port()
    coordinator_ready = _free_tcp_port()
    bass_ready = _free_tcp_port()
    lead_ready = _free_tcp_port()
    configs = _write_configs(tmp_path, coordinator_port)

    return (
        ProcessSpec(
            name="coordinator",
            command=_distsequencer_command(
                "coordinator",
                "--config",
                str(configs["coordinator"]),
                "--ready-listen",
                f"http://127.0.0.1:{coordinator_ready}",
            ),
            ready_url=f"http://127.0.0.1:{coordinator_ready}/readyz",
        ),
        ProcessSpec(
            name="node-bass",
            command=_distsequencer_command(
                "node",
                "--config",
                str(configs["bass"]),
                "--ready-listen",
                f"http://127.0.0.1:{bass_ready}",
            ),
            ready_url=f"http://127.0.0.1:{bass_ready}/readyz",
        ),
        ProcessSpec(
            name="node-lead",
            command=_distsequencer_command(
                "node",
                "--config",
                str(configs["lead"]),
                "--ready-listen",
                f"http://127.0.0.1:{lead_ready}",
            ),
            ready_url=f"http://127.0.0.1:{lead_ready}/readyz",
        ),
    )


def _read_json(url: str) -> Mapping[str, Any]:
    with urlopen(url, timeout=0.5) as response:
        payload = response.read().decode("utf-8")
    decoded = json.loads(payload)
    if not isinstance(decoded, dict):
        raise AssertionError(f"{url} returned non-object JSON: {decoded!r}")
    return decoded


def _wait_ready(url: str) -> Mapping[str, Any]:
    deadline = time.monotonic() + READY_TIMEOUT_SECONDS
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            payload = _read_json(url)
            if payload.get("status") == "ready":
                return payload
        except (TimeoutError, URLError, json.JSONDecodeError, ConnectionError) as exc:
            last_error = exc
        time.sleep(0.1)
    raise AssertionError(f"{url} did not become ready") from last_error


def _terminate(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def test_process_runtime_assigns_parts_and_recovers_after_node_loss(tmp_path: Path) -> None:
    specs = _runtime_specs(tmp_path)
    processes: list[subprocess.Popen[str]] = []

    try:
        for spec in specs:
            processes.append(
                subprocess.Popen(
                    spec.command,
                    cwd=Path.cwd(),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
            )
            ready = _wait_ready(spec.ready_url)
            assert ready["role"] in {"coordinator", "node"}

        coordinator_state = _read_json(specs[0].ready_url.replace("/readyz", "/snapshot"))
        assert coordinator_state["transport_epoch"] >= 1
        assert coordinator_state["assignments"]["bass"]["node_id"] == "node-bass"
        assert coordinator_state["assignments"]["lead"]["node_id"] == "node-lead"

        _terminate(processes[1])
        deadline = time.monotonic() + READY_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            coordinator_state = _read_json(specs[0].ready_url.replace("/readyz", "/snapshot"))
            assignment = coordinator_state["assignments"]["bass"]
            if assignment["node_id"] != "node-bass" and assignment["generation"] >= 2:
                break
            time.sleep(0.1)
        else:
            raise AssertionError("coordinator did not reassign bass after node-bass loss")
    finally:
        for process in processes:
            _terminate(process)
