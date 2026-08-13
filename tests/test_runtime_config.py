from __future__ import annotations

from pathlib import Path

import pytest

from distributed_sequencer.runtime.config import (
    Endpoint,
    load_coordinator_config,
    load_node_config,
)


def test_loads_reference_runtime_example_configs() -> None:
    coordinator = load_coordinator_config(Path("examples/coordinator.toml"))
    bass = load_node_config(Path("examples/node-bass.toml"))
    lead = load_node_config(Path("examples/node-lead.toml"))

    assert coordinator.listen == Endpoint("tcp", "127.0.0.1", 7000)
    assert coordinator.parts == ("bass", "lead")
    assert coordinator.transport.insecure_dev_mode
    assert bass.node_id == "node-bass"
    assert bass.parts == ("bass",)
    assert lead.parts == ("lead", "bass")


def test_secure_transport_requires_ca_or_insecure_dev_mode(tmp_path: Path) -> None:
    config = tmp_path / "coordinator.toml"
    config.write_text(
        "\n".join(
            [
                "[coordinator]",
                'listen = "tcp://127.0.0.1:7000"',
                'parts = ["bass"]',
                "",
                "[transport]",
                'mode = "tcp"',
                "insecure_dev_mode = false",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="secure transport requires ca_cert"):
        load_coordinator_config(config)


def test_node_config_requires_non_empty_parts(tmp_path: Path) -> None:
    config = tmp_path / "node.toml"
    config.write_text(
        "\n".join(
            [
                "[node]",
                'id = "node-empty"',
                "parts = []",
                'coordinator_url = "tcp://127.0.0.1:7000"',
                "",
                "[transport]",
                'mode = "tcp"',
                "insecure_dev_mode = true",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"node\.parts must not be empty"):
        load_node_config(config)


def test_ready_endpoint_requires_http_scheme() -> None:
    with pytest.raises(ValueError, match="expected 'http' endpoint"):
        Endpoint.parse("tcp://127.0.0.1:8080", expected_scheme="http")
