from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).parents[1] / "scripts" / "start_superdirt.py"
_SPEC = importlib.util.spec_from_file_location("start_superdirt", _SCRIPT)
assert _SPEC is not None
assert _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
render_startup_script = _MODULE.render_startup_script


def test_superdirt_startup_script_uses_configured_port_and_bind_address() -> None:
    script = render_startup_script(port=57121, bind_address="127.0.0.1")

    assert '~dirt.start(57121, [0, 0], NetAddr("127.0.0.1"));' in script
    assert "DISTSEQUENCER_SUPERDIRT_READY" in script
    assert "DISTSEQUENCER_DIRT_EVENT" in script


def test_superdirt_startup_script_rejects_invalid_port() -> None:
    with pytest.raises(ValueError, match="port must be positive"):
        render_startup_script(port=0, bind_address="127.0.0.1")


def test_missing_sclang_message_includes_chocolatey_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        _MODULE.shutil,
        "which",
        lambda name: "choco.exe" if name == "choco" else None,
    )

    message = _MODULE._missing_sclang_message()

    assert "sclang was not found" in message
    assert "choco install SuperCollider superdirt -y" in message
    assert "SCLANG" in message


def test_supercollider_env_adds_sclang_directory_to_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PATH", "C:\\Windows\\System32")

    env = _MODULE._supercollider_env("C:\\Program Files\\SuperCollider-3.12.1\\sclang.exe")

    assert env["PATH"].startswith("C:\\Program Files\\SuperCollider-3.12.1")
    assert "C:\\Windows\\System32" in env["PATH"]


def test_find_sclang_checks_versioned_supercollider_install(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = "C:/Program Files/SuperCollider-3.12.1/sclang.exe"
    monkeypatch.delenv("SCLANG", raising=False)
    monkeypatch.setattr(_MODULE.shutil, "which", lambda _name: None)
    monkeypatch.setattr(
        _MODULE.Path,
        "exists",
        lambda path: str(path).replace("\\", "/") == expected,
    )
    monkeypatch.setattr(_MODULE.Path, "glob", lambda *_args: iter(()))

    assert _MODULE._find_sclang().replace("\\", "/") == expected
