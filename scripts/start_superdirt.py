from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

STARTUP_SCRIPT = """
s.waitForBoot {
    ~dirt = SuperDirt(2, s);
    ~dirt.loadSoundFiles;
    s.sync;
    ~dirt.start($PORT, [0, 0], NetAddr("$BIND_ADDRESS"));
    "DISTSEQUENCER_SUPERDIRT_READY".postln;
};
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Start a local SuperDirt OSC target for the lab.")
    parser.add_argument("--host", default="127.0.0.1", help="documented OSC host")
    parser.add_argument("--port", type=int, default=57120, help="SuperDirt OSC UDP port")
    parser.add_argument("--bind-address", default="0.0.0.0", help="SuperDirt listen address")
    parser.add_argument("--check", action="store_true", help="check SuperCollider availability")
    parser.add_argument("--foreground", action="store_true", help="run sclang in the foreground")
    parser.add_argument("--pid-file", type=Path, default=Path(".tmp/superdirt/sclang.pid"))
    parser.add_argument("--log-file", type=Path, default=Path(".tmp/superdirt/sclang.log"))
    parser.add_argument(
        "--script-file",
        type=Path,
        default=Path(".tmp/superdirt/start_superdirt.scd"),
    )
    parser.add_argument("--wait-seconds", type=float, default=20.0)
    args = parser.parse_args()

    sclang = _find_sclang()
    if sclang is None:
        raise SystemExit(_missing_sclang_message())
    if args.check:
        print(f"sclang: {sclang}")
        print(f"OSC target: {args.host}:{args.port}")
        print("SuperDirt startup will be verified by `make superdirt`.")
        return

    if _pid_running(args.pid_file):
        print(f"SuperDirt launcher already running from {args.pid_file}")
        print(f"OSC target: {args.host}:{args.port}")
        return

    args.script_file.parent.mkdir(parents=True, exist_ok=True)
    args.log_file.parent.mkdir(parents=True, exist_ok=True)
    args.script_file.write_text(
        render_startup_script(port=args.port, bind_address=args.bind_address),
        encoding="utf-8",
    )

    if args.foreground:
        subprocess.run([sclang, str(args.script_file)], check=True)
        return

    log = args.log_file.open("ab")
    process = subprocess.Popen(
        [sclang, str(args.script_file)],
        stdout=log,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        creationflags=_creation_flags(),
    )
    args.pid_file.write_text(str(process.pid), encoding="utf-8")
    print(f"Started SuperDirt launcher with PID {process.pid}")
    print(f"OSC target: {args.host}:{args.port}")
    print(f"Log: {args.log_file}")
    _wait_until_ready(process, args.log_file, timeout_seconds=args.wait_seconds)


def _find_sclang() -> str | None:
    configured = os.environ.get("SCLANG")
    if configured:
        return configured
    found = shutil.which("sclang")
    if found:
        return found
    candidates = (
        Path("C:/Program Files/SuperCollider/sclang.exe"),
        Path("C:/Program Files/SuperCollider-3.14.0/sclang.exe"),
        Path("C:/Program Files/SuperCollider-3.13.0/sclang.exe"),
    )
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return None


def _missing_sclang_message() -> str:
    chocolatey_hint = ""
    if shutil.which("choco"):
        chocolatey_hint = (
            "\nChocolatey is available on this machine. From an Administrator PowerShell, run:\n"
            "  choco install SuperCollider superdirt -y\n"
        )
    return (
        "sclang was not found. Install SuperCollider with the SuperDirt Quark, or set SCLANG "
        "to the sclang executable before running `make superdirt` or `make lab`."
        f"{chocolatey_hint}\n"
        "Example:\n"
        '  $env:SCLANG="C:\\Program Files\\SuperCollider-3.12.1\\sclang.exe"; make lab'
    )


def render_startup_script(*, port: int, bind_address: str) -> str:
    if port <= 0:
        raise ValueError("port must be positive")
    if not bind_address:
        raise ValueError("bind_address must be non-empty")
    return (
        STARTUP_SCRIPT.replace("$PORT", str(port)).replace("$BIND_ADDRESS", bind_address).strip()
        + "\n"
    )


def _pid_running(pid_file: Path) -> bool:
    if not pid_file.exists():
        return False
    try:
        pid = int(pid_file.read_text(encoding="utf-8").strip())
    except ValueError:
        return False
    if sys.platform == "win32":
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}"],
            capture_output=True,
            check=False,
            text=True,
        )
        return str(pid) in result.stdout
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _creation_flags() -> int:
    if sys.platform != "win32":
        return 0
    return subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS


def _wait_until_ready(
    process: subprocess.Popen[bytes],
    log_file: Path,
    *,
    timeout_seconds: float,
) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if process.poll() is not None:
            raise SystemExit(
                f"sclang exited early with status {process.returncode}; see {log_file}"
            )
        log_text = log_file.read_text(encoding="utf-8", errors="replace")
        if "DISTSEQUENCER_SUPERDIRT_READY" in log_text:
            print("SuperDirt ready")
            return
        time.sleep(0.25)
    raise SystemExit(f"Timed out waiting for SuperDirt readiness marker; see {log_file}")


if __name__ == "__main__":
    main()
