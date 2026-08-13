from __future__ import annotations

import json
import sys
from pathlib import Path

KERNEL_NAME = "distsequencer"
DISPLAY_NAME = "Distributed Sequencer (.venv)"


def main() -> None:
    venv = Path(sys.prefix)
    kernel_dir = venv / "share" / "jupyter" / "kernels" / KERNEL_NAME
    kernel_dir.mkdir(parents=True, exist_ok=True)
    kernel_json = {
        "argv": [
            str(Path(sys.executable)),
            "-m",
            "ipykernel_launcher",
            "-f",
            "{connection_file}",
        ],
        "display_name": DISPLAY_NAME,
        "language": "python",
        "metadata": {
            "debugger": True,
            "supported_encryption": "curve",
        },
        "kernel_protocol_version": "5.5",
    }
    (kernel_dir / "kernel.json").write_text(
        json.dumps(kernel_json, indent=2) + "\n",
        encoding="utf-8",
    )
    for notebook in Path("notebooks").glob("*.ipynb"):
        update_notebook_kernel(notebook)
    print(f"Installed Jupyter kernel {KERNEL_NAME!r} at {kernel_dir}")
    print(f"Kernel Python: {sys.executable}")


def update_notebook_kernel(path: Path) -> None:
    notebook = json.loads(path.read_text(encoding="utf-8"))
    metadata = notebook.setdefault("metadata", {})
    metadata["kernelspec"] = {
        "display_name": DISPLAY_NAME,
        "language": "python",
        "name": KERNEL_NAME,
    }
    language_info = metadata.setdefault("language_info", {})
    language_info["name"] = "python"
    path.write_text(json.dumps(notebook, indent=1) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
