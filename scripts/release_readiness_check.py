from __future__ import annotations

from pathlib import Path

REQUIRED_FILES = (
    "docs/release-readiness.md",
    "Dockerfile",
    ".dockerignore",
    ".github/workflows/ci.yml",
    ".github/workflows/release.yml",
    "examples/coordinator.toml",
    "examples/node-bass.toml",
    "examples/node-lead.toml",
)


def main() -> None:
    missing = [path for path in REQUIRED_FILES if not Path(path).is_file()]
    if missing:
        raise SystemExit(f"Missing release readiness assets: {', '.join(missing)}")


if __name__ == "__main__":
    main()
