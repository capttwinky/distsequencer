# Release Readiness

This checklist defines the local gates for a reference implementation release. It assumes the
working tree is clean and the release branch is `main`.

## Quality Gates

Refresh generated API documentation before running the full local check:

```bash
make docs-api
```

Run the full local check before building artifacts:

```bash
make check
```

This runs Ruff linting, formatting checks, strict mypy, unit tests, BDD tests, and the
process-level E2E test that starts real coordinator/node subprocesses.

## Container Gates

Build and run the runtime image with Docker:

```bash
make docker-build IMAGE=distsequencer:release
make docker-run IMAGE=distsequencer:release
```

Build and run the same image with Podman:

```bash
make podman-build IMAGE=distsequencer:release
make podman-run IMAGE=distsequencer:release
```

On this Windows workstation, use the explicit Podman binary if `podman` is not first on `PATH`:

```bash
make podman-build PODMAN=C:/Users/Joel/AppData/Local/Programs/Podman/podman.exe IMAGE=distsequencer:release
make podman-run PODMAN=C:/Users/Joel/AppData/Local/Programs/Podman/podman.exe IMAGE=distsequencer:release
```

## Operational Artifacts

Generate the release evidence artifacts:

```bash
make benchmark
make manifest
make pki
```

Expected outputs:

- `artifacts/benchmarks.json`
- `artifacts/deployment.json`
- `.local/pki/ca.cert.pem`
- `.local/pki/node-1.cert.pem`
- `.local/pki/node-1.key.pem`

These files are local evidence artifacts and are not committed by default.

## Process-Level E2E Gate

`tests/test_process_e2e.py` starts the config-driven reference runtime:

```bash
uv run distsequencer coordinator --config examples/coordinator.toml --ready-listen http://127.0.0.1:8081
uv run distsequencer node --config examples/node-bass.toml --ready-listen http://127.0.0.1:8082
uv run distsequencer node --config examples/node-lead.toml --ready-listen http://127.0.0.1:8083
```

Required readiness probes:

- `GET /readyz` returns JSON with `{"status": "ready", "role": "coordinator"}` for the coordinator.
- `GET /readyz` returns JSON with `{"status": "ready", "role": "node"}` for each node.
- `GET /snapshot` on the coordinator returns `transport_epoch` and part assignments.

Before tagging, run the process E2E explicitly as a focused smoke test:

```bash
uv run pytest -q tests/test_process_e2e.py
```

## GitHub Push Credentials

Use the `capttwinky` GitHub credential override for this checkout:

```bash
git -c credential.https://github.com.helper= -c credential.https://github.com.helper=manager push origin main
```

Confirm the credential resolves to `capttwinky` if authentication changes:

```bash
git -c credential.https://github.com.helper= -c credential.https://github.com.helper=manager credential fill
```

Input for `credential fill`:

```text
protocol=https
host=github.com
```

## Tag And Release

Build distributions from a clean tree:

```bash
make build
```

Tag and push the release:

```bash
make release VERSION=0.1.0
```

The GitHub release workflow builds distributions and attaches them to a GitHub Release. It does not
publish to PyPI.
