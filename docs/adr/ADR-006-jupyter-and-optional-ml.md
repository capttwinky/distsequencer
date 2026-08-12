# ADR-006 Jupyter And Optional ML

## Status

Accepted.

## Decision

The base environment stays CPU-friendly. `make lab` binds JupyterLab to `127.0.0.1` with normal
token authentication. Optional ML dependencies live in the `ml` uv group and are not required by
`make check`, `make sim`, or `make demo`.

## Consequences

Raspberry Pi-class node installs do not inherit workstation ML dependencies. Notebook diagnostics
must gracefully report that ML runtimes are absent.
