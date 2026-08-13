SHELL := /bin/bash
.DEFAULT_GOAL := help

UV ?= uv
UV_CACHE_DIR ?= $(CURDIR)/.uv-cache
PYTHON_VERSION ?= 3.12
JUPYTER_IP ?= 127.0.0.1
JUPYTER_PORT ?= 8888
JUPYTER_RUNTIME_DIR ?= $(CURDIR)/.tmp/jupyter/runtime
JUPYTER_CONFIG_DIR ?= $(CURDIR)/.tmp/jupyter/config
JUPYTER_DATA_DIR ?= $(CURDIR)/.tmp/jupyter/data
SUPERDIRT_HOST ?= 127.0.0.1
SUPERDIRT_PORT ?= 57120
SUPERDIRT_BIND ?= 0.0.0.0
SUPERDIRT_WAIT ?= 20
GIT_REMOTE ?= origin
VERSION ?=
IMAGE ?= distsequencer:latest
CONTAINER ?= docker
CONTAINER_BUILD_FLAGS ?=
CONTAINER_RUN_FLAGS ?=
PODMAN ?= podman
PODMAN_BUILD_FLAGS ?= --network=host
PODMAN_RUN_FLAGS ?= --network=none
PYTEST_RUN_ID ?= $(shell python -c "import uuid; print(uuid.uuid4().hex)")
PYTEST_BASETEMP ?= $(CURDIR)/.tmp/pytest-basetemp-$(PYTEST_RUN_ID)
PYTEST_FLAGS ?= --basetemp=$(PYTEST_BASETEMP) -p no:cacheprovider

export UV_CACHE_DIR SUPERDIRT_HOST SUPERDIRT_PORT JUPYTER_RUNTIME_DIR JUPYTER_CONFIG_DIR JUPYTER_DATA_DIR

.PHONY: help bootstrap sync ml kernel superdirt superdirt-check superdirt-foreground lab lab-server lab-remote sim demo benchmark pki manifest release-readiness-check container-build container-run docker-build docker-run podman-build podman-run test test-unit test-bdd lint format format-check typecheck check build clean push pr promote release

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*## "; printf "\nTargets:\n"} /^[a-zA-Z0-9_.-]+:.*## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

bootstrap: ## Install Python with uv and synchronize the environment
	$(UV) python install $(PYTHON_VERSION)
	$(UV) sync --python $(PYTHON_VERSION)

sync: ## Synchronize the uv environment
	$(UV) sync --python $(PYTHON_VERSION)

ml: ## Synchronize optional ML dependency group
	$(UV) sync --python $(PYTHON_VERSION) --group ml

kernel: ## Install the project .venv Jupyter kernel and pin notebooks to it
	$(UV) run python scripts/install_jupyter_kernel.py

superdirt: ## Start local SuperDirt for notebook OSC audition
	$(UV) run python scripts/start_superdirt.py --host $(SUPERDIRT_HOST) --port $(SUPERDIRT_PORT) --bind-address $(SUPERDIRT_BIND) --wait-seconds $(SUPERDIRT_WAIT)

superdirt-check: ## Check local SuperCollider/SuperDirt launch prerequisites
	$(UV) run python scripts/start_superdirt.py --host $(SUPERDIRT_HOST) --port $(SUPERDIRT_PORT) --bind-address $(SUPERDIRT_BIND) --check

superdirt-foreground: ## Run SuperDirt launcher in the foreground for debugging
	$(UV) run python scripts/start_superdirt.py --host $(SUPERDIRT_HOST) --port $(SUPERDIRT_PORT) --bind-address $(SUPERDIRT_BIND) --foreground

lab: kernel superdirt ## Launch JupyterLab locally with SuperDirt OSC target
	$(UV) run python -c "import os, pathlib; [pathlib.Path(os.environ[name]).mkdir(parents=True, exist_ok=True) for name in ('JUPYTER_RUNTIME_DIR', 'JUPYTER_CONFIG_DIR', 'JUPYTER_DATA_DIR')]"
	$(UV) run jupyter lab --ip=$(JUPYTER_IP) --port=$(JUPYTER_PORT) --no-browser --ServerApp.root_dir=.

lab-server: ## Launch JupyterLab on all interfaces (token authentication remains enabled)
	$(MAKE) lab JUPYTER_IP=0.0.0.0

lab-remote: lab-server ## Explicit alias for remotely reachable JupyterLab

sim: ## Run the in-process coordinator/node simulation
	$(UV) run distsequencer sim

demo: sim ## Run the distributed MVP demo

benchmark: ## Write a local benchmark record
	$(UV) run distsequencer benchmark

pki: ## Create local development CA and node certificate
	$(UV) run distsequencer pki --dir .local/pki --node-id node-1

manifest: ## Write a physical deployment manifest
	$(UV) run distsequencer manifest

release-readiness-check: ## Verify release readiness docs and static release assets exist
	$(UV) run python scripts/release_readiness_check.py

container-build: ## Build the runtime container image
	$(CONTAINER) build $(CONTAINER_BUILD_FLAGS) -t $(IMAGE) .

container-run: ## Run the simulator in the runtime container image
	$(CONTAINER) run --rm $(CONTAINER_RUN_FLAGS) $(IMAGE) sim

docker-build: container-build ## Build with Docker

docker-run: container-run ## Run with Docker

podman-build: ## Build with Podman
	$(MAKE) container-build CONTAINER=$(PODMAN) CONTAINER_BUILD_FLAGS="$(PODMAN_BUILD_FLAGS)"

podman-run: ## Run with Podman
	$(MAKE) container-run CONTAINER=$(PODMAN) CONTAINER_RUN_FLAGS="$(PODMAN_RUN_FLAGS)"

test: ## Run unit and BDD tests
	$(UV) run python -c "import pathlib; pathlib.Path('.tmp').mkdir(exist_ok=True)"
	$(UV) run pytest -q $(PYTEST_FLAGS)

test-unit: ## Run non-BDD unit tests
	$(UV) run python -c "import pathlib; pathlib.Path('.tmp').mkdir(exist_ok=True)"
	$(UV) run pytest -q tests -k "not bdd" $(PYTEST_FLAGS)

test-bdd: ## Run BDD scenarios
	$(UV) run python -c "import pathlib; pathlib.Path('.tmp').mkdir(exist_ok=True)"
	$(UV) run pytest -q tests/test_bdd_autonomy.py $(PYTEST_FLAGS)

lint: ## Run Ruff lint checks
	$(UV) run ruff check .

format: ## Format source and tests
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

format-check: ## Check formatting without modifying files
	$(UV) run ruff format --check .

typecheck: ## Run strict mypy checking
	$(UV) run mypy

check: lint format-check typecheck test ## Run all local quality gates

build: check ## Build wheel and source distribution
	$(UV) run python -c "import shutil; shutil.rmtree('dist', ignore_errors=True)"
	$(UV) build

clean: ## Remove generated caches and distributions
	$(UV) run python -c "import pathlib, shutil; [shutil.rmtree(path, ignore_errors=True) for path in ['.pytest_cache', '.mypy_cache', '.ruff_cache', 'dist', 'build']]; [shutil.rmtree(path, ignore_errors=True) for path in pathlib.Path('.').rglob('__pycache__')]"

push: check ## Push the current branch to GitHub
	git push -u $(GIT_REMOTE) HEAD

pr: ## Create a GitHub pull request for the current branch (requires gh)
	@command -v gh >/dev/null || { echo "gh CLI is required for make pr" >&2; exit 1; }
	@gh pr view >/dev/null 2>&1 || gh pr create --fill

promote: push pr ## Run checks, push the branch, and create a PR if needed

release: build ## Tag and push a release; GitHub Actions publishes the artifacts
	@test -n "$(VERSION)" || { echo "Usage: make release VERSION=0.2.0" >&2; exit 2; }
	@test -z "$$(git status --porcelain)" || { echo "Working tree must be clean" >&2; exit 2; }
	git tag -a "v$(VERSION)" -m "Release v$(VERSION)"
	git push $(GIT_REMOTE) "v$(VERSION)"
