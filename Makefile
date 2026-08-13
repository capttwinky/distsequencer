SHELL := /bin/bash
.DEFAULT_GOAL := help

UV ?= uv
UV_CACHE_DIR ?= $(CURDIR)/.uv-cache
PYTHON_VERSION ?= 3.12
JUPYTER_IP ?= 127.0.0.1
JUPYTER_PORT ?= 8888
GIT_REMOTE ?= origin
VERSION ?=
IMAGE ?= distsequencer:latest
CONTAINER ?= docker

export UV_CACHE_DIR

.PHONY: help bootstrap sync ml lab lab-server lab-remote sim demo benchmark pki manifest container-build container-run docker-build docker-run podman-build podman-run test test-unit test-bdd lint format format-check typecheck check build clean push pr promote release

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*## "; printf "\nTargets:\n"} /^[a-zA-Z0-9_.-]+:.*## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

bootstrap: ## Install Python with uv and synchronize the environment
	$(UV) python install $(PYTHON_VERSION)
	$(UV) sync --python $(PYTHON_VERSION)

sync: ## Synchronize the uv environment
	$(UV) sync --python $(PYTHON_VERSION)

ml: ## Synchronize optional ML dependency group
	$(UV) sync --python $(PYTHON_VERSION) --group ml

lab: ## Launch JupyterLab locally
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

container-build: ## Build the runtime container image
	$(CONTAINER) build -t $(IMAGE) .

container-run: ## Run the simulator in the runtime container image
	$(CONTAINER) run --rm $(IMAGE) sim

docker-build: container-build ## Build with Docker

docker-run: container-run ## Run with Docker

podman-build: ## Build with Podman
	$(MAKE) container-build CONTAINER=podman

podman-run: ## Run with Podman
	$(MAKE) container-run CONTAINER=podman

test: ## Run unit and BDD tests
	$(UV) run pytest -q

test-unit: ## Run non-BDD unit tests
	$(UV) run pytest -q tests -k "not bdd"

test-bdd: ## Run BDD scenarios
	$(UV) run pytest -q tests/test_bdd_autonomy.py

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
