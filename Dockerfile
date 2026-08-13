# syntax=docker/dockerfile:1.7

FROM ghcr.io/astral-sh/uv:0.8.24-python3.12-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_DEV=1

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY src ./src

RUN uv sync --locked --no-dev

FROM python:3.12-slim-bookworm AS runtime

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

RUN addgroup --system distsequencer \
    && adduser --system --ingroup distsequencer --home /app distsequencer

COPY --from=builder --chown=distsequencer:distsequencer /app /app

USER distsequencer

ENTRYPOINT ["distsequencer"]
CMD ["sim"]
