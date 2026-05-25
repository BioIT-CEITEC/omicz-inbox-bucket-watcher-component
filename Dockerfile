FROM python:3.14-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv


RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project

COPY . .

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-cache


ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PATH="/app/.venv/bin:$PATH"

ENTRYPOINT ["python", "-m", "src"]
CMD ["--config", "config.yaml"]
