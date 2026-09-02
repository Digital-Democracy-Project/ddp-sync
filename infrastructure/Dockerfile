# OPEN-193: containerizes ddp-sync, following ddp-broker-py's own
# infra/docker/Dockerfile.broker pattern (non-root "python" user, pip install from a plain
# requirements-style install, gunicorn/uvicorn as the final CMD) so all three services on this
# host end up managed the same way.

FROM python:3.11-slim-bookworm AS app

LABEL description="ddp-sync"

ENV PYTHONUNBUFFERED=1

ARG UID=1000
ARG GID=1000

RUN groupadd -g "${GID}" python \
    && useradd --create-home --no-log-init -u "${UID}" -g "${GID}" python

RUN mkdir -p /app && chown -R python:python /app

USER python
WORKDIR /app

ENV PATH="${PATH}:/home/python/.local/bin"

# No lock file (pyproject.toml is the only dependency source, see PRIMITIVES.md/README) --
# installed straight from it rather than a generated requirements.txt, matching how this repo
# is actually installed for local dev (`pip install -e .`).
COPY --chown=python:python pyproject.toml /app/pyproject.toml
COPY --chown=python:python src/ /app/src/
RUN pip install --no-cache-dir --user .

EXPOSE 8001

CMD ["python3", "-m", "uvicorn", "ddp_sync.app:app", "--host", "0.0.0.0", "--port", "8001", "--workers", "1"]
