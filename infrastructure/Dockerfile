# OPEN-193: containerizes ddp-sync, following ddp-broker-py's own
# infra/docker/Dockerfile.broker pattern (non-root "python" user, pip install from a plain
# requirements-style install, gunicorn/uvicorn as the final CMD) so all three services on this
# host end up managed the same way.
#
# OPEN-248: cloud_loader.py's import step shells out to `os-update` (openstates-core's CLI),
# which needs its own Python 3.9 + openstates-core/openstates-scrapers environment -- a
# completely different, older toolchain than ddp-sync's own Python 3.11 app. This used to be
# satisfied by bind-mounting the EC2 host's own /opt/ddp-open-states venv into this container,
# but that venv's own python3.9 binary is a symlink to the HOST's system interpreter, which
# doesn't exist inside this image at all -- found live 2026-09-02, "FileNotFoundError:
# os-update", plus a real risk of a glibc/shared-library mismatch even if the binary itself
# were bind-mounted directly. The whole point of Docker is that the image carries its own
# runtime, not one borrowed from whatever host happens to run it -- so this toolchain is now
# built INTO the image, in its own stage, exactly like the root ddp-open-states Dockerfile
# already does it for the Fargate scraper image (same two-stage builder pattern, same GitHub
# PAT secret mount, same editable openstates-core install). The builder stage below is pinned
# to bookworm (matching this file's own final-stage Debian release) specifically so the
# self-contained venv it produces is glibc-compatible with the image it gets copied into --
# using a DIFFERENT Debian release for builder vs. final would silently reproduce this exact
# bug one layer down.

FROM python:3.9-slim-bookworm AS openstates-builder

# git: clones the two forks below. build-essential/libxml2-dev/libxslt-dev/libssl-dev: in case
# a package in requirements-openstates.txt (lxml, cryptography) has no prebuilt wheel for this
# platform and falls back to a source build. Mirrors ddp-open-states' own root Dockerfile
# exactly -- see that file's own comments for the reasoning behind each choice here.
RUN apt-get update && apt-get install -y --no-install-recommends \
        git build-essential libxml2-dev libxslt1-dev libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# requirements-openstates.txt lives in the SEPARATE ddp-open-states repo, not this one --
# reached via BuildKit's additional build context (docker-compose.prod.yml's
# `additional_contexts: openstates=/opt/ddp-open-states`, matching that same absolute path
# OPENSTATES_ROOT already points at below), not a duplicated copy of the file in this repo.
# Duplicating a ~150-line pinned-dependency file that already has exactly one owner is the
# kind of drift this project has been careful to avoid elsewhere; referencing the real file
# directly, on the one host layout this image is actually built on, avoids it entirely.
COPY --from=openstates requirements-openstates.txt .
RUN python3 -m venv /opt/venv-openstates \
    && /opt/venv-openstates/bin/pip install --no-cache-dir 'pip<24.1' \
    && /opt/venv-openstates/bin/pip install --no-cache-dir --no-deps -r requirements-openstates.txt

# venv's own bin/python3 is an ABSOLUTE symlink to the generic /usr/local/bin/python3 -- fine
# within this builder stage alone, where that name unambiguously means python3.9. Confirmed
# live: copied as-is into the final (python:3.11-slim-bookworm) stage, it silently resolves
# through THAT image's own pre-existing /usr/local/bin/python3 -> python3.11 instead (the
# generic name is already claimed there) -- worse than a clean crash, os-update partially
# starts under the wrong Python entirely, then fails with "No module named 'openstates'"
# since 3.11 doesn't share 3.9's site-packages. Repointing these three symlinks at the
# unambiguous, version-specific /usr/local/bin/python3.9 (also copied into the final stage
# below, at that exact same path) makes the venv correct regardless of whatever OTHER Python
# install coexists alongside it.
RUN rm -f /opt/venv-openstates/bin/python3 /opt/venv-openstates/bin/python3.9 /opt/venv-openstates/bin/python \
    && ln -s /usr/local/bin/python3.9 /opt/venv-openstates/bin/python3.9 \
    && ln -s python3.9 /opt/venv-openstates/bin/python3 \
    && ln -s python3 /opt/venv-openstates/bin/python

# Cloned via a BuildKit secret (a GitHub PAT) so the credential is never written into an image
# layer, exactly as the root Dockerfile's own builder stage does it. .git removed in this same
# layer, before it can ever occupy a final-image layer -- see that file's own comment for why
# that matters (a `git clone` URL embeds the token in .git/config, and the secret mount alone
# does not keep it out of the checkout it produces).
ARG OPENSTATES_CORE_REF=main
ARG OPENSTATES_SCRAPERS_REF=main
RUN --mount=type=secret,id=github_token \
    TOKEN=$(cat /run/secrets/github_token) && \
    git clone --branch "$OPENSTATES_CORE_REF" --depth 1 \
        "https://x-access-token:${TOKEN}@github.com/Digital-Democracy-Project/openstates-core.git" \
        /opt/openstates-core && \
    git clone --branch "$OPENSTATES_SCRAPERS_REF" --depth 1 \
        "https://x-access-token:${TOKEN}@github.com/Digital-Democracy-Project/openstates-scrapers.git" \
        /opt/openstates-scrapers && \
    rm -rf /opt/openstates-core/.git /opt/openstates-scrapers/.git

# Editable install for openstates-core only -- DDP's patches take effect this way, exactly as
# the root Dockerfile's own venv does it. openstates-scrapers is deliberately NOT pip-installed
# (see PYTHONPATH below and the root Dockerfile's own note on why an editable install of that
# package fails outright).
RUN /opt/venv-openstates/bin/pip install --no-cache-dir --no-deps -e /opt/openstates-core


FROM python:3.11-slim-bookworm AS app

LABEL description="ddp-sync"

# libgdal32: os-update --import loads Django, which eagerly imports django.contrib.gis at
# startup (a GIS-enabled model field somewhere in openstates-core's schema) -- that module
# loads libgdal via ctypes at IMPORT time, not something a Python dependency pin can satisfy.
# Found missing by actually running a real --import here (2026-09-02): "Could not find the
# GDAL library ... Is GDAL installed?" -- runtime-only (ctypes.util.find_library(), not a
# build-time link), so the bare runtime package is enough; no -dev headers or CLI tools needed.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgdal32 \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1

ARG UID=1000
ARG GID=1000

RUN groupadd -g "${GID}" python \
    && useradd --create-home --no-log-init -u "${UID}" -g "${GID}" python

RUN mkdir -p /app && chown -R python:python /app

# OPEN-248: the bundled openstates toolchain, copied in before switching to the non-root
# "python" user below -- matching the root ddp-open-states Dockerfile's own final stage,
# which copies its equivalent /opt/venv the same way, before its own USER switch, with no
# extra chown needed (a standard venv/pip install is world-readable+executable by default).
#
# The venv alone is not enough: its own bin/python3.9 is a symlink back to whatever created
# it (the builder stage's SYSTEM python3.9), which does not exist anywhere in this final
# image at all -- confirmed live: without also bringing that system install over, the
# symlink silently resolved to THIS image's own python3.11 instead (worse than a clean
# crash -- it partially started, then failed with "No module named 'openstates'", since
# 3.11 doesn't share 3.9's site-packages). These three paths are additive, not overwrites --
# python3.9-named files/dirs never collide with this image's own python3.11-named ones.
COPY --from=openstates-builder /usr/local/bin/python3.9 /usr/local/bin/python3.9
COPY --from=openstates-builder /usr/local/lib/python3.9 /usr/local/lib/python3.9
COPY --from=openstates-builder /usr/local/lib/libpython3.9.so.1.0 /usr/local/lib/libpython3.9.so.1.0
COPY --from=openstates-builder /opt/venv-openstates /opt/venv-openstates
COPY --from=openstates-builder /opt/openstates-core /opt/openstates-core
COPY --from=openstates-builder /opt/openstates-scrapers /opt/openstates-scrapers

# pm-review: PYTHONPATH pointing at openstates-scrapers must not leak into ddp-sync's OWN
# Python 3.11 app -- a bare `ENV PYTHONPATH=...` here would apply to every process in this
# image, this one included. Checked live that nothing in ddp-sync's own source or the
# openstates-scrapers top-level package names (two-letter state codes, "utils", "config",
# "usa") actually collides today, but "checked safe today" isn't the same guarantee as
# "structurally can't leak" -- a thin wrapper scopes it correctly by construction: exported
# only inside this script's own exec, never touching ddp-sync's own already-running
# environment. openstates-scrapers is deliberately NOT pip-installed (its pyproject.toml's
# pre-poetry-core backend has no PEP 660 editable-install support, per the root Dockerfile's
# own note), so its scrapers/ subdirectory is pointed at this way instead of an install.
RUN printf '#!/bin/sh\nexport PYTHONPATH=/opt/openstates-scrapers/scrapers\nexec /opt/venv-openstates/bin/os-update "$@"\n' \
        > /opt/venv-openstates/bin/os-update-wrapper \
    && chmod +x /opt/venv-openstates/bin/os-update-wrapper

# cloud_loader.py's _run_import() does `os.environ.get("OS_UPDATE", "os-update")` with no
# explicit `env=` override on its own subprocess.Popen call, so it inherits this container's
# full environment -- this is what actually makes that inherited environment point at the
# bundled toolchain rather than a bare, unresolvable "os-update" PATH lookup.
ENV OS_UPDATE=/opt/venv-openstates/bin/os-update-wrapper

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
