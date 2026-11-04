# One Dockerfile, two images:
#   stream  producer and consumer (small, runs 24/7)
#   batch   Spark jobs (adds Java and PySpark; added in step 7.1d)
#
#   docker build --target stream -t tfl-stream .

# ---- base: what every target shares -------------------------------------
FROM python:3.12-slim AS base

# Logs appear immediately in `docker logs`; no pip download cache in the image.
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Run as an unprivileged user, not root.
RUN useradd --create-home --uid 1000 app

# ---- stream: producer and consumer --------------------------------------
FROM base AS stream

# Rarely-changing project metadata first, then the code, so Docker can reuse
# the cached layers above when only the code changes.
COPY pyproject.toml ./
COPY src/ ./src/
# Remove the source and build leftovers: the installed package is all we need.
RUN pip install . && rm -rf /app/src /app/build

USER app

# No CMD on purpose: docker-compose sets the command for each service, e.g.
#   ["python", "-m", "tfl_pipeline.producer"]
