# One Dockerfile, two images:
#   stream  producer and consumer (small, runs 24/7)
#   batch   Spark jobs (adds Java, PySpark and the S3 connector)
#
#   docker build --target stream -t tfl-stream .
#   docker build --target batch  -t tfl-batch  .

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

# ---- batch: Spark jobs ---------------------------------------------------
FROM base AS batch

# Java 21 runtime only (no compiler, no GUI libraries); drop apt's package lists
# in the same layer so they don't take up space in the image.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# PySpark before the code: it's large and rarely changes, so this layer stays cached.
RUN pip install "pyspark>=4.0"

# Download the S3A connector (hadoop-aws + AWS SDK, ~675 MB) at build time, as the
# app user so it lands in /home/app/.ivy2*. Containers start empty, so otherwise every
# run would download it again. Keep the version in sync with jobs/common.py.
# Ivy keeps each jar twice (its cache plus a jars/ copy for Spark); hard-linking the
# copies to the cache files in the same layer stores them only once (saves ~675 MB).
USER app
RUN python -c "from pyspark.sql import SparkSession; \
SparkSession.builder.master('local[1]') \
.config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.5.0') \
.getOrCreate().stop()" \
    && cd /home/app/.ivy2* \
    && for jar in jars/*.jar; do \
         ln -f "$(find cache -name "${jar#jars/*_}" | head -n 1)" "$jar"; \
       done
USER root

# The code last: a code change only rebuilds from here.
COPY pyproject.toml ./
COPY src/ ./src/
RUN pip install ".[spark]" && rm -rf /app/src /app/build

# Quieter Spark logs, the same settings as locally.
COPY conf/ ./conf/
ENV SPARK_CONF_DIR=/app/conf

USER app

# No CMD on purpose: each run says which job and day, e.g.
#   python -m tfl_pipeline.jobs.silver.arrivals --lake s3a://<bucket> --dt 2026-11-03
