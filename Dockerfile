# syntax=docker/dockerfile:1

FROM rust:1.78.0-slim-bookworm AS rust-toolchain

# Stage 1: Builder - Install dependencies with build tools
FROM python:3.13-slim AS builder

COPY --from=rust-toolchain /usr/local/cargo /usr/local/cargo
COPY --from=rust-toolchain /usr/local/rustup /usr/local/rustup
ENV CARGO_HOME=/usr/local/cargo RUSTUP_HOME=/usr/local/rustup

WORKDIR /opt

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:/usr/local/cargo/bin:$PATH"

# Install Python dependencies into venv
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt

# Native wheel built for this image's architecture; no Rust tools in runtime.
COPY native/trev /opt/native/trev
RUN pip install --no-cache-dir /opt/native/trev

# Stage 2: Runtime - Lean production image
FROM python:3.13-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV TREV_COLD_BACKEND=rust

WORKDIR /code

# Existing Compose healthchecks use pgrep to verify long-running workers.
RUN apt-get update && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder (no gcc/build-essential!)
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy application code (but volume mount overrides in development)
COPY . /code/
