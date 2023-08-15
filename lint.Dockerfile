FROM docker-registry.dexguru.biz/dex.guru/utils/python:3.10-slim as base
RUN pip install --no-cache-dir --upgrade pip

FROM base AS base-pyright
RUN apt-get update \
    && apt-get install -y curl \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && npm install -g pyright \
    && npm install -g pyright-to-gitlab-ci \
    && apt-get remove -y curl \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /var/cache/*

FROM base-pyright AS base-linters
RUN pip install --no-cache-dir \
    mypy \
    mypy-gitlab-code-quality \
    ruff \
    black

FROM base-linters
RUN mkdir /tmp/app
WORKDIR /tmp/app
COPY requirements.txt /tmp/app/
RUN pip install --no-cache-dir -r requirements.txt

USER nobody
