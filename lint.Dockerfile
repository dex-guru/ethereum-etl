FROM python:3.10

RUN pip install --no-cache-dir --upgrade pip
RUN mkdir /tmp/app
WORKDIR /tmp/app
COPY requirements.txt /tmp/app/
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install --no-cache-dir \
    mypy \
    mypy-gitlab-code-quality \
    ruff \
    black

RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && npm install -g pyright \
    && npm install -g pyright-to-gitlab-ci \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /var/cache/*
