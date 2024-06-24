FROM python:3.10-slim

RUN pip install --no-cache-dir --upgrade pip
RUN apt-get update && apt-get install -y --no-install-recommends \
    git gcc g++ \
    && rm -rf /var/lib/apt/lists/*
RUN mkdir /tmp/app
WORKDIR /tmp/app
COPY requirements.txt /tmp/app/
RUN pip install --no-cache-dir -r requirements.txt
COPY . /tmp/app
RUN pip install --no-cache-dir '.[streaming]'
RUN apt-get purge -y gcc
RUN apt-get autoremove -y
WORKDIR /
RUN mkdir /app
WORKDIR /app
COPY alembic.ini /app
COPY db /app/db
RUN rm -rf /tmp/app
USER nobody
RUN ethereumetl stream --help

CMD ["ethereumetl", "stream", "--help"]

