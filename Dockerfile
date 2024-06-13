FROM python:3.10-slim

RUN pip install --no-cache-dir --upgrade pip
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*
RUN mkdir /tmp/app
WORKDIR /tmp/app
COPY requirements.txt /tmp/app/
RUN pip install --no-cache-dir -r requirements.txt
COPY . /tmp/app
RUN pip install --no-cache-dir '.[streaming]'
WORKDIR /
RUN rm -rf /tmp/app
USER nobody
RUN ethereumetl stream --help

CMD ["ethereumetl", "stream", "--help"]

