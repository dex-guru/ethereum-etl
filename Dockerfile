FROM python:3.10

RUN pip install --upgrade pip
RUN mkdir /tmp/app
WORKDIR /tmp/app
COPY requirements.freeze.txt /tmp/app/
RUN pip install --no-cache-dir -r requirements.freeze.txt
COPY . /tmp/app
RUN pip install --no-cache-dir '.[streaming]'
RUN cd / && ethereumetl stream --help

FROM python:3.10
COPY --from=0 /usr/local /usr/local
CMD ["ethereumetl", "stream", "--help"]
