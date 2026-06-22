FROM apache/airflow:2.10.4-python3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    docker.io \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY source/requirements.txt /tmp/requirements-source.txt

RUN pip install --no-cache-dir -r /tmp/requirements-source.txt
