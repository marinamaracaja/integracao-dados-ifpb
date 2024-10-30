FROM apache/airflow:2.10.2

USER root
RUN apt-get update

USER airflow

RUN pip install -U pip
RUN pip install --no-cache-dir minio