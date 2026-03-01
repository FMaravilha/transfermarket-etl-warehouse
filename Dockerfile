FROM apache/airflow:2.9.3

USER root

RUN apt-get update \
    && apt-get install -y ca-certificates \
    && update-ca-certificates

USER airflow