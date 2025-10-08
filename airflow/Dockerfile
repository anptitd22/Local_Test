FROM apache/airflow:3.0.0

USER root

RUN apt-get update && apt-get install -y curl && apt-get install -y git && rm -rf /var/lib/apt/lists/*

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk curl gcc python3-dev libffi-dev libssl-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# location java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

USER airflow

ENV PATH="/home/airflow/.local/bin:${PATH}"

# install my_minio & boto3
RUN pip install --no-cache-dir minio boto3

# file dataset
COPY ../dataset /opt/airflow/dataset

COPY ../src /opt/airflow/src

ENV PYTHONPATH="/opt/airflow/src:/opt/airflow/dags:/opt/airflow/plugins:${PYTHONPATH}"
