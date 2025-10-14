FROM apache/airflow:3.0.0
ARG AIRFLOW_VERSION=3.0.0
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt"

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

RUN pip install --no-cache-dir --constraint "${CONSTRAINT_URL}" \
    apache-airflow-providers-trino \
    apache-airflow-providers-common-sql \
    trino \
    sqlalchemy-trino

# install 
RUN pip install --no-cache-dir \
    "pyarrow==17.0.0" \
    "pydantic<=2.12.0" \
    minio \
    boto3 \
    "pyiceberg==0.10.0" \
    "pyiceberg[hive]==0.10.0" \
    "pyiceberg[pyiceberg-core]==0.10.0"

# file dataset
# COPY ../dataset /opt/airflow/dataset

# COPY ../src /opt/airflow/src

# ENV PYTHONPATH="/opt/airflow/src:/opt/airflow/dags:/opt/airflow/plugins:${PYTHONPATH}"

ENV PYTHONPATH="/opt/airflow/dags:/opt/airflow/plugins:${PYTHONPATH}"
