# Airflow's Linux image avoids installing Spark/Java/Airflow directly on Windows.
FROM apache/airflow:2.10.5-python3.11
USER root
# Spark needs Java. The JDBC driver lets Spark read PostgreSQL tables.
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /opt/jdbc && curl -fsSL https://jdbc.postgresql.org/download/postgresql-42.7.5.jar -o /opt/jdbc/postgresql.jar
USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
# Separate environments prevent dbt's protobuf requirements from changing Airflow.
COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /home/airflow/dbt-venv && /home/airflow/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYTHONPATH=/opt/demo
WORKDIR /opt/demo
