# Airflow image + a minimal JRE so PySpark can run inside the same container.
# No Hadoop/YARN, no standalone Hive metastore — Spark runs in local mode
# with its embedded Derby metastore (enableHiveSupport()).
FROM apache/airflow:2.10.5-python3.11

USER root

# Headless JRE is enough to run Spark's local-mode JVM (driver == executor).
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
