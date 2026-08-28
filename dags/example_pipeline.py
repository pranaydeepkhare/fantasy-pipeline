"""Smoke-test DAG — NOT the real fantasy pipeline.

Confirms the whole local stack actually works end to end:
  1. Airflow (LocalExecutor) can schedule a task.
  2. That task can start a single-JVM Spark session with Hive support
     (embedded Derby metastore, no standalone metastore service, no HDFS).
  3. Spark can read a CSV from the git-tracked data/ volume, register it
     as a Hive table, run a trivial HiveQL COUNT(*), and the result shows
     up in the Airflow task logs.

Replace this once the real ingestion DAGs exist.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pendulum
from airflow.decorators import dag, task

CSV_PATH = "/opt/airflow/data/csv/sample_players.csv"
WAREHOUSE_DIR = "/opt/airflow/data/spark-warehouse"
METASTORE_DIR = "/opt/airflow/data/metastore_db"

log = logging.getLogger(__name__)


@dag(
    dag_id="example_pipeline",
    description="Smoke test: CSV -> Spark local mode -> Hive (Derby) -> COUNT(*)",
    schedule=None,  # manual trigger only — this is not a real pipeline
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["smoke-test"],
)
def example_pipeline():
    @task
    def spark_hive_count() -> int:
        if not Path(CSV_PATH).exists():
            raise FileNotFoundError(
                f"{CSV_PATH} not found — is ./data mounted into the container?"
            )

        # Local import: keeps pyspark off the scheduler's DAG-parsing path,
        # it's only needed once this task actually runs.
        from pyspark.sql import SparkSession

        Path(WAREHOUSE_DIR).mkdir(parents=True, exist_ok=True)
        Path(METASTORE_DIR).mkdir(parents=True, exist_ok=True)

        # Hive's embedded Derby metastore uses Datanucleus bytecode
        # enhancement, which needs reflective access that JDK 17 blocks by
        # default (JEP 403 strong encapsulation). Without these --add-opens,
        # enableHiveSupport() fails with "Unable to instantiate
        # SessionHiveMetaStoreClient" on JDK 17.
        jdk17_add_opens = " ".join(
            f"--add-opens=java.base/{pkg}=ALL-UNNAMED"
            for pkg in [
                "java.lang",
                "java.lang.invoke",
                "java.lang.reflect",
                "java.io",
                "java.net",
                "java.nio",
                "java.util",
                "java.util.concurrent",
                "java.util.concurrent.atomic",
                "sun.nio.ch",
                "sun.nio.cs",
                "sun.security.action",
                "sun.util.calendar",
            ]
        )

        spark = (
            SparkSession.builder.appName("fantasy-pipeline-smoke-test")
            .master("local[2]")  # single JVM, no cluster
            .config("spark.driver.memory", "1g")
            .config("spark.driver.extraJavaOptions", jdk17_add_opens)
            .config("spark.sql.pyspark.jvmStacktrace.enabled", "true")
            .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
            .config(
                "javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={METASTORE_DIR};create=true",
            )
            .enableHiveSupport()
            .getOrCreate()
        )

        try:
            df = spark.read.csv(CSV_PATH, header=True, inferSchema=True)

            spark.sql("CREATE DATABASE IF NOT EXISTS smoke_test")
            df.write.mode("overwrite").saveAsTable("smoke_test.players")

            result = spark.sql("SELECT COUNT(*) AS row_count FROM smoke_test.players")
            row_count = result.collect()[0]["row_count"]

            log.info("Spark+Hive smoke test OK — smoke_test.players has %s rows", row_count)
            return row_count
        finally:
            spark.stop()

    spark_hive_count()


example_pipeline()
