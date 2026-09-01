"""fantasy_ingestion — scaffold DAG, NOT wired to real data yet.

Four sequential placeholder stages (task_1_extract -> task_2_extract ->
task_3_extract -> task_4_extract), each backed by its own script under
include/scripts/. Rename tasks/scripts and fill in the real
extraction + conversion-to-table (CSV or Hive) logic per stage as the
data source firms up.

Runs successfully today as a no-op — useful for confirming the wiring
before any real logic exists. See dags/example_pipeline.py for the
Spark + Hive pattern each stage will likely use once it does real work.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

from scripts.task_1_extract import run as run_task_1
from scripts.task_2_extract import run as run_task_2
from scripts.task_3_extract import run as run_task_3
from scripts.task_4_extract import run as run_task_4


@dag(
    dag_id="fantasy_ingestion",
    description="Scaffold: 4 sequential extraction/conversion stages (placeholders)",
    schedule=None,  # manual trigger only until the real pipeline is ready
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["scaffold", "ingestion"],
)
def fantasy_ingestion():
    @task(task_id="task_1_extract")
    def task_1():
        run_task_1()

    @task(task_id="task_2_extract")
    def task_2():
        run_task_2()

    @task(task_id="task_3_extract")
    def task_3():
        run_task_3()

    @task(task_id="task_4_extract")
    def task_4():
        run_task_4()

    task_1() >> task_2() >> task_3() >> task_4()


fantasy_ingestion()
