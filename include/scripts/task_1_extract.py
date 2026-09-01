"""Placeholder — stage 1 of the fantasy_ingestion DAG.

Replace with real logic: pull raw data from <source TBD> and land it as a
CSV under data/csv/ and/or a Hive table (via Spark — see
dags/example_pipeline.py for the enableHiveSupport() pattern).

Keep the `run()` signature — the DAG imports and calls it directly, and it
also works standalone for local testing:
    python include/scripts/task_1_extract.py
"""
import logging

log = logging.getLogger(__name__)


def run() -> None:
    log.info("task_1_extract: placeholder — no real logic yet")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
