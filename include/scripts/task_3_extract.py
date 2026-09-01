"""Placeholder — stage 3 of the fantasy_ingestion DAG.

Runs after task_2_extract succeeds. Replace with real logic.
See task_1_extract.py for the pattern.
"""
import logging

log = logging.getLogger(__name__)


def run() -> None:
    log.info("task_3_extract: placeholder — no real logic yet")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
