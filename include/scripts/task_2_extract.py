"""Placeholder — stage 2 of the fantasy_ingestion DAG.

Runs after task_1_extract succeeds. Replace with real logic.
See task_1_extract.py for the pattern.
"""
import logging

log = logging.getLogger(__name__)


def run() -> None:
    log.info("task_2_extract: placeholder — no real logic yet")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
