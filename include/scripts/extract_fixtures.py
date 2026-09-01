"""Stage 0b — pull the season's fixture list from the FPL API.

Writes: data/csv/fixtures.csv

No dependency on any other stage — runs in parallel with extract_bootstrap.
"""
import logging
from pathlib import Path

import pandas as pd

from scripts.fpl_api import get_fixtures_data

log = logging.getLogger(__name__)

DATA_DIR = Path("/opt/airflow/data/csv")


def run() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    fixtures = get_fixtures_data()
    fixtures_df = pd.DataFrame(fixtures)
    fixtures_df.to_csv(DATA_DIR / "fixtures.csv", index=False)
    log.info("extract_fixtures: wrote %d fixtures to fixtures.csv", len(fixtures_df))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
