"""Stage 2 — per-player fetch. The DAG maps this once per player_id via
Airflow dynamic task mapping (.expand()), so ~700+ instances run in
parallel, capped by AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG in
docker-compose.yaml (currently 4).

Reads:  nothing (takes player_id as input, from clean_players' return value)
Writes: data/csv/players/{player_id}/history.csv   (past-seasons summary)
        data/csv/players/{player_id}/gw.csv        (this season, gw-by-gw)
"""
import logging
from pathlib import Path

import pandas as pd

from scripts.fpl_api import get_individual_player_data

log = logging.getLogger(__name__)

DATA_DIR = Path("/opt/airflow/data/csv/players")


def run(player_id: int) -> None:
    data = get_individual_player_data(player_id)
    out_dir = DATA_DIR / str(player_id)
    out_dir.mkdir(parents=True, exist_ok=True)

    history_past = data.get("history_past", [])
    if history_past:
        pd.DataFrame(history_past).to_csv(out_dir / "history.csv", index=False)

    history = data.get("history", [])
    if history:
        pd.DataFrame(history).to_csv(out_dir / "gw.csv", index=False)

    log.info(
        "extract_player_gw: player_id=%s history_past=%d rows, history=%d rows",
        player_id, len(history_past), len(history),
    )


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    run(int(sys.argv[1]))
