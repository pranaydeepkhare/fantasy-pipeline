"""Stage 0 — pull bootstrap-static (players, teams, gameweeks) from the FPL API.

Writes:
  data/csv/players_raw.csv   — one row per player (element), all fields FPL returns
  data/csv/teams.csv         — one row per Premier League team
  data/csv/events.csv        — one row per gameweek (event), including which is current

No dependency on any other stage — runs in parallel with extract_fixtures.
Everything downstream (clean_players, and transitively the per-player
fan-out) depends on players_raw.csv existing.
"""
import logging
from pathlib import Path

import pandas as pd

from scripts.fpl_api import get_data

log = logging.getLogger(__name__)

DATA_DIR = Path("/opt/airflow/data/csv")


def run() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    bootstrap = get_data()

    players_df = pd.DataFrame(bootstrap["elements"])
    players_df.to_csv(DATA_DIR / "players_raw.csv", index=False)
    log.info("extract_bootstrap: wrote %d players to players_raw.csv", len(players_df))

    teams_df = pd.DataFrame(bootstrap["teams"])
    teams_df.to_csv(DATA_DIR / "teams.csv", index=False)
    log.info("extract_bootstrap: wrote %d teams to teams.csv", len(teams_df))

    events_df = pd.DataFrame(bootstrap["events"])
    events_df.to_csv(DATA_DIR / "events.csv", index=False)
    current = events_df.loc[events_df["is_current"] == True, "id"]
    current_gw = int(current.iloc[0]) if not current.empty else None
    log.info(
        "extract_bootstrap: wrote %d events to events.csv (current GW=%s)",
        len(events_df), current_gw,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
