"""Stage 1 — clean players_raw.csv and produce the player-ID list every
downstream per-player task fans out over.

Reads:   data/csv/players_raw.csv (written by extract_bootstrap)
Writes:  data/csv/cleaned_players.csv, data/csv/player_idlist.csv
Returns: list[int] of player IDs — the DAG maps extract_player_gw over
         this return value via Airflow dynamic task mapping (.expand()).
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

DATA_DIR = Path("/opt/airflow/data/csv")

POSITION_MAP = {1: "GK", 2: "DEF", 3: "MID", 4: "FWD", 5: "AM"}

CLEANED_COLUMNS = [
    "id", "first_name", "second_name", "web_name", "team", "element_type",
    "now_cost", "total_points", "minutes", "goals_scored", "assists",
    "clean_sheets", "goals_conceded", "bonus", "bps", "influence",
    "creativity", "threat", "ict_index", "selected_by_percent",
    "yellow_cards", "red_cards", "value_per_m",
]


def run(player_limit: int | None = None) -> list[int]:
    raw_path = DATA_DIR / "players_raw.csv"
    if not raw_path.exists():
        raise FileNotFoundError(f"{raw_path} not found — did extract_bootstrap run first?")

    df = pd.read_csv(raw_path)
    df["position"] = df["element_type"].map(POSITION_MAP)

    # value_per_m = total_points / (now_cost / 10); now_cost is in tenths of £m
    cost_m = df["now_cost"] / 10.0
    df["value_per_m"] = (df["total_points"] / cost_m).where(cost_m > 0).round(1)

    cleaned = df[[c for c in CLEANED_COLUMNS if c in df.columns] + ["position"]]
    cleaned.to_csv(DATA_DIR / "cleaned_players.csv", index=False)
    log.info("clean_players: wrote %d rows to cleaned_players.csv", len(cleaned))

    idlist = df[["id", "first_name", "second_name"]]
    idlist.to_csv(DATA_DIR / "player_idlist.csv", index=False)

    player_ids = idlist["id"].astype(int).tolist()
    if player_limit is not None:
        player_ids = player_ids[:player_limit]
        log.info("clean_players: player_limit=%d applied", player_limit)

    log.info("clean_players: %d player IDs ready for per-player fan-out", len(player_ids))
    return player_ids


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ids = run()
    print(f"{len(ids)} player IDs")
