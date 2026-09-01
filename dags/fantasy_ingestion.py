"""fantasy_ingestion — FPL extract/load/transform pipeline.

Shape (see README's "Ingestion scaffold" section for the full reasoning):

    extract_bootstrap ───┐
                          ├─→ clean_players ─→ extract_player_gw (mapped, 1 per player, parallel)
    extract_fixtures ─────┼──────────────────────────────────────┘
                          └─────────────────────────────────────→ collect_gw ─→ merge_gw

- extract_bootstrap / extract_fixtures: independent API pulls, run in parallel.
- clean_players: needs players_raw.csv from extract_bootstrap; produces the
  player-ID list every per-player fetch fans out over.
- extract_player_gw: Airflow dynamic task mapping (.expand()) — one mapped
  instance per player, ~700+ of them, capped by
  AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG in docker-compose.yaml.
- collect_gw: fan-in — waits for every extract_player_gw instance AND
  extract_fixtures. Still a placeholder (see include/scripts/collect_gw.py).
- merge_gw: placeholder, runs after collect_gw.

Trigger with an optional player_limit param for fast local testing, e.g.:
    airflow dags trigger fantasy_ingestion --conf '{"player_limit": 5}'
Leave player_limit null (the default) for a full run over every player.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

from scripts import (
    clean_players,
    collect_gw,
    extract_bootstrap,
    extract_fixtures,
    extract_player_gw,
    merge_gw,
)


@dag(
    dag_id="fantasy_ingestion",
    description="FPL extract/load/transform: bootstrap+fixtures (parallel) -> clean/id -> per-player fetch (parallel fan-out) -> collect_gw -> merge_gw",
    schedule=None,  # manual trigger only for now
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ingestion", "fpl"],
    params={
        "player_limit": Param(
            None,
            type=["null", "integer"],
            description="Cap the number of players fetched in the per-player "
            "fan-out stage, for fast local testing. Leave null for all players.",
        )
    },
)
def fantasy_ingestion():
    @task(task_id="extract_bootstrap")
    def extract_bootstrap_task():
        extract_bootstrap.run()

    @task(task_id="extract_fixtures")
    def extract_fixtures_task():
        extract_fixtures.run()

    @task(task_id="clean_players")
    def clean_players_task(**context) -> list[int]:
        limit = context["params"].get("player_limit")
        return clean_players.run(player_limit=limit)

    @task(task_id="extract_player_gw")
    def extract_player_gw_task(player_id: int):
        extract_player_gw.run(player_id)

    @task(task_id="collect_gw")
    def collect_gw_task():
        collect_gw.run()

    @task(task_id="merge_gw")
    def merge_gw_task():
        merge_gw.run()

    bootstrap = extract_bootstrap_task()
    fixtures = extract_fixtures_task()
    clean = clean_players_task()
    player_fetches = extract_player_gw_task.expand(player_id=clean)
    collect = collect_gw_task()
    merge = merge_gw_task()

    bootstrap >> clean
    [player_fetches, fixtures] >> collect >> merge


fantasy_ingestion()
