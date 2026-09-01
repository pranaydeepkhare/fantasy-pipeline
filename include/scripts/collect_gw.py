"""Stage 3 — fan-in placeholder.

Real logic (not yet ported) should mirror the reference repo's
collector.collect_gw(): for the current gameweek, join every player's
gw.csv (written by extract_player_gw) with teams.csv / fixtures.csv /
cleaned_players.csv into one gw{N}.csv row set.

Waits for ALL extract_player_gw mapped instances to finish, plus
extract_fixtures — see dags/fantasy_ingestion.py for the dependency
wiring.
"""
import logging

log = logging.getLogger(__name__)


def run() -> None:
    log.info("collect_gw: placeholder — no real logic yet")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
