"""Stage 3b — fan-in placeholder, runs after collect_gw.

Real logic (not yet ported) should mirror the reference repo's
collector.merge_gw() / regenerate_merged_gw(): append/rebuild
merged_gw.csv across gameweeks, handling schema drift between weeks.
"""
import logging

log = logging.getLogger(__name__)


def run() -> None:
    log.info("merge_gw: placeholder — no real logic yet")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
