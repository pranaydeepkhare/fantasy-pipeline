import time
import requests

BASE_URL = "https://fantasy.premierleague.com/api"
DEFAULT_TIMEOUT = 30
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_BACKOFF_SECONDS = 5


def _get_json(url: str, max_attempts: int = DEFAULT_MAX_ATTEMPTS) -> dict | list:
    """Shared fetch-with-retry logic used by every getter below."""
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(url, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_error = e
            if attempt < max_attempts:
                time.sleep(DEFAULT_BACKOFF_SECONDS)
        except requests.exceptions.HTTPError as e:
            # 4xx/5xx — don't blindly retry a 404, but do retry a 5xx/429
            status = e.response.status_code if e.response is not None else None
            if status in (429, 500, 502, 503, 504) and attempt < max_attempts:
                last_error = e
                time.sleep(DEFAULT_BACKOFF_SECONDS)
            else:
                raise

    raise RuntimeError(f"Failed to fetch {url} after {max_attempts} attempts") from last_error


def get_data() -> dict:
    """Fetch bootstrap-static: elements, teams, events, element_types, phases."""
    return _get_json(f"{BASE_URL}/bootstrap-static/")


def get_fixtures_data() -> list:
    """Fetch the full fixtures list for the season."""
    return _get_json(f"{BASE_URL}/fixtures/")


def get_individual_player_data(player_id: int) -> dict:
    """Fetch element-summary for one player: history, history_past, fixtures."""
    return _get_json(f"{BASE_URL}/element-summary/{player_id}/")


def get_live_gameweek_data(gw: int) -> dict:
    """Fetch live per-player stats for one gameweek."""
    return _get_json(f"{BASE_URL}/event/{gw}/live/")


if __name__ == "__main__":
    # quick manual smoke test
    bootstrap = get_data()
    print("elements:", len(bootstrap["elements"]))
    print("teams:", len(bootstrap["teams"]))

    fixtures = get_fixtures_data()
    print("fixtures:", len(fixtures))
