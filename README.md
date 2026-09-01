# fantasy-pipeline

Local dev environment for fantasy-team-optimization data work: Airflow
orchestrates DAGs that call Spark (local mode, embedded Hive metastore) over
SQL/HQL scripts checked into this repo.

**Scope of this repo right now:** infrastructure + a smoke-test DAG only.
Real data ingestion and the PuLP/OR-Tools optimizer are separate,
not-yet-started pieces of work (see "Out of scope" below).

## Stack

| Component | How it runs | Notes |
|---|---|---|
| Airflow webserver + scheduler | Docker Compose, `LocalExecutor` | No Celery/Redis |
| Airflow metadata DB | Postgres 13 (Docker), metadata only | |
| Spark | Local mode (`local[2]`), single JVM, inside the Airflow container | `enableHiveSupport()` + embedded Derby metastore. No HDFS, no YARN, no standalone Hive metastore service. |

## Repo layout

```
fantasy-pipeline/
├── docker-compose.yaml
├── Dockerfile              # Airflow base image + JRE + pyspark
├── dags/                   # Airflow DAG definitions (Python)
│   ├── example_pipeline.py    # smoke test: CSV -> Spark/Hive -> COUNT(*)
│   └── fantasy_ingestion.py   # scaffold: 4 sequential placeholder stages
├── include/
│   ├── sql/                # .sql files
│   ├── hql/                # .hql / HiveQL scripts
│   └── scripts/             # Python extraction/conversion scripts, one per DAG stage
├── data/
│   └── csv/                # raw input CSVs land here (gitignored except the sample)
├── optimizer/              # PuLP/OR-Tools code — empty for now, separate from the SQL layer
├── requirements.txt        # pip deps installed into the Airflow image (pyspark, pandas)
└── README.md
```

## Prerequisites

- Docker Desktop (WSL2 backend) — installed and running.
- Git.

## Start the stack

```bash
cd fantasy-pipeline
docker compose build      # first time only, or after Dockerfile/requirements.txt changes
docker compose up -d
```

Airflow UI: http://localhost:8080 — login `admin` / `admin` (dev-only credentials,
created by the one-shot `airflow-init` service; change before this ever leaves
your laptop).

Wait for `airflow-webserver` and `airflow-scheduler` to report healthy:

```bash
docker compose ps
```

## Stop the stack

```bash
docker compose down
```

Add `-v` to also drop the Postgres volume (wipes DAG run history / connections):

```bash
docker compose down -v
```

## Picking up new DAGs / SQL / HQL from git

`./dags`, `./include`, and `./data` are bind-mounted into the Airflow
containers, so a plain `git pull` on the host is enough — the scheduler
picks up DAG file changes on its normal parsing cycle. No rebuild, no
restart:

```bash
git pull
# within ~30s (AIRFLOW__SCHEDULER__PARSING_PROCESSES cycle) the scheduler
# re-parses ./dags and the Airflow UI reflects any DAG changes
```

A rebuild (`docker compose build`) is only needed when `Dockerfile` or
`requirements.txt` changes (i.e. new Python/Java dependencies).

## Smoke-test DAG

`dags/example_pipeline.py` is *not* the real pipeline — it exists to prove
the stack works end to end:

1. Reads `data/csv/sample_players.csv`.
2. Starts a local-mode Spark session with `enableHiveSupport()`.
3. Registers the CSV as Hive table `smoke_test.players` (embedded Derby
   metastore, written to `data/metastore_db/` — gitignored, ephemeral).
4. Runs `SELECT COUNT(*) FROM smoke_test.players` and logs the result.

Trigger it from the UI, or:

```bash
docker compose exec airflow-scheduler airflow dags trigger example_pipeline
```

Check the task log for a line like:

```
Spark+Hive smoke test OK — smoke_test.players has 6 rows
```

## Ingestion pipeline (`fantasy_ingestion` DAG)

Data source: the official FPL API (`fantasy.premierleague.com/api`, no
auth needed). Client + task scripts were reverse-engineered from the
[vaastav/Fantasy-Premier-League](https://github.com/vaastav/Fantasy-Premier-League)
reference scraper to figure out the real dependency graph between API
pulls — see git history on `dags/fantasy_ingestion.py` for the full
breakdown of which pulls are independent vs. which need another pull's
output first.

Shape — a fan-out/fan-in graph, not a straight chain:

```
extract_bootstrap ───┐
                      ├─→ clean_players ─→ extract_player_gw (mapped, 1 per player, parallel)
extract_fixtures ─────┼──────────────────────────────────────┘
                      └─────────────────────────────────────→ collect_gw ─→ merge_gw
```

| Task | Script | Status |
|---|---|---|
| `extract_bootstrap` | `include/scripts/extract_bootstrap.py` | **Real** — pulls bootstrap-static, writes `players_raw.csv`, `teams.csv`, `events.csv` |
| `extract_fixtures` | `include/scripts/extract_fixtures.py` | **Real** — writes `fixtures.csv`. Runs in parallel with `extract_bootstrap`, no shared dependency |
| `clean_players` | `include/scripts/clean_players.py` | **Real** — writes `cleaned_players.csv` + `player_idlist.csv`; returns the player-ID list the next stage maps over |
| `extract_player_gw` | `include/scripts/extract_player_gw.py` | **Real** — one Airflow dynamic-mapped instance per player (`.expand()`), writes `data/csv/players/{id}/{history,gw}.csv` |
| `collect_gw` | `include/scripts/collect_gw.py` | Placeholder — needs collector.py's gameweek-assembly logic ported over |
| `merge_gw` | `include/scripts/merge_gw.py` | Placeholder — needs schema-drift-aware merge logic ported over |

`include/scripts/fpl_api.py` is the shared HTTP client (retry + backoff,
handles 429/5xx) every extract script imports from.

**Verified against the live API**: a full run (no `player_limit`) pulled
all **629 players** — bootstrap + fixtures in parallel, then a 629-way
parallel fan-out (capped at 4 concurrent by
`AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG`), then `collect_gw` correctly
waited for every one of those plus `extract_fixtures` before starting.
Zero failures. Wall time ~14.5 minutes at that concurrency; scheduler
memory barely moved (~500MB — these are lightweight I/O-bound tasks, not
JVM-heavy like the Spark smoke test).

For fast local iteration, trigger with `player_limit` to skip most of the
fan-out:

```bash
docker compose exec airflow-scheduler airflow dags trigger fantasy_ingestion --conf '{"player_limit": 5}'
```

Leave it unset (or `null`) for a full run over every player. Each script
also runs standalone, no Airflow needed:

```bash
docker compose exec airflow-scheduler python /opt/airflow/include/scripts/extract_bootstrap.py
```

This works because `PYTHONPATH=/opt/airflow/include` is set on the
Airflow containers (see `docker-compose.yaml`), so any script can
`from scripts.fpl_api import ...` regardless of which one is running.

**Next to fill in**: `collect_gw.py` and `merge_gw.py` — port
`collector.py`'s `collect_gw()`/`merge_gw()` logic from the reference
repo, joining each player's `gw.csv` with `teams.csv`/`fixtures.csv`
into one gameweek's rows, then into `merged_gw.csv`. That's also the
natural point to switch from CSV to loading into a Hive table via
Spark, following `example_pipeline.py`'s `enableHiveSupport()` pattern.

## Memory footprint

Target: whole stack (Airflow + Postgres + Spark) under ~8GB resident, on a
16GB / Intel i5 laptop.

Per-container hard caps set in `docker-compose.yaml` (`mem_limit`), sized so
the total never exceeds budget even at worst case:

| Service | `mem_limit` |
|---|---|
| postgres | 400m |
| airflow-webserver (2 gunicorn workers) | 1200m |
| airflow-scheduler (also hosts the Spark JVM while a task runs) | 3000m |

**Measured** (`docker stats --no-stream`) on this laptop (16GB RAM, i5),
2026-08-28, all three containers up:

| Service | Idle | While `spark_hive_count` task is running |
|---|---|---|
| postgres | 39 MiB | 39 MiB |
| airflow-webserver | 999 MiB | 999 MiB |
| airflow-scheduler | 379 MiB | 912 MiB (Spark JVM as its child process) |
| **Total** | **~1.4 GB** | **~1.9 GB** |

Comfortably inside the ~8GB target — roughly 6GB of headroom versus budget.
The `airflow-webserver` figure (999 MiB / 1200 MiB limit, ~83%) is the
biggest single consumer and the closest to its cap; if you add more DAGs
and it starts getting OOM-killed, raise its `mem_limit` or drop
`AIRFLOW__WEBSERVER__WORKERS` from 2 to 1.

**Update, 2026-09-01, after adding `fantasy_ingestion`**: `airflow-webserver`
has crept up further on its own (no config change) to **1.14 GiB / 1.17 GiB
— 97%** with two DAGs now registered, just from normal UI/DB-polling
overhead over a longer-running session, not from the ingestion DAG itself
(`airflow-scheduler` barely moved during the 629-player fan-out — ~500 MiB,
since these are lightweight `requests` calls, not JVM processes). Worth
raising `airflow-webserver`'s `mem_limit` or dropping to 1 worker before
adding more DAGs, rather than waiting for an OOM kill.

**This does not include Docker Desktop's own WSL2 VM overhead**, which is
separate from the container numbers above and typically adds another
1-2GB of host RAM usage while Docker Desktop is running at all, regardless
of what's inside it. On this machine specifically, host RAM was already
under heavy pressure (~1GB free out of 15.75GB, mostly Chrome tabs) before
this stack was even started — see "Deviations" below.

## Deviations from the original brief

- **Docker Desktop wasn't installed on this laptop.** It was installed via
  `winget install Docker.DockerDesktop` as part of this setup (WSL2 backend
  was already present, so no OS-level reboot was required). If you're
  reading this on a different machine, budget ~5-10 minutes for that
  install + first launch before any of the commands below will work.
- **Host RAM was already under heavy pressure before the stack started**:
  free system RAM was ~1GB out of 15.75GB total (dozens of Chrome tabs and
  several Claude sessions holding the rest), before Docker Desktop's own
  VM (another 1-2GB) or any container. The container-level numbers above
  are healthy and inside budget, but on *this specific machine* you likely
  need to close some Chrome tabs for the stack to run smoothly day-to-day —
  it worked here, but with very little slack outside the containers
  themselves.
- Added `./logs` as a bind-mounted volume (not requested explicitly) so
  Airflow task logs survive `docker compose down` / are inspectable from
  Windows without `docker compose exec`. Zero cost, standard practice in
  Airflow's own quick-start compose file.
- Airflow image is a custom build (`Dockerfile`) rather than plain
  `_PIP_ADDITIONAL_REQUIREMENTS`, because PySpark needs a JVM
  (`openjdk-17-jre-headless`) that a pip-only install can't provide.
- `mem_limit` caps added per-service (not in the original brief) as a
  concrete, enforced way to stay inside the 8GB target rather than relying
  on hope.
- `airflow-init`'s command includes an explicit `chown -R 50000:0` of the
  bind-mounted `dags/`, `logs/`, `data/`, `include/` folders. Without it,
  Windows/WSL2 bind mounts can end up with directories owned by root,
  which then blocks the non-root Airflow processes from writing task
  logs — this exact failure mode showed up during setup and is why the
  step is there.
- **Known quirk, not fully root-caused**: the very first time
  `spark_hive_count` initializes a brand-new embedded Derby metastore via
  a scheduler-forked LocalExecutor task, it failed twice with
  `Unable to instantiate SessionHiveMetaStoreClient` (a known class of
  error tied to JDK 17's stricter reflection rules, addressed in the DAG
  via `spark.driver.extraJavaOptions` `--add-opens` flags). Running the
  same task once via `airflow tasks test` (foreground, not
  LocalExecutor-forked) succeeded immediately and created the metastore
  schema on disk; every run afterwards — including normal
  scheduler-triggered runs — has succeeded. If you hit this on a clean
  `data/metastore_db`, the workaround is a one-time:
  ```bash
  docker compose exec -u 50000 airflow-scheduler airflow tasks test example_pipeline spark_hive_count 2026-01-01
  ```
  before relying on normal triggers. Worth a proper root-cause if this
  becomes the real pipeline's pattern rather than a one-off smoke test.

## Out of scope (unchanged from the brief)

- Real fantasy data ingestion — data source still TBD.
- The optimization algorithm itself (PuLP/OR-Tools) — `optimizer/` is an
  empty placeholder.
- Any cloud deployment.

## Before the next step

- This repo is pushed to `github.com/pranaydeepkhare/fantasy-pipeline`
  (private, `main` branch). Local `origin` remote already points there.
- Fill in `include/scripts/task_1_extract.py` through `task_4_extract.py`
  with real extraction + conversion logic once the data source is known,
  and rename the tasks/scripts in `dags/fantasy_ingestion.py` to match.
- Confirm the real CSV/data source for fantasy data so ingestion DAGs can
  be written against a known schema.
- Decide what goes in `optimizer/` (PuLP vs. OR-Tools) and how it's invoked
  from Airflow (separate container? same image?).
