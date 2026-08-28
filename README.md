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
├── include/
│   ├── sql/                # .sql files
│   └── hql/                # .hql / HiveQL scripts
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
16.08.2026, all three containers up:

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

- Confirm the real CSV/data source for fantasy data so ingestion DAGs can
  be written against a known schema.
- Decide what goes in `optimizer/` (PuLP vs. OR-Tools) and how it's invoked
  from Airflow (separate container? same image?).
