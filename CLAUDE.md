# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ELT pipeline and analytics platform for Budapest public transit (BKK). Collects real-time vehicle positions from the BKK FUTAR API and weather data from OpenWeather API, loads into PostgreSQL (PostGIS), transforms into a Star Schema DWH, and visualizes via Metabase. Orchestrated by Prefect.

## Infrastructure

Everything runs in Docker. Start with `docker compose up -d`.

- **Database**: PostGIS (postgis/postgis:15-3.3) on port 5432
- **Metabase**: Dashboard at localhost:3000
- **Prefect Server**: Orchestration UI at localhost:4200
- **Prefect Worker**: Runs all scheduled flows automatically
- **DB credentials**: Stored in `.env` as `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`
- **Config**: `config/config.yaml` (gitignored) — copy from `config/config.yaml.template` and fill in API keys (BKK API key, OpenWeather API key)
- **DB_HOST env var**: Overrides `config.yaml` database host. Set to `db` in Docker, defaults to `localhost` for local dev.

## Project Structure

```
src/                  # Pure business logic (no Prefect imports)
flows/                # Thin Prefect wrappers around src/ classes
Dockerfile            # Python 3.12-slim image for the worker
docker-compose.yml    # All services (db, metabase, prefect-server, prefect-worker)
prefect.yaml          # Deployment definitions with schedules
```

## Python Environment (local dev)

- Python version managed via pyenv, virtual env name: `bkk-transit`
- Dependencies: `pip install -r requirements.txt`

## Orchestration (Prefect)

Flows are defined in `flows/` as thin wrappers around business logic classes in `src/`. Deployments and schedules are defined in `prefect.yaml`.

**Scheduled flows:**
- `collect-bkk-vehicles`: Every 5-15 min (varies by time of day)
- `collect-weather`: Twice daily (09:00, 21:00)
- `elt-pipeline`: Daily at 22:00 (raw_to_stage -> stage_to_dwh)
- `load-gtfs`: Manual trigger only (via Prefect UI)

**Manual runs**: Open Prefect UI at localhost:4200 -> Deployments -> Quick Run

## Running Scripts Directly (without Prefect)

All scripts can still be run from the project root:

```bash
python src/bkk_collector.py           # Collect vehicle data
python src/weather_collector.py       # Collect weather data
python src/gtfs_loader.py             # Load GTFS data
python src/raw_to_stage.py            # Load raw JSON into staging
python src/stage_to_dwh.py            # Transform staging to DWH
```

All ETL scripts accept `--config` for a custom config path (default: `config/config.yaml`).

## Database Schema (3 schemas)

- **staging**: Raw data landing zone — `stg_vehicles`, `stg_weather`, `stg_gtfs_stops`, `stg_gtfs_routes`, `stg_gtfs_trips`, `stg_gtfs_stop_times`
- **dwh**: Star schema — `dim_time`, `dim_weather`, `dim_route`, `dim_stop`, `dim_vehicle`, `fact_vehicle_event`
- **mart**: Analytics view — `v_analytics_master` (joins all dimensions with business logic columns like `part_of_day`, `weather_category`)

Schema DDL is in `sql/schema.sql`, mart view in `sql/mart.sql`. These are auto-run on first DB startup via docker-entrypoint-initdb.d mount.

## Architecture Notes

- **BKK API IDs have agency prefixes** (e.g., `BKK_1234`). `stage_to_dwh.py` strips the prefix before the first `_` to match GTFS IDs.
- **Delay calculation** in `stage_to_dwh.py` uses Haversine distance to match vehicles within 50m of scheduled stops, then computes `actual_time - scheduled_time`.
- **GTFS times can exceed 24:00:00** (per GTFS spec for trips crossing midnight). The parser in `stage_to_dwh.py` handles this.
- **Weather dimension is hourly** — raw weather is deduplicated by rounding to the nearest hour.
- **raw_to_stage.py truncates staging tables** before each full reload (not incremental).
- Data flows: `raw JSON files -> staging (raw_to_stage.py) -> dwh (stage_to_dwh.py) -> mart (SQL view)`
- **Orchestration separates from business logic**: `src/` has zero Prefect imports. `flows/` contains only thin wrappers.
