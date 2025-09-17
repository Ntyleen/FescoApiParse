# FESCO Container Tracking Parser

FescoApiParse is an asynchronous orchestration service that collects and
synchronises container tracking data from the FESCO public API, Firebird
repositories and Google Sheets.  The application was designed for 24/7
operations: it performs retries on flaky HTTP calls, persists operational state
in cache and Firebird, and exposes Prometheus metrics for observability.

## Architecture overview

```
┌────────────────────┐      ┌────────────────┐
│ CLI / Scheduler    │─────▶│ Service layer  │
│  (main.py/cli.py)  │      │ (services/)    │
└────────────────────┘      └────────────────┘
             │                         │
             ▼                         ▼
    ┌───────────────┐        ┌────────────────────┐
    │ Fesco API     │◀──────▶│ Transport & Cache  │
    │ Transport     │        │ (api/transport.py, │
    │ (aiohttp)     │        │  api/business.py)  │
    └───────────────┘        └────────────────────┘
             │                         │
             ▼                         ▼
    ┌────────────────┐        ┌────────────────────┐
    │ Firebird DB    │◀──────▶│ Container engine   │
    │ (utils/db/…)   │        │ (processing/…)     │
    └────────────────┘        └────────────────────┘
             │                         │
             ▼                         ▼
    ┌────────────────┐        ┌────────────────────┐
    │ Redis / file   │        │ Google Sheets sync │
    │ cache          │        │ (batch updates)    │
    └────────────────┘        └────────────────────┘
```

* **Transport layer** performs resilient HTTP calls with retry/backoff policies
  and connection limiting.
* **Business layer** adds caching, statistics and negative cache handling.
* **Service layer** (see `services/`) is responsible for CLI workflows,
  scheduling and coordinating batches in the tracking engine.
* **Processing engine** reads container metadata from Firebird, groups by
  orders, writes updates back to the database and synchronises Google Sheets in
  batches.
* **Monitoring** is powered by Prometheus metrics (`utils/metrics.py`) and
  structured logs suitable for Loki ingestion.

## Configuration

The application loads YAML configuration from `deploy/config/app_global.yaml`.
Important sections:

- `api`: FESCO API connection parameters (timeout, retry_attempts,
  retry_backoff_seconds, max_parallel, user agent).
- `cache`: Redis or file cache settings.
- `google_sheets`: worksheet identifiers and `batch_size` used for batch
  updates.
- `processing` and `database`: Firebird entity table mapping and batch sizes.

Credentials (FESCO token, Redis password, etc.) can be supplied through
environment variables referenced from the YAML or via the `.env` files under
`deploy/.env/`.

## Installation & local development

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Firebird client libraries are required for database access.  On Debian based
systems install `libfbclient2` (the Dockerfile demonstrates the commands).  The
project expects Python 3.11 or newer.

## Running the CLI

All command line entry points are exposed through `cli.py` / `main.py`.  The
metrics exporter can be enabled for any mode via `--metrics-port`.

```bash
# Test a handful of containers using the FESCO API only
python main.py test --count 5 --metrics-port 8000

# Process containers from a text file
python main.py file containers.txt

# Run the full Firebird workflow with retries and batch Google Sheets updates
python main.py db --batch-size 100 --metrics-port 9000

# Print live system statistics
python main.py monitor

# Start the scheduler loop (uses cron expression from config)
python main.py schedule --metrics-port 8000
```

The CLI initialises logging from the configuration, validates connectivity to
Firebird/Redis and surfaces clear messages on unexpected errors.  Prometheus
metrics are exposed at `http://localhost:<metrics-port>/metrics`.

## Google Sheets synchronisation

`processing/google_sheets_sync.py` performs idempotent upserts into Google
Sheets.  It supports:

* Mapping tracking statuses to user-friendly labels.
* Negative cache hits to avoid repeated lookups for missing orders.
* Batch updates using `WorksheetAdapter.batch_update_rows` respecting the
  configured `batch_size`.  The engine aggregates row updates and submits them in
  chunks to reduce the number of API calls.

## Observability and logging

- **Prometheus** – run the CLI with `--metrics-port` and point Prometheus at the
  `/metrics` endpoint.  Metrics include API request counts, retry totals,
  container processing counters and per-order duration histograms.
- **Loki** – the application logs structured messages to STDOUT, which can be
  collected by Loki/Promtail.  Configure Docker or your orchestrator to ship
  `/var/log/FescoApiParser` into Loki for long term analysis.

## Docker deployment

A ready-to-run Dockerfile and docker-compose definition are available under the
`deploy/docker/` directory.  Key traits:

* Installs only required packages, removes apt caches and runs the process as
  the unprivileged `appuser`.
* `docker-compose` wires Redis (for caching) and the parser container, enables a
  Prometheus healthcheck and applies resource limits.
* Metrics are exposed on port `8000` by default via the compose `command`
  override.

Launch everything with:

```bash
cd deploy/docker
docker compose up -d
```

## Testing

The project uses `pytest` and `pytest-asyncio` for both unit and integration
coverage.  Test suites cover the API client (including retry logic and negative
cache), the tracking engine, Google Sheets synchronisation and CLI workflows.
Run all tests with:

```bash
pytest
```

## Monitoring stack (optional)

A sample monitoring stack consists of:

1. **Prometheus** scraping the `/metrics` endpoint started via
   `--metrics-port`.
2. **Grafana** dashboards charting API request counts, processing latencies and
   Google Sheets activity.
3. **Loki + Promtail** shipping container logs for search and alerting.

Refer to your infrastructure automation to provision these services; the README
focuses on the application side integration points.

## Contributing

1. Fork the repository and create a feature branch.
2. Ensure `pytest` succeeds and run type checking / linters if available.
3. Update documentation when behaviour changes (README, configuration examples).
4. Submit a merge request describing the motivation and testing performed.

---

For further architectural documentation (such as Firebird entity schemas) refer
to the `docs/` directory when available.
