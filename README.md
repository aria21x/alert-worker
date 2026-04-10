# Solana Insider Tracker — Rust-first free-tier build

This version makes the **live ingest path Rust-first** so you can stay on a free-tier style setup while keeping Python for:
- enrichment
- wallet profiling
- clustering
- scoring
- Discord alerts
- API

## Architecture

- `apps/ingest_rust/` — active live Helius WebSocket ingest that queues signatures into Postgres
- `apps/enrich_worker/` — pulls queued signatures, fetches Helius Enhanced Transactions, normalizes trades
- `apps/scoring_worker/` — updates wallet scores and token state
- `apps/alert_worker/` — applies alert gates and sends Discord webhook alerts
- `apps/api_server/` — debug and inspection API

## Why this build
LaserStream is not free-tier friendly. This build keeps the cheaper path:
- Rust for live stream stability and speed
- Helius Enhanced WebSocket / `transactionSubscribe`
- Python for the heavier interpretation logic

## Boot

1. Copy `.env.example` to `.env`
2. Start Postgres and Redis:

```bash
docker compose up -d postgres redis
```

3. Apply schema:

```bash
psql postgresql://postgres:postgres@localhost:5432/solana_tracker -f core/db/schema.sql
```

4. Start the Rust ingestor:

```bash
cd apps/ingest_rust
cargo run --release
```

5. In separate terminals, start the Python workers from the repo root:

```bash
python -m apps.enrich_worker.main
python -m apps.scoring_worker.main
python -m apps.alert_worker.main
uvicorn apps.api_server.main:app --host 0.0.0.0 --port 8000
```

## Notes
- `apps/ingest_python/` is kept only as a fallback reference path.
- The Rust worker inserts rows into `raw_signatures` with source `helius_wss_rust`.
- Fill the `.env` values before starting.

## Service package
This archive is the **alert-worker** service split from the full tracker.

Railway start command:
`python -m apps.alert_worker.main`
