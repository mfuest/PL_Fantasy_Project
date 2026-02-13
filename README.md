# FPL Data Ingestion Pipeline

A robust, repeatable data pipeline for Fantasy Premier League (FPL): HTTP fetch → bronze (raw JSON) → silver (SQLite). Built for a transfer recommender; no auth required.

**Stack:** Python 3.11–3.12, SQLite, SQLAlchemy 2, `requests`. Idempotent upserts (`ON CONFLICT DO UPDATE`), rate limiting, retries with exponential backoff, and optional run auditing via `meta_ingestions.run_id`.

---

## How to run

```bash
# From project root; use Python 3.11 or 3.12 (requires-python = ">=3.11,<3.13")
python -m venv .venv
source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -e ".[dev]"

# Create DB and bronze dirs (optional; pipeline creates them if missing)
mkdir -p data/bronze/bootstrap-static data/bronze/fixtures data/bronze/element-summary data/bronze/entry

# Global options (--db-path, --bronze-dir) go before the subcommand:
# 1. Core data (bootstrap-static + fixtures → teams, element_types, events, players, fixtures)
python -m src.pipeline update_core

# 2. Element summaries for top N players by total_points (default 250)
python -m src.pipeline update_element_summaries --mode top --n 250

# 3. All players (slower; ~600+ requests with 0.25s sleep)
python -m src.pipeline update_element_summaries --mode all

# 4. Pull a manager’s squad for a gameweek (bronze + log; bank/FT not promised)
python -m src.pipeline pull_team --team_id 12345 --gw 25

# 5. Entry history for bank/FT context (public, no auth)
python -m src.pipeline update_entry_history --team_id 12345

# 6. Data quality checks (fails with exit 1 if invalid)
python -m src.pipeline validate
```

**CLI options:**

- `--db-path` — SQLite file (default: `data/fpl.sqlite`)
- `--bronze-dir` — Bronze JSON root (default: `data/bronze`)
- **update_element_summaries:** `--since-hours N` — skip players last fetched within N hours (incremental updates)

---

## Refresh strategy

- **update_core:** Run weekly or before each gameweek deadline. Refreshes teams, positions, events, players, and fixtures.
- **update_element_summaries:** Run after `update_core` when you need per-player history and future fixtures. Use `--mode top --n 250` for faster runs; add `--since-hours 12` to skip players fetched in the last 12 hours (fewer API calls). Batched: every 20 players we commit a transaction so a single bad payload doesn’t lose the whole run.
- **pull_team:** On demand for a given `team_id` and gameweek; stores bronze and logs squad (bank/FT only if the API returns them).
- **update_entry_history:** Fetch `entry/{team_id}/history/` for current-season GW history and past seasons (bank/FT context for transfer suggester).
- **validate:** Run data quality checks (counts, null %, referential integrity, minutes/points ranges); exits 1 if any check fails. Also run automatically at end of `update_core` and `update_element_summaries`.

---

## Schema overview (8 tables)

| Table | Purpose |
|-------|--------|
| **meta_ingestions** | Append-only log of each fetch: request_key, run_id, url, payload_path, payload_sha256 (optional). Indexed for “latest bootstrap-static?”, “did I ingest player X today?”. |
| **teams** | FPL teams (id, name, short_name, strength). |
| **element_types** | Positions (GKP, DEF, MID, FWD). |
| **events** | Gameweeks (id, name, deadline_time, finished, is_current, is_next). |
| **players** | Elements: id, web_name, team_id, element_type_id, **now_cost** (in tenths, e.g. 55 = £5.5), total_points, selected_by_percent, form, xG/xA, etc. Use view **v_player_latest.now_cost_million** for £ display. |
| **fixtures** | All fixtures (id, event_id, team_h, team_a, kickoff_time, finished, difficulties). |
| **player_match_history** | Per-player per-fixture history from element-summary; PK (player_id, fixture_id_effective). |
| **player_future_fixtures** | Upcoming fixtures per player; minimal (player_id, fixture_id, difficulty, etc.); join to `fixtures` for kickoff/teams. |

All silver tables use **ingested_at_utc** (time we last wrote the row). SQLite foreign keys are enabled per connection in `db.py` (`PRAGMA foreign_keys = ON`, `journal_mode = WAL`).

**Analytics views** (created by `init_marts` after `update_core`):

| View | Purpose |
|------|--------|
| **v_player_latest** | Players joined with teams and positions; **now_cost_million** = `now_cost / 10.0` for £ (e.g. 5.5). |
| **v_fixture_upcoming** | Fixtures not finished (join to teams for short names). |
| **v_player_form** | Rolling last 5 games per player: games_last5, minutes_last5, points_last5, ppg_last5. |

---

## How to extend

1. **New endpoint:** In `fpl_client.py`, extend URL parsing so `_derive_endpoint_and_request_key` returns the right `(endpoint, request_key)` and bronze path (e.g. hierarchical for high-volume endpoints).
2. **New silver table:** Add an ORM model in `models.py`, create normalizer in `normalize.py` (use `to_int`/`to_float`/`to_dt` for FPL’s stringy numbers/dates), then in `pipeline.py` add a fetch step and bulk upsert with `insert(...).on_conflict_do_update(...)`.
3. **Tests:** Add sample JSON under `tests/fixtures/` and unit tests in `tests/test_normalize.py` (and/or integration tests that hit `db.py`).

---

## Tests

```bash
pip install -e ".[dev]"
python -m pytest tests/ -v
```

Unit tests cover normalizers using `tests/fixtures/bootstrap_static_sample.json`, `element_summary_sample.json`, and `fixtures_sample.json`.
