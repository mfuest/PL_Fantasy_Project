# FPL Data Ingestion Pipeline

A robust, repeatable data pipeline for Fantasy Premier League (FPL): HTTP fetch → bronze (raw JSON) → silver (SQLite). Built for a transfer recommender; no auth required.

**Stack:** Python 3.11–3.12, SQLite, SQLAlchemy 2, `requests`. Idempotent upserts (`ON CONFLICT DO UPDATE`), rate limiting, retries with exponential backoff, and optional run auditing via `meta_ingestions.run_id`.

---

## Changelog (Pipeline Hardening)

- **Validation:** Split into hard-fail vs warn. Hard: null PKs, FK integrity, missing core tables, minutes &lt;0 or &gt;130. Warn: row count ranges, total_points outside [-10, 40]. CLI `validate --level hard|strict|warn`; auto-validate at end of update_core/update_element_summaries uses `--level hard`.
- **v_player_form:** Last-5 ordering by fixture `kickoff_time` (join to fixtures), fallback to `event_id` when `kickoff_time` is NULL (double-GW and postponed handled correctly).
- **Indexes:** `fixtures(kickoff_time)`, `meta_ingestions(request_key, fetched_at_utc DESC)`, `player_match_history(event_id)` (idempotent).
- **update_element_summaries:** `--force` (ignore skip logic), `--max-age-hours` (refetch if last fetch older than N hours, or never fetched).
- **Tests:** Validation level behavior, v_player_form view creation.

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

# 6. Data quality checks (exit 1 based on --level; default hard)
python -m src.pipeline validate --level hard
python -m src.pipeline validate --level strict   # exit 1 on warnings too
python -m src.pipeline validate --level warn     # never exit 1, report only

# 7. Baseline expected points (next N gameweeks; no ML; runs validate at end)
python -m src.pipeline build_xpts --horizon 3
```

**CLI options:**

- `--db-path` — SQLite file (default: `data/fpl.sqlite`)
- `--bronze-dir` — Bronze JSON root (default: `data/bronze`)
- **validate:** `--level hard|strict|warn` — `hard`: only hard failures exit 1 (default); `strict`: hard + warnings exit 1; `warn`: never exit 1, print report only.
- **update_element_summaries:** `--since-hours N` — skip players last fetched within N hours (incremental updates). `--force` — ignore skip logic and fetch all candidates. `--max-age-hours N` — if set, always refetch players whose last fetch is older than N hours (and never-fetched); interacts with `--since-hours` by adding these players to the fetch set even when they would otherwise be skipped.
- **build_xpts:** `--horizon N` — number of upcoming gameweeks to compute expected points for (default 3). Runs validation (hard) at end; exit 1 if validation fails.

---

## Baseline xPts

A **baseline expected points** layer (no ML) produces per-player expected FPL points per upcoming gameweek for transfer optimisation. Uses only FPL data already ingested (no odds or external sources).

- **Table:** `player_expected_points` — one row per (player_id, event_id) with `xmins`, `xpts`, `xpts_att`, `xpts_def`, `xpts_app`, `computed_at_utc`.
- **Formula (interpretable):**  
  - **Expected minutes (xmins):** From `v_player_form` (last 5 games): if games_last5 ≥ 3 use min(90, minutes_last5/games_last5); else fallback to season minutes / finished_events_count, capped at 90. Status `i`/`s`/`u`/`d` apply a 0.4× multiplier.  
  - **Appearance (xpts_app):** Crude proxy: 1 pt for playing + 1 pt for 60+ mins → `1*(xmins>0) + 1*clamp(xmins/90,0,1)`.  
  - **Form + fixture:** Base points-per-90 from last 5 (or `points_per_game`), strip ~2 for appearance; scale by (xmins/90) × difficulty multiplier (1→1.15, 2→1.05, 3→1.0, 4→0.92, 5→0.85). Non-app points split by position: GK/DEF 40% att / 60% def, MID 70/30, FWD 90/10.
- **View:** `v_player_xpts_next` — join `player_expected_points` with `v_player_latest` for the next event_id (min event in xpts table); includes name, team, position, now_cost_million, xmins, xpts.

Run after `update_core` (and ideally `update_element_summaries` for form):  
`python -m src.pipeline build_xpts --horizon 3`

---

## Transfer engine and API

- **Transfer engine** (`src/transfer_engine.py`): Best XI from 15 players (1 GKP, 3–5 DEF, 2–5 MID, 1–3 FWD by xPts); one-transfer swaps with budget (bank + sell price), position (same role), and max-3-per-team constraints; ranked by xPts delta; returns top 10 suggestions + team xPts per suggestion.
- **Squad source:** `src/squad_source.py` — `get_squad_from_api(client, team_id, gw)` fetches entry picks and returns 15 player IDs + bank (same as `pull_team`).
- **API** (`src/api.py`): One endpoint `POST /suggestions` with form fields `team_id`, `gw`, optional `bank` → fetch squad from FPL API → transfer engine. Response: `current_team_xpts` and `suggestions` (list of `out_player_id`, `in_player_id`, `out_web_name`, `in_web_name`, `team_xpts_delta`, `new_team_xpts`, `cost_delta_million`).

Run the API (requires `build_xpts` to have been run):

```bash
pip install -e ".[api]"
python -m src.api --port 8000
# POST /suggestions with form: team_id=12345&gw=25&bank=0.5
```

**Web UI:** Open [http://localhost:8000](http://localhost:8000) in a browser. Enter your FPL Team ID and gameweek (and optional bank) to get transfer suggestions in a simple table. The same server serves the static page and the `/suggestions` API.

---

## Refresh strategy

- **update_core:** Run weekly or before each gameweek deadline. Refreshes teams, positions, events, players, and fixtures.
- **update_element_summaries:** Run after `update_core` when you need per-player history and future fixtures. Use `--mode top --n 250` for faster runs; add `--since-hours 12` to skip players fetched in the last 12 hours (fewer API calls). Batched: every 20 players we commit a transaction so a single bad payload doesn’t lose the whole run.
- **pull_team:** On demand for a given `team_id` and gameweek; stores bronze and logs squad (bank/FT only if the API returns them). Use this to get the 15 element IDs for transfer suggestions (e.g. `suggest_transfers --squad` or API `/suggestions`).
- **update_entry_history:** Fetch `entry/{team_id}/history/` for current-season GW history and past seasons (bank/FT context for transfer suggester).
- **validate:** Run data quality checks (counts, null %, referential integrity, minutes [0–130] hard fail, total_points anomaly warn). Use `--level hard` (default) so only hard failures cause exit 1; `--level strict` to also exit 1 on warnings; `--level warn` to never exit 1. Also run automatically at end of `update_core` and `update_element_summaries` with level `hard`.
- **build_xpts:** Run after `update_core` (and ideally `update_element_summaries`) to compute baseline expected points for the next N gameweeks. Writes to `player_expected_points` and runs validation (hard) at end.

---

## Schema overview (9 tables)

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
| **player_expected_points** | Baseline xPts per player per upcoming gameweek: xmins, xpts, xpts_att, xpts_def, xpts_app, computed_at_utc. Filled by `build_xpts`. |

All silver tables use **ingested_at_utc** (or **computed_at_utc** for `player_expected_points`) (time we last wrote the row). SQLite foreign keys are enabled per connection in `db.py` (`PRAGMA foreign_keys = ON`, `journal_mode = WAL`).

**Analytics views** (created by `init_marts` after `update_core`):

| View | Purpose |
|------|--------|
| **v_player_latest** | Players joined with teams and positions; **now_cost_million** = `now_cost / 10.0` for £ (e.g. 5.5). |
| **v_fixture_upcoming** | Fixtures not finished (join to teams for short names). |
| **v_player_form** | Rolling last 5 games per player (ordered by fixture kickoff_time, fallback event_id): games_last5, minutes_last5, points_last5, ppg_last5. |
| **v_player_xpts_next** | Expected points for the next GW: join `player_expected_points` with `v_player_latest` for min(event_id); name, team, position, now_cost_million, xmins, xpts. |

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

Unit tests cover normalizers using `tests/fixtures/bootstrap_static_sample.json`, `element_summary_sample.json`, and `fixtures_sample.json`. `tests/test_xpts.py` covers the baseline xPts layer: difficulty multiplier, xmins logic, xpts components, and an integration test that runs `build_xpts` on a minimal DB.
