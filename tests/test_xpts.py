"""Unit and integration tests for baseline xPts: difficulty, xmins, xpts formula, build_xpts."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import text

from src.db import get_engine, get_session, init_db
from src.marts import init_marts
from src.pipeline import _upsert_player_expected_points
from src.xpts import (
    FormRow,
    PlayerRow,
    build_xpts_rows,
    clamp,
    compute_xmins,
    compute_xpts_components,
    get_difficulty_multiplier,
)


# ----- Unit: difficulty multiplier -----


def test_get_difficulty_multiplier() -> None:
    assert get_difficulty_multiplier(1) == 1.15
    assert get_difficulty_multiplier(2) == 1.05
    assert get_difficulty_multiplier(3) == 1.00
    assert get_difficulty_multiplier(4) == 0.92
    assert get_difficulty_multiplier(5) == 0.85
    assert get_difficulty_multiplier(None) == 1.0
    assert get_difficulty_multiplier(0) == 1.0
    assert get_difficulty_multiplier(6) == 1.0


# ----- Unit: clamp -----


def test_clamp() -> None:
    assert clamp(0.5, 0, 1) == 0.5
    assert clamp(-0.1, 0, 1) == 0.0
    assert clamp(1.5, 0, 1) == 1.0


# ----- Unit: xmins -----


def test_compute_xmins_from_form() -> None:
    """When games_last5 >= 3, xmins = min(90, minutes_last5 / games_last5)."""
    form = FormRow(player_id=1, games_last5=5, minutes_last5=450, points_last5=25, ppg_last5=5.0)
    player = PlayerRow(1, team_id=1, element_type_id=3, minutes=1000, points_per_game=5.0, status="a")
    xmins = compute_xmins(form, player, finished_events_count=10)
    assert xmins == 90.0  # 450/5 = 90, capped at 90


def test_compute_xmins_from_form_under_90() -> None:
    form = FormRow(player_id=1, games_last5=5, minutes_last5=300, points_last5=15, ppg_last5=3.0)
    player = PlayerRow(1, team_id=1, element_type_id=3, minutes=500, points_per_game=4.0, status="a")
    xmins = compute_xmins(form, player, finished_events_count=10)
    assert xmins == 60.0  # 300/5 = 60


def test_compute_xmins_fallback_no_form() -> None:
    """When form has < 3 games, use players.minutes / finished_events_count."""
    player = PlayerRow(1, team_id=1, element_type_id=3, minutes=900, points_per_game=5.0, status="a")
    xmins = compute_xmins(None, player, finished_events_count=10)
    assert xmins == 90.0  # 900/10 = 90


def test_compute_xmins_status_penalty() -> None:
    """Status i/s/u/d applies XMINS_STATUS_MULTIPLIER (0.4)."""
    form = FormRow(player_id=1, games_last5=5, minutes_last5=450, points_last5=25, ppg_last5=5.0)
    player = PlayerRow(1, team_id=1, element_type_id=3, minutes=500, points_per_game=4.0, status="d")
    xmins = compute_xmins(form, player, finished_events_count=10)
    assert xmins == pytest.approx(90.0 * 0.4, rel=1e-5)


# ----- Unit: xpts components (synthetic player) -----


def test_compute_xpts_components_synthetic_mid() -> None:
    """Synthetic MID: 90 mins, form 5 pts per 90 (after stripping 2 app pts -> 3 non-app), diff 1.0."""
    form = FormRow(
        player_id=1,
        games_last5=5,
        minutes_last5=450,  # 90 avg
        points_last5=25,    # 25/450*90 = 5 pp90
        ppg_last5=5.0,
    )
    player = PlayerRow(1, team_id=1, element_type_id=3, minutes=900, points_per_game=5.0, status="a")
    xmins = 90.0
    difficulty_mult = 1.0
    xpts_app, xpts_att, xpts_def, xpts = compute_xpts_components(xmins, form, player, difficulty_mult)
    # base_pp90 = 5, base_nonapp_pp90 = max(0, 5-2) = 3, xpts_nonapp = 3 * 1 * 1 = 3
    # MID: 70% att, 30% def -> xpts_att=2.1, xpts_def=0.9
    assert xpts_app == pytest.approx(2.0, rel=1e-5)  # 1 for playing + 1 for 60+
    assert xpts_att == pytest.approx(2.1, rel=1e-5)
    assert xpts_def == pytest.approx(0.9, rel=1e-5)
    assert xpts == pytest.approx(5.0, rel=1e-5)


def test_compute_xpts_components_fwd_split() -> None:
    """FWD: 90% att, 10% def."""
    form = None
    player = PlayerRow(1, team_id=1, element_type_id=4, minutes=900, points_per_game=6.0, status="a")
    xmins = 90.0
    # base_pp90 = 6, base_nonapp = 4, xpts_nonapp = 4 * 1 * 1 = 4; att=3.6, def=0.4
    xpts_app, xpts_att, xpts_def, xpts = compute_xpts_components(xmins, form, player, 1.0)
    assert xpts_att == pytest.approx(3.6, rel=1e-5)
    assert xpts_def == pytest.approx(0.4, rel=1e-5)
    assert xpts == pytest.approx(6.0, rel=1e-5)


# ----- Integration: build_xpts writes rows -----


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@pytest.fixture
def temp_db_engine(tmp_path):
    """SQLite engine (file) with minimal schema and data for build_xpts. Uses tmp_path so one DB is shared across connections."""
    db_path = str(tmp_path / "test_fpl.sqlite")
    engine = get_engine(db_path)
    init_db(engine)
    init_marts(engine)

    now = _utc_now().strftime("%Y-%m-%d %H:%M:%S")
    # Insert in FK order via raw SQL to avoid ORM flush order issues.
    with engine.connect() as conn:
        conn.execute(text("INSERT INTO element_types (id, singular_name_short, singular_name, squad_select, ingested_at_utc) VALUES (3, 'MID', 'Midfielder', 5, :now)"), {"now": now})
        conn.execute(text("INSERT INTO teams (id, name, short_name, strength, ingested_at_utc) VALUES (1, 'Team A', 'TA', 4, :now)"), {"now": now})
        conn.execute(text("INSERT INTO teams (id, name, short_name, strength, ingested_at_utc) VALUES (2, 'Team B', 'TB', 4, :now)"), {"now": now})
        conn.execute(text("INSERT INTO events (id, name, finished, ingested_at_utc) VALUES (1, 'GW1', 1, :now)"), {"now": now})
        conn.execute(text("INSERT INTO events (id, name, finished, ingested_at_utc) VALUES (2, 'GW2', 0, :now)"), {"now": now})
        conn.execute(
            text("""INSERT INTO players (id, web_name, team_id, element_type_id, minutes, points_per_game, status, ingested_at_utc)
                   VALUES (100, 'TestPlayer', 1, 3, 900, 5.0, 'a', :now)"""),
            {"now": now},
        )
        conn.execute(
            text("""INSERT INTO fixtures (id, event_id, team_h, team_a, finished, team_h_difficulty, team_a_difficulty, ingested_at_utc)
                   VALUES (1, 2, 1, 2, 0, 2, 4, :now)"""),
            {"now": now},
        )
        conn.commit()
    return engine


def test_build_xpts_rows_returns_rows(temp_db_engine) -> None:
    """build_xpts_rows with minimal DB returns at least one row for the player with upcoming fixture."""
    rows = build_xpts_rows(temp_db_engine, horizon=1)
    assert len(rows) >= 1
    r = rows[0]
    assert "player_id" in r and "event_id" in r and "xmins" in r and "xpts" in r
    assert r["player_id"] == 100
    assert r["event_id"] == 2


def test_build_xpts_upsert_integration(temp_db_engine) -> None:
    """Run build_xpts_rows, upsert into player_expected_points, assert rows in DB."""
    rows = build_xpts_rows(temp_db_engine, horizon=1)
    assert len(rows) >= 1

    with get_session(temp_db_engine) as session:
        n = _upsert_player_expected_points(session, rows)
    assert n == len(rows)

    with temp_db_engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM player_expected_points")).scalar()
        assert count == len(rows)
        row = conn.execute(
            text("SELECT player_id, event_id, xmins, xpts FROM player_expected_points LIMIT 1")
        ).fetchone()
        assert row[0] == 100
        assert row[1] == 2
        assert row[2] >= 0
        assert row[3] >= 0
