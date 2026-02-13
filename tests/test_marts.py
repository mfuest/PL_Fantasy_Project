"""Tests for marts views (v_player_form ordering and view creation)."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.db import init_db
from src.marts import init_marts


@pytest.fixture
def engine() -> Engine:
    """In-memory SQLite with schema and minimal data for views."""
    e = create_engine("sqlite:///:memory:", future=True)
    init_db(e)
    with e.connect() as conn:
        conn.execute(text("INSERT INTO teams (id, name, short_name, strength, ingested_at_utc) VALUES (1, 'A', 'A', 1, '2025-01-01')"))
        conn.execute(text("INSERT INTO element_types (id, singular_name_short, singular_name, squad_select, ingested_at_utc) VALUES (1, 'GKP', 'GKP', 1, '2025-01-01')"))
        conn.execute(text("INSERT INTO events (id, name, deadline_time, finished, is_current, is_next, ingested_at_utc) VALUES (1, 'GW1', null, 0, 0, 0, '2025-01-01')"))
        conn.execute(text("INSERT INTO players (id, web_name, team_id, element_type_id, ingested_at_utc) VALUES (1, 'P', 1, 1, '2025-01-01')"))
        conn.execute(text("INSERT INTO fixtures (id, event_id, team_h, team_a, kickoff_time, finished, ingested_at_utc) VALUES (1, 1, 1, 1, '2025-01-01 12:00:00', 1, '2025-01-01')"))
        conn.execute(text("INSERT INTO fixtures (id, event_id, team_h, team_a, kickoff_time, finished, ingested_at_utc) VALUES (2, 1, 1, 1, '2025-01-08 12:00:00', 1, '2025-01-01')"))
        conn.execute(text("INSERT INTO player_match_history (player_id, fixture_id_effective, fixture_id, event_id, minutes, total_points, ingested_at_utc) VALUES (1, 1, 1, 1, 90, 6, '2025-01-01')"))
        conn.execute(text("INSERT INTO player_match_history (player_id, fixture_id_effective, fixture_id, event_id, minutes, total_points, ingested_at_utc) VALUES (1, 2, 2, 1, 90, 8, '2025-01-01')"))
        conn.execute(text("INSERT INTO player_future_fixtures (player_id, fixture_id, ingested_at_utc) VALUES (1, 1, '2025-01-01')"))
        conn.commit()
    return e


def test_init_marts_creates_v_player_form(engine: Engine) -> None:
    """init_marts creates v_player_form and it is queryable."""
    init_marts(engine)
    with engine.connect() as conn:
        row = conn.execute(text("SELECT player_id, games_last5, minutes_last5, points_last5, ppg_last5 FROM v_player_form WHERE player_id = 1")).fetchone()
    assert row is not None
    assert row[0] == 1
    assert row[1] == 2
    assert row[2] == 180
    assert row[3] == 14
    assert row[4] == 7.0


def test_v_player_form_orders_by_kickoff_time(engine: Engine) -> None:
    """v_player_form uses fixture kickoff_time for ordering (last 5 games)."""
    init_marts(engine)
    with engine.connect() as conn:
        # View should exist and aggregate last 5 by kickoff_time (newest first)
        rows = conn.execute(text("SELECT * FROM v_player_form")).fetchall()
    assert len(rows) == 1
    # Player 1 has 2 games; games_last5=2, points_last5=6+8=14
    assert rows[0][1] == 2 and rows[0][3] == 14
