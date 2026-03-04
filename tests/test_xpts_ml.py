"""Unit tests for ML xPts: feature builder, train_model, build_xpts_rows_ml."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import text

from src.db import get_engine, get_session, init_db
from src.marts import init_marts
from src.pipeline import _upsert_player_expected_points
from src.xpts_ml import (
    FEATURE_COLUMNS,
    build_training_data,
    build_xpts_rows_ml,
    train_model,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


REQUIRED_ROW_KEYS = {
    "player_id",
    "event_id",
    "xmins",
    "xpts",
    "xpts_att",
    "xpts_def",
    "xpts_app",
    "computed_at_utc",
}


@pytest.fixture
def ml_db_engine(tmp_path):
    """Minimal DB with schema + player_match_history so build_training_data returns rows."""
    db_path = str(tmp_path / "test_ml.sqlite")
    engine = get_engine(db_path)
    init_db(engine)
    init_marts(engine)
    now = _utc_now().strftime("%Y-%m-%d %H:%M:%S")
    with engine.connect() as conn:
        conn.execute(
            text(
                "INSERT INTO element_types (id, singular_name_short, singular_name, squad_select, ingested_at_utc) "
                "VALUES (1,'GKP','Goalkeeper',2,:now), (2,'DEF','Defender',5,:now), (3,'MID','Midfielder',5,:now), (4,'FWD','Forward',3,:now)"
            ),
            {"now": now},
        )
        conn.execute(
            text(
                "INSERT INTO teams (id, name, short_name, strength, ingested_at_utc) "
                "VALUES (1,'Team A','TA',4,:now), (2,'Team B','TB',4,:now)"
            ),
            {"now": now},
        )
        conn.execute(
            text(
                "INSERT INTO events (id, name, finished, ingested_at_utc) "
                "VALUES (1,'GW1',1,:now), (2,'GW2',0,:now)"
            ),
            {"now": now},
        )
        conn.execute(
            text(
                """INSERT INTO players (id, web_name, team_id, element_type_id, minutes, points_per_game, form, status, ingested_at_utc)
                   VALUES (100, 'P1', 1, 3, 900, 5.0, 5.0, 'a', :now),
                          (101, 'P2', 2, 4, 800, 4.5, 4.5, 'a', :now)"""
            ),
            {"now": now},
        )
        conn.execute(
            text(
                """INSERT INTO fixtures (id, event_id, team_h, team_a, finished, team_h_difficulty, team_a_difficulty, ingested_at_utc)
                   VALUES (1, 1, 1, 2, 1, 2, 4, :now), (2, 2, 1, 2, 0, 2, 4, :now),
                          (3, 1, 1, 2, 1, 3, 3, :now), (4, 1, 1, 2, 1, 4, 2, :now), (5, 1, 1, 2, 1, 1, 5, :now),
                          (6, 1, 2, 1, 1, 4, 2, :now), (7, 1, 2, 1, 1, 3, 3, :now), (8, 1, 2, 1, 1, 2, 4, :now)"""
            ),
            {"now": now},
        )
        # >= 10 rows so train_model succeeds. PK (player_id, fixture_id_effective).
        conn.execute(
            text(
                """INSERT INTO player_match_history (player_id, fixture_id_effective, event_id, total_points, minutes, ingested_at_utc)
                   VALUES (100, 1, 1, 6, 90, :now), (101, 1, 1, 5, 80, :now),
                          (100, 3, 1, 4, 70, :now), (101, 3, 1, 5, 85, :now),
                          (100, 4, 1, 7, 90, :now), (101, 4, 1, 3, 60, :now),
                          (100, 5, 1, 8, 90, :now), (101, 5, 1, 4, 75, :now),
                          (100, 6, 1, 2, 45, :now), (101, 6, 1, 6, 90, :now)"""
            ),
            {"now": now},
        )
        conn.commit()
    return engine


def test_build_training_data_columns(ml_db_engine) -> None:
    """build_training_data returns X with FEATURE_COLUMNS and y aligned."""
    X, y = build_training_data(ml_db_engine)
    assert list(X.columns) == FEATURE_COLUMNS
    assert len(X) == len(y)
    assert len(X) >= 1


def test_build_training_data_empty_returns_correct_shape(tmp_path) -> None:
    """With no player_match_history, build_training_data returns empty X with FEATURE_COLUMNS and empty y."""
    db_path = str(tmp_path / "empty.sqlite")
    engine = get_engine(db_path)
    init_db(engine)
    init_marts(engine)
    now = _utc_now().strftime("%Y-%m-%d %H:%M:%S")
    with engine.connect() as conn:
        conn.execute(text("INSERT INTO element_types (id, singular_name_short, singular_name, squad_select, ingested_at_utc) VALUES (3,'MID','Midfielder',5,:now)"), {"now": now})
        conn.execute(text("INSERT INTO teams (id, name, short_name, strength, ingested_at_utc) VALUES (1,'A','A',3,:now)"), {"now": now})
        conn.execute(text("INSERT INTO events (id, name, finished, ingested_at_utc) VALUES (1,'GW1',1,:now)"), {"now": now})
        conn.execute(
            text("INSERT INTO players (id, web_name, team_id, element_type_id, minutes, points_per_game, status, ingested_at_utc) VALUES (1,'X',1,3,0,0,'a',:now)"),
            {"now": now},
        )
        conn.commit()
    X, y = build_training_data(engine)
    assert list(X.columns) == FEATURE_COLUMNS
    assert len(X) == 0
    assert len(y) == 0


def test_build_xpts_rows_ml_returns_required_keys(ml_db_engine, tmp_path) -> None:
    """build_xpts_rows_ml returns list of dicts with required keys and types."""
    model_path = tmp_path / "xpts_gbm.json"
    train_model(ml_db_engine, model_path, validation_fraction=0.2)
    rows = build_xpts_rows_ml(ml_db_engine, horizon=1, model_path=model_path)
    assert isinstance(rows, list)
    for r in rows:
        assert set(r.keys()) >= REQUIRED_ROW_KEYS
        assert isinstance(r["player_id"], int)
        assert isinstance(r["event_id"], int)
        assert isinstance(r["xmins"], (int, float))
        assert isinstance(r["xpts"], (int, float))
        assert isinstance(r["xpts_att"], (int, float))
        assert isinstance(r["xpts_def"], (int, float))
        assert isinstance(r["xpts_app"], (int, float))
        assert r["xpts"] >= 0


def test_build_xpts_rows_ml_upsert_integration(ml_db_engine, tmp_path) -> None:
    """build_xpts_rows_ml output can be upserted into player_expected_points."""
    model_path = tmp_path / "xpts_gbm.json"
    train_model(ml_db_engine, model_path, validation_fraction=0.2)
    rows = build_xpts_rows_ml(ml_db_engine, horizon=1, model_path=model_path)
    assert len(rows) >= 1
    with get_session(ml_db_engine) as session:
        n = _upsert_player_expected_points(session, rows)
    assert n == len(rows)
    with ml_db_engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM player_expected_points")).scalar()
        assert count == len(rows)


def test_build_xpts_rows_ml_missing_model_raises(ml_db_engine) -> None:
    """build_xpts_rows_ml raises FileNotFoundError when model path does not exist."""
    with pytest.raises(FileNotFoundError, match="Model file not found|Run.*train_xpts"):
        build_xpts_rows_ml(ml_db_engine, horizon=1, model_path="/nonexistent/xpts_gbm")
