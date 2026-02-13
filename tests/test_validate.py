"""Tests for validation severity levels and exit behavior."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.db import init_db
from src.validate import (
    ValidationReport,
    run_validation,
    should_exit_nonzero,
)


@pytest.fixture
def engine() -> Engine:
    """In-memory SQLite engine with schema (no data)."""
    e = create_engine("sqlite:///:memory:", future=True)
    init_db(e)
    return e


def _seed_minimal_valid(engine: Engine) -> None:
    """Seed enough rows to pass count checks (warn thresholds: players>600, fixtures 300-400)."""
    with engine.connect() as conn:
        conn.execute(text("INSERT INTO teams (id, name, short_name, strength, ingested_at_utc) VALUES (1, 'A', 'A', 1, '2025-01-01')"))
        conn.execute(text("INSERT INTO element_types (id, singular_name_short, singular_name, squad_select, ingested_at_utc) VALUES (1, 'GKP', 'Goalkeeper', 1, '2025-01-01')"))
        conn.execute(text("INSERT INTO events (id, name, deadline_time, finished, is_current, is_next, ingested_at_utc) VALUES (1, 'GW1', '2025-01-01', 1, 0, 0, '2025-01-01')"))
        for i in range(1, 602):
            conn.execute(
                text(
                    "INSERT INTO players (id, web_name, team_id, element_type_id, ingested_at_utc) VALUES (:i, 'P', 1, 1, '2025-01-01')"
                ),
                {"i": i},
            )
        for i in range(1, 351):
            conn.execute(
                text(
                    "INSERT INTO fixtures (id, event_id, team_h, team_a, kickoff_time, finished, ingested_at_utc) VALUES (:i, 1, 1, 1, '2025-01-01', 0, '2025-01-01')"
                ),
                {"i": i},
            )
        conn.execute(
            text(
                "INSERT INTO player_match_history (player_id, fixture_id_effective, minutes, total_points, ingested_at_utc) VALUES (1, 1, 90, 5, '2025-01-01')"
            )
        )
        conn.execute(
            text(
                "INSERT INTO player_future_fixtures (player_id, fixture_id, ingested_at_utc) VALUES (1, 1, '2025-01-01')"
            )
        )
        conn.commit()


def _seed_hard_fail_minutes(engine: Engine) -> None:
    """One row with minutes > 130 to trigger hard fail (update existing row)."""
    with engine.connect() as conn:
        conn.execute(
            text(
                "UPDATE player_match_history SET minutes = 150 WHERE player_id = 1 AND fixture_id_effective = 1"
            )
        )
        conn.commit()


def test_validation_level_hard_exit_on_errors(engine: Engine) -> None:
    """With level=hard, exit 1 only when there are hard errors."""
    _seed_minimal_valid(engine)
    _seed_hard_fail_minutes(engine)
    report = run_validation(engine)
    assert not report.is_ok()
    assert report.errors
    assert should_exit_nonzero("hard", report) is True
    assert should_exit_nonzero("strict", report) is True
    assert should_exit_nonzero("warn", report) is False


def test_validation_level_strict_exit_on_warnings(engine: Engine) -> None:
    """With level=strict, exit 1 when there are warnings (even if no hard errors)."""
    with engine.connect() as conn:
        conn.execute(text("INSERT INTO teams (id, name, short_name, strength, ingested_at_utc) VALUES (1, 'A', 'A', 1, '2025-01-01')"))
        conn.execute(text("INSERT INTO element_types (id, singular_name_short, singular_name, squad_select, ingested_at_utc) VALUES (1, 'GKP', 'GKP', 1, '2025-01-01')"))
        conn.execute(text("INSERT INTO events (id, name, deadline_time, finished, is_current, is_next, ingested_at_utc) VALUES (1, 'GW1', null, 0, 0, 0, '2025-01-01')"))
        conn.execute(text("INSERT INTO players (id, web_name, team_id, element_type_id, ingested_at_utc) VALUES (1, 'P', 1, 1, '2025-01-01')"))
        conn.execute(text("INSERT INTO fixtures (id, event_id, team_h, team_a, kickoff_time, finished, ingested_at_utc) VALUES (1, 1, 1, 1, '2025-01-01', 0, '2025-01-01')"))
        conn.execute(text("INSERT INTO player_match_history (player_id, fixture_id_effective, minutes, total_points, ingested_at_utc) VALUES (1, 1, 90, 5, '2025-01-01')"))
        conn.execute(text("INSERT INTO player_future_fixtures (player_id, fixture_id, ingested_at_utc) VALUES (1, 1, '2025-01-01')"))
        conn.commit()
    report = run_validation(engine)
    assert report.is_ok()
    assert report.has_warnings()
    assert should_exit_nonzero("hard", report) is False
    assert should_exit_nonzero("strict", report) is True
    assert should_exit_nonzero("warn", report) is False


def test_validation_level_warn_never_exit(engine: Engine) -> None:
    """With level=warn, never exit 1."""
    _seed_minimal_valid(engine)
    _seed_hard_fail_minutes(engine)
    report = run_validation(engine)
    assert should_exit_nonzero("warn", report) is False


def test_validation_report_has_errors_and_warnings(engine: Engine) -> None:
    """Report separates errors (hard) and warnings."""
    report = ValidationReport()
    report.fail("hard one")
    report.warn("warn one")
    assert report.is_ok() is False
    assert report.has_warnings() is True
    assert "hard one" in report.errors
    assert "warn one" in report.warnings
