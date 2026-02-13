"""Unit tests for normalizers using saved example JSON in tests/fixtures/."""

import json
from pathlib import Path

import pytest

from src.normalize import (
    normalize_bootstrap_static,
    normalize_element_summary_fixtures,
    normalize_element_summary_history,
    normalize_fixtures,
    to_float,
    to_int,
)


def _load_fixture(path: Path) -> dict | list:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def test_normalize_teams_from_bootstrap(bootstrap_static_path: Path) -> None:
    """Load bootstrap_static_sample.json and assert teams list shape and sample values."""
    payload = _load_fixture(bootstrap_static_path)
    out = normalize_bootstrap_static(payload)
    assert "teams" in out
    assert "element_types" in out
    assert "events" in out
    assert "players" in out
    assert "fixtures" not in out

    teams = out["teams"]
    assert len(teams) == 2
    t0 = teams[0]
    assert t0["id"] == 1
    assert t0["name"] == "Arsenal"
    assert t0["short_name"] == "ARS"
    assert t0["strength"] == 4


def test_normalize_players_from_bootstrap(bootstrap_static_path: Path) -> None:
    """Assert players list, required fields, and .get() handling for missing expected_goals."""
    payload = _load_fixture(bootstrap_static_path)
    out = normalize_bootstrap_static(payload)
    players = out["players"]
    assert len(players) == 2

    p0 = players[0]
    assert p0["id"] == 1
    assert p0["web_name"] == "Raya"
    assert p0["team_id"] == 1
    assert p0["now_cost"] == 55
    assert p0["total_points"] == 45
    assert p0["selected_by_percent"] == 12.5
    assert p0["form"] == 4.5
    assert p0["expected_goals"] == 0.0

    p1 = players[1]
    assert p1["id"] == 2
    assert p1["web_name"] == "Saka"
    assert p1["total_points"] == 120
    assert p1["expected_goals"] is None
    assert p1["expected_assists"] is None


def test_normalize_element_summary_history(element_summary_path: Path) -> None:
    """Load element_summary_sample.json and assert history rows have player_id and fixture_id_effective."""
    payload = _load_fixture(element_summary_path)
    player_id = 42
    rows = normalize_element_summary_history(player_id, payload)
    assert len(rows) == 2

    r0 = rows[0]
    assert r0["player_id"] == player_id
    assert r0["fixture_id_effective"] == 1
    assert r0["fixture_id"] == 1
    assert r0["event_id"] == 1
    assert r0["total_points"] == 6
    assert r0["expected_goals"] == 0.1

    r1 = rows[1]
    assert r1["player_id"] == player_id
    assert r1["fixture_id_effective"] == 2
    assert r1["fixture_id"] == 2
    assert r1["event_id"] == 2
    assert r1["goals_scored"] == 1


def test_normalize_fixtures(fixtures_sample_path: Path) -> None:
    """Load fixtures_sample.json and assert normalized fixture rows."""
    payload = _load_fixture(fixtures_sample_path)
    rows = normalize_fixtures(payload)
    assert len(rows) == 2
    assert rows[0]["id"] == 1
    assert rows[0]["team_h"] == 1
    assert rows[0]["team_a"] == 2
    assert rows[0]["finished"] is True


def test_to_int_to_float() -> None:
    """Type helpers handle None, strings, and invalid values."""
    assert to_int(None) is None
    assert to_int("42") == 42
    assert to_int(42) == 42
    assert to_int("") is None
    assert to_float("12.5") == 12.5
    assert to_float(None) is None
    assert to_float("") is None
