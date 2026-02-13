"""Normalize FPL API JSON into row dicts for silver tables. Uses to_int/to_float/to_dt for safe typing."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

# ---------- Type helpers (FPL often returns numbers/dates as strings) ----------


def to_int(x: Any) -> Optional[int]:
    """Safe parse to int. Returns None for None, empty string, or invalid."""
    if x is None:
        return None
    if isinstance(x, int):
        return x
    if isinstance(x, float):
        return int(x) if x == x else None
    try:
        return int(float(str(x).strip()))
    except (ValueError, TypeError):
        return None


def to_float(x: Any) -> Optional[float]:
    """Safe parse to float. Returns None for None, empty string, or invalid."""
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    try:
        return float(str(x).strip())
    except (ValueError, TypeError):
        return None


def to_dt(x: Any) -> Optional[datetime]:
    """Parse ISO timestamp to UTC datetime. Returns None if missing or invalid."""
    if x is None:
        return None
    if isinstance(x, datetime):
        return x.astimezone(timezone.utc) if x.tzinfo else x.replace(tzinfo=timezone.utc)
    try:
        s = str(x).strip()
        if not s:
            return None
        # Handle Z and +00:00
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).astimezone(timezone.utc).replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


# ---------- Bootstrap-static (teams, element_types, events, players only; no fixtures) ----------


def normalize_bootstrap_static(payload: dict[str, Any]) -> dict[str, Any]:
    """Extract teams, element_types, events, players. Fixtures come from /fixtures/ only."""
    out: dict[str, Any] = {"teams": [], "element_types": [], "events": [], "players": []}
    for t in payload.get("teams") or []:
        out["teams"].append(
            {
                "id": to_int(t.get("id")),
                "name": t.get("name"),
                "short_name": t.get("short_name"),
                "strength": to_int(t.get("strength")),
            }
        )
    for e in payload.get("element_types") or []:
        out["element_types"].append(
            {
                "id": to_int(e.get("id")),
                "singular_name_short": e.get("singular_name_short"),
                "singular_name": e.get("singular_name"),
                "squad_select": to_int(e.get("squad_select")),
            }
        )
    for ev in payload.get("events") or []:
        out["events"].append(
            {
                "id": to_int(ev.get("id")),
                "name": ev.get("name"),
                "deadline_time": to_dt(ev.get("deadline_time")),
                "finished": ev.get("finished"),
                "is_current": ev.get("is_current"),
                "is_next": ev.get("is_next"),
            }
        )
    for p in payload.get("elements") or []:
        out["players"].append(
            {
                "id": to_int(p.get("id")),
                "web_name": p.get("web_name"),
                "first_name": p.get("first_name"),
                "second_name": p.get("second_name"),
                "team_id": to_int(p.get("team")),
                "element_type_id": to_int(p.get("element_type")),
                "now_cost": to_int(p.get("now_cost")),
                "status": p.get("status"),
                "minutes": to_int(p.get("minutes")),
                "total_points": to_int(p.get("total_points")),
                "selected_by_percent": to_float(p.get("selected_by_percent")),
                "form": to_float(p.get("form")),
                "points_per_game": to_float(p.get("points_per_game")),
                "expected_goals": to_float(p.get("expected_goals")),
                "expected_assists": to_float(p.get("expected_assists")),
                "expected_goal_involvements": to_float(p.get("expected_goal_involvements")),
                "expected_goals_conceded": to_float(p.get("expected_goals_conceded")),
            }
        )
    return out


# ---------- Fixtures (from /fixtures/) ----------


def normalize_fixtures(payload: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert fixtures array to silver row dicts."""
    rows = []
    for f in payload or []:
        rows.append(
            {
                "id": to_int(f.get("id")),
                "event_id": to_int(f.get("event")),
                "team_h": to_int(f.get("team_h")),
                "team_a": to_int(f.get("team_a")),
                "kickoff_time": to_dt(f.get("kickoff_time")),
                "finished": f.get("finished"),
                "team_h_difficulty": to_int(f.get("team_h_difficulty")),
                "team_a_difficulty": to_int(f.get("team_a_difficulty")),
            }
        )
    return rows


# ---------- Element-summary: history ----------


def _effective_fixture_id(fixture_id: Optional[int], event_id: Optional[int], index: int) -> int:
    """PK uses fixture_id_effective: real fixture_id when present, else synthetic."""
    if fixture_id is not None:
        return fixture_id
    e = event_id or 0
    return -(e * 1000 + index)


def normalize_element_summary_history(player_id: int, payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert element-summary 'history' to player_match_history rows. Uses fixture_id_effective for PK."""
    rows = []
    for i, h in enumerate(payload.get("history") or []):
        fid = to_int(h.get("fixture"))
        eid = to_int(h.get("round"))
        effective = _effective_fixture_id(fid, eid, i)
        rows.append(
            {
                "player_id": player_id,
                "fixture_id_effective": effective,
                "fixture_id": fid,
                "event_id": eid,
                "minutes": to_int(h.get("minutes")),
                "total_points": to_int(h.get("total_points")),
                "goals_scored": to_int(h.get("goals_scored")),
                "assists": to_int(h.get("assists")),
                "clean_sheets": to_int(h.get("clean_sheets")),
                "goals_conceded": to_int(h.get("goals_conceded")),
                "expected_goals": to_float(h.get("expected_goals")),
                "expected_assists": to_float(h.get("expected_assists")),
                "expected_goal_involvements": to_float(h.get("expected_goal_involvements")),
                "expected_goals_conceded": to_float(h.get("expected_goals_conceded")),
            }
        )
    return rows


# ---------- Element-summary: fixtures (upcoming) ----------


def normalize_element_summary_fixtures(player_id: int, payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert element-summary 'fixtures' to player_future_fixtures rows. Minimal: player_id, fixture_id, difficulty, etc."""
    rows = []
    for f in payload.get("fixtures") or []:
        rows.append(
            {
                "player_id": player_id,
                "fixture_id": to_int(f.get("id")),
                "event_id": to_int(f.get("event")),
                "is_home": f.get("is_home"),
                "opponent_team": to_int(f.get("opponent_team")),
                "kickoff_time": to_dt(f.get("kickoff_time")),
                "difficulty": to_int(f.get("difficulty")),
            }
        )
    return rows


# ---------- Entry picks (optional) ----------


def normalize_entry_picks(payload: dict[str, Any]) -> dict[str, Any]:
    """Parse entry/{id}/event/{gw}/picks for squad (picks), entry_history if present. Bank/FT not promised."""
    picks = payload.get("picks") or []
    entry_history = payload.get("entry_history") or {}
    return {
        "picks": [
            {
                "element": to_int(p.get("element")),
                "position": to_int(p.get("position")),
                "is_captain": p.get("is_captain"),
                "is_vice_captain": p.get("is_vice_captain") or p.get("multiplier") == 0,
            }
            for p in picks
        ],
        "entry_history": {
            "bank": entry_history.get("bank"),
            "value": entry_history.get("value"),
            "event_transfers": entry_history.get("event_transfers"),
            "event_transfers_cost": entry_history.get("event_transfers_cost"),
        },
    }


# ---------- Entry history (entry/{team_id}/history/) for bank/FT context ----------


def normalize_entry_history(payload: dict[str, Any]) -> dict[str, Any]:
    """Parse entry/{id}/history/ for current season GW history and past seasons. Used for bank/FT context."""
    current = payload.get("current") or []
    past = payload.get("past") or []
    return {
        "current": [
            {
                "event": to_int(gw.get("event")),
                "points": to_int(gw.get("points")),
                "total_points": to_int(gw.get("total_points")),
                "rank": to_int(gw.get("rank")),
                "event_transfers": to_int(gw.get("event_transfers")),
                "event_transfers_cost": to_int(gw.get("event_transfers_cost")),
                "value": to_int(gw.get("value")),
                "points_on_bench": to_int(gw.get("points_on_bench")),
            }
            for gw in current
        ],
        "past": [
            {
                "season_name": s.get("season_name"),
                "total_points": to_int(s.get("total_points")),
                "rank": to_int(s.get("rank")),
            }
            for s in past
        ],
    }
