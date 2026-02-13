"""Baseline expected points (xPts) layer. No ML: minutes + fixture difficulty + simple form.

Assumptions documented in docstrings. Uses only FPL data already in silver (no odds, no understat).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# --- Fixture difficulty: FPL 1 (easiest) to 5 (hardest). Tune later. ---
DIFFICULTY_MULTIPLIER: dict[int, float] = {
    1: 1.15,
    2: 1.05,
    3: 1.00,
    4: 0.92,
    5: 0.85,
}

# Status codes that reduce expected minutes (injured, suspended, unavailable, doubtful).
# FPL: 'a' = available. We apply a penalty multiplier for non-available.
# Assumption: 40% of normal minutes for doubtful/unavailable; 0 for injured/suspended (or treat all as 0.4).
XMINS_STATUS_PENALTY: set[str] = {"i", "s", "u", "d"}
XMINS_STATUS_MULTIPLIER: float = 0.4  # If status in XMINS_STATUS_PENALTY, xmins *= this.

# Position ids: FPL element_type 1=GKP, 2=DEF, 3=MID, 4=FWD
ELEMENT_TYPE_GKP = 1
ELEMENT_TYPE_DEF = 2
ELEMENT_TYPE_MID = 3
ELEMENT_TYPE_FWD = 4


def get_difficulty_multiplier(difficulty: int | None) -> float:
    """Map FPL difficulty (1–5) to an xPts multiplier. Default 1.0 if out of range or None."""
    if difficulty is None or difficulty not in DIFFICULTY_MULTIPLIER:
        return 1.0
    return DIFFICULTY_MULTIPLIER[difficulty]


def clamp(value: float, lo: float, hi: float) -> float:
    """Clamp value to [lo, hi]."""
    return max(lo, min(hi, value))


@dataclass
class FormRow:
    """One row from v_player_form (last 5 games)."""

    player_id: int
    games_last5: int
    minutes_last5: int | None
    points_last5: int | None
    ppg_last5: float | None


@dataclass
class PlayerRow:
    """Player fields needed for xPts: id, team_id, element_type_id, minutes, points_per_game, status."""

    player_id: int
    team_id: int | None
    element_type_id: int | None
    minutes: int | None
    points_per_game: float | None
    status: str | None


def compute_xmins(
    form: FormRow | None,
    player: PlayerRow,
    finished_events_count: int,
) -> float:
    """Expected minutes for next gameweek.

    - If games_last5 >= 3: xmins = min(90, minutes_last5 / games_last5).
    - Else: fallback to players.minutes / max(1, finished_events_count), capped at 90.
    - If status in {'i','s','u','d'}: apply XMINS_STATUS_MULTIPLIER (default 0.4).
    """
    if form and form.games_last5 >= 3 and form.minutes_last5 is not None:
        avg = form.minutes_last5 / form.games_last5
        raw = min(90.0, avg)
    else:
        total_minutes = (player.minutes or 0) or 0
        events = max(1, finished_events_count)
        raw = min(90.0, total_minutes / events)
    status = (player.status or "").strip().lower()
    if status in XMINS_STATUS_PENALTY:
        raw *= XMINS_STATUS_MULTIPLIER
    return max(0.0, raw)


def compute_xpts_components(
    xmins: float,
    form: FormRow | None,
    player: PlayerRow,
    difficulty_mult: float,
) -> tuple[float, float, float, float]:
    """Compute xpts_app, xpts_att, xpts_def, and total xpts.

    Appearance: crude proxy p60 = clamp(xmins/90, 0, 1); xpts_app = 1*(xmins>0) + 1*p60
    (approx 1pt for playing + 1pt for 60+ mins).

    Form rate: base_pp90 from last 5 or points_per_game; strip ~2*(xmins/90) for appearance;
    base_nonapp_pp90 = max(0, base_pp90 - 2.0). Then xpts_nonapp = base_nonapp_pp90 * (xmins/90) * difficulty_mult.
    Split non-app by position: GK/DEF 40% att / 60% def; MID 70/30; FWD 90/10.
    """
    # --- Appearance component (approximation: 1pt for playing + 1pt for 60+ mins) ---
    p60 = clamp(xmins / 90.0, 0.0, 1.0)
    xpts_app = (1.0 if xmins > 0 else 0.0) + 1.0 * p60

    # --- Form points per 90 ---
    minutes_last5 = (form.minutes_last5 if form else None) or 0
    points_last5 = (form.points_last5 if form else None) or 0
    if minutes_last5 > 0:
        base_pp90 = (points_last5 / minutes_last5) * 90.0
    else:
        ppg = player.points_per_game if player.points_per_game is not None else 0.0
        base_pp90 = ppg  # FPL PPG is per game, treat as per-90 proxy

    # Strip appearance points crudely: ~2 pts per 90 for playing+60
    base_nonapp_pp90 = max(0.0, base_pp90 - 2.0)

    # Scale by minutes and fixture
    xpts_nonapp = base_nonapp_pp90 * (xmins / 90.0) * difficulty_mult

    # Split into att/def by position
    etype = player.element_type_id or 0
    if etype in (ELEMENT_TYPE_GKP, ELEMENT_TYPE_DEF):
        att_frac, def_frac = 0.4, 0.6
    elif etype == ELEMENT_TYPE_MID:
        att_frac, def_frac = 0.7, 0.3
    else:
        att_frac, def_frac = 0.9, 0.1

    xpts_att = xpts_nonapp * att_frac
    xpts_def = xpts_nonapp * def_frac
    xpts = xpts_app + xpts_nonapp
    return xpts_app, xpts_att, xpts_def, xpts


def _fetch_upcoming_event_ids(conn: Any, horizon: int) -> list[int]:
    """Return the next N event_ids that have at least one upcoming fixture, ordered by event_id."""
    rows = conn.execute(
        text("""
            SELECT DISTINCT event_id FROM v_fixture_upcoming
            WHERE event_id IS NOT NULL
            ORDER BY event_id
            LIMIT :horizon
        """),
        {"horizon": horizon},
    ).fetchall()
    return [r[0] for r in rows]


def _fetch_finished_events_count(conn: Any) -> int:
    """Number of finished gameweeks (for fallback xmins)."""
    row = conn.execute(text("SELECT COUNT(*) FROM events WHERE finished = 1")).fetchone()
    return row[0] if row else 0


def _fetch_form_by_player(conn: Any) -> dict[int, FormRow]:
    """player_id -> FormRow from v_player_form."""
    rows = conn.execute(
        text("""
            SELECT player_id, games_last5, minutes_last5, points_last5, ppg_last5
            FROM v_player_form
        """)
    ).fetchall()
    out: dict[int, FormRow] = {}
    for r in rows:
        out[r[0]] = FormRow(
            player_id=r[0],
            games_last5=r[1] or 0,
            minutes_last5=r[2],
            points_last5=r[3],
            ppg_last5=r[4],
        )
    return out


def _fetch_players(conn: Any) -> list[PlayerRow]:
    """All players with id, team_id, element_type_id, minutes, points_per_game, status."""
    rows = conn.execute(
        text("""
            SELECT id, team_id, element_type_id, minutes, points_per_game, status
            FROM players
        """)
    ).fetchall()
    return [
        PlayerRow(
            player_id=r[0],
            team_id=r[1],
            element_type_id=r[2],
            minutes=r[3],
            points_per_game=r[4],
            status=r[5],
        )
        for r in rows
    ]


def _fetch_difficulty_for_team_event(conn: Any) -> dict[tuple[int, int], int]:
    """(team_id, event_id) -> difficulty (1-5). From v_fixture_upcoming: if team is home use team_h_difficulty else team_a_difficulty."""
    rows = conn.execute(
        text("""
            SELECT event_id, team_h, team_a, team_h_difficulty, team_a_difficulty
            FROM v_fixture_upcoming
            WHERE event_id IS NOT NULL AND team_h IS NOT NULL AND team_a IS NOT NULL
        """)
    ).fetchall()
    out: dict[tuple[int, int], int] = {}
    for r in rows:
        event_id, team_h, team_a, diff_h, diff_a = r[0], r[1], r[2], r[3], r[4]
        if team_h is not None:
            out[(team_h, event_id)] = diff_h if diff_h is not None else 3
        if team_a is not None:
            out[(team_a, event_id)] = diff_a if diff_a is not None else 3
    return out


def build_xpts_rows(engine: Engine, horizon: int) -> list[dict[str, Any]]:
    """Compute expected points for each player and each upcoming event in the horizon.

    Returns list of dicts suitable for upsert into player_expected_points:
    player_id, event_id, xmins, xpts, xpts_att, xpts_def, xpts_app, computed_at_utc.
    Only includes players with a team_id and only events that have upcoming fixtures.
    """
    computed_at = datetime.now(timezone.utc).replace(tzinfo=None)
    rows_out: list[dict[str, Any]] = []

    with engine.connect() as conn:
        event_ids = _fetch_upcoming_event_ids(conn, horizon)
        if not event_ids:
            logger.warning("No upcoming event_ids found; run update_core and ensure fixtures have event_id")
            return rows_out

        finished_count = _fetch_finished_events_count(conn)
        form_by_player = _fetch_form_by_player(conn)
        players = _fetch_players(conn)
        diff_map = _fetch_difficulty_for_team_event(conn)

    for player in players:
        if player.team_id is None:
            continue
        form = form_by_player.get(player.player_id)
        for event_id in event_ids:
            difficulty = diff_map.get((player.team_id, event_id))
            if difficulty is None:
                # No fixture for this team in this event (e.g. blank GW); skip or use neutral
                difficulty = 3
            difficulty_mult = get_difficulty_multiplier(difficulty)
            xmins = compute_xmins(form, player, finished_count)
            xpts_app, xpts_att, xpts_def, xpts = compute_xpts_components(
                xmins, form, player, difficulty_mult
            )
            rows_out.append({
                "player_id": player.player_id,
                "event_id": event_id,
                "xmins": round(xmins, 2),
                "xpts": round(xpts, 2),
                "xpts_att": round(xpts_att, 2),
                "xpts_def": round(xpts_def, 2),
                "xpts_app": round(xpts_app, 2),
                "computed_at_utc": computed_at,
            })
    return rows_out
