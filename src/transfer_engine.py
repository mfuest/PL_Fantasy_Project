"""Transfer engine: best XI from 15, one-transfer swaps with budget/position/team constraints.

Ranks by xPts delta; returns top 10 suggestions + team xPts per suggestion.
Uses player_expected_points and players (cost, position, team) from DB.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# FPL element_type: 1=GKP, 2=DEF, 3=MID, 4=FWD. Squad: 2 GKP, 5 DEF, 5 MID, 3 FWD.
# Best XI: 1 GKP, then (n_def, n_mid, n_fwd) with n_def in [3,5], n_mid in [2,5], n_fwd in [1,3], n_def+n_mid+n_fwd=10.
VALID_DEF = (3, 4, 5)
VALID_MID = (2, 3, 4, 5)
VALID_FWD = (1, 2, 3)
MAX_PLAYERS_PER_TEAM = 3


@dataclass
class PlayerInfo:
    """Minimal player info for transfer engine: id, cost (tenths), position, team_id."""

    player_id: int
    now_cost: int  # FPL tenths (55 = £5.5)
    element_type_id: int
    team_id: int | None


@dataclass
class Suggestion:
    """One transfer suggestion: out → in, with team xPts delta and new team xPts."""

    out_player_id: int
    in_player_id: int
    out_web_name: str
    in_web_name: str
    team_xpts_delta: float
    new_team_xpts: float
    cost_delta_million: float  # positive = spending more


def _fetch_xpts_for_event(conn: Any, event_id: int) -> dict[int, float]:
    """player_id -> xpts for the given event_id from player_expected_points."""
    rows = conn.execute(
        text("""
            SELECT player_id, xpts FROM player_expected_points
            WHERE event_id = :event_id
        """),
        {"event_id": event_id},
    ).fetchall()
    return {r[0]: float(r[1]) for r in rows}


def _fetch_player_info(conn: Any, player_ids: list[int] | None = None) -> dict[int, PlayerInfo]:
    """player_id -> PlayerInfo. If player_ids given, filter to those; else all players."""
    if player_ids:
        placeholders = ",".join(":p" + str(i) for i in range(len(player_ids)))
        params = {"p" + str(i): pid for i, pid in enumerate(player_ids)}
        where = f" AND p.id IN ({placeholders})"
    else:
        params = {}
        where = ""
    rows = conn.execute(
        text(f"""
            SELECT p.id, p.now_cost, p.element_type_id, p.team_id
            FROM players p
            WHERE p.now_cost IS NOT NULL {where}
        """),
        params,
    ).fetchall()
    return {
        r[0]: PlayerInfo(
            player_id=r[0],
            now_cost=r[1] or 0,
            element_type_id=r[2] or 0,
            team_id=r[3],
        )
        for r in rows
    }


def _fetch_web_names(conn: Any, player_ids: list[int]) -> dict[int, str]:
    """player_id -> web_name."""
    if not player_ids:
        return {}
    placeholders = ",".join(":p" + str(i) for i in range(len(player_ids)))
    params = {"p" + str(i): pid for i, pid in enumerate(player_ids)}
    rows = conn.execute(
        text(f"SELECT id, web_name FROM players WHERE id IN ({placeholders})"),
        params,
    ).fetchall()
    return {r[0]: (r[1] or "") for r in rows}


def _count_by_position(squad_ids: list[int], info: dict[int, PlayerInfo]) -> tuple[int, int, int, int]:
    """Return (n_gkp, n_def, n_mid, n_fwd) for the squad."""
    gkp = def_ = mid = fwd = 0
    for pid in squad_ids:
        pinfo = info.get(pid)
        if not pinfo:
            continue
        if pinfo.element_type_id == 1:
            gkp += 1
        elif pinfo.element_type_id == 2:
            def_ += 1
        elif pinfo.element_type_id == 3:
            mid += 1
        elif pinfo.element_type_id == 4:
            fwd += 1
    return gkp, def_, mid, fwd


def _team_counts(squad_ids: list[int], info: dict[int, PlayerInfo]) -> dict[int, int]:
    """team_id -> number of players in squad (only teams with at least one)."""
    counts: dict[int, int] = {}
    for pid in squad_ids:
        pinfo = info.get(pid)
        if pinfo and pinfo.team_id is not None:
            counts[pinfo.team_id] = counts.get(pinfo.team_id, 0) + 1
    return counts


def _best_xi(squad_ids: list[int], xpts: dict[int, float], info: dict[int, PlayerInfo]) -> list[int]:
    """From 15 squad players, pick best XI by xPts respecting formation (1 GKP, 3-5 DEF, 2-5 MID, 1-3 FWD)."""
    gkp_ids = []
    def_ids = []
    mid_ids = []
    fwd_ids = []
    for pid in squad_ids:
        pinfo = info.get(pid)
        if not pinfo:
            continue
        pts = xpts.get(pid, 0.0)
        if pinfo.element_type_id == 1:
            gkp_ids.append((pid, pts))
        elif pinfo.element_type_id == 2:
            def_ids.append((pid, pts))
        elif pinfo.element_type_id == 3:
            mid_ids.append((pid, pts))
        elif pinfo.element_type_id == 4:
            fwd_ids.append((pid, pts))

    # Best 1 GKP
    gkp_ids.sort(key=lambda x: -x[1])
    xi = [gkp_ids[0][0]] if gkp_ids else []

    # Sort by xpts descending for DEF, MID, FWD
    def_ids.sort(key=lambda x: -x[1])
    mid_ids.sort(key=lambda x: -x[1])
    fwd_ids.sort(key=lambda x: -x[1])

    # Try valid formations (n_def, n_mid, n_fwd) and pick the one that maximizes total xPts
    best_total = -1.0
    best_xi: list[int] = []
    for n_def in VALID_DEF:
        for n_mid in VALID_MID:
            for n_fwd in VALID_FWD:
                if n_def + n_mid + n_fwd != 10:
                    continue
                if n_def > len(def_ids) or n_mid > len(mid_ids) or n_fwd > len(fwd_ids):
                    continue
                team = (
                    xi
                    + [p for p, _ in def_ids[:n_def]]
                    + [p for p, _ in mid_ids[:n_mid]]
                    + [p for p, _ in fwd_ids[:n_fwd]]
                )
                total = sum(xpts.get(p, 0.0) for p in team)
                if total > best_total:
                    best_total = total
                    best_xi = team
    return best_xi if best_xi else xi + [p for p, _ in def_ids[:3]] + [p for p, _ in mid_ids[:4]] + [p for p, _ in fwd_ids[:3]]


def _squad_budget_tenths(squad_ids: list[int], info: dict[int, PlayerInfo], bank_tenths: int) -> int:
    """Total budget in tenths: sum of current costs of squad + bank (bank in tenths, e.g. 5 = £0.5)."""
    total = bank_tenths
    for pid in squad_ids:
        pinfo = info.get(pid)
        if pinfo:
            total += pinfo.now_cost
    return total


def run_transfer_engine(
    engine: Engine,
    squad_player_ids: list[int],
    event_id: int,
    bank_million: float = 0.0,
    top_n: int = 10,
) -> tuple[float, list[Suggestion]]:
    """Run one-transfer suggestions for the given 15-man squad and gameweek.

    Returns (current_team_xpts, list of top_n suggestions with team_xpts_delta and new_team_xpts).
    """
    if len(squad_player_ids) != 15:
        logger.warning("Squad should have 15 players; got %s", len(squad_player_ids))

    with engine.connect() as conn:
        xpts = _fetch_xpts_for_event(conn, event_id)
        info = _fetch_player_info(conn, squad_player_ids)
        all_info = _fetch_player_info(conn, None)
        names = _fetch_web_names(conn, squad_player_ids)

    # Ensure we have info for all squad players (from DB or all_info)
    for pid in squad_player_ids:
        if pid not in info and pid in all_info:
            info[pid] = all_info[pid]
        if pid not in names:
            with engine.connect() as c2:
                names.update(_fetch_web_names(c2, [pid]))

    best_xi_ids = _best_xi(squad_player_ids, xpts, info)
    current_team_xpts = sum(xpts.get(p, 0.0) for p in best_xi_ids)
    squad_set = set(squad_player_ids)
    bank_tenths = int(round(bank_million * 10))

    # Build list of (out_id, in_id, delta, new_xpts) for valid one-transfer swaps
    suggestions: list[tuple[int, int, float, float]] = []
    team_counts = _team_counts(squad_player_ids, info)

    for out_id in squad_player_ids:
        out_info = info.get(out_id)
        if not out_info:
            continue
        # Budget if we sell out_id: bank + sell price (use current price as sell price)
        budget_tenths = bank_tenths + out_info.now_cost
        out_team_id = out_info.team_id
        pos = out_info.element_type_id

        # Candidates: same position, not in squad, cost <= budget, and after swap no team > 3
        for in_id, in_info in all_info.items():
            if in_id in squad_set:
                continue
            if in_info.element_type_id != pos:
                continue
            if in_info.now_cost > budget_tenths:
                continue
            # Team constraint: max 3 per team. After swap, in_team gains one (unless out was same team).
            in_team = in_info.team_id
            if in_team is not None:
                current_in_team = team_counts.get(in_team, 0)
                if out_team_id == in_team:
                    pass  # same team: count unchanged, still must be <= 3
                else:
                    if current_in_team >= MAX_PLAYERS_PER_TEAM:
                        continue  # already 3 from that team

            # New squad and new XI
            new_squad = [p if p != out_id else in_id for p in squad_player_ids]
            new_info = {**info}
            new_info[in_id] = in_info
            if out_id in new_info:
                del new_info[out_id]
            new_xi = _best_xi(new_squad, xpts, new_info)
            new_team_xpts = sum(xpts.get(p, 0.0) for p in new_xi)
            delta = new_team_xpts - current_team_xpts
            suggestions.append((out_id, in_id, delta, new_team_xpts))

    # Sort by delta descending, take top_n
    suggestions.sort(key=lambda x: -x[2])
    top = suggestions[:top_n]

    with engine.connect() as conn:
        all_names = _fetch_web_names(conn, [s[0] for s in top] + [s[1] for s in top])
    names.update(all_names)

    result = []
    for out_id, in_id, delta, new_xpts in top:
        cost_out = info.get(out_id)
        cost_in = all_info.get(in_id)
        cost_delta = 0.0
        if cost_out and cost_in:
            cost_delta = (cost_in.now_cost - cost_out.now_cost) / 10.0  # in million
        result.append(
            Suggestion(
                out_player_id=out_id,
                in_player_id=in_id,
                out_web_name=names.get(out_id, ""),
                in_web_name=names.get(in_id, ""),
                team_xpts_delta=round(delta, 2),
                new_team_xpts=round(new_xpts, 2),
                cost_delta_million=round(cost_delta, 2),
            )
        )
    return round(current_team_xpts, 2), result
