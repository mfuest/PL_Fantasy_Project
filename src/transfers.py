"""Transfer suggestion engine. Uses player_expected_points and best-XI logic only (no new model)."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# FPL element_type: 1=GKP, 2=DEF, 3=MID, 4=FWD
ELEMENT_TYPE_GKP = 1
ELEMENT_TYPE_DEF = 2
ELEMENT_TYPE_MID = 3
ELEMENT_TYPE_FWD = 4

# Valid formations (n_gkp, n_def, n_mid, n_fwd) with 1 GKP, 3-5 DEF, 2-5 MID, 1-3 FWD
FORMATION_SLOTS: list[tuple[int, int, int, int]] = [
    (1, 3, 5, 2),
    (1, 3, 4, 3),
    (1, 4, 5, 1),
    (1, 4, 4, 2),
    (1, 4, 3, 3),
    (1, 5, 4, 1),
    (1, 5, 3, 2),
]

MAX_PLAYERS_PER_TEAM = 3


@dataclass
class PlayerInfo:
    """Minimal player info for transfers: id, cost, position, team."""

    player_id: int
    now_cost: int  # FPL tenths (e.g. 55 = £5.5)
    element_type_id: int
    team_id: int
    web_name: str = ""


def get_next_event_id(engine: Engine) -> int | None:
    """Next gameweek: MIN(event_id) in player_expected_points."""
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT MIN(event_id) FROM player_expected_points")
        ).fetchone()
    if row and row[0] is not None:
        return int(row[0])
    return None


def _load_players(engine: Engine) -> dict[int, PlayerInfo]:
    """All players: id -> PlayerInfo (now_cost, element_type_id, team_id, web_name)."""
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT id, now_cost, element_type_id, team_id, COALESCE(web_name, '')
                FROM players
                WHERE now_cost IS NOT NULL AND element_type_id IS NOT NULL AND team_id IS NOT NULL
            """)
        ).fetchall()
    return {
        r[0]: PlayerInfo(
            player_id=r[0],
            now_cost=int(r[1]) if r[1] is not None else 0,
            element_type_id=int(r[2]),
            team_id=int(r[3]),
            web_name=r[4] or "",
        )
        for r in rows
    }


def _load_xpts_for_event(engine: Engine, event_id: int) -> dict[int, float]:
    """player_id -> xpts for the given event_id."""
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT player_id, xpts FROM player_expected_points WHERE event_id = :eid
            """),
            {"eid": event_id},
        ).fetchall()
    return {r[0]: float(r[1]) for r in rows}


def _split_squad_by_position(
    squad_ids: list[int],
    players_by_id: dict[int, PlayerInfo],
) -> dict[int, list[int]]:
    """Return {ELEMENT_TYPE_*: [player_id, ...]} for players in squad that we have info for."""
    by_pos: dict[int, list[int]] = {
        ELEMENT_TYPE_GKP: [],
        ELEMENT_TYPE_DEF: [],
        ELEMENT_TYPE_MID: [],
        ELEMENT_TYPE_FWD: [],
    }
    for pid in squad_ids:
        p = players_by_id.get(pid)
        if p is None:
            continue
        if p.element_type_id in by_pos:
            by_pos[p.element_type_id].append(pid)
    return by_pos


def _pick_best_for_slots(
    candidates: list[int],
    n_slots: int,
    xpts: dict[int, float],
    players_by_id: dict[int, PlayerInfo],
    team_used: dict[int, int],
) -> list[int]:
    """Pick up to n_slots from candidates by xpts desc, respecting max 3 per team. Returns selected ids."""
    sorted_ids = sorted(
        candidates,
        key=lambda pid: xpts.get(pid, 0.0),
        reverse=True,
    )
    selected: list[int] = []
    for pid in sorted_ids:
        if len(selected) >= n_slots:
            break
        p = players_by_id.get(pid)
        if p is None:
            continue
        if team_used.get(p.team_id, 0) >= MAX_PLAYERS_PER_TEAM:
            continue
        selected.append(pid)
        team_used[p.team_id] = team_used.get(p.team_id, 0) + 1
    return selected


def best_xi(
    squad_ids: list[int],
    players_by_id: dict[int, PlayerInfo],
    xpts: dict[int, float],
) -> tuple[list[int], float]:
    """Pick best XI from the 15 squad players.

    Constraints: 1 GKP, 3-5 DEF, 2-5 MID, 1-3 FWD, max 3 per team.
    Tries all valid formations and returns the XI with highest total xPts.

    Returns (list of 11 player_ids, total_xpts).
    """
    by_pos = _split_squad_by_position(squad_ids, players_by_id)
    best_xi_ids: list[int] = []
    best_total: float = -1.0

    for n_gkp, n_def, n_mid, n_fwd in FORMATION_SLOTS:
        team_used: dict[int, int] = {}
        gkp = _pick_best_for_slots(
            by_pos[ELEMENT_TYPE_GKP], n_gkp, xpts, players_by_id, team_used
        )
        if len(gkp) < n_gkp:
            continue
        defs = _pick_best_for_slots(
            by_pos[ELEMENT_TYPE_DEF], n_def, xpts, players_by_id, team_used
        )
        if len(defs) < n_def:
            continue
        mids = _pick_best_for_slots(
            by_pos[ELEMENT_TYPE_MID], n_mid, xpts, players_by_id, team_used
        )
        if len(mids) < n_mid:
            continue
        fwds = _pick_best_for_slots(
            by_pos[ELEMENT_TYPE_FWD], n_fwd, xpts, players_by_id, team_used
        )
        if len(fwds) < n_fwd:
            continue
        xi: list[int] = gkp + defs + mids + fwds
        total = sum(xpts.get(pid, 0.0) for pid in xi)
        if total > best_total:
            best_total = total
            best_xi_ids = xi
    return best_xi_ids, max(0.0, best_total)


def _team_counts(squad_ids: list[int], players_by_id: dict[int, PlayerInfo]) -> dict[int, int]:
    """Count players per team in squad."""
    counts: dict[int, int] = {}
    for pid in squad_ids:
        p = players_by_id.get(pid)
        if p is not None:
            counts[p.team_id] = counts.get(p.team_id, 0) + 1
    return counts


def _can_add_player(
    buy_id: int,
    sell_id: int,
    squad_ids: list[int],
    players_by_id: dict[int, PlayerInfo],
) -> bool:
    """After selling sell_id and adding buy_id, would we exceed max 3 per team?"""
    sell_team = None
    for pid in squad_ids:
        p = players_by_id.get(pid)
        if p is None:
            continue
        if pid == sell_id:
            sell_team = p.team_id
            break
    buy = players_by_id.get(buy_id)
    if buy is None:
        return False
    counts = _team_counts(squad_ids, players_by_id)
    if sell_team is not None:
        counts[sell_team] = counts.get(sell_team, 1) - 1
    return counts.get(buy.team_id, 0) < MAX_PLAYERS_PER_TEAM


@dataclass
class TransferSuggestion:
    """One suggested transfer with sell/buy and expected points impact."""

    sell_id: int
    buy_id: int
    sell_name: str
    buy_name: str
    expected_points_difference: float
    new_team_xpts: float


@dataclass
class SuggestTransfersResult:
    """Result of suggest_transfers: top suggestions plus current team xPts for context."""

    suggestions: list[TransferSuggestion]
    current_team_xpts: float
    event_id: int | None


def suggest_transfers(
    engine: Engine,
    squad_ids: list[int],
    bank: float = 0.0,
    top_n: int = 10,
) -> SuggestTransfersResult:
    """Suggest single transfers: sell one of 15, buy same position, budget and max-3-per-team ok.

    Uses next GW = MIN(event_id) from player_expected_points.
    Budget: (sell now_cost + bank) >= buy now_cost. Use FPL tenths for both (e.g. 55 = £5.5m;
    if API returns bank in millions, multiply by 10 to get tenths).
    Returns top_n by expected points difference descending, plus current team xPts and event_id.
    """
    event_id = get_next_event_id(engine)
    if event_id is None:
        logger.warning("No event_id in player_expected_points; run build_xpts first")
        return SuggestTransfersResult(suggestions=[], current_team_xpts=0.0, event_id=None)

    players_by_id = _load_players(engine)
    xpts = _load_xpts_for_event(engine, event_id)

    squad_set = set(squad_ids)
    if len(squad_ids) != 15:
        logger.warning("Squad should have 15 players; got %s", len(squad_ids))

    current_xi_ids, current_team_xpts = best_xi(squad_ids, players_by_id, xpts)

    # All same-position candidates not in squad (with xpts for this event)
    by_pos: dict[int, list[int]] = {
        ELEMENT_TYPE_GKP: [],
        ELEMENT_TYPE_DEF: [],
        ELEMENT_TYPE_MID: [],
        ELEMENT_TYPE_FWD: [],
    }
    for pid, p in players_by_id.items():
        if pid in squad_set:
            continue
        if p.element_type_id in by_pos and pid in xpts:
            by_pos[p.element_type_id].append(pid)

    # Bank in tenths to match now_cost (user can pass bank in units and we multiply by 10, or already in tenths)
    # Assume bank is already in same units as now_cost (FPL tenths)
    bank_tenths = int(round(bank)) if isinstance(bank, (int, float)) else 0

    results: list[tuple[int, int, float, float]] = []  # sell_id, buy_id, delta, new_team_xpts

    for sell_id in squad_ids:
        sell = players_by_id.get(sell_id)
        if sell is None:
            continue
        sell_price = sell.now_cost
        position = sell.element_type_id
        candidates = by_pos.get(position, [])
        for buy_id in candidates:
            buy = players_by_id.get(buy_id)
            if buy is None:
                continue
            if sell_price + bank_tenths < buy.now_cost:
                continue
            if not _can_add_player(buy_id, sell_id, squad_ids, players_by_id):
                continue
            new_squad = [buy_id if pid == sell_id else pid for pid in squad_ids]
            _, new_team_xpts = best_xi(new_squad, players_by_id, xpts)
            delta = new_team_xpts - current_team_xpts
            results.append((sell_id, buy_id, delta, new_team_xpts))

    results.sort(key=lambda r: -r[2])
    top = results[:top_n]

    out: list[TransferSuggestion] = []
    for sell_id, buy_id, delta, new_xpts in top:
        sell = players_by_id.get(sell_id)
        buy = players_by_id.get(buy_id)
        out.append(
            TransferSuggestion(
                sell_id=sell_id,
                buy_id=buy_id,
                sell_name=sell.web_name if sell else str(sell_id),
                buy_name=buy.web_name if buy else str(buy_id),
                expected_points_difference=round(delta, 2),
                new_team_xpts=round(new_xpts, 2),
            )
        )
    return SuggestTransfersResult(
        suggestions=out,
        current_team_xpts=round(current_team_xpts, 2),
        event_id=event_id,
    )
