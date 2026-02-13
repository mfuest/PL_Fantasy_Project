"""Tests for transfer suggestion engine: best-XI and suggest_transfers."""

from __future__ import annotations

import pytest

from src.db import get_engine, init_db
from src.marts import init_marts
from src.transfers import (
    ELEMENT_TYPE_DEF,
    ELEMENT_TYPE_FWD,
    ELEMENT_TYPE_GKP,
    ELEMENT_TYPE_MID,
    PlayerInfo,
    best_xi,
    get_next_event_id,
    suggest_transfers,
)


def test_get_next_event_id_empty_db(tmp_path) -> None:
    """No player_expected_points -> None."""
    engine = get_engine(str(tmp_path / "empty.sqlite"))
    init_db(engine)
    init_marts(engine)
    assert get_next_event_id(engine) is None


def test_get_next_event_id_returns_min(tmp_path) -> None:
    """Returns MIN(event_id) from player_expected_points."""
    from sqlalchemy import text

    engine = get_engine(str(tmp_path / "xpts.sqlite"))
    init_db(engine)
    init_marts(engine)
    with engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO player_expected_points (player_id, event_id, xmins, xpts, xpts_att, xpts_def, xpts_app, computed_at_utc)
                VALUES (1, 3, 0, 5.0, 0, 0, 0, '2025-01-01'),
                       (2, 2, 0, 4.0, 0, 0, 0, '2025-01-01')
            """)
        )
        conn.commit()
    assert get_next_event_id(engine) == 2


def test_best_xi_picks_eleven_and_respects_formation() -> None:
    """Best XI returns 11 players and respects 1 GKP, 3-5 DEF, 2-5 MID, 1-3 FWD."""
    # Squad: 2 GKP, 5 DEF, 5 MID, 3 FWD (15). Give higher xpts to one GKP, four DEF, four MID, two FWD.
    players = {
        1: PlayerInfo(1, 50, ELEMENT_TYPE_GKP, 1, "G1"),
        2: PlayerInfo(2, 50, ELEMENT_TYPE_GKP, 2, "G2"),
        3: PlayerInfo(3, 55, ELEMENT_TYPE_DEF, 1, "D1"),
        4: PlayerInfo(4, 55, ELEMENT_TYPE_DEF, 2, "D2"),
        5: PlayerInfo(5, 55, ELEMENT_TYPE_DEF, 3, "D3"),
        6: PlayerInfo(6, 54, ELEMENT_TYPE_DEF, 1, "D4"),
        7: PlayerInfo(7, 54, ELEMENT_TYPE_DEF, 2, "D5"),
        8: PlayerInfo(8, 60, ELEMENT_TYPE_MID, 1, "M1"),
        9: PlayerInfo(9, 58, ELEMENT_TYPE_MID, 2, "M2"),
        10: PlayerInfo(10, 57, ELEMENT_TYPE_MID, 3, "M3"),
        11: PlayerInfo(11, 56, ELEMENT_TYPE_MID, 1, "M4"),
        12: PlayerInfo(12, 55, ELEMENT_TYPE_MID, 2, "M5"),
        13: PlayerInfo(13, 62, ELEMENT_TYPE_FWD, 1, "F1"),
        14: PlayerInfo(14, 61, ELEMENT_TYPE_FWD, 2, "F2"),
        15: PlayerInfo(15, 50, ELEMENT_TYPE_FWD, 3, "F3"),
    }
    xpts = {i: (10.0 - (i % 5)) for i in range(1, 16)}  # varied xpts
    xpts[1], xpts[2] = 5.0, 4.0  # G1 best GKP
    xpts[13], xpts[14], xpts[15] = 8.0, 7.0, 3.0  # F1, F2 best FWDs

    squad_ids = list(range(1, 16))
    xi_ids, total = best_xi(squad_ids, players, xpts)
    assert len(xi_ids) == 11
    assert total >= 0
    assert 1 in xi_ids  # best GKP
    assert 13 in xi_ids and 14 in xi_ids  # best two FWDs
    # Check position counts
    gkp = [p for p in xi_ids if players[p].element_type_id == ELEMENT_TYPE_GKP]
    defs = [p for p in xi_ids if players[p].element_type_id == ELEMENT_TYPE_DEF]
    mids = [p for p in xi_ids if players[p].element_type_id == ELEMENT_TYPE_MID]
    fwds = [p for p in xi_ids if players[p].element_type_id == ELEMENT_TYPE_FWD]
    assert len(gkp) == 1
    assert 3 <= len(defs) <= 5
    assert 2 <= len(mids) <= 5
    assert 1 <= len(fwds) <= 3


def test_best_xi_max_three_per_team() -> None:
    """Best XI has at most 3 players from any team."""
    # 4 DEF from team 1, 4 MID from team 1, etc. - force spread
    players = {}
    pid = 1
    for pos, etype in [(2, ELEMENT_TYPE_GKP), (2, ELEMENT_TYPE_GKP)]:
        for t in [1, 2]:
            players[pid] = PlayerInfo(pid, 50, etype, t, f"P{pid}")
            pid += 1
    for etype in [ELEMENT_TYPE_DEF, ELEMENT_TYPE_MID, ELEMENT_TYPE_FWD]:
        for t in [1, 2, 3]:
            for _ in range(2):
                players[pid] = PlayerInfo(pid, 50, etype, t, f"P{pid}")
                pid += 1
    # 2+2 + 6+6+6 = 22, we only need 15; take first 15
    squad_ids = list(players.keys())[:15]
    xpts = {p: 5.0 for p in squad_ids}
    xi_ids, _ = best_xi(squad_ids, players, xpts)
    from collections import Counter
    team_counts = Counter(players[p].team_id for p in xi_ids)
    assert all(c <= 3 for c in team_counts.values())


@pytest.fixture
def transfer_db(tmp_path):
    """DB with players and player_expected_points for one event for suggest_transfers."""
    from datetime import datetime, timezone
    from sqlalchemy import text

    db_path = str(tmp_path / "transfers.sqlite")
    engine = get_engine(db_path)
    init_db(engine)
    init_marts(engine)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    with engine.connect() as conn:
        conn.execute(text("INSERT INTO element_types (id, singular_name_short, singular_name, squad_select, ingested_at_utc) VALUES (1,'GKP','GKP',2,:n),(2,'DEF','DEF',5,:n),(3,'MID','MID',5,:n),(4,'FWD','FWD',3,:n)"), {"n": now})
        for i in range(1, 6):
            conn.execute(
                text("INSERT INTO teams (id, name, short_name, strength, ingested_at_utc) VALUES (:id, 'T'||:id, 'T'||:id, 4, :n)"),
                {"id": i, "n": now},
            )
        conn.execute(text("INSERT INTO events (id, name, finished, ingested_at_utc) VALUES (10, 'GW10', 0, :n)"), {"n": now})
        # 15 squad players: 2 GKP, 5 DEF, 5 MID, 3 FWD; plus 5 extra per position for buy candidates
        # ids 1-2 GKP, 3-7 DEF, 8-12 MID, 13-15 FWD (squad). 16-17 GKP, 18-22 DEF, 23-27 MID, 28-30 FWD (candidates).
        for pid, team_id, etype, cost, name in [
            (1, 1, 1, 50, "G1"), (2, 2, 1, 50, "G2"),
            (3, 1, 2, 55, "D1"), (4, 2, 2, 55, "D2"), (5, 3, 2, 55, "D3"), (6, 1, 2, 54, "D4"), (7, 2, 2, 54, "D5"),
            (8, 1, 3, 80, "M1"), (9, 2, 3, 75, "M2"), (10, 3, 3, 70, "M3"), (11, 1, 3, 65, "M4"), (12, 2, 3, 60, "M5"),
            (13, 1, 4, 100, "F1"), (14, 2, 4, 95, "F2"), (15, 3, 4, 90, "F3"),
            (16, 3, 1, 55, "G3"), (17, 4, 1, 52, "G4"),
            (18, 4, 2, 56, "D6"), (19, 5, 2, 56, "D7"), (20, 3, 2, 55, "D8"), (21, 4, 2, 54, "D9"), (22, 5, 2, 54, "D10"),
            (23, 4, 3, 85, "M6"), (24, 5, 3, 82, "M7"), (25, 3, 3, 78, "M8"), (26, 4, 3, 72, "M9"), (27, 5, 3, 68, "M10"),
            (28, 4, 4, 105, "F4"), (29, 5, 4, 102, "F5"), (30, 4, 4, 98, "F6"),
        ]:
            conn.execute(
                text("""INSERT INTO players (id, web_name, team_id, element_type_id, now_cost, status, ingested_at_utc)
                        VALUES (:id, :name, :tid, :etype, :cost, 'a', :n)"""),
                {"id": pid, "name": name, "tid": team_id, "etype": etype, "cost": cost, "n": now},
            )
        for pid in range(1, 31):
            conn.execute(
                text("""INSERT INTO player_expected_points (player_id, event_id, xmins, xpts, xpts_att, xpts_def, xpts_app, computed_at_utc)
                        VALUES (:pid, 10, 90, :xpts, 0, 0, 2, :n)"""),
                {"pid": pid, "xpts": 3.0 + (pid % 10) * 0.5, "n": now},  # varied xpts
            )
        conn.commit()
    return engine


def test_suggest_transfers_returns_result(transfer_db) -> None:
    """suggest_transfers returns SuggestTransfersResult with event_id, current_team_xpts, suggestions."""
    squad_ids = list(range(1, 16))
    result = suggest_transfers(transfer_db, squad_ids, bank=0, top_n=5)
    assert result.event_id == 10
    assert result.current_team_xpts >= 0
    # May have 0 suggestions if no valid budget swap, or several
    assert len(result.suggestions) <= 5
    for s in result.suggestions:
        assert s.sell_id in squad_ids
        assert s.buy_id not in squad_ids
        assert s.expected_points_difference is not None
        assert s.new_team_xpts >= 0


def test_suggest_transfers_respects_budget(transfer_db) -> None:
    """With bank=0, we can only buy players with now_cost <= sell now_cost."""
    squad_ids = list(range(1, 16))
    result = suggest_transfers(transfer_db, squad_ids, bank=0, top_n=20)
    from src.transfers import _load_players
    players = _load_players(transfer_db)
    for s in result.suggestions:
        sell = players.get(s.sell_id)
        buy = players.get(s.buy_id)
        assert sell is not None and buy is not None
        assert sell.now_cost >= buy.now_cost  # with bank=0


def test_suggest_transfers_with_bank(transfer_db) -> None:
    """With positive bank, more expensive buys become possible."""
    squad_ids = list(range(1, 16))
    result_no_bank = suggest_transfers(transfer_db, squad_ids, bank=0, top_n=20)
    result_with_bank = suggest_transfers(transfer_db, squad_ids, bank=50, top_n=20)
    # With bank we may have more or different suggestions (e.g. upgrading to costlier players)
    assert result_with_bank.event_id == 10
    assert result_no_bank.event_id == 10
