"""CLI and orchestration for FPL data pipeline. Batched upserts with ON CONFLICT DO UPDATE."""

from __future__ import annotations

import argparse
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from src.db import get_engine, get_session, init_db
from src.fpl_client import FPLClient
from src.models import (
    ElementType,
    Event,
    Fixture,
    MetaIngestion,
    Player,
    PlayerFutureFixture,
    PlayerMatchHistory,
    Team,
)
from src.normalize import (
    normalize_bootstrap_static,
    normalize_element_summary_fixtures,
    normalize_element_summary_history,
    normalize_entry_picks,
    normalize_fixtures,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://fantasy.premierleague.com/api/"
FIXED_SLEEP = 0.25


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _record_meta(session, run_id: str | None, meta: dict) -> None:
    session.add(
        MetaIngestion(
            run_id=run_id,
            request_key=meta["request_key"],
            endpoint=meta["endpoint"],
            url=meta["url"],
            fetched_at_utc=meta["fetched_at_utc"],
            http_status=meta.get("http_status"),
            payload_path=meta.get("payload_path"),
            payload_sha256=meta.get("payload_sha256"),
        )
    )


def _upsert_teams(session, rows: list[dict]) -> int:
    if not rows:
        return 0
    now = _now_utc()
    for r in rows:
        r["ingested_at_utc"] = now
    stmt = sqlite_insert(Team).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"],
        set_={
            "name": stmt.excluded.name,
            "short_name": stmt.excluded.short_name,
            "strength": stmt.excluded.strength,
            "ingested_at_utc": stmt.excluded.ingested_at_utc,
        },
    )
    session.execute(stmt)
    return len(rows)


def _upsert_element_types(session, rows: list[dict]) -> int:
    if not rows:
        return 0
    now = _now_utc()
    for r in rows:
        r["ingested_at_utc"] = now
    stmt = sqlite_insert(ElementType).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"],
        set_={
            "singular_name_short": stmt.excluded.singular_name_short,
            "singular_name": stmt.excluded.singular_name,
            "squad_select": stmt.excluded.squad_select,
            "ingested_at_utc": stmt.excluded.ingested_at_utc,
        },
    )
    session.execute(stmt)
    return len(rows)


def _upsert_events(session, rows: list[dict]) -> int:
    if not rows:
        return 0
    now = _now_utc()
    for r in rows:
        r["ingested_at_utc"] = now
    stmt = sqlite_insert(Event).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"],
        set_={
            "name": stmt.excluded.name,
            "deadline_time": stmt.excluded.deadline_time,
            "finished": stmt.excluded.finished,
            "is_current": stmt.excluded.is_current,
            "is_next": stmt.excluded.is_next,
            "ingested_at_utc": stmt.excluded.ingested_at_utc,
        },
    )
    session.execute(stmt)
    return len(rows)


def _upsert_players(session, rows: list[dict]) -> int:
    if not rows:
        return 0
    now = _now_utc()
    for r in rows:
        r["ingested_at_utc"] = now
    stmt = sqlite_insert(Player).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"],
        set_={
            "web_name": stmt.excluded.web_name,
            "first_name": stmt.excluded.first_name,
            "second_name": stmt.excluded.second_name,
            "team_id": stmt.excluded.team_id,
            "element_type_id": stmt.excluded.element_type_id,
            "now_cost": stmt.excluded.now_cost,
            "status": stmt.excluded.status,
            "minutes": stmt.excluded.minutes,
            "total_points": stmt.excluded.total_points,
            "selected_by_percent": stmt.excluded.selected_by_percent,
            "form": stmt.excluded.form,
            "points_per_game": stmt.excluded.points_per_game,
            "expected_goals": stmt.excluded.expected_goals,
            "expected_assists": stmt.excluded.expected_assists,
            "expected_goal_involvements": stmt.excluded.expected_goal_involvements,
            "expected_goals_conceded": stmt.excluded.expected_goals_conceded,
            "ingested_at_utc": stmt.excluded.ingested_at_utc,
        },
    )
    session.execute(stmt)
    return len(rows)


def _upsert_fixtures(session, rows: list[dict]) -> int:
    if not rows:
        return 0
    now = _now_utc()
    for r in rows:
        r["ingested_at_utc"] = now
    stmt = sqlite_insert(Fixture).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"],
        set_={
            "event_id": stmt.excluded.event_id,
            "team_h": stmt.excluded.team_h,
            "team_a": stmt.excluded.team_a,
            "kickoff_time": stmt.excluded.kickoff_time,
            "finished": stmt.excluded.finished,
            "team_h_difficulty": stmt.excluded.team_h_difficulty,
            "team_a_difficulty": stmt.excluded.team_a_difficulty,
            "ingested_at_utc": stmt.excluded.ingested_at_utc,
        },
    )
    session.execute(stmt)
    return len(rows)


def _upsert_player_match_history(session, rows: list[dict]) -> int:
    if not rows:
        return 0
    now = _now_utc()
    for r in rows:
        r["ingested_at_utc"] = now
    stmt = sqlite_insert(PlayerMatchHistory).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["player_id", "fixture_id_effective"],
        set_={
            "fixture_id": stmt.excluded.fixture_id,
            "event_id": stmt.excluded.event_id,
            "minutes": stmt.excluded.minutes,
            "total_points": stmt.excluded.total_points,
            "goals_scored": stmt.excluded.goals_scored,
            "assists": stmt.excluded.assists,
            "clean_sheets": stmt.excluded.clean_sheets,
            "goals_conceded": stmt.excluded.goals_conceded,
            "expected_goals": stmt.excluded.expected_goals,
            "expected_assists": stmt.excluded.expected_assists,
            "expected_goal_involvements": stmt.excluded.expected_goal_involvements,
            "expected_goals_conceded": stmt.excluded.expected_goals_conceded,
            "ingested_at_utc": stmt.excluded.ingested_at_utc,
        },
    )
    session.execute(stmt)
    return len(rows)


def _upsert_player_future_fixtures(session, rows: list[dict]) -> int:
    if not rows:
        return 0
    now = _now_utc()
    for r in rows:
        r["ingested_at_utc"] = now
    stmt = sqlite_insert(PlayerFutureFixture).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["player_id", "fixture_id"],
        set_={
            "event_id": stmt.excluded.event_id,
            "is_home": stmt.excluded.is_home,
            "opponent_team": stmt.excluded.opponent_team,
            "kickoff_time": stmt.excluded.kickoff_time,
            "difficulty": stmt.excluded.difficulty,
            "ingested_at_utc": stmt.excluded.ingested_at_utc,
        },
    )
    session.execute(stmt)
    return len(rows)


def cmd_update_core(engine, bronze_dir: Path, client: FPLClient) -> None:
    run_id = str(uuid.uuid4())
    logger.info("update_core run_id=%s", run_id)

    with get_session(engine) as session:
        init_db(engine)

        url = f"{BASE_URL}bootstrap-static/"
        logger.info("Fetching bootstrap-static...")
        data, meta = client.get_json(url, bronze_dir, save_bronze=True)
        _record_meta(session, run_id, meta)
        normalized = normalize_bootstrap_static(data)
        n_teams = _upsert_teams(session, normalized["teams"])
        n_et = _upsert_element_types(session, normalized["element_types"])
        n_ev = _upsert_events(session, normalized["events"])
        n_players = _upsert_players(session, normalized["players"])
        logger.info("Upserted %s teams, %s element_types, %s events, %s players", n_teams, n_et, n_ev, n_players)

        url = f"{BASE_URL}fixtures/"
        logger.info("Fetching fixtures...")
        data_fixtures, meta_f = client.get_json(url, bronze_dir, save_bronze=True)
        _record_meta(session, run_id, meta_f)
        fixture_rows = normalize_fixtures(data_fixtures)
        n_fixtures = _upsert_fixtures(session, fixture_rows)
        logger.info("Upserted %s fixtures", n_fixtures)


def cmd_update_element_summaries(
    engine, bronze_dir: Path, client: FPLClient, mode: str, n: int
) -> None:
    with get_session(engine) as session:
        result = session.execute(select(Player.id))
        all_ids = [r[0] for r in result.fetchall()]
    if mode == "top":
        with get_session(engine) as session:
            result = session.execute(
                select(Player.id).order_by(Player.total_points.desc().nullslast()).limit(n)
            )
            player_ids = [r[0] for r in result.fetchall()]
    else:
        player_ids = all_ids

    total_hist = 0
    total_fut = 0
    for idx, pid in enumerate(player_ids, 1):
        url = f"{BASE_URL}element-summary/{pid}/"
        try:
            data, meta = client.get_json(url, bronze_dir, player_id=pid, save_bronze=True)
        except Exception as e:
            logger.warning("element-summary %s failed: %s", pid, e)
            continue
        hist_rows = normalize_element_summary_history(pid, data)
        fut_rows = normalize_element_summary_fixtures(pid, data)
        with get_session(engine) as session:
            total_hist += _upsert_player_match_history(session, hist_rows)
            total_fut += _upsert_player_future_fixtures(session, fut_rows)
        if idx % 50 == 0 or idx == len(player_ids):
            logger.info("Element summaries [%s/%s] id=%s", idx, len(player_ids), pid)
    logger.info("Upserted %s player_match_history, %s player_future_fixtures", total_hist, total_fut)


def cmd_pull_team(engine, bronze_dir: Path, client: FPLClient, team_id: int, gw: int) -> None:
    url = f"{BASE_URL}entry/{team_id}/event/{gw}/picks/"
    logger.info("Fetching entry %s event %s picks...", team_id, gw)
    data, meta = client.get_json(url, bronze_dir, save_bronze=True)
    with get_session(engine) as session:
        _record_meta(session, None, meta)
    parsed = normalize_entry_picks(data)
    picks = parsed.get("picks") or []
    logger.info("Squad: %s players", len(picks))
    for p in picks:
        logger.info(
            "  element=%s position=%s captain=%s vice=%s",
            p.get("element"),
            p.get("position"),
            p.get("is_captain"),
            p.get("is_vice_captain"),
        )
    eh = parsed.get("entry_history") or {}
    if eh.get("bank") is not None or eh.get("value") is not None:
        logger.info("Bank: %s Value: %s", eh.get("bank"), eh.get("value"))
    else:
        logger.info("Bank/FT: not present in response")


def main() -> int:
    parser = argparse.ArgumentParser(description="FPL data pipeline")
    parser.add_argument("--db-path", default="data/fpl.sqlite", help="SQLite database path")
    parser.add_argument("--bronze-dir", default="data/bronze", type=Path, help="Bronze JSON root directory")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("update_core", help="Fetch bootstrap-static + fixtures, upsert silver")

    p_elem = sub.add_parser("update_element_summaries", help="Fetch element-summary for players")
    p_elem.add_argument("--mode", choices=["all", "top"], default="top")
    p_elem.add_argument("--n", type=int, default=250, help="When mode=top, number of players by total_points")

    p_team = sub.add_parser("pull_team", help="Fetch entry team picks for a gameweek")
    p_team.add_argument("--team_id", type=int, required=True)
    p_team.add_argument("--gw", type=int, required=True, help="Gameweek number")

    args = parser.parse_args()
    engine = get_engine(args.db_path)
    client = FPLClient(sleep_after_request=FIXED_SLEEP)

    if args.command == "update_core":
        cmd_update_core(engine, args.bronze_dir, client)
    elif args.command == "update_element_summaries":
        cmd_update_element_summaries(engine, args.bronze_dir, client, args.mode, args.n)
    elif args.command == "pull_team":
        cmd_pull_team(engine, args.bronze_dir, client, args.team_id, args.gw)
    else:
        parser.error("Unknown command")
    return 0


if __name__ == "__main__":
    sys.exit(main())
