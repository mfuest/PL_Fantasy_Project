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
from src.marts import init_marts
from src.models import (
    ElementType,
    Event,
    Fixture,
    MetaIngestion,
    Player,
    PlayerExpectedPoints,
    PlayerFutureFixture,
    PlayerMatchHistory,
    Team,
)
from src.transfers import suggest_transfers
from src.xpts import build_xpts_rows
from src.xpts_ml import build_xpts_rows_ml, train_model as train_xpts_model
from src.normalize import (
    normalize_bootstrap_static,
    normalize_element_summary_fixtures,
    normalize_element_summary_history,
    normalize_entry_history,
    normalize_entry_picks,
    normalize_fixtures,
)
from src.validate import print_report, run_validation, should_exit_nonzero

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://fantasy.premierleague.com/api/"
FIXED_SLEEP = 0.25
ELEMENT_SUMMARY_BATCH_SIZE = 20


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


def _upsert_player_expected_points(session, rows: list[dict]) -> int:
    if not rows:
        return 0
    stmt = sqlite_insert(PlayerExpectedPoints).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["player_id", "event_id"],
        set_={
            "xmins": stmt.excluded.xmins,
            "xpts": stmt.excluded.xpts,
            "xpts_att": stmt.excluded.xpts_att,
            "xpts_def": stmt.excluded.xpts_def,
            "xpts_app": stmt.excluded.xpts_app,
            "computed_at_utc": stmt.excluded.computed_at_utc,
        },
    )
    session.execute(stmt)
    return len(rows)


def cmd_update_core(engine, bronze_dir: Path, client: FPLClient) -> None:
    run_id = str(uuid.uuid4())
    logger.info("update_core run_id=%s", run_id)

    init_db(engine)
    init_marts(engine)

    with get_session(engine) as session:
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

    report = run_validation(engine)
    print_report(report)
    if should_exit_nonzero("hard", report):
        raise SystemExit(1)


def _player_ids_to_fetch(
    engine,
    mode: str,
    n: int,
    since_hours: float | None,
    force: bool = False,
    max_age_hours: float | None = None,
) -> list[int]:
    """Return player ids to fetch.

    - force: ignore since_hours skip logic; fetch all candidates.
    - max_age_hours: if set, always include players whose last fetch is older than
      this (refetch when age > max_age), even if they would otherwise be skipped by since_hours.
    """
    with get_session(engine) as session:
        result = session.execute(select(Player.id))
        all_ids = [r[0] for r in result.fetchall()]
    if mode == "top":
        with get_session(engine) as session:
            result = session.execute(
                select(Player.id).order_by(Player.total_points.desc().nullslast()).limit(n)
            )
            candidate_ids = [r[0] for r in result.fetchall()]
    else:
        candidate_ids = all_ids

    if force:
        return candidate_ids

    from datetime import datetime as _dt, timedelta, timezone

    from sqlalchemy import text as sql_text

    now_utc = _dt.now(timezone.utc).replace(tzinfo=None)
    ids_to_fetch = set(candidate_ids)

    if since_hours is not None and since_hours > 0:
        cutoff = now_utc - timedelta(hours=since_hours)
        cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
        with get_session(engine) as session:
            rows = session.execute(
                sql_text("""
                    SELECT request_key, MAX(fetched_at_utc) AS last_fetched
                    FROM meta_ingestions
                    WHERE request_key LIKE 'element-summary:%'
                    GROUP BY request_key
                    HAVING last_fetched >= :cutoff
                """),
                {"cutoff": cutoff_str},
            ).fetchall()
        recently_fetched = set()
        for request_key, _ in rows:
            if request_key and request_key.startswith("element-summary:"):
                try:
                    recently_fetched.add(int(request_key.split(":")[1]))
                except ValueError:
                    pass
        ids_to_fetch -= recently_fetched

    if max_age_hours is not None and max_age_hours > 0:
        max_age_cutoff = now_utc - timedelta(hours=max_age_hours)
        max_age_str = max_age_cutoff.strftime("%Y-%m-%d %H:%M:%S")
        with get_session(engine) as session:
            # Refetch if last fetch older than max_age (or never fetched: no row => not in this result)
            rows = session.execute(
                sql_text("""
                    SELECT request_key
                    FROM meta_ingestions
                    WHERE request_key LIKE 'element-summary:%'
                    GROUP BY request_key
                    HAVING MAX(fetched_at_utc) < :cutoff
                """),
                {"cutoff": max_age_str},
            ).fetchall()
        stale_pids = set()
        for (request_key,) in rows:
            if request_key and request_key.startswith("element-summary:"):
                try:
                    stale_pids.add(int(request_key.split(":")[1]))
                except ValueError:
                    pass
        with get_session(engine) as session:
            fetched_keys = session.execute(
                sql_text(
                    "SELECT DISTINCT request_key FROM meta_ingestions WHERE request_key LIKE 'element-summary:%'"
                )
            ).fetchall()
        ever_fetched = set()
        for (rk,) in fetched_keys:
            if rk and rk.startswith("element-summary:"):
                try:
                    ever_fetched.add(int(rk.split(":")[1]))
                except ValueError:
                    pass
        never_fetched = [p for p in candidate_ids if p not in ever_fetched]
        ids_to_fetch |= stale_pids | set(never_fetched)

    return [pid for pid in candidate_ids if pid in ids_to_fetch]


def cmd_update_element_summaries(
    engine,
    bronze_dir: Path,
    client: FPLClient,
    mode: str,
    n: int,
    since_hours: float | None,
    force: bool = False,
    max_age_hours: float | None = None,
) -> None:
    player_ids = _player_ids_to_fetch(
        engine, mode, n, since_hours, force=force, max_age_hours=max_age_hours
    )
    if not player_ids:
        logger.info("No players to fetch (all within --since-hours or none in DB)")
        return

    run_id = str(uuid.uuid4())
    batch_size = ELEMENT_SUMMARY_BATCH_SIZE
    buffer_hist: list[dict] = []
    buffer_fut: list[dict] = []
    buffer_metas: list[dict] = []
    total_hist = 0
    total_fut = 0

    def flush_buffers() -> None:
        nonlocal total_hist, total_fut
        if buffer_hist or buffer_fut or buffer_metas:
            with get_session(engine) as session:
                for m in buffer_metas:
                    _record_meta(session, run_id, m)
                total_hist += _upsert_player_match_history(session, buffer_hist)
                total_fut += _upsert_player_future_fixtures(session, buffer_fut)
            buffer_hist.clear()
            buffer_fut.clear()
            buffer_metas.clear()

    for idx, pid in enumerate(player_ids, 1):
        url = f"{BASE_URL}element-summary/{pid}/"
        try:
            data, meta = client.get_json(url, bronze_dir, player_id=pid, save_bronze=True)
        except Exception as e:
            logger.warning("element-summary %s failed: %s", pid, e)
            continue
        buffer_metas.append(meta)
        hist_rows = normalize_element_summary_history(pid, data)
        fut_rows = normalize_element_summary_fixtures(pid, data)
        buffer_hist.extend(hist_rows)
        buffer_fut.extend(fut_rows)
        if idx % batch_size == 0:
            flush_buffers()
            logger.info("Element summaries [%s/%s] id=%s (flushed batch)", idx, len(player_ids), pid)
        elif idx % 50 == 0 or idx == len(player_ids):
            logger.info("Element summaries [%s/%s] id=%s", idx, len(player_ids), pid)
    flush_buffers()
    logger.info("Upserted %s player_match_history, %s player_future_fixtures", total_hist, total_fut)

    report = run_validation(engine)
    print_report(report)
    if should_exit_nonzero("hard", report):
        raise SystemExit(1)


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


def cmd_update_entry_history(engine, bronze_dir: Path, client: FPLClient, team_id: int) -> None:
    """Fetch entry/{team_id}/history/ for bank/FT/chips context. Public, no auth."""
    url = f"{BASE_URL}entry/{team_id}/history/"
    logger.info("Fetching entry %s history...", team_id)
    data, meta = client.get_json(url, bronze_dir, save_bronze=True)
    with get_session(engine) as session:
        _record_meta(session, None, meta)
    parsed = normalize_entry_history(data)
    current = parsed.get("current") or []
    past = parsed.get("past") or []
    logger.info("Current season: %s gameweeks", len(current))
    if current:
        latest = current[-1]
        logger.info("Latest GW: event=%s points=%s total_points=%s value=%s event_transfers=%s",
            latest.get("event"), latest.get("points"), latest.get("total_points"),
            latest.get("value"), latest.get("event_transfers"))
    logger.info("Past seasons: %s", len(past))


def cmd_validate(engine, level: str) -> int:
    """Run data quality checks. Exit 1 based on --level: hard (default), strict, or warn."""
    report = run_validation(engine)
    print_report(report)
    return 1 if should_exit_nonzero(level, report) else 0


def cmd_train_xpts(
    engine,
    model_path: str | Path,
    validation_fraction: float = 0.2,
) -> None:
    """Build training data from DB, train GBM, save model. No write to player_expected_points."""
    init_db(engine)
    init_marts(engine)
    train_xpts_model(engine, model_path, validation_fraction=validation_fraction)


def cmd_build_xpts(
    engine,
    horizon: int,
    method: str = "baseline",
    model_path: str | Path | None = None,
) -> None:
    """Compute xPts for next N gameweeks; upsert into player_expected_points; run validate (hard)."""
    init_db(engine)
    init_marts(engine)

    if method == "ml":
        path = model_path or Path("data/models/xpts_gbm")
        rows = build_xpts_rows_ml(engine, horizon, path)
    else:
        rows = build_xpts_rows(engine, horizon)
    if not rows:
        logger.warning("No xPts rows computed (check upcoming fixtures and players)")
    else:
        with get_session(engine) as session:
            n = _upsert_player_expected_points(session, rows)
        logger.info("Upserted %s rows into player_expected_points", n)

        next_ev = min(r["event_id"] for r in rows)
        top10 = sorted(
            [r for r in rows if r["event_id"] == next_ev],
            key=lambda x: -x["xpts"],
        )[:10]
        logger.info("Top 10 by xpts for next GW (event_id=%s): %s", next_ev, top10)

    report = run_validation(engine)
    print_report(report)
    if should_exit_nonzero("hard", report):
        raise SystemExit(1)


def cmd_suggest_transfers(
    engine,
    squad_ids: list[int],
    bank: float = 0.0,
    top_n: int = 10,
) -> None:
    """Suggest single transfers; print top N by expected points difference."""
    init_db(engine)
    init_marts(engine)
    result = suggest_transfers(engine, squad_ids, bank=bank, top_n=top_n)
    if result.event_id is not None:
        logger.info(
            "Next GW event_id=%s | Team xPts (no transfer)=%.2f",
            result.event_id,
            result.current_team_xpts,
        )
    for i, s in enumerate(result.suggestions, 1):
        logger.info(
            "%s. Sell %s (id=%s) -> Buy %s (id=%s) | delta=%.2f | new_team_xpts=%.2f",
            i,
            s.sell_name,
            s.sell_id,
            s.buy_name,
            s.buy_id,
            s.expected_points_difference,
            s.new_team_xpts,
        )
    if not result.suggestions:
        logger.info("No transfer suggestions (check squad, xPts, and budget)")


def main() -> int:
    parser = argparse.ArgumentParser(description="FPL data pipeline")
    parser.add_argument("--db-path", default="data/fpl.sqlite", help="SQLite database path")
    parser.add_argument("--bronze-dir", default="data/bronze", type=Path, help="Bronze JSON root directory")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("update_core", help="Fetch bootstrap-static + fixtures, upsert silver")

    p_elem = sub.add_parser("update_element_summaries", help="Fetch element-summary for players")
    p_elem.add_argument("--mode", choices=["all", "top"], default="top")
    p_elem.add_argument("--n", type=int, default=250, help="When mode=top, number of players by total_points")
    p_elem.add_argument("--since-hours", type=float, default=None, help="Skip players last fetched within this many hours")
    p_elem.add_argument("--force", action="store_true", help="Ignore skip logic; fetch all candidates")
    p_elem.add_argument(
        "--max-age-hours",
        type=float,
        default=None,
        help="If set, always refetch players whose last fetch is older than this (and never-fetched)",
    )

    p_team = sub.add_parser("pull_team", help="Fetch entry team picks for a gameweek")
    p_team.add_argument("--team_id", type=int, required=True)
    p_team.add_argument("--gw", type=int, required=True, help="Gameweek number")

    p_entry_hist = sub.add_parser("update_entry_history", help="Fetch entry/{team_id}/history/ for bank/FT context")
    p_entry_hist.add_argument("--team_id", type=int, required=True)

    p_validate = sub.add_parser("validate", help="Run data quality checks; exit 1 based on --level")
    p_validate.add_argument(
        "--level",
        choices=["hard", "strict", "warn"],
        default="hard",
        help="hard: only hard failures exit 1; strict: hard+warn exit 1; warn: never exit 1",
    )

    p_train_xpts = sub.add_parser("train_xpts", help="Train ML xPts model from player_match_history; save model file")
    p_train_xpts.add_argument(
        "--model-path",
        default="data/models/xpts_gbm",
        help="Output model path (default: data/models/xpts_gbm; .json added for XGBoost)",
    )
    p_train_xpts.add_argument(
        "--validation-fraction",
        type=float,
        default=0.2,
        help="Fraction of data for validation (default 0.2)",
    )

    p_xpts = sub.add_parser("build_xpts", help="Build expected points for next N gameweeks (baseline or ML)")
    p_xpts.add_argument("--horizon", type=int, default=3, help="Number of upcoming gameweeks (default 3)")
    p_xpts.add_argument(
        "--method",
        choices=["baseline", "ml"],
        default="baseline",
        help="baseline: rule-based; ml: use GBM model (default baseline)",
    )
    p_xpts.add_argument(
        "--model-path",
        default=None,
        help="Path to saved model when --method ml (default: data/models/xpts_gbm)",
    )

    p_transfers = sub.add_parser("suggest_transfers", help="Suggest single transfers by expected points gain")
    p_transfers.add_argument(
        "--squad",
        type=str,
        required=True,
        help="Comma-separated 15 player element IDs (e.g. from pull_team)",
    )
    p_transfers.add_argument(
        "--bank",
        type=float,
        default=0.0,
        help="Bank in FPL tenths (e.g. 5 = £0.5m); if API returns millions, multiply by 10 (default 0)",
    )
    p_transfers.add_argument("--top-n", type=int, default=10, help="Number of suggestions to return (default 10)")

    args = parser.parse_args()
    engine = get_engine(args.db_path)
    client = FPLClient(sleep_after_request=FIXED_SLEEP)

    if args.command == "update_core":
        cmd_update_core(engine, args.bronze_dir, client)
    elif args.command == "update_element_summaries":
        cmd_update_element_summaries(
            engine,
            args.bronze_dir,
            client,
            args.mode,
            args.n,
            getattr(args, "since_hours", None),
            force=getattr(args, "force", False),
            max_age_hours=getattr(args, "max_age_hours", None),
        )
    elif args.command == "pull_team":
        cmd_pull_team(engine, args.bronze_dir, client, args.team_id, args.gw)
    elif args.command == "update_entry_history":
        cmd_update_entry_history(engine, args.bronze_dir, client, args.team_id)
    elif args.command == "validate":
        return cmd_validate(engine, args.level)
    elif args.command == "train_xpts":
        cmd_train_xpts(
            engine,
            getattr(args, "model_path", "data/models/xpts_gbm"),
            validation_fraction=getattr(args, "validation_fraction", 0.2),
        )
        return 0
    elif args.command == "build_xpts":
        cmd_build_xpts(
            engine,
            getattr(args, "horizon", 3),
            method=getattr(args, "method", "baseline"),
            model_path=getattr(args, "model_path", None),
        )
        return 0
    elif args.command == "suggest_transfers":
        squad_ids = [int(x.strip()) for x in args.squad.split(",") if x.strip()]
        cmd_suggest_transfers(
            engine,
            squad_ids,
            bank=getattr(args, "bank", 0.0),
            top_n=getattr(args, "top_n", 10),
        )
        return 0
    else:
        parser.error("Unknown command")
    return 0


if __name__ == "__main__":
    sys.exit(main())
