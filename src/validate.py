"""Data quality checks. Fails loudly on invalid or partial data."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

MIN_PLAYERS = 600
FIXTURES_MIN = 300
FIXTURES_MAX = 400
MINUTES_MIN = 0
MINUTES_MAX = 120
TOTAL_POINTS_MIN = -5
TOTAL_POINTS_MAX = 30
WEIRD_ROWS_LIMIT = 10


@dataclass
class ValidationReport:
    """Short report: counts, null pct, weird rows, and failures."""

    counts: dict[str, int] = field(default_factory=dict)
    null_pct: dict[str, dict[str, float]] = field(default_factory=dict)
    weird_rows: list[dict[str, Any]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def fail(self, msg: str) -> None:
        self.errors.append(msg)

    def is_ok(self) -> bool:
        return len(self.errors) == 0


def run_validation(engine: Engine) -> ValidationReport:
    """Run all data quality checks. Populates report; call report.is_ok() and report.errors to fail loudly."""
    r = ValidationReport()

    with engine.connect() as conn:
        # ----- Counts by table -----
        for table in ("teams", "element_types", "events", "players", "fixtures", "player_match_history", "player_future_fixtures"):
            try:
                row = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()
                r.counts[table] = row[0] if row else 0
            except Exception as e:
                r.fail(f"count {table}: {e}")
                r.counts[table] = 0

        if r.counts.get("players", 0) <= MIN_PLAYERS:
            r.fail(f"players.count={r.counts.get('players', 0)} (expected > {MIN_PLAYERS})")
        if not (FIXTURES_MIN <= r.counts.get("fixtures", 0) <= FIXTURES_MAX):
            r.fail(
                f"fixtures.count={r.counts.get('fixtures', 0)} (expected {FIXTURES_MIN}-{FIXTURES_MAX})"
            )

        # ----- Null PKs / key columns -----
        _check_nulls(conn, r)

        # ----- player_match_history: minutes [0,120], total_points [-5,30] -----
        _check_match_history_ranges(conn, r)

        # ----- Referential integrity -----
        _check_referential_integrity(conn, r)

        # ----- Top 10 weird rows (e.g. minutes > 120) -----
        _collect_weird_rows(conn, r)

    return r


def _check_nulls(conn: Any, r: ValidationReport) -> None:
    """Percent null in key columns."""
    key_columns = [
        ("players", "id", "players.id"),
        ("players", "team_id", "players.team_id"),
        ("fixtures", "id", "fixtures.id"),
        ("fixtures", "event_id", "fixtures.event_id"),  # can be null for postponed
        ("player_match_history", "player_id", "player_match_history.player_id"),
        ("player_match_history", "fixture_id_effective", "player_match_history.fixture_id_effective"),
    ]
    for table, col, label in key_columns:
        try:
            total = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar() or 0
            if total == 0:
                continue
            nulls = conn.execute(text(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")).scalar() or 0
            pct = 100.0 * nulls / total
            if label not in r.null_pct:
                r.null_pct[label] = {}
            r.null_pct[label]["pct_null"] = round(pct, 2)
            # PKs that must not be null (except fixtures.event_id which can be null)
            if col in ("id", "player_id", "fixture_id_effective") and nulls > 0:
                r.fail(f"Unexpected nulls in {label}: {nulls} ({pct:.1f}%)")
            if col == "team_id" and pct > 0:
                r.fail(f"players.team_id has {nulls} nulls ({pct:.1f}%)")
        except Exception as e:
            r.fail(f"null check {label}: {e}")


def _check_match_history_ranges(conn: Any, r: ValidationReport) -> None:
    bad_minutes = conn.execute(
        text(
            f"SELECT COUNT(*) FROM player_match_history WHERE minutes IS NOT NULL AND (minutes < :lo OR minutes > :hi)"
        ),
        {"lo": MINUTES_MIN, "hi": MINUTES_MAX},
    ).scalar() or 0
    if bad_minutes > 0:
        r.fail(f"player_match_history: {bad_minutes} rows with minutes outside [{MINUTES_MIN},{MINUTES_MAX}]")

    bad_points = conn.execute(
        text(
            "SELECT COUNT(*) FROM player_match_history WHERE total_points IS NOT NULL AND (total_points < :lo OR total_points > :hi)"
        ),
        {"lo": TOTAL_POINTS_MIN, "hi": TOTAL_POINTS_MAX},
    ).scalar() or 0
    if bad_points > 0:
        r.fail(
            f"player_match_history: {bad_points} rows with total_points outside [{TOTAL_POINTS_MIN},{TOTAL_POINTS_MAX}]"
        )


def _check_referential_integrity(conn: Any, r: ValidationReport) -> None:
    # players.team_id must exist in teams (where not null)
    orphan_team = conn.execute(
        text(
            "SELECT COUNT(*) FROM players p LEFT JOIN teams t ON p.team_id = t.id WHERE p.team_id IS NOT NULL AND t.id IS NULL"
        )
    ).scalar() or 0
    if orphan_team > 0:
        r.fail(f"Referential integrity: {orphan_team} players with team_id not in teams")

    # fixtures.event_id must exist in events (where not null; postponed may have null event_id)
    orphan_event = conn.execute(
        text(
            "SELECT COUNT(*) FROM fixtures f LEFT JOIN events e ON f.event_id = e.id WHERE f.event_id IS NOT NULL AND e.id IS NULL"
        )
    ).scalar() or 0
    if orphan_event > 0:
        r.fail(f"Referential integrity: {orphan_event} fixtures with event_id not in events")


def _collect_weird_rows(conn: Any, r: ValidationReport) -> None:
    """Top 10 weird rows: e.g. minutes > 120, total_points outside range."""
    weird = conn.execute(
        text("""
            SELECT player_id, fixture_id_effective, minutes, total_points
            FROM player_match_history
            WHERE (minutes IS NOT NULL AND (minutes < :min_lo OR minutes > :min_hi))
               OR (total_points IS NOT NULL AND (total_points < :pts_lo OR total_points > :pts_hi))
            LIMIT 10
        """),
        {
            "min_lo": MINUTES_MIN,
            "min_hi": MINUTES_MAX,
            "pts_lo": TOTAL_POINTS_MIN,
            "pts_hi": TOTAL_POINTS_MAX,
        },
    ).fetchall()
    for row in weird:
        r.weird_rows.append({
            "player_id": row[0],
            "fixture_id_effective": row[1],
            "minutes": row[2],
            "total_points": row[3],
        })


def print_report(r: ValidationReport) -> None:
    """Log a short report: counts, null pct, top weird rows."""
    logger.info("=== Validation report ===")
    logger.info("Counts: %s", r.counts)
    if r.null_pct:
        logger.info("Null %% (key columns): %s", r.null_pct)
    if r.weird_rows:
        logger.info("Top %s weird rows (minutes/points out of range): %s", WEIRD_ROWS_LIMIT, r.weird_rows)
    if r.errors:
        for e in r.errors:
            logger.error("Validation error: %s", e)
